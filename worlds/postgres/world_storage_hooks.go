package postgresworld

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
	"github.com/fxamacker/cbor/v2"
	"github.com/jackc/pgx/v5"
)

// errClaimRetry restarts a forced claim whose token owner changed between
// choosing the run lock order and appending.
var errClaimRetry = errors.New("workflow/postgres: hook token owner changed concurrently")

const claimAttempts = 20

type tokenHolder struct {
	hookID worlds.HookID
	runID  worlds.RunID
}

func (w *World) HooksGet(ctx context.Context, input worlds.HooksGetInput) (worlds.Hook, error) {
	hook, err := scanHook(w.pool.QueryRow(ctx, w.q(`SELECT `+hookColumns(input.ResolveData)+` FROM workflow.workflow_hooks h`+hookJoin(input.ResolveData)+` WHERE h.hook_id = $1 AND `+hookAvailable("$2")), input.HookID, now()))
	if errors.Is(err, pgx.ErrNoRows) {
		return worlds.Hook{}, &worlds.HookNotFoundError{Token: string(input.HookID)}
	}
	return hook, err
}

// hookAvailable is the condition on hooks h under which a hook is readable
// at the time the argument at names, as world-postgres reads it: while its
// token retention lasts or its run is unfinished.
func hookAvailable(at string) string {
	return "(h.token_retention_until > " + at + " OR EXISTS (SELECT 1 FROM workflow.workflow_runs ar WHERE ar.id = h.run_id AND ar.status IN ('pending', 'running')))"
}

func (w *World) HooksGetByToken(ctx context.Context, input worlds.HooksGetByTokenInput) (worlds.Hook, error) {
	// An expired retention counts as released without writing. The row itself
	// is removed by the next claim of the token.
	hook, err := scanHook(w.pool.QueryRow(ctx, w.q(`SELECT `+hookColumns(input.ResolveData)+` FROM workflow.workflow_hooks h
		JOIN workflow.workflow_runs r ON r.id = h.run_id`+hookJoin(input.ResolveData)+` WHERE h.token = $1 AND h.claims_token AND (r.status IN ('pending', 'running') OR h.token_retention_until > $2)`), input.Token, now()))
	if errors.Is(err, pgx.ErrNoRows) {
		return worlds.Hook{}, &worlds.HookNotFoundError{Token: input.Token}
	}
	return hook, err
}

func (w *World) HooksList(ctx context.Context, input worlds.HooksListInput) (worlds.Page[worlds.Hook], error) {
	// The list's defaults are world-postgres's.
	input.Pagination = input.Pagination.WithDefaults(worlds.SortOrderAscending, 100)
	var q query
	q.where(hookAvailable(q.arg(now())))
	if input.RunID != "" {
		q.where("h.run_id = " + q.arg(input.RunID))
	}
	descending := input.Pagination.SortOrder == worlds.SortOrderDescending
	if err := q.keyset(input.Pagination.Cursor, "h.created_at", "h.hook_id", descending); err != nil {
		return worlds.Page[worlds.Hook]{}, err
	}
	rows, err := w.pool.Query(ctx, w.q(`SELECT `+hookColumns(input.ResolveData)+` FROM workflow.workflow_hooks h`+hookJoin(input.ResolveData)+q.clause()+order("h.created_at", "h.hook_id", descending)+q.limit(input.Pagination.Limit)), q.args...)
	if err != nil {
		return worlds.Page[worlds.Hook]{}, err
	}
	hooks, err := collect(rows, scanHook)
	if err != nil {
		return worlds.Page[worlds.Hook]{}, err
	}
	return keysetPage(hooks, input.Pagination.Limit, func(hook worlds.Hook) (time.Time, string) { return hook.CreatedAt, string(hook.HookID) }), nil
}

func (w *World) createHook(ctx context.Context, input worlds.EventsCreateInput) (worlds.EventResult, error) {
	// Lock the claimant run (and, for a forced claim, the current owner's run,
	// in run ID order to avoid deadlocks), then the token row, and append
	// hook_disposed, naming the claimant in ForceClaimedBy, to a live owner
	// before the claimant's hook_created, all in one transaction.
	if err := ctx.Err(); err != nil {
		return worlds.EventResult{}, err
	}
	if input.Event.EventType != worlds.EventTypeHookCreated {
		return worlds.EventResult{}, worlds.ErrEventInvalid
	}
	var created worlds.DataHookCreated
	if err := json.Unmarshal(input.Event.Data, &created); err != nil || created.Token == "" || input.Event.CorrelationID == string(worlds.StartHookID(input.RunID)) {
		// The start hook's ID is the start hook's alone.
		return worlds.EventResult{}, worlds.ErrEventInvalid
	}
	if created.TokenRetentionUntil != nil && created.TokenRetentionUntil.After(now().Add(w.hookRetentionLimit)) {
		return worlds.EventResult{}, fmt.Errorf("%w: hook retention cannot exceed %s in the Postgres World", worlds.ErrEventInvalid, w.hookRetentionLimit)
	}
	for attempt := 1; ; attempt++ {
		result, err := w.claimToken(ctx, input, created.Token)
		if !errors.Is(err, errClaimRetry) || attempt == claimAttempts {
			return result, err
		}
	}
}

func (w *World) claimToken(ctx context.Context, input worlds.EventsCreateInput, token string) (worlds.EventResult, error) {
	original := input.Event
	var result worlds.EventResult
	err := w.inTx(ctx, func(tx pgx.Tx) error {
		input.Event = original
		var created worlds.DataHookCreated
		_ = json.Unmarshal(original.Data, &created)
		result = worlds.EventResult{}
		var expectedOwner worlds.RunID
		if created.Force {
			err := tx.QueryRow(ctx, w.q(`SELECT run_id FROM workflow.workflow_hooks WHERE token = $1 AND claims_token`), token).Scan(&expectedOwner)
			if err != nil && !errors.Is(err, pgx.ErrNoRows) {
				return err
			}
		}
		ids := []worlds.RunID{input.RunID}
		if expectedOwner != "" && expectedOwner != input.RunID {
			ids = append(ids, expectedOwner)
			slices.Sort(ids)
		}
		runs := map[worlds.RunID]*appender{}
		for _, id := range ids {
			run, err := w.lockRun(ctx, tx, id)
			if errors.Is(err, worlds.ErrRunNotFound) && id != input.RunID {
				continue
			}
			if err != nil {
				return err
			}
			runs[id] = run
		}
		claimant := runs[input.RunID]
		before := claimant.count
		if claimant.run.Status.Terminal() {
			return worlds.FinishedRunRefusal(claimant.run.Status, input.Event)
		}
		if err := claimant.fence(input.Options.InBandCount); err != nil {
			return err
		}
		// A hook is created once, as world-postgres's unique index on a
		// run's creations keeps it.
		if earlier, err := claimant.replay(ctx, worlds.EventTypeHookCreated, input.Event.CorrelationID, false); err != nil {
			return err
		} else if earlier.EventID != "" {
			return fmt.Errorf("%w: hook %s already created", worlds.ErrEntityConflict, input.Event.CorrelationID)
		}
		holder, err := w.lockToken(ctx, tx, token, now())
		if err != nil {
			return err
		}
		delta, err := claimant.delta(ctx, input.Options)
		if err != nil {
			return err
		}

		if holder.hookID != "" {
			if !created.Force {
				created.ConflictingRunID = holder.runID
				input.Event.EventType = worlds.EventTypeHookConflict
			} else {
				owner := runs[holder.runID]
				if owner == nil {
					return errClaimRetry
				}
				// The claim names a live owner's workflow, which the runtime
				// wakes to read its disposal, as world-postgres names it. A
				// finished owner has nothing to read or wake.
				created.ForceClaimedFrom = &worlds.HookForceClaim{RunID: holder.runID, HookID: holder.hookID}
				if owner.run.Status.Terminal() {
					// A finished owner's hook goes with its token.
					if _, err := tx.Exec(ctx, w.q(`DELETE FROM workflow.workflow_hooks WHERE hook_id = $1`), holder.hookID); err != nil {
						return err
					}
				} else {
					claim := created.ForceClaimedFrom
					claim.WorkflowName, claim.DeploymentID, claim.RunSpecVersion = owner.run.WorkflowName, owner.run.DeploymentID, owner.run.SpecVersion
					data, _ := json.Marshal(worlds.HookDisposedEventData{Token: token, ForceClaimedBy: &worlds.HookForceClaim{RunID: input.RunID, HookID: worlds.HookID(input.Event.CorrelationID)}})
					forced, err := owner.append(ctx, worlds.EventsCreateEvent{EventType: worlds.EventTypeHookDisposed, CorrelationID: string(holder.hookID), Data: data})
					if err != nil {
						return err
					}
					if owner == claimant {
						delta = append(delta, forced)
					}
				}
			}
		}
		input.Event.Data, _ = json.Marshal(created)

		event, err := claimant.append(ctx, input.Event)
		if _, ok := errors.AsType[*worlds.HookConflictError](err); ok {
			// A claimant inserted the token after lockToken saw none.
			return errClaimRetry
		}
		if err != nil {
			return err
		}
		allocated := int(claimant.count - before)
		claimant.advance(input.Options.InBandCount, allocated)
		if result, err = claimant.result(ctx, event, true); err != nil {
			return err
		}
		result.Events = delta
		result.AllocatedEventCount = allocated
		for _, run := range runs {
			if err := run.flush(ctx); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return worlds.EventResult{}, err
	}
	return result, nil
}

// hookColumns are the columns scanHook reads from hooks as h, with the
// metadata from the event hookJoin joins, unless resolve is ResolveDataNone.
func hookColumns(resolve worlds.ResolveData) string {
	metadata := "me.type, me.payload_cbor"
	if resolve == worlds.ResolveDataNone {
		metadata = "NULL::varchar, NULL::bytea"
	}
	return "h.hook_id, h.run_id, h.resume_context, h.token, " + metadata + ", coalesce(h.is_webhook, false), h.created_at, h.token_retention_until, h.claimed_from, coalesce(h.spec_version, 0)"
}

// hookJoin joins the event holding the hook's metadata, its hook_created or
// its run's run_created for a start hook, unless resolve is
// ResolveDataNone.
func hookJoin(resolve worlds.ResolveData) string {
	if resolve == worlds.ResolveDataNone {
		return ""
	}
	return " LEFT JOIN workflow.workflow_events me ON me.run_id = h.run_id AND me.id = 'evnt_' || lpad(h.metadata_slot::text, 26, '0')" + runLive("h.run_id")
}

func scanHook(row pgx.Row) (worlds.Hook, error) {
	var hook worlds.Hook
	var metadataType *worlds.EventType
	var resume, metadata, claimed []byte
	if err := row.Scan(&hook.HookID, &hook.RunID, &resume, &hook.Token, &metadataType, &metadata, &hook.IsWebhook, &hook.CreatedAt, &hook.TokenRetentionUntil, &claimed, &hook.SpecVersion); err != nil {
		return worlds.Hook{}, err
	}
	var decoded resumeContext
	if err := cbor.Unmarshal(resume, &decoded); err != nil {
		return worlds.Hook{}, fmt.Errorf("workflow/postgres: hook %s resume_context: %w", hook.HookID, err)
	}
	hook.ResumeContext = worlds.HookResumeContext(decoded)
	if metadataType != nil && metadata != nil {
		data, err := eventData(*metadataType, metadata)
		if err != nil {
			return worlds.Hook{}, err
		}
		if *metadataType == worlds.EventTypeRunCreated {
			// A start hook's metadata is in its run's run_created.
			if start := worlds.RunStartHook(data); start != nil {
				hook.Metadata = start.Metadata
			}
		} else {
			hook.Metadata = worlds.EventPayload(worlds.Event{EventType: *metadataType, Data: data})
		}
	}
	if len(claimed) > 0 {
		if err := json.Unmarshal(claimed, &hook.ClaimedFrom); err != nil {
			return worlds.Hook{}, err
		}
	}
	hook.CreatedAt, hook.TokenRetentionUntil = hook.CreatedAt.UTC(), utc(hook.TokenRetentionUntil)
	return hook, nil
}

func (w *World) loadHook(ctx context.Context, tx pgx.Tx, hookID worlds.HookID, resolve worlds.ResolveData) (worlds.Hook, bool, error) {
	hook, err := scanHook(tx.QueryRow(ctx, w.q(`SELECT `+hookColumns(resolve)+` FROM workflow.workflow_hooks h`+hookJoin(resolve)+` WHERE h.hook_id = $1 FOR UPDATE OF h`), hookID))
	if errors.Is(err, pgx.ErrNoRows) {
		return worlds.Hook{}, false, nil
	}
	return hook, err == nil, err
}

// tokenOwner returns the hook that claims token, or "".
func (w *World) tokenOwner(ctx context.Context, tx pgx.Tx, token string) (worlds.HookID, error) {
	var owner worlds.HookID
	err := tx.QueryRow(ctx, w.q(`SELECT hook_id FROM workflow.workflow_hooks WHERE token = $1 AND claims_token`), token).Scan(&owner)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	return owner, err
}

// lockToken locks the hook claiming token, deletes it when its run finished
// and its retention expired at at, and returns the remaining
// holder. A token nobody claims is not locked: the claimant's insert detects
// a concurrent claim instead. A token is held while its run is live or its
// retention lasts (world-postgres storage.js).
func (w *World) lockToken(ctx context.Context, tx pgx.Tx, token string, at time.Time) (tokenHolder, error) {
	var holder tokenHolder
	var retainUntil *time.Time
	var status worlds.Status
	err := tx.QueryRow(ctx, w.q(`SELECT h.hook_id, h.run_id, h.token_retention_until, r.status::text FROM workflow.workflow_hooks h
		JOIN workflow.workflow_runs r ON r.id = h.run_id WHERE h.token = $1 AND h.claims_token FOR UPDATE OF h`), token).
		Scan(&holder.hookID, &holder.runID, &retainUntil, &status)
	if errors.Is(err, pgx.ErrNoRows) {
		return tokenHolder{}, nil
	}
	if err != nil {
		return tokenHolder{}, err
	}
	if !status.Terminal() || retainUntil != nil && retainUntil.After(at) {
		return holder, nil
	}
	// The lapsed holder's hook goes with its token, as world-postgres
	// deletes it.
	_, err = tx.Exec(ctx, w.q(`DELETE FROM workflow.workflow_hooks WHERE hook_id = $1`), holder.hookID)
	return tokenHolder{}, err
}

// resumeContext is a hook's resume_context: the part of its run that
// addresses the deliveries its payloads queue, in the SDK's
// HookResumeContext shape, as CBOR. world-postgres declares the column but
// never writes it, so the SDK's runtime reads the run on every resume.
type resumeContext struct {
	DeploymentID        string `cbor:"deploymentId"`
	WorkflowName        string `cbor:"workflowName"`
	EncryptionPublicKey string `cbor:"encryptionPublicKey,omitempty"`
	RunSpecVersion      int    `cbor:"runSpecVersion,omitempty"`
}

// encodeResumeContext returns a hook's resume_context.
func encodeResumeContext(resume worlds.HookResumeContext) []byte {
	encoded, err := cbor.Marshal(resumeContext(resume))
	if err != nil {
		panic(err) // A struct of strings and an int always encodes.
	}
	return encoded
}
