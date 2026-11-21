package postgresworld

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
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
	hook, err := scanHook(w.pool.QueryRow(ctx, w.q(`SELECT `+hookColumns(input.ResolveData)+` FROM workflow.workflow_hooks h`+hookJoin(input.ResolveData)+` WHERE h.hook_id = $1`), input.HookID))
	if errors.Is(err, pgx.ErrNoRows) {
		return worlds.Hook{}, worlds.ErrHookNotFound
	}
	return hook, err
}

func (w *World) HooksGetByToken(ctx context.Context, input worlds.HooksGetByTokenInput) (worlds.Hook, error) {
	// An expired retention counts as released without writing. The row itself
	// is removed by the next claim of the token.
	hook, err := scanHook(w.pool.QueryRow(ctx, w.q(`SELECT `+hookColumns(input.ResolveData)+` FROM workflow.workflow_hooks h
		JOIN workflow.workflow_runs r ON r.id = h.run_id`+hookJoin(input.ResolveData)+` WHERE h.token = $1 AND h.claims_token AND (r.status IN ('pending', 'running') OR h.token_retention_until > $2)`), input.Token, now()))
	if errors.Is(err, pgx.ErrNoRows) {
		return worlds.Hook{}, worlds.ErrHookNotFound
	}
	return hook, err
}

func (w *World) HooksList(ctx context.Context, input worlds.HooksListInput) (worlds.Page[worlds.Hook], error) {
	var q query
	if input.RunID != "" {
		q.where("h.run_id = " + q.arg(input.RunID))
	}
	if err := q.keyset(input.Pagination.Cursor, "h.created_at", "h.hook_id", false); err != nil {
		return worlds.Page[worlds.Hook]{}, err
	}
	rows, err := w.pool.Query(ctx, w.q(`SELECT `+hookColumns(input.ResolveData)+` FROM workflow.workflow_hooks h`+hookJoin(input.ResolveData)+q.clause()+order("h.created_at", "h.hook_id", false)+q.limit(input.Pagination.Limit)), q.args...)
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
			return worlds.ErrRunTerminal
		}
		if err := claimant.fence(input.Options.InBandCount); err != nil {
			return err
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
				created.ForceClaimedFrom = &worlds.HookForceClaim{RunID: holder.runID, HookID: holder.hookID, WorkflowName: owner.run.WorkflowName, DeploymentID: owner.run.DeploymentID, RunSpecVersion: owner.run.SpecVersion}
				if owner.run.Status.Terminal() {
					// A finished owner's hook goes with its token.
					if _, err := tx.Exec(ctx, w.q(`DELETE FROM workflow.workflow_hooks WHERE hook_id = $1`), holder.hookID); err != nil {
						return err
					}
				} else {
					data, _ := json.Marshal(worlds.HookDisposedEventData{ForceClaimedBy: &worlds.HookForceClaim{RunID: input.RunID, HookID: worlds.HookID(input.Event.CorrelationID)}})
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
		if errors.Is(err, worlds.ErrHookConflict) {
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
	return "h.hook_id, h.run_id, h.workflow_name, h.deployment_id, h.encryption_public_key, h.token, " + metadata + ", coalesce(h.is_webhook, false), h.created_at, h.updated_at, h.received_at, h.token_retention_until, h.claimed_from, coalesce(h.spec_version, 0), h.run_spec_version"
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
	var metadata, claimed []byte
	if err := row.Scan(&hook.HookID, &hook.RunID, &hook.ResumeContext.WorkflowName, &hook.ResumeContext.DeploymentID, &hook.ResumeContext.EncryptionPublicKey, &hook.Token, &metadataType, &metadata, &hook.IsWebhook, &hook.CreatedAt, &hook.UpdatedAt, &hook.ReceivedAt, &hook.TokenRetentionUntil, &claimed, &hook.SpecVersion, &hook.ResumeContext.RunSpecVersion); err != nil {
		return worlds.Hook{}, err
	}
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
	hook.CreatedAt, hook.UpdatedAt = hook.CreatedAt.UTC(), hook.UpdatedAt.UTC()
	hook.ReceivedAt, hook.TokenRetentionUntil = utc(hook.ReceivedAt), utc(hook.TokenRetentionUntil)
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
