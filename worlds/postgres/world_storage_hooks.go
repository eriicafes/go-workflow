package postgresworld

import (
	"context"
	"encoding/json"
	"errors"
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
	hook, err := scanHook(w.pool.QueryRow(ctx, `SELECT `+hookColumns("", input.ResolveData)+` FROM go_workflow_hooks WHERE hook_id = $1`, input.HookID))
	if errors.Is(err, pgx.ErrNoRows) {
		return worlds.Hook{}, worlds.ErrHookNotFound
	}
	return hook, err
}

func (w *World) HooksGetByToken(ctx context.Context, input worlds.HooksGetByTokenInput) (worlds.Hook, error) {
	// An expired retention counts as released without writing. The row itself
	// is removed by the next claim of the token.
	hook, err := scanHook(w.pool.QueryRow(ctx, `SELECT `+hookColumns("h.", input.ResolveData)+` FROM go_workflow_hook_tokens t
		JOIN go_workflow_hooks h ON h.hook_id = t.hook_id JOIN go_workflow_runs r ON r.run_id = h.run_id WHERE t.token = $1 AND h.disposed_at IS NULL AND (r.status IN ('pending', 'running') OR h.token_retention_until > $2)`, input.Token, now()))
	if errors.Is(err, pgx.ErrNoRows) {
		return worlds.Hook{}, worlds.ErrHookNotFound
	}
	return hook, err
}

func (w *World) HooksList(ctx context.Context, input worlds.HooksListInput) (worlds.Page[worlds.Hook], error) {
	var q query
	if input.RunID != "" {
		q.where("run_id = " + q.arg(input.RunID))
	}
	if err := q.keyset(input.Pagination.Cursor, "created_at", "hook_id", false); err != nil {
		return worlds.Page[worlds.Hook]{}, err
	}
	rows, err := w.pool.Query(ctx, `SELECT `+hookColumns("", input.ResolveData)+` FROM go_workflow_hooks`+q.clause()+order("created_at", "hook_id", false)+q.limit(input.Pagination.Limit), q.args...)
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
	if err := json.Unmarshal(input.Event.Data, &created); err != nil || created.Token == "" {
		return worlds.EventResult{}, worlds.ErrEventInvalid
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
			err := tx.QueryRow(ctx, `SELECT h.run_id FROM go_workflow_hook_tokens t JOIN go_workflow_hooks h ON h.hook_id = t.hook_id WHERE t.token = $1`, token).Scan(&expectedOwner)
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
			run, err := lockRun(ctx, tx, id)
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
		holder, err := lockToken(ctx, tx, token, now())
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
				created.ForceClaimedFrom = &worlds.HookForceClaim{RunID: holder.runID, HookID: holder.hookID}
				owner := runs[holder.runID]
				if owner == nil {
					return errClaimRetry
				}
				if owner.run.Status.Terminal() {
					if _, err := tx.Exec(ctx, `DELETE FROM go_workflow_hook_tokens WHERE token = $1`, token); err != nil {
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

func hookColumns(prefix string, resolve worlds.ResolveData) string {
	metadata := prefix + "metadata"
	if resolve == worlds.ResolveDataNone {
		metadata = "NULL::bytea"
	}
	return prefix + "hook_id, " + prefix + "run_id, " + prefix + "token, " + metadata + ", " + prefix + "webhook, " + prefix + "webhook_event_id, " +
		prefix + "created_at, " + prefix + "updated_at, " + prefix + "received_at, " + prefix + "token_retention_until, " + prefix + "claimed_from, " + prefix + "force_claimed_by, " + prefix + "disposed_at"
}

func scanHook(row pgx.Row) (worlds.Hook, error) {
	var hook worlds.Hook
	var webhook, claimed, taken []byte
	if err := row.Scan(&hook.HookID, &hook.RunID, &hook.Token, &hook.Metadata, &webhook, &hook.WebhookEventID, &hook.CreatedAt, &hook.UpdatedAt, &hook.ReceivedAt, &hook.TokenRetentionUntil, &claimed, &taken, &hook.DisposedAt); err != nil {
		return worlds.Hook{}, err
	}
	if len(claimed) > 0 {
		if err := json.Unmarshal(claimed, &hook.ClaimedFrom); err != nil {
			return worlds.Hook{}, err
		}
	}
	if len(taken) > 0 {
		if err := json.Unmarshal(taken, &hook.ForceClaimedBy); err != nil {
			return worlds.Hook{}, err
		}
	}
	if webhook != nil {
		hook.Webhook = &worlds.Webhook{}
		if err := json.Unmarshal(webhook, hook.Webhook); err != nil {
			return worlds.Hook{}, err
		}
	}
	hook.CreatedAt, hook.UpdatedAt = hook.CreatedAt.UTC(), hook.UpdatedAt.UTC()
	hook.ReceivedAt, hook.TokenRetentionUntil = utc(hook.ReceivedAt), utc(hook.TokenRetentionUntil)
	return hook, nil
}

func loadHook(ctx context.Context, tx pgx.Tx, hookID worlds.HookID) (worlds.Hook, bool, error) {
	hook, err := scanHook(tx.QueryRow(ctx, `SELECT `+hookColumns("", worlds.ResolveDataAll)+` FROM go_workflow_hooks WHERE hook_id = $1 FOR UPDATE`, hookID))
	if errors.Is(err, pgx.ErrNoRows) {
		return worlds.Hook{}, false, nil
	}
	return hook, err == nil, err
}

func tokenOwner(ctx context.Context, tx pgx.Tx, token string) (worlds.HookID, error) {
	var owner worlds.HookID
	err := tx.QueryRow(ctx, `SELECT hook_id FROM go_workflow_hook_tokens WHERE token = $1`, token).Scan(&owner)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	return owner, err
}

// lockToken locks the token row, releases it when its retention expired at
// at, and returns the remaining holder. A missing row is not locked, because
// the later insert in createHook detects a concurrent claimant instead.
func lockToken(ctx context.Context, tx pgx.Tx, token string, at time.Time) (tokenHolder, error) {
	var holder tokenHolder
	var retainUntil *time.Time
	var status worlds.Status
	err := tx.QueryRow(ctx, `SELECT t.hook_id, h.run_id, h.token_retention_until, r.status FROM go_workflow_hook_tokens t
		JOIN go_workflow_hooks h ON h.hook_id = t.hook_id JOIN go_workflow_runs r ON r.run_id = h.run_id WHERE t.token = $1 FOR UPDATE OF t`, token).
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
	if _, err := tx.Exec(ctx, `DELETE FROM go_workflow_hook_tokens WHERE token = $1`, token); err != nil {
		return tokenHolder{}, err
	}
	_, err = tx.Exec(ctx, `UPDATE go_workflow_hooks SET token_retention_until = NULL WHERE hook_id = $1`, holder.hookID)
	return tokenHolder{}, err
}

func releaseToken(ctx context.Context, tx pgx.Tx, hook worlds.Hook) error {
	_, err := tx.Exec(ctx, `DELETE FROM go_workflow_hook_tokens WHERE token = $1 AND hook_id = $2`, hook.Token, hook.HookID)
	return err
}
