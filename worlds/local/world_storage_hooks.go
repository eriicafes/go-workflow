package localworld

import (
	"context"
	"encoding/json"
	"errors"
	"path"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

func (w *World) HooksGet(ctx context.Context, input worlds.HooksGetInput) (worlds.Hook, error) {
	if err := ctx.Err(); err != nil {
		return worlds.Hook{}, err
	}
	unlock, err := w.readLock()
	if err != nil {
		return worlds.Hook{}, err
	}
	defer unlock()
	hook, err := w.readHook(input.HookID)
	if err != nil {
		return worlds.Hook{}, err
	}
	if hook == nil {
		return worlds.Hook{}, worlds.ErrHookNotFound
	}
	return cloneHook(*hook, input.ResolveData), nil
}

func (w *World) HooksGetByToken(ctx context.Context, input worlds.HooksGetByTokenInput) (worlds.Hook, error) {
	// A token whose retention has passed is unclaimed. Its expired record is
	// replaced by the next claim.
	if err := ctx.Err(); err != nil {
		return worlds.Hook{}, err
	}
	unlock, err := w.readLock()
	if err != nil {
		return worlds.Hook{}, err
	}
	defer unlock()
	owner, err := w.readToken(input.Token)
	if err != nil {
		return worlds.Hook{}, err
	}
	if !owner.held(time.Now()) {
		return worlds.Hook{}, worlds.ErrHookNotFound
	}
	hook, err := w.readHook(owner.HookID)
	if err != nil {
		return worlds.Hook{}, err
	}
	if hook == nil {
		return worlds.Hook{}, worlds.ErrHookNotFound
	}
	return cloneHook(*hook, input.ResolveData), nil
}

func (w *World) HooksList(ctx context.Context, input worlds.HooksListInput) (worlds.Page[worlds.Hook], error) {
	if err := ctx.Err(); err != nil {
		return worlds.Page[worlds.Hook]{}, err
	}
	unlock, err := w.readLock()
	if err != nil {
		return worlds.Page[worlds.Hook]{}, err
	}
	defer unlock()
	names, err := listJSON(w.root, "hooks")
	if err != nil {
		return worlds.Page[worlds.Hook]{}, err
	}
	hooks := make([]worlds.Hook, 0, len(names))
	for _, name := range names {
		var hook worlds.Hook
		if err := readJSON(w.root, path.Join("hooks", name), &hook); err != nil {
			return worlds.Page[worlds.Hook]{}, err
		}
		if input.RunID == "" || hook.RunID == input.RunID {
			hooks = append(hooks, cloneHook(hook, input.ResolveData))
		}
	}
	pagination := input.Pagination
	pagination.SortOrder = worlds.SortOrderAscending
	return keyPage(hooks, pagination, func(hook worlds.Hook) string { return sortKey(hook.CreatedAt, string(hook.HookID)) })
}

func (w *World) createHook(ctx context.Context, input worlds.EventsCreateInput) (worlds.EventResult, error) {
	// The claimant's hook_created is appended only when the token is free. A
	// forced claim over a live owner first commits hook_disposed, naming the
	// claimant in ForceClaimedBy, to the owner's run, which releases the token.
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
	unlock, err := w.writeLock()
	if err != nil {
		return worlds.EventResult{}, err
	}
	defer unlock()
	claimant, err := w.begin(input.RunID)
	if err != nil {
		return worlds.EventResult{}, err
	}
	if claimant.run.Status.Terminal() {
		return worlds.EventResult{}, worlds.ErrRunTerminal
	}
	if err := claimant.fence(input.Options.InBandCount); err != nil {
		return worlds.EventResult{}, err
	}
	delta, err := w.delta(claimant.run, input.Options)
	if err != nil {
		return worlds.EventResult{}, err
	}
	now := time.Now().UTC()
	owner, err := w.readToken(created.Token)
	if err != nil {
		return worlds.EventResult{}, err
	}

	var result worlds.EventResult
	if owner.held(now) {
		previous, err := w.readHook(owner.HookID)
		if err != nil {
			return worlds.EventResult{}, err
		}
		if !created.Force {
			created.ConflictingRunID = previous.RunID
			input.Event.EventType = worlds.EventTypeHookConflict
		} else {
			created.ClaimedFrom = &worlds.HookForceClaim{RunID: previous.RunID, HookID: previous.HookID}
			input.Event.Data, _ = json.Marshal(created)
			if previous.RunID != input.RunID {
				if err := writeJSON(w.root, "hook-handoff.json", hookHandoff{RunID: input.RunID, Event: input.Event, InBandCount: input.Options.InBandCount, Previous: *created.ClaimedFrom, At: now}); err != nil {
					return worlds.EventResult{}, err
				}
			}
			if _, err := w.forceRelease(claimant, owner.HookID, worlds.HookID(input.Event.CorrelationID), now); err != nil {
				if recoveryErr := w.recoverHandoff(); recoveryErr != nil {
					w.broken = recoveryErr
				}
				return worlds.EventResult{}, err
			}
		}
	}
	input.Event.Data, _ = json.Marshal(created)

	event, err := claimant.append(input.Event, now)
	if err != nil {
		if recoveryErr := w.recoverHandoff(); recoveryErr != nil {
			w.broken = recoveryErr
		}
		return worlds.EventResult{}, err
	}
	delta = append(delta, claimant.appended[:len(claimant.appended)-1]...)
	claimant.advance(input.Options.InBandCount, len(claimant.appended))
	if err := claimant.commit(); err != nil {
		if recoveryErr := w.recoverHandoff(); recoveryErr != nil {
			w.broken = recoveryErr
		}
		return worlds.EventResult{}, err
	}
	if err := remove(w.root, "hook-handoff.json"); err != nil {
		w.broken = err
		return worlds.EventResult{}, err
	}
	w.signal()
	result = claimant.result(event, delta)
	result.AllocatedEventCount = len(claimant.appended)
	return result, nil
}

// forceRelease records hook_disposed, naming claimantHook in ForceClaimedBy,
// in a live owner's run. A terminal or missing owner is simply replaced by the
// claim. An owner in the claimant's own run is recorded in the claimant's txn
// so both events commit together.
func (w *World) forceRelease(claimant *txn, hookID, claimantHook worlds.HookID, now time.Time) (worlds.RunID, error) {
	hook, err := w.readHook(hookID)
	if err != nil || hook == nil {
		return "", err
	}
	data, err := json.Marshal(worlds.HookDisposedEventData{ForceClaimedBy: &worlds.HookForceClaim{RunID: claimant.run.RunID, HookID: claimantHook}})
	if err != nil {
		return "", err
	}
	event := worlds.EventsCreateEvent{EventType: worlds.EventTypeHookDisposed, CorrelationID: string(hookID), Data: data}
	if hook.RunID == claimant.run.RunID {
		_, err := claimant.append(event, now)
		return hook.RunID, err
	}
	owner, err := w.begin(hook.RunID)
	if errors.Is(err, worlds.ErrRunNotFound) {
		return "", nil
	}
	if err != nil || owner.run.Status.Terminal() {
		return "", err
	}
	if _, err := owner.append(event, now); err != nil {
		return "", err
	}
	if err := owner.commit(); err != nil {
		return "", err
	}
	// The claimant's staged view of the token predates the owner's release.
	delete(claimant.tokens, hook.Token)
	return hook.RunID, nil
}
