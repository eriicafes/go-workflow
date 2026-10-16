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

func (w *World) HooksClaim(ctx context.Context, input worlds.HooksClaimInput) (worlds.HooksClaimResult, error) {
	// The claimant's hook_created is appended only when the token is free. A
	// forced claim over a live owner first commits hook_disposed, naming the
	// claimant in ForceClaimedBy, to the owner's run, which releases the token.
	if err := ctx.Err(); err != nil {
		return worlds.HooksClaimResult{}, err
	}
	if input.Event.EventType != worlds.EventTypeHookCreated {
		return worlds.HooksClaimResult{}, worlds.ErrEventInvalid
	}
	var created hookCreatedData
	if err := json.Unmarshal(input.Event.Data, &created); err != nil || created.Token == "" {
		return worlds.HooksClaimResult{}, worlds.ErrEventInvalid
	}
	unlock, err := w.writeLock()
	if err != nil {
		return worlds.HooksClaimResult{}, err
	}
	defer unlock()
	claimant, err := w.begin(input.RunID)
	if err != nil {
		return worlds.HooksClaimResult{}, err
	}
	if claimant.run.Status.Terminal() {
		return worlds.HooksClaimResult{}, worlds.ErrRunTerminal
	}
	if err := claimant.fence(input.Options.InBandCount); err != nil {
		return worlds.HooksClaimResult{}, err
	}
	delta, err := w.delta(claimant.run, input.Options)
	if err != nil {
		return worlds.HooksClaimResult{}, err
	}
	now := time.Now().UTC()
	owner, err := w.readToken(created.Token)
	if err != nil {
		return worlds.HooksClaimResult{}, err
	}
	var result worlds.HooksClaimResult
	if owner.held(now) {
		if !input.Force {
			return worlds.HooksClaimResult{}, worlds.ErrHookConflict
		}
		forcedRunID, err := w.forceRelease(claimant, owner.HookID, worlds.HookID(input.Event.CorrelationID), now)
		if err != nil {
			return worlds.HooksClaimResult{}, err
		}
		result.ForcedRunID = forcedRunID
	}
	event, err := claimant.append(input.Event, now)
	if err != nil {
		return worlds.HooksClaimResult{}, err
	}
	claimant.advance(input.Options.InBandCount, 1)
	if err := claimant.commit(); err != nil {
		return worlds.HooksClaimResult{}, err
	}
	w.signal()
	result.EventResult = claimant.result(event, delta)
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
