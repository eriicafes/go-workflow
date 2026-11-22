package localworld

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

func (w *World) HooksGet(ctx context.Context, input worlds.HooksGetInput) (worlds.Hook, error) {
	var hook worlds.Hook
	err := w.read(ctx, func(s *store) error {
		record, err := s.getHook(input.HookID)
		if err != nil {
			return err
		}
		if record == nil {
			return &worlds.HookNotFoundError{Token: string(input.HookID)}
		}
		if available, err := s.hookAvailable(*record, time.Now()); err != nil || !available {
			if err == nil {
				err = &worlds.HookNotFoundError{Token: string(input.HookID)}
			}
			return err
		}
		hook, err = hookView(*record, s.committed(record.RunID), input.ResolveData)
		return err
	})
	return hook, err
}

// hookAvailable reports whether a hook's row is readable at now, as
// world-local's isAvailable decides: one that is not disposed, while its
// run is unfinished or its token retention lasts.
func (s *store) hookAvailable(record hookRecord, now time.Time) (bool, error) {
	if disposal, err := s.getDisposal(record.HookID); err != nil || disposal != nil {
		return false, err
	}
	if record.TokenRetentionUntil != nil && record.TokenRetentionUntil.After(now) {
		return true, nil
	}
	run, err := s.getRunAnyTag(record.RunID)
	if errors.Is(err, worlds.ErrRunNotFound) {
		return false, nil
	}
	return err == nil && !run.Status.Terminal(), err
}

func (w *World) HooksGetByToken(ctx context.Context, input worlds.HooksGetByTokenInput) (worlds.Hook, error) {
	// A token whose retention has passed is unclaimed. Its expired row is
	// replaced by the next claim.
	var hook worlds.Hook
	err := w.read(ctx, func(s *store) error {
		owner, err := s.getToken(input.Token)
		if err != nil {
			return err
		}
		if !owner.held(time.Now()) {
			return &worlds.HookNotFoundError{Token: input.Token}
		}
		record, err := s.getHook(owner.HookID)
		if err != nil {
			return err
		}
		if record == nil {
			return &worlds.HookNotFoundError{Token: input.Token}
		}
		hook, err = hookView(*record, s.committed(record.RunID), input.ResolveData)
		return err
	})
	return hook, err
}

func (w *World) HooksList(ctx context.Context, input worlds.HooksListInput) (worlds.Page[worlds.Hook], error) {
	// The list's defaults are world-local's.
	input.Pagination = input.Pagination.WithDefaults(worlds.SortOrderAscending, 20)
	var page worlds.Page[worlds.Hook]
	err := w.read(ctx, func(s *store) error {
		records, err := s.listHooks(input.RunID)
		if err != nil {
			return err
		}
		hooks := make([]worlds.Hook, 0, len(records))
		now := time.Now()
		for _, record := range records {
			if available, err := s.hookAvailable(record, now); err != nil {
				return err
			} else if !available {
				continue
			}
			hook, err := hookView(record, s.committed(record.RunID), input.ResolveData)
			if err != nil {
				return err
			}
			hooks = append(hooks, hook)
		}
		page, err = keyPage(hooks, input.Pagination, func(hook worlds.Hook) string { return sortKey(hook.CreatedAt, string(hook.HookID)) })
		return err
	})
	return page, err
}

// createHook appends a hook_created when its token is free. A forced claim
// over a live owner first appends hook_disposed, naming the claimant in
// ForceClaimedBy, to the owner's run, which releases the token, in the same
// transaction.
func (w *World) createHook(ctx context.Context, input worlds.EventsCreateInput) (worlds.EventResult, error) {
	var created worlds.DataHookCreated
	if err := json.Unmarshal(input.Event.Data, &created); err != nil || created.Token == "" || input.Event.CorrelationID == string(worlds.StartHookID(input.RunID)) {
		// The start hook's ID is the start hook's alone.
		return worlds.EventResult{}, worlds.ErrEventInvalid
	}
	now := time.Now().UTC()
	if created.TokenRetentionUntil != nil && created.TokenRetentionUntil.After(now.Add(w.hookRetentionLimit)) {
		return worlds.EventResult{}, fmt.Errorf("%w: hook retention cannot exceed %s in the local World", worlds.ErrEventInvalid, w.hookRetentionLimit)
	}
	var result worlds.EventResult
	err := w.write(ctx, func(s *store) error {
		var err error
		result, err = claimHook(s, input, created, now)
		return err
	})
	return result, err
}

func claimHook(s *store, input worlds.EventsCreateInput, created worlds.DataHookCreated, now time.Time) (worlds.EventResult, error) {
	claimant, err := begin(s, input.RunID)
	if err != nil {
		return worlds.EventResult{}, err
	}
	if claimant.run.Status.Terminal() {
		return worlds.EventResult{}, worlds.FinishedRunRefusal(claimant.run.Status, input.Event)
	}
	if err := claimant.fence(input.Options.InBandCount); err != nil {
		return worlds.EventResult{}, err
	}
	// A hook is created once, as in world-local.
	id := worlds.HookID(input.Event.CorrelationID)
	if disposal, err := claimant.disposal(id); err != nil {
		return worlds.EventResult{}, err
	} else if disposal != nil {
		return worlds.EventResult{}, fmt.Errorf("%w: hook %s already created", worlds.ErrEntityConflict, id)
	}
	if earlier, err := claimant.findEvent(worlds.EventTypeHookCreated, string(id)); err != nil {
		return worlds.EventResult{}, err
	} else if earlier != "" {
		return worlds.EventResult{}, fmt.Errorf("%w: hook %s already created", worlds.ErrEntityConflict, id)
	}
	delta, err := s.delta(claimant.run, input.Options)
	if err != nil {
		return worlds.EventResult{}, err
	}
	owner, err := s.getToken(created.Token)
	if err != nil {
		return worlds.EventResult{}, err
	}
	if owner.held(now) {
		previous, err := s.getHook(owner.HookID)
		if err != nil {
			return worlds.EventResult{}, err
		}
		if previous == nil {
			previous = &hookRecord{Hook: worlds.Hook{RunID: owner.RunID, HookID: owner.HookID}}
		}
		if !created.Force {
			created.ConflictingRunID = previous.RunID
			input.Event.EventType = worlds.EventTypeHookConflict
		} else {
			if created.ForceClaimedFrom, err = forceRelease(claimant, previous, created.Token, worlds.HookID(input.Event.CorrelationID), now); err != nil {
				return worlds.EventResult{}, err
			}
		}
	}
	if input.Event.Data, err = json.Marshal(created); err != nil {
		return worlds.EventResult{}, err
	}
	event, err := claimant.append(input.Event, eventTime(input.Event, input.Options.InBandCount, now))
	if err != nil {
		return worlds.EventResult{}, err
	}
	delta = append(delta, claimant.appended[:len(claimant.appended)-1]...)
	claimant.advance(input.Options.InBandCount, len(claimant.appended))
	if err := claimant.commit(); err != nil {
		return worlds.EventResult{}, err
	}
	result, err := claimant.result(event, delta)
	result.AllocatedEventCount = len(claimant.appended)
	return result, err
}

// forceRelease releases hook, whose token the claimant's claimantHook takes
// by force, as world-local releases it, and returns the claim's
// ForceClaimedFrom. A live owner's run records hook_disposed naming
// claimantHook in ForceClaimedBy, and ForceClaimedFrom names its workflow so
// the runtime wakes it. A finished or missing owner's hook is disposed of
// without an event or a wake. An owner in the claimant's own run is recorded
// in the claimant's txn so both events commit together.
func forceRelease(claimant *txn, hook *hookRecord, token string, claimantHook worlds.HookID, now time.Time) (*worlds.HookForceClaim, error) {
	by := &worlds.HookForceClaim{RunID: claimant.run.RunID, HookID: claimantHook}
	from := &worlds.HookForceClaim{RunID: hook.RunID, HookID: hook.HookID}
	data, err := json.Marshal(worlds.HookDisposedEventData{Token: token, ForceClaimedBy: by})
	if err != nil {
		return nil, err
	}
	event := worlds.EventsCreateEvent{EventType: worlds.EventTypeHookDisposed, CorrelationID: string(hook.HookID), Data: data}
	if hook.RunID == claimant.run.RunID {
		from.WorkflowName, from.DeploymentID, from.RunSpecVersion = claimant.run.WorkflowName, claimant.run.DeploymentID, claimant.run.SpecVersion
		_, err := claimant.append(event, now)
		return from, err
	}
	run, err := claimant.s.getRunAnyTag(hook.RunID)
	if err != nil && !errors.Is(err, worlds.ErrRunNotFound) {
		return nil, err
	}
	if err != nil || run.Status.Terminal() {
		if err := claimant.s.putDisposal(hook.HookID, &disposalRecord{RunID: hook.RunID, ForceClaimedBy: by}); err != nil {
			return nil, err
		}
		return from, claimant.s.deleteHook(hook.HookID)
	}
	from.WorkflowName, from.DeploymentID, from.RunSpecVersion = run.WorkflowName, run.DeploymentID, run.SpecVersion
	owner := newTxn(claimant.s, run)
	if _, err := owner.append(event, now); err != nil {
		return nil, err
	}
	if err := owner.commit(); err != nil {
		return nil, err
	}
	// The claimant's staged view of the token predates the owner's release.
	delete(claimant.tokens, hook.Token)
	return from, nil
}
