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
	if err := checkID("hook ID", string(input.HookID)); err != nil {
		return worlds.Hook{}, err
	}
	var hook worlds.Hook
	err := w.read(ctx, func(s *store) error {
		record, err := s.getHook(input.HookID)
		if err != nil {
			return err
		}
		if record == nil {
			return &worlds.HookNotFoundError{Token: string(input.HookID)}
		}
		if available, err := s.hookAvailable(*record, clock()); err != nil || !available {
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

// hookAvailable reports whether a hook's row is readable at now: one that
// is not disposed, while its run is unfinished or its token retention
// lasts.
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

// tokenHeld reports whether a token's owner still holds it at now. An owner
// has let go once its hook is disposed, its run is gone, or its run has
// finished and the token's retention is over. Its row stays until the next
// claim replaces it.
func (s *store) tokenHeld(owner *tokenRecord, now time.Time) (bool, error) {
	if owner == nil {
		return false, nil
	}
	if disposal, err := s.getDisposal(owner.HookID); err != nil || disposal != nil {
		return false, err
	}
	run, err := s.getRunAnyTag(owner.RunID)
	if errors.Is(err, worlds.ErrRunNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !run.Status.Terminal() {
		return true, nil
	}
	return owner.TokenRetentionUntil != nil && owner.TokenRetentionUntil.After(now), nil
}

func (w *World) HooksGetByToken(ctx context.Context, input worlds.HooksGetByTokenInput) (worlds.Hook, error) {
	// The token's owner answers while its hook is readable. One snapshot
	// reads both: a claim moves the token and deletes the previous owner's
	// hook in one commit.
	var hook worlds.Hook
	err := w.read(ctx, func(s *store) error {
		owner, err := s.getToken(input.Token)
		if err != nil {
			return err
		}
		if owner == nil {
			return &worlds.HookNotFoundError{Token: input.Token}
		}
		record, err := s.getHook(owner.HookID)
		if err != nil {
			return err
		}
		if record == nil || record.Token != input.Token {
			return &worlds.HookNotFoundError{Token: input.Token}
		}
		if available, err := s.hookAvailable(*record, clock()); err != nil {
			return err
		} else if !available {
			return &worlds.HookNotFoundError{Token: input.Token}
		}
		hook, err = hookView(*record, s.committed(record.RunID), input.ResolveData)
		return err
	})
	return hook, err
}

func (w *World) HooksList(ctx context.Context, input worlds.HooksListInput) (worlds.Page[worlds.Hook], error) {
	input.Pagination = input.Pagination.WithDefaults(worlds.SortOrderAscending, 20)
	var page worlds.Page[worlds.Hook]
	err := w.read(ctx, func(s *store) error {
		records, err := s.listHooks(input.RunID)
		if err != nil {
			return err
		}
		hooks := make([]worlds.Hook, 0, len(records))
		now := clock()
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
		page = entityPage(hooks, input.Pagination, func(hook worlds.Hook) (time.Time, string) { return hook.CreatedAt, string(hook.HookID) })
		return nil
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
	now := clock()
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
	// A superseded writer is refused before anything else is checked.
	if err := claimant.fence(input.Options.InBandCount); err != nil {
		return worlds.EventResult{}, err
	}
	if err := worlds.CheckRunSpecVersion(claimant.run.RunID, claimant.run.SpecVersion); err != nil {
		return worlds.EventResult{}, err
	}
	if claimant.run.Status.Terminal() {
		return worlds.EventResult{}, worlds.FinishedRunRefusal(claimant.run.Status, input.Event)
	}
	// A hook is created once.
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
	if held, err := s.tokenHeld(owner, now); err != nil {
		return worlds.EventResult{}, err
	} else if held {
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
// by force, and returns the claim's ForceClaimedFrom. A live owner's run
// records hook_disposed naming claimantHook in ForceClaimedBy, and
// ForceClaimedFrom names its workflow so the runtime wakes it. A finished or missing owner's hook is disposed of
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
