package awsworld

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	awsv2 "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/eriicafes/go-workflow/worlds"
)

// maxAppendAttempts bounds optimistic retries of one append.
const maxAppendAttempts = 64

func (w *World) EventsCreateRun(ctx context.Context, input worlds.EventsCreateRunInput) (worlds.EventResult, error) {
	if err := ctx.Err(); err != nil {
		return worlds.EventResult{}, err
	}
	if input.RunID == "" {
		return worlds.EventResult{}, worlds.ErrEventInvalid
	}
	data, err := json.Marshal(runCreatedData{WorkflowName: input.WorkflowName, Input: input.Input, Attributes: input.Attributes, DeploymentID: input.DeploymentID})
	if err != nil {
		return worlds.EventResult{}, err
	}
	now := time.Now().UTC()
	attributes := maps.Clone(input.Attributes)
	if attributes == nil {
		attributes = map[string]string{}
	}
	view := runView{Run: worlds.Run{RunID: input.RunID, WorkflowName: input.WorkflowName, Status: worlds.StatusPending, Input: input.Input, Attributes: attributes, DeploymentID: input.DeploymentID, CreatedAt: now, UpdatedAt: now}, eventCount: 1}
	event := worlds.Event{RunID: input.RunID, EventID: worlds.EventIDForSlot(1), EventType: worlds.EventTypeRunCreated, Data: data, CreatedAt: now}
	err = w.transact(ctx, []ddb.TransactWriteItem{put(w.config.TableName, runItem(view)), put(w.config.TableName, eventItem(event, 1))})
	if conditional(err) {
		return worlds.EventResult{}, worlds.ErrRunAlreadyExists
	}
	if err != nil {
		return worlds.EventResult{}, err
	}
	return worlds.EventResult{EventID: event.EventID, EventType: event.EventType, AllocatedEventCount: 1}, nil
}

func (w *World) EventsCreate(ctx context.Context, input worlds.EventsCreateInput) (worlds.EventResult, error) {
	if err := ctx.Err(); err != nil {
		return worlds.EventResult{}, err
	}
	if input.Event.EventType == worlds.EventTypeRunCreated || !input.Event.EventType.Valid() {
		return worlds.EventResult{}, worlds.ErrEventInvalid
	}
	var result worlds.EventResult
	err := w.appendRetry(ctx, func(tx *appendTx) error {
		if err := tx.preload(ctx, input.RunID, []worlds.EventsCreateEvent{input.Event}); err != nil {
			return err
		}
		run, err := tx.run(ctx, input.RunID)
		if err != nil {
			return err
		}
		replay, err := tx.admit(ctx, run, input.Event)
		if err != nil {
			return err
		}
		if replay != nil {
			result = *replay
		} else {
			event, err := tx.append(ctx, run, input.Event)
			if err != nil {
				return err
			}
			result = tx.result(event, true)
		}
		// The delta is read before the commit, so a failed read does not
		// report a committed append as failed. Events up to run.seen are
		// already in the log and do not change.
		result.Events, err = w.delta(ctx, input.RunID, run.seen, input.Options)
		return err
	})
	if err != nil {
		return worlds.EventResult{}, err
	}
	return result, nil
}

func (w *World) EventsCreateBatch(ctx context.Context, input worlds.EventsCreateBatchInput) (worlds.EventBatchResult, error) {
	if err := ctx.Err(); err != nil {
		return worlds.EventBatchResult{}, err
	}
	if len(input.Events) == 0 {
		return worlds.EventBatchResult{}, nil
	}
	var results []worlds.EventResult
	err := w.appendRetry(ctx, func(tx *appendTx) error {
		results = make([]worlds.EventResult, 0, len(input.Events))
		if err := tx.preload(ctx, input.RunID, input.Events); err != nil {
			return err
		}
		run, err := tx.run(ctx, input.RunID)
		if err != nil {
			return err
		}
		for _, value := range input.Events {
			if value.EventType == worlds.EventTypeRunCreated || !value.EventType.Valid() {
				return worlds.ErrEventInvalid
			}
			replay, err := tx.admit(ctx, run, value)
			if err != nil {
				return err
			}
			if replay != nil {
				return fmt.Errorf("%w: %s would replay %s", worlds.ErrEventInvalid, value.EventType, replay.EventID)
			}
			event, err := tx.append(ctx, run, value)
			if err != nil {
				return err
			}
			results = append(results, tx.result(event, true))
		}
		return nil
	})
	if err != nil {
		return worlds.EventBatchResult{}, err
	}
	return worlds.EventBatchResult{Events: results}, nil
}

func (w *World) HooksClaim(ctx context.Context, input worlds.HooksClaimInput) (worlds.HooksClaimResult, error) {
	// The token moves to the claimant in one transaction. A forced claim from a
	// live owner also appends hook_disposed, naming the claimant in
	// ForceClaimedBy, to the owner's run. The transaction requires both runs'
	// event counts and the token item to be unchanged, so exactly one
	// concurrent claimant wins.
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
	var result worlds.HooksClaimResult
	err := w.appendRetry(ctx, func(tx *appendTx) error {
		result = worlds.HooksClaimResult{}
		if err := tx.preload(ctx, input.RunID, []worlds.EventsCreateEvent{input.Event}); err != nil {
			return err
		}
		claimant, err := tx.run(ctx, input.RunID)
		if err != nil {
			return err
		}
		if claimant.view.Status.Terminal() {
			return worlds.ErrRunTerminal
		}
		token, err := tx.token(ctx, created.Token)
		if err != nil {
			return err
		}
		if token.cur.expired(tx.now) {
			token.cur = nil
		}
		if token.cur != nil {
			if !input.Force {
				return worlds.ErrHookConflict
			}
			owner, err := tx.run(ctx, token.cur.runID)
			if err != nil && !errors.Is(err, worlds.ErrRunNotFound) {
				return err
			}
			if owner == nil || owner.view.Status.Terminal() {
				token.cur = nil
			} else {
				data, err := json.Marshal(worlds.HookDisposedEventData{ForceClaimedBy: &worlds.HookForceClaim{RunID: input.RunID, HookID: worlds.HookID(input.Event.CorrelationID)}})
				if err != nil {
					return err
				}
				if _, err := tx.append(ctx, owner, worlds.EventsCreateEvent{EventType: worlds.EventTypeHookDisposed, CorrelationID: string(token.cur.hookID), Data: data}); err != nil {
					return err
				}
				result.ForcedRunID = owner.view.RunID
			}
		}
		event, err := tx.append(ctx, claimant, input.Event)
		if err != nil {
			return err
		}
		result.EventResult = tx.result(event, true)
		// Read before the commit, as in EventsCreate.
		result.Events, err = w.delta(ctx, input.RunID, claimant.seen, input.Options)
		return err
	})
	if err != nil {
		return worlds.HooksClaimResult{}, err
	}
	return result, nil
}

func (w *World) EventsGet(ctx context.Context, input worlds.EventsGetInput) (worlds.Event, error) {
	if err := ctx.Err(); err != nil {
		return worlds.Event{}, err
	}
	if slot, err := worlds.EventSlot(input.EventID); err == nil {
		item, found, err := w.item(ctx, eventKey(string(input.RunID), slot))
		if err != nil {
			return worlds.Event{}, err
		}
		if found {
			event := eventFromItem(item)
			if input.ResolveData == worlds.EventsResolveDataNone {
				event.Data = nil
			}
			return event, nil
		}
	}
	if err := w.runExists(ctx, input.RunID); err != nil {
		return worlds.Event{}, err
	}
	return worlds.Event{}, worlds.ErrEventNotFound
}

func (w *World) EventsList(ctx context.Context, input worlds.EventsListInput) (worlds.Page[worlds.Event], error) {
	return w.eventsPage(ctx, input.RunID, "", input.Pagination, input.ResolveData)
}

func (w *World) EventsListByCorrelationID(ctx context.Context, input worlds.EventsListByCorrelationIDInput) (worlds.Page[worlds.Event], error) {
	return w.eventsPage(ctx, input.RunID, input.CorrelationID, input.Pagination, input.ResolveData)
}

// eventsPage queries the run's events in slot order. The cursor is the
// EventID of the last returned event, in either direction.
func (w *World) eventsPage(ctx context.Context, runID worlds.RunID, correlationID string, pagination worlds.Pagination, resolve worlds.EventsResolveData) (worlds.Page[worlds.Event], error) {
	if err := ctx.Err(); err != nil {
		return worlds.Page[worlds.Event]{}, err
	}
	descending := pagination.SortOrder == worlds.SortOrderDescending
	from, to := uint64(1), uint64(1<<63)
	if pagination.Cursor != "" {
		slot, err := worlds.EventSlot(worlds.EventID(pagination.Cursor))
		if err != nil {
			return worlds.Page[worlds.Event]{}, err
		}
		if descending {
			to = slot - 1
		} else {
			from = slot + 1
		}
	}
	var events []worlds.Event
	if from <= to {
		input := &dynamodb.QueryInput{
			KeyConditionExpression:    awsv2.String("pk = :pk AND sk BETWEEN :from AND :to"),
			ExpressionAttributeValues: map[string]ddb.AttributeValue{":pk": avs(runPartitionKey(string(runID))), ":from": avs(eventSortKey(from)), ":to": avs(eventSortKey(to))},
			ConsistentRead:            awsv2.Bool(true),
			ScanIndexForward:          awsv2.Bool(!descending),
		}
		if correlationID != "" {
			input.FilterExpression = awsv2.String("correlationId = :correlation")
			input.ExpressionAttributeValues[":correlation"] = avs(correlationID)
		} else if pagination.Limit > 0 {
			input.Limit = awsv2.Int32(int32(min(pagination.Limit+1, 1000)))
		}
		err := w.query(ctx, input, func(item map[string]ddb.AttributeValue) bool {
			events = append(events, eventFromItem(item))
			return pagination.Limit <= 0 || len(events) <= pagination.Limit
		})
		if err != nil {
			return worlds.Page[worlds.Event]{}, err
		}
	}
	if len(events) == 0 && pagination.Cursor == "" {
		if err := w.runExists(ctx, runID); err != nil {
			return worlds.Page[worlds.Event]{}, err
		}
	}
	page := worlds.Page[worlds.Event]{Data: stripEvents(events, resolve)}
	if pagination.Limit > 0 && len(events) > pagination.Limit {
		page.Data, page.HasMore = events[:pagination.Limit], true
		page.Cursor = string(events[pagination.Limit-1].EventID)
	}
	return page, nil
}

// delta returns the events selected by options from the log as it was
// before an append, which had seen events.
func (w *World) delta(ctx context.Context, runID worlds.RunID, seen int, options worlds.EventsCreateOptions) ([]worlds.Event, error) {
	start := seen
	if options.EventCount != nil {
		start = min(max(*options.EventCount, 0), seen)
	} else if options.SinceCursor != "" {
		if slot, err := worlds.EventSlot(worlds.EventID(options.SinceCursor)); err == nil {
			start = int(min(slot, uint64(seen)))
		}
	}
	if start >= seen {
		return []worlds.Event{}, nil
	}
	events := make([]worlds.Event, 0, seen-start)
	err := w.query(ctx, &dynamodb.QueryInput{
		KeyConditionExpression:    awsv2.String("pk = :pk AND sk BETWEEN :from AND :to"),
		ExpressionAttributeValues: map[string]ddb.AttributeValue{":pk": avs(runPartitionKey(string(runID))), ":from": avs(eventSortKey(uint64(start + 1))), ":to": avs(eventSortKey(uint64(seen)))},
		ConsistentRead:            awsv2.Bool(true),
	}, func(item map[string]ddb.AttributeValue) bool {
		events = append(events, eventFromItem(item))
		return true
	})
	return stripEvents(events, options.ResolveData), err
}

func (w *World) runExists(ctx context.Context, runID worlds.RunID) error {
	_, found, err := w.item(ctx, runKey(string(runID)))
	if err != nil {
		return err
	}
	if !found {
		return worlds.ErrRunNotFound
	}
	return nil
}

// appendRetry runs build against freshly read state and commits the result,
// retrying with jittered backoff when another writer changed that state
// first.
func (w *World) appendRetry(ctx context.Context, build func(*appendTx) error) error {
	for attempt := 0; ; attempt++ {
		tx := newAppendTx(w)
		if err := build(tx); err != nil {
			return err
		}
		err := tx.commit(ctx)
		if err == nil || !contended(err) {
			return err
		}
		if attempt+1 >= maxAppendAttempts {
			return fmt.Errorf("workflow/aws: append still contended after %d attempts: %w", maxAppendAttempts, err)
		}
		if err := sleep(ctx, backoff(attempt, 5*time.Millisecond, 250*time.Millisecond)); err != nil {
			return err
		}
	}
}

// appendTx holds the items read for one append and their pending state.
// Admission and materialization run against it, so a batch sees its own
// earlier events, and commit writes every changed item in one transaction.
type appendTx struct {
	world  *World
	now    time.Time
	runs   map[worlds.RunID]*runEntry
	steps  map[string]*stepEntry
	hooks  map[worlds.HookID]*hookEntry
	tokens map[string]*tokenEntry
	waits  map[string]*waitEntry
}

func newAppendTx(world *World) *appendTx {
	return &appendTx{
		world:  world,
		now:    time.Now().UTC(),
		runs:   map[worlds.RunID]*runEntry{},
		steps:  map[string]*stepEntry{},
		hooks:  map[worlds.HookID]*hookEntry{},
		tokens: map[string]*tokenEntry{},
		waits:  map[string]*waitEntry{},
	}
}

type runEntry struct {
	view       runView
	seen       int
	seenStatus worlds.Status
	events     []worlds.Event
}

type stepEntry struct {
	view  *stepView
	dirty bool
}

type hookEntry struct {
	hook  *worlds.Hook
	dirty bool
}

type tokenEntry struct {
	seen, cur *tokenView
}

type waitEntry struct {
	runID         worlds.RunID
	correlationID string
	eventID       worlds.EventID
	created       bool
}

// preload reads the run and every item the events may need, which are steps,
// hooks, wait markers and tokens, with batched consistent reads.
func (tx *appendTx) preload(ctx context.Context, runID worlds.RunID, events []worlds.EventsCreateEvent) error {
	keys := []map[string]ddb.AttributeValue{runKey(string(runID))}
	for _, event := range events {
		switch event.EventType {
		case worlds.EventTypeStepCreated, worlds.EventTypeStepStarted, worlds.EventTypeStepCompleted, worlds.EventTypeStepFailed, worlds.EventTypeStepRetrying, worlds.EventTypeStepCancelled:
			keys = append(keys, stepKey(string(runID), event.CorrelationID))
		case worlds.EventTypeHookReceived, worlds.EventTypeHookDisposed:
			keys = append(keys, hookKey(event.CorrelationID))
		case worlds.EventTypeHookCreated:
			var data hookCreatedData
			if json.Unmarshal(event.Data, &data) == nil && data.Token != "" {
				keys = append(keys, tokenKey(data.Token))
			}
		case worlds.EventTypeWaitCompleted:
			keys = append(keys, waitKey(string(runID), event.CorrelationID))
		}
	}
	if err := tx.load(ctx, keys); err != nil {
		return err
	}
	var tokens []map[string]ddb.AttributeValue
	for _, entry := range tx.hooks {
		if entry.hook != nil && entry.hook.Token != "" {
			if _, ok := tx.tokens[entry.hook.Token]; !ok {
				tokens = append(tokens, tokenKey(entry.hook.Token))
			}
		}
	}
	return tx.load(ctx, tokens)
}

// load reads keys and records each as present or absent.
func (tx *appendTx) load(ctx context.Context, keys []map[string]ddb.AttributeValue) error {
	if len(keys) == 0 {
		return nil
	}
	items, err := tx.world.items(ctx, keys...)
	if err != nil {
		return err
	}
	for _, key := range keys {
		item, found := items[itemID(key)]
		pk, sk := read(key, "pk"), read(key, "sk")
		switch {
		case strings.HasPrefix(pk, "RUN#") && sk == "META":
			id := worlds.RunID(strings.TrimPrefix(pk, "RUN#"))
			if _, ok := tx.runs[id]; ok {
				continue
			}
			if found {
				view := runFromItem(item)
				tx.runs[id] = &runEntry{view: view, seen: view.eventCount, seenStatus: view.Status}
			} else {
				tx.runs[id] = nil
			}
		case strings.HasPrefix(sk, "STEP#"):
			if _, ok := tx.steps[itemID(key)]; ok {
				continue
			}
			entry := &stepEntry{}
			if found {
				view := stepFromItem(item)
				entry.view = &view
			}
			tx.steps[itemID(key)] = entry
		case strings.HasPrefix(sk, "WAIT#"):
			if _, ok := tx.waits[itemID(key)]; ok {
				continue
			}
			tx.waits[itemID(key)] = &waitEntry{runID: worlds.RunID(strings.TrimPrefix(pk, "RUN#")), correlationID: strings.TrimPrefix(sk, "WAIT#"), eventID: worlds.EventID(read(item, "eventId"))}
		case strings.HasPrefix(pk, "HOOK#"):
			id := worlds.HookID(strings.TrimPrefix(pk, "HOOK#"))
			if _, ok := tx.hooks[id]; ok {
				continue
			}
			entry := &hookEntry{}
			if found {
				hook, err := hookFromItem(item)
				if err != nil {
					return err
				}
				entry.hook = &hook
			}
			tx.hooks[id] = entry
		case strings.HasPrefix(pk, "TOKEN#"):
			token := strings.TrimPrefix(pk, "TOKEN#")
			if _, ok := tx.tokens[token]; ok {
				continue
			}
			entry := &tokenEntry{}
			if found {
				entry.seen = tokenFromItem(item)
				current := *entry.seen
				entry.cur = &current
			}
			tx.tokens[token] = entry
		}
	}
	return nil
}

func (tx *appendTx) run(ctx context.Context, id worlds.RunID) (*runEntry, error) {
	if _, ok := tx.runs[id]; !ok {
		if err := tx.load(ctx, []map[string]ddb.AttributeValue{runKey(string(id))}); err != nil {
			return nil, err
		}
	}
	if tx.runs[id] == nil {
		return nil, worlds.ErrRunNotFound
	}
	return tx.runs[id], nil
}

func (tx *appendTx) step(ctx context.Context, runID worlds.RunID, stepID string) (*stepEntry, error) {
	key := stepKey(string(runID), stepID)
	if _, ok := tx.steps[itemID(key)]; !ok {
		if err := tx.load(ctx, []map[string]ddb.AttributeValue{key}); err != nil {
			return nil, err
		}
	}
	return tx.steps[itemID(key)], nil
}

func (tx *appendTx) hook(ctx context.Context, id worlds.HookID) (*hookEntry, error) {
	if _, ok := tx.hooks[id]; !ok {
		if err := tx.load(ctx, []map[string]ddb.AttributeValue{hookKey(string(id))}); err != nil {
			return nil, err
		}
	}
	return tx.hooks[id], nil
}

func (tx *appendTx) token(ctx context.Context, token string) (*tokenEntry, error) {
	if _, ok := tx.tokens[token]; !ok {
		if err := tx.load(ctx, []map[string]ddb.AttributeValue{tokenKey(token)}); err != nil {
			return nil, err
		}
	}
	return tx.tokens[token], nil
}

func (tx *appendTx) wait(ctx context.Context, runID worlds.RunID, correlationID string) (*waitEntry, error) {
	key := waitKey(string(runID), correlationID)
	if _, ok := tx.waits[itemID(key)]; !ok {
		if err := tx.load(ctx, []map[string]ddb.AttributeValue{key}); err != nil {
			return nil, err
		}
	}
	return tx.waits[itemID(key)], nil
}

// owns reports whether hook currently holds its token.
func (tx *appendTx) owns(ctx context.Context, hook *worlds.Hook) (*tokenEntry, bool, error) {
	if hook.Token == "" {
		return nil, false, nil
	}
	token, err := tx.token(ctx, hook.Token)
	if err != nil {
		return nil, false, err
	}
	return token, token.cur != nil && token.cur.hookID == hook.HookID, nil
}

// admit applies the transition rules. It returns a replay result when the
// requested event was already admitted, and an error when it is rejected.
func (tx *appendTx) admit(ctx context.Context, run *runEntry, requested worlds.EventsCreateEvent) (*worlds.EventResult, error) {
	replay := func(id worlds.EventID) *worlds.EventResult {
		return &worlds.EventResult{EventID: id, EventType: requested.EventType}
	}
	if run.view.Status.Terminal() {
		if requested.EventType == worlds.EventTypeRunCancelled && run.view.Status == worlds.StatusCancelled {
			return replay(run.view.cancelledEventID), nil
		}
		return nil, worlds.ErrRunTerminal
	}
	switch requested.EventType {
	case worlds.EventTypeRunStarted:
		if run.view.Status == worlds.StatusRunning {
			return replay(run.view.startedEventID), nil
		}
	case worlds.EventTypeStepStarted, worlds.EventTypeStepCancelled:
		entry, err := tx.step(ctx, run.view.RunID, requested.CorrelationID)
		if err != nil {
			return nil, err
		}
		if entry.view == nil {
			return nil, worlds.ErrStepNotStartable
		}
		step := entry.view
		if requested.EventType == worlds.EventTypeStepStarted {
			// A running step may be started again, because its previous attempt's
			// delivery may have died, and each start records a new attempt.
			if step.Status != worlds.StatusPending && step.Status != worlds.StatusRunning {
				return nil, worlds.ErrStepNotStartable
			}
		} else {
			if step.Status == worlds.StatusCancelled {
				return replay(step.cancelledEventID), nil
			}
			if step.Status.Terminal() {
				return nil, worlds.ErrStepNotStartable
			}
		}
	case worlds.EventTypeWaitCompleted:
		entry, err := tx.wait(ctx, run.view.RunID, requested.CorrelationID)
		if err != nil {
			return nil, err
		}
		if entry.eventID != "" {
			return replay(entry.eventID), nil
		}
	case worlds.EventTypeHookReceived:
		var received worlds.WebhookReceivedEventData
		if json.Unmarshal(requested.Data, &received) != nil || (received.Request == nil && received.Response == nil) {
			return nil, nil
		}
		entry, err := tx.hook(ctx, worlds.HookID(requested.CorrelationID))
		if err != nil {
			return nil, err
		}
		if entry.hook == nil {
			return nil, worlds.ErrHookNotActive
		}
		hook := entry.hook
		_, owned, err := tx.owns(ctx, hook)
		if err != nil {
			return nil, err
		}
		if received.Request != nil && (hook.Webhook == nil || hook.ReceivedEventID != "" || !owned) {
			return nil, worlds.ErrHookNotActive
		}
		if received.Response != nil && (hook.Webhook == nil || hook.ReceivedEventID == "" || received.ResponseTo != hook.ReceivedEventID || !owned) {
			return nil, worlds.ErrHookNotActive
		}
	case worlds.EventTypeHookCreated:
		var created hookCreatedData
		if json.Unmarshal(requested.Data, &created) == nil && created.Token != "" {
			token, err := tx.token(ctx, created.Token)
			if err != nil {
				return nil, err
			}
			if token.cur.expired(tx.now) {
				token.cur = nil
			}
			if token.cur != nil {
				return nil, worlds.ErrHookConflict
			}
		}
	}
	return nil, nil
}

// append assigns the run's next slot to the event and materializes it.
func (tx *appendTx) append(ctx context.Context, run *runEntry, requested worlds.EventsCreateEvent) (worlds.Event, error) {
	run.view.eventCount++
	event := worlds.Event{
		RunID:         run.view.RunID,
		EventID:       worlds.EventIDForSlot(uint64(run.view.eventCount)),
		EventType:     requested.EventType,
		CorrelationID: requested.CorrelationID,
		Data:          bytes.Clone(requested.Data),
		CreatedAt:     time.Now().UTC(),
	}
	run.events = append(run.events, event)
	return event, tx.materialize(ctx, run, event)
}

func (tx *appendTx) materialize(ctx context.Context, run *runEntry, event worlds.Event) error {
	now := event.CreatedAt
	view := &run.view
	view.UpdatedAt = now
	switch event.EventType {
	case worlds.EventTypeRunStarted:
		view.Status, view.StartedAt, view.startedEventID = worlds.StatusRunning, &now, event.EventID
	case worlds.EventTypeRunCompleted:
		view.Status, view.CompletedAt, view.Output = worlds.StatusCompleted, &now, event.Data
	case worlds.EventTypeRunFailed:
		view.Status, view.CompletedAt, view.Error = worlds.StatusFailed, &now, event.Data
	case worlds.EventTypeRunCancelled:
		view.Status, view.CompletedAt, view.cancelledEventID = worlds.StatusCancelled, &now, event.EventID
	case worlds.EventTypeAttrSet:
		var data attrSetData
		if json.Unmarshal(event.Data, &data) == nil {
			view.Attributes = maps.Clone(view.Attributes)
			for _, change := range data.Changes {
				if change.Value == nil {
					delete(view.Attributes, change.Key)
				} else {
					view.Attributes[change.Key] = *change.Value
				}
			}
		}
	case worlds.EventTypeWaitCompleted:
		entry, err := tx.wait(ctx, run.view.RunID, event.CorrelationID)
		if err != nil {
			return err
		}
		if entry.eventID == "" {
			entry.eventID, entry.created = event.EventID, true
		}
	case worlds.EventTypeStepCreated:
		var data stepCreatedData
		_ = json.Unmarshal(event.Data, &data)
		entry, err := tx.step(ctx, run.view.RunID, event.CorrelationID)
		if err != nil {
			return err
		}
		entry.view = &stepView{Step: worlds.Step{RunID: run.view.RunID, StepID: worlds.StepID(event.CorrelationID), StepName: data.StepName, Status: worlds.StatusPending, Input: data.Input, CreatedAt: now, UpdatedAt: now}}
		entry.dirty = true
	case worlds.EventTypeStepStarted:
		return tx.updateStep(ctx, event, worlds.StatusRunning)
	case worlds.EventTypeStepCompleted:
		return tx.updateStep(ctx, event, worlds.StatusCompleted)
	case worlds.EventTypeStepFailed:
		return tx.updateStep(ctx, event, worlds.StatusFailed)
	case worlds.EventTypeStepRetrying:
		return tx.updateStep(ctx, event, worlds.StatusPending)
	case worlds.EventTypeStepCancelled:
		return tx.updateStep(ctx, event, worlds.StatusCancelled)
	case worlds.EventTypeHookCreated:
		var data hookCreatedData
		_ = json.Unmarshal(event.Data, &data)
		id := worlds.HookID(event.CorrelationID)
		tx.hooks[id] = &hookEntry{hook: &worlds.Hook{RunID: run.view.RunID, HookID: id, Token: data.Token, Metadata: data.Metadata, Webhook: data.Webhook, MinRetention: data.MinRetention, CreatedAt: now, UpdatedAt: now}, dirty: true}
		if data.Token != "" {
			token, err := tx.token(ctx, data.Token)
			if err != nil {
				return err
			}
			token.cur = &tokenView{hookID: id, runID: run.view.RunID}
		}
	case worlds.EventTypeHookReceived:
		entry, err := tx.hook(ctx, worlds.HookID(event.CorrelationID))
		if err != nil || entry.hook == nil {
			return err
		}
		hook := entry.hook
		entry.dirty = true
		hook.UpdatedAt, hook.ReceivedAt = now, &now
		var data worlds.WebhookReceivedEventData
		if json.Unmarshal(event.Data, &data) == nil {
			if data.Request != nil {
				hook.ReceivedEventID = event.EventID
			}
			if data.Response != nil && data.ResponseTo == hook.ReceivedEventID {
				return tx.completeHook(ctx, hook, now)
			}
		}
		if hook.Webhook == nil || (!hook.Webhook.Manual && hook.ReceivedEventID != "") {
			return tx.completeHook(ctx, hook, now)
		}
	case worlds.EventTypeHookDisposed:
		entry, err := tx.hook(ctx, worlds.HookID(event.CorrelationID))
		if err != nil || entry.hook == nil {
			return err
		}
		token, owned, err := tx.owns(ctx, entry.hook)
		if err != nil {
			return err
		}
		if owned {
			token.cur = nil
		}
	}
	return nil
}

// completeHook releases the hook's token now, or keeps it until the hook's
// minimum retention elapses.
func (tx *appendTx) completeHook(ctx context.Context, hook *worlds.Hook, now time.Time) error {
	token, owned, err := tx.owns(ctx, hook)
	if err != nil {
		return err
	}
	if hook.MinRetention <= 0 {
		hook.RetainUntil = nil
		if owned {
			token.cur = nil
		}
		return nil
	}
	until := now.Add(hook.MinRetention)
	hook.RetainUntil = &until
	if owned {
		token.cur = &tokenView{hookID: token.cur.hookID, runID: token.cur.runID, retainUntil: &until}
	}
	return nil
}

func (tx *appendTx) updateStep(ctx context.Context, event worlds.Event, status worlds.Status) error {
	entry, err := tx.step(ctx, event.RunID, event.CorrelationID)
	if err != nil || entry.view == nil {
		return err
	}
	step := entry.view
	entry.dirty = true
	step.Status, step.UpdatedAt = status, event.CreatedAt
	switch status {
	case worlds.StatusRunning:
		step.Attempt++
		step.StartedAt = &event.CreatedAt
	case worlds.StatusCompleted, worlds.StatusFailed:
		step.CompletedAt = &event.CreatedAt
	case worlds.StatusCancelled:
		step.cancelledEventID = event.EventID
	}
	var result stepResultData
	if json.Unmarshal(event.Data, &result) == nil {
		step.Output, step.Error, step.RetryAfter = result.Output, result.Error, result.RetryAfter
	}
	return nil
}

func (tx *appendTx) result(event worlds.Event, created bool) worlds.EventResult {
	result := worlds.EventResult{EventID: event.EventID, EventType: event.EventType}
	if created {
		result.AllocatedEventCount = 1
	}
	if event.EventType == worlds.EventTypeStepCreated {
		if entry := tx.steps[itemID(stepKey(string(event.RunID), event.CorrelationID))]; entry != nil && entry.view != nil {
			step := entry.view.Step
			step.Input, step.Output, step.Error = bytes.Clone(step.Input), bytes.Clone(step.Output), bytes.Clone(step.Error)
			result.Step, result.StepCreated = &step, created
		}
	}
	return result
}

// commit writes every changed item in one transaction. Each run's META
// update requires the event count read before the append, so a concurrent
// append to any involved run cancels the transaction.
func (tx *appendTx) commit(ctx context.Context) error {
	table := awsv2.String(tx.world.config.TableName)
	var items []ddb.TransactWriteItem
	for _, run := range tx.runs {
		if run == nil || len(run.events) == 0 {
			continue
		}
		if err := checkItemSize(runItem(run.view)); err != nil {
			return fmt.Errorf("%w: %w", worlds.ErrEventInvalid, err)
		}
		set, remove := runUpdate(run.view)
		if run.view.Status == run.seenStatus {
			// status is projected into the indexes, so leaving it out keeps
			// ordinary appends from writing to them.
			delete(set, "status")
		}
		set[":seen"] = avn(int64(run.seen))
		update, names, values := updateExpression(set, remove)
		items = append(items, ddb.TransactWriteItem{Update: &ddb.Update{
			TableName:                 table,
			Key:                       runKey(string(run.view.RunID)),
			UpdateExpression:          awsv2.String(update),
			ConditionExpression:       awsv2.String("eventCount = :seen"),
			ExpressionAttributeNames:  names,
			ExpressionAttributeValues: values,
		}})
		for _, event := range run.events {
			slot, _ := worlds.EventSlot(event.EventID)
			items = append(items, put(*table, eventItem(event, slot)))
		}
	}
	if len(items) == 0 {
		return nil
	}
	for _, entry := range tx.steps {
		if entry.dirty && entry.view != nil {
			items = append(items, ddb.TransactWriteItem{Put: &ddb.Put{TableName: table, Item: stepItem(*entry.view)}})
		}
	}
	for _, entry := range tx.hooks {
		if entry.dirty && entry.hook != nil {
			item, err := hookItem(*entry.hook)
			if err != nil {
				return err
			}
			items = append(items, ddb.TransactWriteItem{Put: &ddb.Put{TableName: table, Item: item}})
		}
	}
	for token, entry := range tx.tokens {
		if sameToken(entry.seen, entry.cur) {
			continue
		}
		condition, values := tokenCondition(entry.seen)
		if entry.cur == nil {
			items = append(items, ddb.TransactWriteItem{Delete: &ddb.Delete{TableName: table, Key: tokenKey(token), ConditionExpression: awsv2.String(condition), ExpressionAttributeValues: values}})
		} else {
			items = append(items, ddb.TransactWriteItem{Put: &ddb.Put{TableName: table, Item: tokenItem(token, *entry.cur), ConditionExpression: awsv2.String(condition), ExpressionAttributeValues: values}})
		}
	}
	for _, entry := range tx.waits {
		if entry.created {
			items = append(items, put(*table, merge(waitKey(string(entry.runID), entry.correlationID), map[string]ddb.AttributeValue{"eventId": avs(string(entry.eventID))})))
		}
	}
	if err := tx.world.transact(ctx, items); err != nil {
		if errors.Is(err, errWriteTooLarge) {
			return fmt.Errorf("%w: %w", worlds.ErrEventInvalid, err)
		}
		return err
	}
	return nil
}

func sameToken(a, b *tokenView) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.hookID != b.hookID || (a.retainUntil == nil) != (b.retainUntil == nil) {
		return false
	}
	return a.retainUntil == nil || a.retainUntil.Equal(*b.retainUntil)
}

// updateExpression builds "SET #a = :a, ... REMOVE #b" from attribute
// values. Values whose name starts with ':' are passed through as
// expression values only.
func updateExpression(set map[string]ddb.AttributeValue, remove []string) (string, map[string]string, map[string]ddb.AttributeValue) {
	names := map[string]string{}
	values := map[string]ddb.AttributeValue{}
	var assignments []string
	index := 0
	for _, name := range sortedKeys(set) {
		if strings.HasPrefix(name, ":") {
			values[name] = set[name]
			continue
		}
		placeholder := fmt.Sprintf("a%d", index)
		index++
		names["#"+placeholder] = name
		values[":"+placeholder] = set[name]
		assignments = append(assignments, "#"+placeholder+" = :"+placeholder)
	}
	expression := "SET " + strings.Join(assignments, ", ")
	if len(remove) > 0 {
		removals := make([]string, len(remove))
		for i, name := range remove {
			placeholder := fmt.Sprintf("r%d", i)
			names["#"+placeholder] = name
			removals[i] = "#" + placeholder
		}
		expression += " REMOVE " + strings.Join(removals, ", ")
	}
	return expression, names, values
}

func sortedKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}
