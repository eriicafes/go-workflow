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
	// The run ID and attributes are checked as the Workflow SDK's Worlds
	// check them.
	if err := worlds.CheckRunCreation(input, time.Now()); err != nil {
		return worlds.EventResult{}, err
	}
	data, err := worlds.RunCreatedData(input)
	if err != nil {
		return worlds.EventResult{}, err
	}
	now := time.Now().UTC()
	attributes := maps.Clone(input.Attributes)
	if attributes == nil {
		attributes = map[string]string{}
	}
	// run_created takes the first position, which counts as in-band.
	view := runView{Run: worlds.Run{RunID: input.RunID, WorkflowName: input.WorkflowName, Status: worlds.StatusPending, Attributes: attributes, DeploymentID: input.DeploymentID, EncryptionPublicKey: input.EncryptionPublicKey, ExecutionContext: input.ExecutionContext, InBandCount: 1, SpecVersion: worlds.SpecVersion, CreatedAt: now, UpdatedAt: now, StartHook: worlds.RunEchoStartHook(data)}, eventCount: 1}
	event := worlds.Event{RunID: input.RunID, EventID: worlds.EventIDForSlot(1), EventType: worlds.EventTypeRunCreated, Data: data, CreatedAt: now, SpecVersion: worlds.SpecVersion}
	items := []ddb.TransactWriteItem{{Put: &ddb.Put{TableName: awsv2.String(w.config.TableName), Item: runItem(view), ConditionExpression: awsv2.String("attribute_not_exists(pk)")}}, put(w.config.TableName, eventItem(event, 1))}
	if input.StartHook != nil {
		// A candidate's decision is permanent.
		if item, found, err := w.item(ctx, startHookDecisionKey(string(input.RunID))); err != nil {
			return worlds.EventResult{}, err
		} else if found && read(item, "decision") == "rejected" {
			return worlds.EventResult{}, &worlds.HookConflictError{RunID: worlds.RunID(read(item, "conflictingRunId"))}
		} else if !found && worlds.StartHookCandidateExpired(input.RunID, now) {
			return worlds.EventResult{}, worlds.ErrStartHookRejected
		}
		hookItems, err := w.startHookItems(ctx, input, now)
		var conflict *worlds.HookConflictError
		if errors.As(err, &conflict) {
			return worlds.EventResult{}, w.rejectStart(ctx, input, conflict)
		}
		if errors.Is(err, worlds.ErrRunAlreadyExists) {
			return w.repeatedStart(ctx, input)
		}
		if err != nil {
			return worlds.EventResult{}, err
		}
		items = append(items, hookItems...)
		items = append(items, put(w.config.TableName, merge(startHookDecisionKey(string(input.RunID)), map[string]ddb.AttributeValue{"decision": avs("accepted"), "token": avs(input.StartHook.Token)})))
	}
	err = w.transact(ctx, items)
	if conditional(err) {
		if input.StartHook != nil {
			// The run may exist, or a concurrent claimant took the token.
			if _, found, getErr := w.item(ctx, runKey(string(input.RunID))); getErr == nil && !found {
				if item, found, getErr := w.item(ctx, tokenKey(input.StartHook.Token)); getErr == nil && found {
					return worlds.EventResult{}, w.rejectStart(ctx, input, &worlds.HookConflictError{RunID: tokenFromItem(item).runID})
				}
			}
		}
		return w.repeatedStart(ctx, input)
	}
	if err != nil {
		return worlds.EventResult{}, err
	}
	return worlds.EventResult{EventID: event.EventID, EventType: event.EventType, CreatedAt: now, AllocatedEventCount: 1, InBandCount: 1}, nil
}

// repeatedStart returns the run_created of an accepted start that input
// repeats, or ErrRunAlreadyExists.
func (w *World) repeatedStart(ctx context.Context, input worlds.EventsCreateRunInput) (worlds.EventResult, error) {
	created, err := w.EventsGet(ctx, worlds.EventsGetInput{RunID: input.RunID, EventID: worlds.EventIDForSlot(1)})
	if err == nil && worlds.RepeatsAcceptedStart(created.Data, input) {
		return worlds.EventResult{EventID: created.EventID, EventType: created.EventType, CreatedAt: created.CreatedAt, InBandCount: 1}, nil
	}
	return worlds.EventResult{}, worlds.ErrRunAlreadyExists
}

// rejectStart records the refusal of a start hook candidate, which is
// permanent, and returns it.
func (w *World) rejectStart(ctx context.Context, input worlds.EventsCreateRunInput, conflict *worlds.HookConflictError) error {
	item := merge(startHookDecisionKey(string(input.RunID)), map[string]ddb.AttributeValue{"decision": avs("rejected"), "token": avs(input.StartHook.Token), "conflictingRunId": avs(string(conflict.RunID))})
	err := w.transact(ctx, []ddb.TransactWriteItem{{Put: &ddb.Put{TableName: awsv2.String(w.config.TableName), Item: item, ConditionExpression: awsv2.String("attribute_not_exists(pk)")}}})
	if err != nil && !conditional(err) {
		return errors.Join(conflict, err)
	}
	return conflict
}

func (w *World) EventsCreate(ctx context.Context, input worlds.EventsCreateInput) (worlds.EventResult, error) {
	if input.Event.EventType == worlds.EventTypeHookCreated {
		return w.createHook(ctx, input)
	}
	if err := ctx.Err(); err != nil {
		return worlds.EventResult{}, err
	}
	if input.Event.EventType == worlds.EventTypeRunCreated || !input.Event.EventType.Valid() {
		return worlds.EventResult{}, worlds.ErrEventInvalid
	}
	// A resume's digest must be its payload's, as world-postgres checks it.
	if err := worlds.CheckResumeDigest(input.Event, input.Options.ResumePayloadDigest); err != nil {
		return worlds.EventResult{}, err
	}
	if input.Event.EventType == worlds.EventTypeRunStarted {
		// A first flow's run_started creates a run whose run_created never
		// landed, and is stored without the creation it carries.
		if err := worlds.CreateRunForStart(ctx, w, input.RunID, input.Event.Data); err != nil {
			return worlds.EventResult{}, err
		}
		input.Event.Data = nil
	}
	var result worlds.EventResult
	var finished worlds.Run
	err := w.appendRetry(ctx, func(tx *appendTx) error {
		if err := tx.preload(ctx, input.RunID, []worlds.EventsCreateEvent{input.Event}); err != nil {
			return err
		}
		run, err := tx.run(ctx, input.RunID)
		if err != nil {
			return err
		}
		defer func() { finished = run.view.Run }()
		if err := fence(run, input.Options.InBandCount); err != nil {
			return err
		}
		var resume *resumeEntry
		if input.Event.EventType == worlds.EventTypeHookReceived && input.Options.ResumeID != "" {
			if resume, err = tx.resume(ctx, input.RunID, input.Options.ResumeID); err != nil {
				return err
			}
		}
		var replay *worlds.EventResult
		if resume != nil && resume.eventID != "" {
			// A repeat of a resume returns the event it committed.
			if resume.digest != input.Options.ResumePayloadDigest {
				return fmt.Errorf("%w: resume %s was sent with another payload", worlds.ErrEventInvalid, input.Options.ResumeID)
			}
			replay = &worlds.EventResult{EventID: resume.eventID, EventType: worlds.EventTypeHookReceived}
		} else if replay, err = tx.admit(ctx, run, input.Event); err != nil {
			return err
		}
		if replay != nil {
			result = *replay
		} else {
			event, err := tx.append(ctx, run, input.Event, createdAt(input.Event, input.Options.InBandCount))
			if err != nil {
				return err
			}
			if resume != nil {
				run.events[len(run.events)-1].ResumeID, event.ResumeID = input.Options.ResumeID, input.Options.ResumeID
				resume.digest, resume.eventID, resume.created = input.Options.ResumePayloadDigest, event.EventID, true
			}
			advance(run, input.Options.InBandCount, 1)
			result = tx.result(event, true)
		}
		result.InBandCount = run.view.InBandCount
		// The delta is read before the commit, so a failed read does not
		// report a committed append as failed. Events up to run.seen are
		// already in the log and do not change.
		result.Events, err = w.delta(ctx, input.RunID, run.seen, input.Options)
		return err
	})
	if err != nil {
		return worlds.EventResult{}, err
	}
	// Also on a replay, so a retried append queues the expiry a failed one
	// could not.
	if finishes(input.Event.EventType) {
		if err := w.scheduleExpiry(ctx, finished); err != nil {
			return worlds.EventResult{}, err
		}
	}
	if err := w.addWait(ctx, input.RunID, input.Event, &result); err != nil {
		return worlds.EventResult{}, err
	}
	return result, nil
}

// addWait adds to the result of a wait_created or wait_completed the wait
// its committed events record.
func (w *World) addWait(ctx context.Context, runID worlds.RunID, event worlds.EventsCreateEvent, result *worlds.EventResult) error {
	if event.EventType != worlds.EventTypeWaitCreated && event.EventType != worlds.EventTypeWaitCompleted {
		return nil
	}
	page, err := w.EventsListByCorrelationID(ctx, worlds.EventsListByCorrelationIDInput{RunID: runID, CorrelationID: event.CorrelationID})
	if err != nil {
		return err
	}
	result.Wait = worlds.WaitOfEvents(runID, event.CorrelationID, page.Data)
	return nil
}

func (w *World) EventsCreateBatch(ctx context.Context, input worlds.EventsCreateBatchInput) (worlds.EventBatchResult, error) {
	if err := ctx.Err(); err != nil {
		return worlds.EventBatchResult{}, err
	}
	for _, value := range input.Events {
		if value.EventType == worlds.EventTypeRunCreated || value.EventType == worlds.EventTypeHookCreated || !value.EventType.Valid() {
			return worlds.EventBatchResult{}, worlds.ErrEventInvalid
		}
	}
	var result worlds.EventBatchResult
	var finished worlds.Run
	err := w.appendRetry(ctx, func(tx *appendTx) error {
		result = worlds.EventBatchResult{Results: make([]worlds.EventBatchItem, len(input.Events))}
		if err := tx.preload(ctx, input.RunID, input.Events); err != nil {
			return err
		}
		run, err := tx.run(ctx, input.RunID)
		if err != nil {
			return err
		}
		defer func() { finished = run.view.Run }()
		// The fence holds for the whole block: the commit requires the event
		// count read here, which every in-band write moves.
		if err := fence(run, input.InBandCount); err != nil {
			return err
		}
		if input.EventCount != nil {
			// Read before the commit, as in EventsCreate.
			result.Events, err = w.delta(ctx, input.RunID, run.seen, worlds.EventsCreateOptions{EventCount: input.EventCount})
			if err != nil {
				return err
			}
		}
		refused := map[string]bool{}
		for index, value := range input.Events {
			item := &result.Results[index]
			if value.CorrelationID != "" && refused[value.CorrelationID] {
				// A step's outcome never lands behind its refused creation.
				item.Err = fmt.Errorf("%w: an earlier event of %s was refused", worlds.ErrStepNotStartable, value.CorrelationID)
				continue
			}
			replay, err := tx.admit(ctx, run, value)
			if err == nil && replay != nil {
				err = fmt.Errorf("%w: batch repeats %s %s", worlds.ErrEventConflict, value.EventType, value.CorrelationID)
			}
			if err != nil {
				if !refusal(err) {
					return err
				}
				item.Err = err
				if value.CorrelationID != "" {
					refused[value.CorrelationID] = true
				}
				continue
			}
			event, err := tx.append(ctx, run, value, createdAt(value, input.InBandCount))
			if err != nil {
				return err
			}
			item.EventResult = tx.result(event, true)
			result.AllocatedEventCount++
		}
		advance(run, input.InBandCount, result.AllocatedEventCount)
		result.InBandCount = run.view.InBandCount
		for index := range result.Results {
			result.Results[index].InBandCount = result.InBandCount
		}
		return nil
	})
	if err != nil {
		return worlds.EventBatchResult{}, err
	}
	if slices.ContainsFunc(input.Events, func(event worlds.EventsCreateEvent) bool { return finishes(event.EventType) }) {
		if err := w.scheduleExpiry(ctx, finished); err != nil {
			return worlds.EventBatchResult{}, err
		}
	}
	for index, event := range input.Events {
		if result.Results[index].Err == nil {
			if err := w.addWait(ctx, input.RunID, event, &result.Results[index].EventResult); err != nil {
				return worlds.EventBatchResult{}, err
			}
		}
	}
	return result, nil
}

// refusal reports whether err is an event's refusal by the transition rules,
// rather than a failure to read or write.
func refusal(err error) bool {
	for _, target := range []error{worlds.ErrEventInvalid, worlds.ErrEventConflict, worlds.ErrEventNotFound, worlds.ErrAttributeInvalid, worlds.ErrTooEarly, worlds.ErrStepNotFound, worlds.ErrRunTerminal, worlds.ErrStepNotStartable, worlds.ErrHookNotFound, worlds.ErrHookNotActive, worlds.ErrHookForceClaimed, worlds.ErrHookConflict} {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}

// createdAt returns the creation time of event: its OccurredAt for an
// in-band write that sets one, and now otherwise.
func createdAt(event worlds.EventsCreateEvent, inBand *int) time.Time {
	if inBand != nil && !event.OccurredAt.IsZero() {
		return event.OccurredAt.UTC()
	}
	return time.Now().UTC()
}

// fence returns ErrInBandSuperseded when an in-band write's count is not the
// run's InBandCount. The commit's condition on the event count keeps the two
// read together.
func fence(run *runEntry, count *int) error {
	if count != nil && *count != run.view.InBandCount {
		return worlds.ErrInBandSuperseded
	}
	return nil
}

// advance adds the positions an in-band write allocated to the run view the
// commit writes.
func advance(run *runEntry, count *int, appended int) {
	if count != nil {
		run.view.InBandCount += appended
	}
}

func (w *World) createHook(ctx context.Context, input worlds.EventsCreateInput) (worlds.EventResult, error) {
	// The token moves to the claimant in one transaction. A forced claim from a
	// live owner also appends hook_disposed, naming the claimant in
	// ForceClaimedBy, to the owner's run. The transaction requires both runs'
	// event counts and the token item to be unchanged, so exactly one
	// concurrent claimant wins.
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
	original := input.Event
	var result worlds.EventResult
	err := w.appendRetry(ctx, func(tx *appendTx) error {
		input.Event = original
		var created worlds.DataHookCreated
		_ = json.Unmarshal(original.Data, &created)
		result = worlds.EventResult{}
		if err := tx.preload(ctx, input.RunID, []worlds.EventsCreateEvent{input.Event}); err != nil {
			return err
		}
		claimant, err := tx.run(ctx, input.RunID)
		if err != nil {
			return err
		}
		if claimant.view.Status.Terminal() {
			return worlds.FinishedRunRefusal(claimant.view.Status, input.Event, "")
		}
		if err := fence(claimant, input.Options.InBandCount); err != nil {
			return err
		}
		token, err := tx.token(ctx, created.Token)
		if err != nil {
			return err
		}
		if err := tx.releaseLapsed(ctx, token); err != nil {
			return err
		}
		if token.cur != nil {
			previous := *token.cur
			if !created.Force {
				created.ConflictingRunID = previous.runID
				input.Event.EventType = worlds.EventTypeHookConflict
			} else {
				created.ForceClaimedFrom = &worlds.HookForceClaim{RunID: previous.runID, HookID: previous.hookID}
				owner, err := tx.run(ctx, previous.runID)
				if err != nil && !errors.Is(err, worlds.ErrRunNotFound) {
					return err
				}
				if owner != nil {
					claim := created.ForceClaimedFrom
					claim.WorkflowName, claim.DeploymentID, claim.RunSpecVersion = owner.view.WorkflowName, owner.view.DeploymentID, owner.view.SpecVersion
				}
				if owner == nil || owner.view.Status.Terminal() {
					token.cur = nil
				} else {
					data, _ := json.Marshal(worlds.HookDisposedEventData{ForceClaimedBy: &worlds.HookForceClaim{RunID: input.RunID, HookID: worlds.HookID(input.Event.CorrelationID)}})
					if _, err := tx.append(ctx, owner, worlds.EventsCreateEvent{EventType: worlds.EventTypeHookDisposed, CorrelationID: string(previous.hookID), Data: data}, time.Now().UTC()); err != nil {
						return err
					}
				}
			}
		}
		input.Event.Data, _ = json.Marshal(created)

		event, err := tx.append(ctx, claimant, input.Event, createdAt(input.Event, input.Options.InBandCount))
		if err != nil {
			return err
		}
		advance(claimant, input.Options.InBandCount, len(claimant.events))
		result = tx.result(event, true)
		result.AllocatedEventCount = len(claimant.events)
		result.InBandCount = claimant.view.InBandCount
		// Read before the commit, as in EventsCreate.
		result.Events, err = w.delta(ctx, input.RunID, claimant.seen, input.Options)
		result.Events = append(result.Events, claimant.events[:len(claimant.events)-1]...)
		return err
	})
	if err != nil {
		return worlds.EventResult{}, err
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
			event.Data = worlds.ResolveEventData(event.EventType, event.Data, input.ResolveData)
			return event, nil
		}
	}
	if err := w.runExists(ctx, input.RunID); err != nil {
		return worlds.Event{}, err
	}
	return worlds.Event{}, worlds.ErrEventNotFound
}

// EventsList reads the run's META item with a consistent read before the
// page, so its snapshot counts no position the page's query cannot see.
func (w *World) EventsList(ctx context.Context, input worlds.EventsListInput) (worlds.EventsPage, error) {
	if err := ctx.Err(); err != nil {
		return worlds.EventsPage{}, err
	}
	item, found, err := w.item(ctx, runKey(string(input.RunID)))
	if err != nil {
		return worlds.EventsPage{}, err
	}
	if !found {
		return worlds.EventsPage{}, worlds.ErrRunNotFound
	}
	view := runFromItem(item)
	page, err := w.eventsPage(ctx, input.RunID, "", input.Pagination, input.ResolveData)
	if err != nil {
		return worlds.EventsPage{}, err
	}
	return worlds.EventsPage{Page: page, Snapshot: worlds.EventsSnapshot{EventCount: view.eventCount, InBandCount: view.InBandCount}}, nil
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
	page := worlds.Page[worlds.Event]{Data: worlds.ResolveEvents(events, resolve)}
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
	return worlds.ResolveEvents(events, options.ResolveData), err
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
	world   *World
	now     time.Time
	runs    map[worlds.RunID]*runEntry
	steps   map[string]*stepEntry
	hooks   map[worlds.HookID]*hookEntry
	tokens  map[string]*tokenEntry
	waits   map[string]*waitEntry
	resumes map[string]*resumeEntry
}

func newAppendTx(world *World) *appendTx {
	return &appendTx{
		world:   world,
		now:     time.Now().UTC(),
		runs:    map[worlds.RunID]*runEntry{},
		steps:   map[string]*stepEntry{},
		hooks:   map[worlds.HookID]*hookEntry{},
		tokens:  map[string]*tokenEntry{},
		waits:   map[string]*waitEntry{},
		resumes: map[string]*resumeEntry{},
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
	// createdEventID is the hook's hook_created event, which holds its
	// metadata.
	createdEventID worlds.EventID
	// disposal is set once the hook is disposed: its item then records only
	// the disposal, as world-local's hook_disposals row.
	disposal *hookDisposal
}

// hookDisposal is a disposed hook's run, its hook_disposed event, which a
// repeated disposal returns, and whether a forced claim disposed it.
type hookDisposal struct {
	runID   worlds.RunID
	eventID worlds.EventID
	forced  bool
}

type tokenEntry struct {
	seen, cur *tokenView
}

// resumeEntry is a deduplicated resume: its payload digest and the event it
// committed, or no event when none is recorded.
type resumeEntry struct {
	runID    worlds.RunID
	resumeID string
	digest   string
	eventID  worlds.EventID
	created  bool
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
		case worlds.EventTypeStepCreated, worlds.EventTypeStepStarted, worlds.EventTypeStepCompleted, worlds.EventTypeStepFailed, worlds.EventTypeStepRetrying:
			keys = append(keys, stepKey(string(runID), event.CorrelationID))
		case worlds.EventTypeHookReceived, worlds.EventTypeHookDisposed:
			keys = append(keys, hookKey(event.CorrelationID))
		case worlds.EventTypeHookCreated:
			var data worlds.DataHookCreated
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
		case strings.HasPrefix(sk, "RESUME#"):
			if _, ok := tx.resumes[itemID(key)]; ok {
				continue
			}
			tx.resumes[itemID(key)] = &resumeEntry{runID: worlds.RunID(strings.TrimPrefix(pk, "RUN#")), resumeID: strings.TrimPrefix(sk, "RESUME#"), digest: read(item, "digest"), eventID: worlds.EventID(read(item, "eventId"))}
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
			if found && isDisposal(item) {
				entry.disposal = disposalFromItem(item)
			} else if found {
				hook, err := hookFromItem(item)
				if err != nil {
					return err
				}
				entry.hook = &hook
				entry.createdEventID = worlds.EventID(read(item, "createdEventId"))
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

func (tx *appendTx) resume(ctx context.Context, runID worlds.RunID, resumeID string) (*resumeEntry, error) {
	key := resumeKey(string(runID), resumeID)
	if _, ok := tx.resumes[itemID(key)]; !ok {
		if err := tx.load(ctx, []map[string]ddb.AttributeValue{key}); err != nil {
			return nil, err
		}
	}
	return tx.resumes[itemID(key)], nil
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

// releaseLapsed clears token's owner when the owning run is gone, or has
// finished and its hook's token retention has lapsed.
func (tx *appendTx) releaseLapsed(ctx context.Context, token *tokenEntry) error {
	if token.cur.expired(tx.now) {
		token.cur = nil
	}
	if token.cur == nil {
		return nil
	}
	owner, err := tx.run(ctx, token.cur.runID)
	if errors.Is(err, worlds.ErrRunNotFound) || err == nil && owner == nil {
		token.cur = nil
		return nil
	}
	if err != nil || !owner.view.Status.Terminal() {
		return err
	}
	entry, err := tx.hook(ctx, token.cur.hookID)
	if err != nil {
		return err
	}
	if entry.hook == nil || entry.hook.TokenRetentionUntil == nil || !entry.hook.TokenRetentionUntil.After(tx.now) {
		token.cur = nil
	}
	return nil
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

// admit applies the transition rules, as the Workflow SDK's Worlds do. An
// event that repeats a creation the log holds is refused with
// ErrEventConflict. It returns a non-nil result, holding no event, for a
// run_started of a run that started before, which appends nothing.
func (tx *appendTx) admit(ctx context.Context, run *runEntry, requested worlds.EventsCreateEvent) (*worlds.EventResult, error) {
	if run.view.Status.Terminal() {
		var stepStatus worlds.Status
		if requested.EventType == worlds.EventTypeStepCompleted || requested.EventType == worlds.EventTypeStepFailed || requested.EventType == worlds.EventTypeStepRetrying {
			entry, err := tx.step(ctx, run.view.RunID, requested.CorrelationID)
			if err != nil {
				return nil, err
			}
			if entry.view != nil {
				stepStatus = entry.view.Status
			}
		}
		if err := worlds.FinishedRunRefusal(run.view.Status, requested, stepStatus); err != nil {
			return nil, err
		}
	}
	switch requested.EventType {
	case worlds.EventTypeRunStarted:
		if run.view.Status == worlds.StatusRunning {
			// A run that started before is not started again.
			return &worlds.EventResult{}, nil
		}
	case worlds.EventTypeStepCreated, worlds.EventTypeStepStarted, worlds.EventTypeStepCompleted, worlds.EventTypeStepFailed, worlds.EventTypeStepRetrying:
		// A step is created once, and its other events need it unfinished,
		// as in the Workflow SDK's Worlds. A started step may be started
		// again, and each start records a new attempt.
		entry, err := tx.step(ctx, run.view.RunID, requested.CorrelationID)
		if err != nil {
			return nil, err
		}
		var status worlds.Status
		var retryAfter *time.Time
		if entry.view != nil {
			status, retryAfter = entry.view.Status, entry.view.RetryAfter
		}
		if err := worlds.CheckStepEvent(requested, entry.view != nil, status, retryAfter, tx.now); err != nil {
			return nil, err
		}
	case worlds.EventTypeAttrSet:
		if err := worlds.CheckAttrSet(run.view.Attributes, requested.Data); err != nil {
			return nil, err
		}
		if requested.CorrelationID != "" && worlds.WorkflowAttrSet(requested.Data) {
			if err := tx.refuseRepeat(ctx, run, requested); err != nil {
				return nil, err
			}
		}
	case worlds.EventTypeWaitCompleted:
		entry, err := tx.wait(ctx, run.view.RunID, requested.CorrelationID)
		if err != nil {
			return nil, err
		}
		if entry.eventID != "" {
			return nil, fmt.Errorf("%w: wait %s already completed", worlds.ErrEventConflict, requested.CorrelationID)
		}
		// A wait completes once it was created.
		created, err := tx.logged(ctx, run, worlds.EventTypeWaitCreated, requested.CorrelationID)
		if err != nil {
			return nil, err
		}
		if !created {
			return nil, fmt.Errorf("%w: wait %s not found", worlds.ErrEventNotFound, requested.CorrelationID)
		}
	case worlds.EventTypeWaitCreated:
		// A wait is created once.
		if requested.CorrelationID == "" {
			break
		}
		if err := tx.refuseRepeat(ctx, run, requested); err != nil {
			return nil, err
		}

	case worlds.EventTypeHookReceived:
		entry, err := tx.hook(ctx, worlds.HookID(requested.CorrelationID))
		if err != nil {
			return nil, err
		}
		if disposal := entry.disposal; disposal != nil && disposal.runID == run.view.RunID {
			// A hook another run took over redirects the delivery.
			if disposal.forced {
				return nil, worlds.ErrHookForceClaimed
			}
			return nil, worlds.ErrHookNotFound
		}
		if entry.hook == nil || entry.hook.RunID != run.view.RunID {
			return nil, worlds.ErrHookNotFound
		}
		_, owned, err := tx.owns(ctx, entry.hook)
		if err != nil {
			return nil, err
		}
		if !owned {
			return nil, worlds.ErrHookNotFound
		}

	case worlds.EventTypeHookDisposed:
		entry, err := tx.hook(ctx, worlds.HookID(requested.CorrelationID))
		if err != nil {
			return nil, err
		}
		// A disposed hook is gone, so disposing it again finds no hook, as
		// in the Workflow SDK's Worlds.
		if entry.hook == nil || entry.hook.RunID != run.view.RunID {
			return nil, worlds.ErrHookNotFound
		}

	case worlds.EventTypeHookCreated:
		// A hook is created once.
		if err := tx.refuseRepeat(ctx, run, requested); err != nil {
			return nil, err
		}
		var created worlds.DataHookCreated
		if json.Unmarshal(requested.Data, &created) == nil && created.Token != "" {
			token, err := tx.token(ctx, created.Token)
			if err != nil {
				return nil, err
			}
			if err := tx.releaseLapsed(ctx, token); err != nil {
				return nil, err
			}
			if token.cur != nil {
				return nil, worlds.ErrHookConflict
			}
		}
	}
	return nil, nil
}

// refuseRepeat refuses requested with ErrEventConflict when the run's log,
// as of the slot the transaction conditions its write on, holds an event of
// its type and correlation ID, or the transaction appended one.
func (tx *appendTx) refuseRepeat(ctx context.Context, run *runEntry, requested worlds.EventsCreateEvent) error {
	logged, err := tx.logged(ctx, run, requested.EventType, requested.CorrelationID)
	if err != nil || !logged {
		return err
	}
	return fmt.Errorf("%w: %s %s already recorded", worlds.ErrEventConflict, requested.EventType, requested.CorrelationID)
}

// logged reports whether the run's log, as of the slot the transaction
// conditions its write on, holds an eventType event of correlationID, or the
// transaction appended one.
func (tx *appendTx) logged(ctx context.Context, run *runEntry, eventType worlds.EventType, correlationID string) (bool, error) {
	for _, event := range run.events {
		if event.EventType == eventType && event.CorrelationID == correlationID {
			return true, nil
		}
	}
	pagination := worlds.Pagination{}
	for {
		page, err := tx.world.eventsPage(ctx, run.view.RunID, correlationID, pagination, worlds.EventsResolveDataNone)
		if err != nil {
			return false, err
		}
		for _, event := range page.Data {
			if event.EventType == eventType {
				return true, nil
			}
		}
		if !page.HasMore {
			return false, nil
		}
		pagination.Cursor = page.Cursor
	}
}

// append assigns the run's next slot to the event, created at createdAt, and
// materializes it. Retention is measured from the World's clock.
func (tx *appendTx) append(ctx context.Context, run *runEntry, requested worlds.EventsCreateEvent, createdAt time.Time) (worlds.Event, error) {
	run.view.eventCount++
	event := worlds.Event{
		RunID:         run.view.RunID,
		EventID:       worlds.EventIDForSlot(uint64(run.view.eventCount)),
		EventType:     requested.EventType,
		CorrelationID: requested.CorrelationID,
		Data:          bytes.Clone(requested.Data),
		CreatedAt:     createdAt,
		SpecVersion:   worlds.EventSpecVersion(requested.SpecVersion),
	}
	now := time.Now().UTC()
	if worlds.RunDataExpired(run.view.Run, now) {
		// A step still running when its run's data was deleted settles after
		// it. What it brought is deleted as it lands.
		event.Data = worlds.ScrubEventData(event.EventType, event.Data)
	}
	run.events = append(run.events, event)
	if err := tx.materialize(ctx, run, event); err != nil {
		return event, err
	}
	if finishes(event.EventType) && run.view.ExpiredAt == nil {
		// The run's data expires with the commit that finishes it under
		// $retention "0", or at its workflow's retention after it.
		if worlds.ReadRunRetention(run.view.Attributes) == worlds.RetainNone {
			run.view.ExpiredAt = &now
		} else if retention := worlds.RetentionFor(tx.world.config.Retention, tx.world.config.Retentions, run.view.WorkflowName); retention > 0 {
			expires := now.Add(retention)
			run.view.ExpiredAt = &expires
		}
	}
	return event, nil
}

func (tx *appendTx) materialize(ctx context.Context, run *runEntry, event worlds.Event) error {
	now := event.CreatedAt
	view := &run.view
	view.UpdatedAt = now
	switch event.EventType {
	case worlds.EventTypeRunStarted:
		view.Status, view.StartedAt, view.startedEventID = worlds.StatusRunning, &now, event.EventID
	case worlds.EventTypeRunCompleted:
		view.Status, view.CompletedAt, view.resultEventID = worlds.StatusCompleted, &now, event.EventID
	case worlds.EventTypeRunFailed:
		view.Status, view.CompletedAt, view.resultEventID = worlds.StatusFailed, &now, event.EventID
		view.ErrorCode = worlds.RunFailedErrorCode(event.Data)
	case worlds.EventTypeRunCancelled:
		if view.Status != worlds.StatusCancelled {
			view.Status, view.CompletedAt, view.cancelledEventID = worlds.StatusCancelled, &now, event.EventID
		}
	case worlds.EventTypeAttrSet:
		var data worlds.DataAttrSet
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
		var data worlds.DataStepCreated
		_ = json.Unmarshal(event.Data, &data)
		entry, err := tx.step(ctx, run.view.RunID, event.CorrelationID)
		if err != nil {
			return err
		}
		// The input is kept in memory only for this append's result.
		entry.view = &stepView{Step: worlds.Step{RunID: run.view.RunID, StepID: worlds.StepID(event.CorrelationID), StepName: data.StepName, Status: worlds.StatusPending, Input: data.Input, SpecVersion: event.SpecVersion, CreatedAt: now, UpdatedAt: now}, createdEventID: event.EventID}
		entry.dirty = true
	case worlds.EventTypeStepStarted:
		if worlds.StartCreatesStep(event.Data) {
			// An inline step's first step_started creates it and holds its
			// input, so the view names it as the step's creation.
			var data worlds.DataStepStarted
			_ = json.Unmarshal(event.Data, &data)
			entry, err := tx.step(ctx, run.view.RunID, event.CorrelationID)
			if err != nil {
				return err
			}
			entry.view = &stepView{Step: worlds.Step{RunID: run.view.RunID, StepID: worlds.StepID(event.CorrelationID), StepName: data.StepName, Status: worlds.StatusPending, SpecVersion: event.SpecVersion, CreatedAt: now, UpdatedAt: now}, createdEventID: event.EventID}
			entry.dirty = true
		}
		return tx.updateStep(ctx, event, worlds.StatusRunning)
	case worlds.EventTypeStepCompleted:
		return tx.updateStep(ctx, event, worlds.StatusCompleted)
	case worlds.EventTypeStepFailed:
		return tx.updateStep(ctx, event, worlds.StatusFailed)
	case worlds.EventTypeStepRetrying:
		return tx.updateStep(ctx, event, worlds.StatusPending)
	case worlds.EventTypeHookCreated:
		var data worlds.DataHookCreated
		_ = json.Unmarshal(event.Data, &data)
		id := worlds.HookID(event.CorrelationID)
		// The metadata is kept in memory only for this append's result.
		tx.hooks[id] = &hookEntry{hook: &worlds.Hook{RunID: run.view.RunID, ResumeContext: worlds.HookResumeContext{DeploymentID: run.view.DeploymentID, WorkflowName: run.view.WorkflowName, EncryptionPublicKey: run.view.EncryptionPublicKey, RunSpecVersion: run.view.SpecVersion}, HookID: id, Token: data.Token, Metadata: data.Metadata, IsWebhook: data.IsWebhook, TokenRetentionUntil: data.TokenRetentionUntil, ClaimedFrom: data.ForceClaimedFrom, SpecVersion: event.SpecVersion, CreatedAt: now}, dirty: true, createdEventID: event.EventID}
		if data.Token != "" {
			token, err := tx.token(ctx, data.Token)
			if err != nil {
				return err
			}
			token.cur = &tokenView{hookID: id, runID: run.view.RunID}
		}
	case worlds.EventTypeHookDisposed:
		entry, err := tx.hook(ctx, worlds.HookID(event.CorrelationID))
		if err != nil || entry.hook == nil {
			return err
		}
		var disposed worlds.HookDisposedEventData
		_ = json.Unmarshal(event.Data, &disposed)
		token, owned, err := tx.owns(ctx, entry.hook)
		if err != nil {
			return err
		}
		if owned {
			token.cur = nil
		}
		// The hook is deleted, leaving the record of its disposal.
		entry.dirty, entry.hook = true, nil
		entry.disposal = &hookDisposal{runID: event.RunID, eventID: event.EventID, forced: disposed.ForceClaimedBy != nil}
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
		if step.StartedAt == nil {
			step.StartedAt = &event.CreatedAt
		}
	case worlds.StatusCompleted, worlds.StatusFailed:
		step.CompletedAt = &event.CreatedAt
	}
	if event.EventType != worlds.EventTypeStepStarted {
		step.resultEventID = event.EventID
	}
	var result worlds.DataStepResult
	if json.Unmarshal(event.Data, &result) == nil {
		step.RetryAfter = result.RetryAfter
	}
	return nil
}

func (tx *appendTx) result(event worlds.Event, created bool) worlds.EventResult {
	result := worlds.EventResult{EventID: event.EventID, EventType: event.EventType, CreatedAt: event.CreatedAt, Data: bytes.Clone(event.Data)}
	if created {
		result.AllocatedEventCount = 1
	}
	if event.EventType == worlds.EventTypeHookCreated {
		if entry := tx.hooks[worlds.HookID(event.CorrelationID)]; entry != nil {
			value := *entry.hook
			result.Hook = &value
		}
	}
	if event.EventType == worlds.EventTypeStepCreated {
		if entry := tx.steps[itemID(stepKey(string(event.RunID), event.CorrelationID))]; entry != nil && entry.view != nil {
			step := entry.view.Step
			step.Input = bytes.Clone(step.Input)
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
		if run.seenStatus.Terminal() {
			// Only the commit finishing a run sets its expiredAt. A later
			// append leaves it, so it cannot write back a deadline a purge
			// has since moved to now.
			delete(set, "expiredAt")
		}
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
	for id, entry := range tx.hooks {
		switch {
		case !entry.dirty:
		case entry.disposal != nil:
			items = append(items, ddb.TransactWriteItem{Put: &ddb.Put{TableName: table, Item: disposalItem(id, *entry.disposal)}})
		case entry.hook != nil:
			item, err := hookItem(*entry.hook)
			if err != nil {
				return err
			}
			setString(item, "createdEventId", string(entry.createdEventID))
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
	for _, entry := range tx.resumes {
		if entry.created {
			items = append(items, ddb.TransactWriteItem{Put: &ddb.Put{TableName: table, Item: merge(resumeKey(string(entry.runID), entry.resumeID), map[string]ddb.AttributeValue{"digest": avs(entry.digest), "eventId": avs(string(entry.eventID))}), ConditionExpression: awsv2.String("attribute_not_exists(pk)")}})
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

// startHookItems returns the writes creating a run's start hook with the run:
// its hook item, naming run_created as its created event, and its token item,
// conditioned on the token being as read. It returns a *HookConflictError
// naming the run holding the token.
func (w *World) startHookItems(ctx context.Context, input worlds.EventsCreateRunInput, now time.Time) ([]ddb.TransactWriteItem, error) {
	start := input.StartHook
	if start.Token == "" {
		return nil, worlds.ErrEventInvalid
	}
	tx := newAppendTx(w)
	tx.now = now
	token, err := tx.token(ctx, start.Token)
	if err != nil {
		return nil, err
	}
	if err := tx.releaseLapsed(ctx, token); err != nil {
		return nil, err
	}
	if token.cur != nil {
		if token.cur.runID == input.RunID {
			// A retried create of a run that already holds its token.
			return nil, worlds.ErrRunAlreadyExists
		}
		return nil, &worlds.HookConflictError{RunID: token.cur.runID}
	}
	id := worlds.StartHookID(input.RunID)
	hook, err := hookItem(worlds.Hook{RunID: input.RunID, HookID: id, Token: start.Token, TokenRetentionUntil: start.TokenRetentionUntil, ResumeContext: worlds.HookResumeContext{DeploymentID: input.DeploymentID, WorkflowName: input.WorkflowName, EncryptionPublicKey: input.EncryptionPublicKey, RunSpecVersion: worlds.SpecVersion}, SpecVersion: worlds.SpecVersion, CreatedAt: now})
	if err != nil {
		return nil, err
	}
	hook["createdEventId"] = avs(string(worlds.EventIDForSlot(1)))
	condition, values := tokenCondition(token.seen)
	table := awsv2.String(w.config.TableName)
	return []ddb.TransactWriteItem{
		put(w.config.TableName, hook),
		{Put: &ddb.Put{TableName: table, Item: tokenItem(start.Token, tokenView{hookID: id, runID: input.RunID}), ConditionExpression: awsv2.String(condition), ExpressionAttributeValues: values}},
	}, nil
}
