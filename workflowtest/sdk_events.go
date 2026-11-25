package workflowtest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

// sdkRun creates a run of workflowName with input and returns its ID.
func sdkRun(t *testing.T, world worlds.World, workflowName string, input []byte) worlds.RunID {
	t.Helper()
	runID := worlds.NewRunID()
	if _, err := world.EventsCreateRun(context.Background(), worlds.EventsCreateRunInput{RunID: runID, WorkflowName: workflowName, Input: input}); err != nil {
		t.Fatalf("EventsCreateRun: %v", err)
	}
	return runID
}

// sdkStartedRun creates a run and starts it.
func sdkStartedRun(t *testing.T, world worlds.World) worlds.RunID {
	t.Helper()
	runID := sdkRun(t, world, "sdk", []byte(`[]`))
	conformAppend(t, world, runID, worlds.EventTypeRunStarted, "", nil)
	return runID
}

// sdkStep appends the step_created of stepID with input.
func sdkStep(t *testing.T, world worlds.World, runID worlds.RunID, stepID, name string, input []byte) worlds.EventResult {
	t.Helper()
	return conformAppend(t, world, runID, worlds.EventTypeStepCreated, stepID, worlds.DataStepCreated{StepName: name, Input: input})
}

// sdkTry appends an event and returns the World's answer.
func sdkTry(world worlds.World, runID worlds.RunID, eventType worlds.EventType, correlationID string, data any) (worlds.EventResult, error) {
	event := worlds.EventsCreateEvent{EventType: eventType, CorrelationID: correlationID}
	switch value := data.(type) {
	case nil:
	case []byte:
		event.Data = value
	default:
		encoded, err := json.Marshal(value)
		if err != nil {
			return worlds.EventResult{}, err
		}
		event.Data = encoded
	}
	return world.EventsCreate(context.Background(), worlds.EventsCreateInput{RunID: runID, Event: event})
}

// sdkTypes returns the types of events, in order.
func sdkTypes(events []worlds.Event) []worlds.EventType {
	types := make([]worlds.EventType, len(events))
	for index, event := range events {
		types[index] = event.EventType
	}
	return types
}

// sdkIDs returns the IDs of events, in order.
func sdkIDs(events []worlds.Event) []worlds.EventID {
	ids := make([]worlds.EventID, len(events))
	for index, event := range events {
		ids[index] = event.EventID
	}
	return ids
}

// sdkSlots returns the log positions of events, in order.
func sdkSlots(t *testing.T, events []worlds.Event) []uint64 {
	t.Helper()
	slots := make([]uint64, len(events))
	for index, event := range events {
		slot, err := worlds.EventSlot(event.EventID)
		if err != nil {
			t.Fatal(err)
		}
		slots[index] = slot
	}
	return slots
}

// sdkDense returns the positions 1 to n.
func sdkDense(n int) []uint64 {
	slots := make([]uint64, n)
	for index := range slots {
		slots[index] = uint64(index + 1)
	}
	return slots
}

// sdkPayloads returns every payload field the run's events still hold.
func sdkPayloads(t *testing.T, world worlds.World, runID worlds.RunID) []string {
	t.Helper()
	var payloads []string
	for _, event := range conformAllEvents(t, world, runID) {
		var data map[string]json.RawMessage
		if len(event.Data) == 0 || json.Unmarshal(event.Data, &data) != nil {
			continue
		}
		for _, field := range []string{"input", "output", "result", "error", "metadata", "payload"} {
			if value, ok := data[field]; ok && string(value) != "null" {
				payloads = append(payloads, fmt.Sprintf("%s %s=%s", event.EventType, field, value))
			}
		}
	}
	return payloads
}

// sdkCount returns how many of the run's events have eventType.
func sdkCount(t *testing.T, world worlds.World, runID worlds.RunID, eventType worlds.EventType) int {
	t.Helper()
	count := 0
	for _, event := range conformAllEvents(t, world, runID) {
		if event.EventType == eventType {
			count++
		}
	}
	return count
}

// sdkListedAfter returns the run's events after the event at cursor's
// position, in log order.
func sdkListedAfter(t *testing.T, world worlds.World, runID worlds.RunID, cursor worlds.EventID) []worlds.Event {
	t.Helper()
	slot, err := worlds.EventSlot(cursor)
	if err != nil {
		t.Fatal(err)
	}
	events := conformAllEvents(t, world, runID)
	return events[slot:]
}

// sdkLastID returns the ID of the run's last event.
func sdkLastID(t *testing.T, world worlds.World, runID worlds.RunID) worlds.EventID {
	t.Helper()
	events := conformAllEvents(t, world, runID)
	return events[len(events)-1].EventID
}

// SDK: packages/world-local/src/storage.test.ts > Storage > events > create > should create a new event
// SDK: packages/world-local/src/storage.test.ts > Storage > events > create > should handle run completed events
// SDK: packages/world-local/src/storage.test.ts > Storage > events > create > skips the run_started preload when requested
func conformSDKEventsCreate(t *testing.T, world worlds.World) {
	t.Run("an event", func(t *testing.T) {
		runID := sdkRun(t, world, "sdk", []byte(`[]`))
		sdkStep(t, world, runID, "corr_123", "test-step", []byte(`[]`))
		result := conformAppend(t, world, runID, worlds.EventTypeStepStarted, "corr_123", nil)
		event, err := world.EventsGet(context.Background(), worlds.EventsGetInput{RunID: runID, EventID: result.EventID})
		if err != nil || event.RunID != runID || event.EventType != worlds.EventTypeStepStarted || event.CorrelationID != "corr_123" || event.CreatedAt.IsZero() || result.CreatedAt.IsZero() {
			t.Fatalf("step_started = %+v, event %+v err = %v", result, event, err)
		}
		if _, err := worlds.EventSlot(result.EventID); err != nil {
			t.Fatalf("event ID %q: %v", result.EventID, err)
		}
	})
	t.Run("run_completed", func(t *testing.T) {
		runID := sdkRun(t, world, "sdk", []byte(`[]`))
		result := conformAppend(t, world, runID, worlds.EventTypeRunCompleted, "", worlds.DataRunCompleted{Output: []byte(`2`)})
		event, err := world.EventsGet(context.Background(), worlds.EventsGetInput{RunID: runID, EventID: result.EventID})
		if err != nil || result.EventType != worlds.EventTypeRunCompleted || event.CorrelationID != "" {
			t.Fatalf("run_completed = %+v, event %+v err = %v", result, event, err)
		}
	})
	t.Run("run_started without a preload", func(t *testing.T) {
		// A write returns the run's events only when asked with EventCount
		// or SinceCursor, so a run_started that asks for neither has none,
		// a repeat included.
		runID := sdkRun(t, world, "sdk", []byte(`[]`))
		started := conformAppend(t, world, runID, worlds.EventTypeRunStarted, "", nil)
		if len(started.Events) != 0 {
			t.Fatalf("run_started events = %+v, want none", started.Events)
		}
		retried, err := sdkTry(world, runID, worlds.EventTypeRunStarted, "", nil)
		if err == nil && len(retried.Events) != 0 {
			t.Fatalf("repeated run_started events = %+v, want none", retried.Events)
		}
		conformStartedAgain(t, retried, err)
	})
}

// SDK: packages/world-local/src/storage.test.ts > Storage > events > inline delta (sinceCursor) > returns the events written since the cursor on step_completed, matching events.list
// SDK: packages/world-local/src/storage.test.ts > Storage > events > inline delta (sinceCursor) > captures in-band events (hook_received) interleaved before the terminal write
// SDK: packages/world-local/src/storage.test.ts > Storage > events > inline delta (sinceCursor) > does not return a delta when sinceCursor is omitted
// SDK: packages/world-local/src/storage.test.ts > Storage > events > inline delta (sinceCursor) > returns a delta for non-terminal event types too
// SDK: packages/world-local/src/storage.test.ts > Storage > events > inline delta (sinceCursor) > returns a delta for a wait_completed write
// SDK: packages/world-local/src/storage.test.ts > Storage > events > inline delta (sinceCursor) > returns a delta for a create that committed hook_conflict
// SDK: packages/world-local/src/storage.test.ts > Storage > events > inline delta (sinceCursor) > does not return a delta on a hook_conflict when sinceCursor is omitted
//
// A Go World's SinceCursor delta holds the events after the cursor that
// were in the log before the write; the write's own event is the result
// itself. Together they are what a listing after the cursor returns.
func conformSDKSinceCursorDeltas(t *testing.T, world worlds.World) {
	ctx := context.Background()
	since := func(t *testing.T, runID worlds.RunID, cursor worlds.EventID, eventType worlds.EventType, correlationID string, data any) worlds.EventResult {
		t.Helper()
		event := conformEvent(t, eventType, correlationID, data)
		result, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: event, Options: worlds.EventsCreateOptions{SinceCursor: string(cursor)}})
		if err != nil {
			t.Fatalf("%s with SinceCursor: %v", eventType, err)
		}
		return result
	}
	// matches checks the delta and the written event are what a listing
	// after the cursor returns.
	matches := func(t *testing.T, runID worlds.RunID, cursor worlds.EventID, result worlds.EventResult) {
		t.Helper()
		listed := sdkListedAfter(t, world, runID, cursor)
		want := sdkIDs(listed)
		got := append(sdkIDs(result.Events), result.EventID)
		if !slices.Equal(got, want) || result.MoreEvents {
			t.Fatalf("delta %v and event %s, more %v; listed after the cursor %v", sdkIDs(result.Events), result.EventID, result.MoreEvents, want)
		}
	}
	t.Run("step_completed", func(t *testing.T) {
		runID := sdkStartedRun(t, world)
		cursor := sdkLastID(t, world, runID)
		sdkStep(t, world, runID, "corr_seq", "seq-step", []byte(`[]`))
		conformAppend(t, world, runID, worlds.EventTypeStepStarted, "corr_seq", worlds.DataStepStarted{StepName: "seq-step"})
		result := since(t, runID, cursor, worlds.EventTypeStepCompleted, "corr_seq", worlds.DataStepResult{StepName: "seq-step", Result: []byte(`1`)})
		if types := append(sdkTypes(result.Events), result.EventType); !slices.Equal(types, []worlds.EventType{worlds.EventTypeStepCreated, worlds.EventTypeStepStarted, worlds.EventTypeStepCompleted}) {
			t.Fatalf("delta types = %v", types)
		}
		matches(t, runID, cursor, result)
	})
	t.Run("an interleaved hook_received", func(t *testing.T) {
		runID := sdkStartedRun(t, world)
		token := "tok_inband:" + string(runID)
		conformAppend(t, world, runID, worlds.EventTypeHookCreated, "corr_hook", worlds.DataHookCreated{Token: token})
		cursor := sdkLastID(t, world, runID)
		sdkStep(t, world, runID, "corr_seq2", "seq-step", []byte(`[]`))
		conformAppend(t, world, runID, worlds.EventTypeStepStarted, "corr_seq2", worlds.DataStepStarted{StepName: "seq-step"})
		conformAppend(t, world, runID, worlds.EventTypeHookReceived, "corr_hook", worlds.DataHookReceived{Token: token, Payload: []byte(`9`)})
		result := since(t, runID, cursor, worlds.EventTypeStepCompleted, "corr_seq2", worlds.DataStepResult{StepName: "seq-step", Result: []byte(`1`)})
		if !slices.Contains(sdkTypes(result.Events), worlds.EventTypeHookReceived) {
			t.Fatalf("delta types = %v, want the hook_received", sdkTypes(result.Events))
		}
		matches(t, runID, cursor, result)
	})
	t.Run("no SinceCursor", func(t *testing.T) {
		runID := sdkStartedRun(t, world)
		sdkStep(t, world, runID, "corr_seq3", "seq-step", []byte(`[]`))
		conformAppend(t, world, runID, worlds.EventTypeStepStarted, "corr_seq3", worlds.DataStepStarted{StepName: "seq-step"})
		result := conformAppend(t, world, runID, worlds.EventTypeStepCompleted, "corr_seq3", worlds.DataStepResult{StepName: "seq-step", Result: []byte(`1`)})
		if len(result.Events) != 0 || result.MoreEvents {
			t.Fatalf("delta without SinceCursor = %+v", result.Events)
		}
	})
	t.Run("step_started", func(t *testing.T) {
		runID := sdkStartedRun(t, world)
		cursor := sdkLastID(t, world, runID)
		sdkStep(t, world, runID, "corr_seq4", "seq-step", []byte(`[]`))
		result := since(t, runID, cursor, worlds.EventTypeStepStarted, "corr_seq4", worlds.DataStepStarted{StepName: "seq-step"})
		if result.EventType != worlds.EventTypeStepStarted {
			t.Fatalf("written event = %s", result.EventType)
		}
		matches(t, runID, cursor, result)
	})
	t.Run("wait_completed", func(t *testing.T) {
		runID := sdkStartedRun(t, world)
		past := time.Now().Add(-time.Second).UTC()
		conformAppend(t, world, runID, worlds.EventTypeWaitCreated, "corr_wait1", worlds.DataWaitCreated{ResumeAt: past})
		cursor := sdkLastID(t, world, runID)
		result := since(t, runID, cursor, worlds.EventTypeWaitCompleted, "corr_wait1", worlds.DataWaitCompleted{ResumeAt: &past})
		if result.EventType != worlds.EventTypeWaitCompleted || result.Wait == nil || result.Wait.Status != worlds.WaitStatusCompleted || result.MoreEvents {
			t.Fatalf("wait_completed = %+v", result)
		}
		matches(t, runID, cursor, result)
	})
	t.Run("hook_conflict", func(t *testing.T) {
		runID := sdkStartedRun(t, world)
		token := "delta-conflict-token:" + string(runID)
		conformAppend(t, world, runID, worlds.EventTypeHookCreated, "corr_hook_owner", worlds.DataHookCreated{Token: token})
		cursor := sdkLastID(t, world, runID)
		result := since(t, runID, cursor, worlds.EventTypeHookCreated, "corr_hook_loser", worlds.DataHookCreated{Token: token})
		if result.EventType != worlds.EventTypeHookConflict {
			t.Fatalf("written event = %s, want hook_conflict", result.EventType)
		}
		matches(t, runID, cursor, result)
	})
	t.Run("hook_conflict without SinceCursor", func(t *testing.T) {
		runID := sdkStartedRun(t, world)
		token := "no-delta-conflict-token:" + string(runID)
		conformAppend(t, world, runID, worlds.EventTypeHookCreated, "corr_hook_owner2", worlds.DataHookCreated{Token: token})
		result := conformAppend(t, world, runID, worlds.EventTypeHookCreated, "corr_hook_loser2", worlds.DataHookCreated{Token: token})
		if result.EventType != worlds.EventTypeHookConflict || len(result.Events) != 0 {
			t.Fatalf("conflict = %+v", result)
		}
	})
}

// SDK: packages/world-local/src/storage.test.ts > Storage > events > list > should list all events for a run
// SDK: packages/world-local/src/storage.test.ts > Storage > events > list > should list events in descending order when explicitly requested (newest first)
// SDK: packages/world-local/src/storage.test.ts > Storage > events > list > should support pagination
// SDK: packages/world-local/src/storage.test.ts > Storage > events > list > returns all remaining events when no limit is set
// SDK: packages/world-local/src/storage.test.ts > Storage > runs > maxEvents > lists no ceiling for a run it does not hold
func conformSDKEventsList(t *testing.T, world worlds.World) {
	ctx := context.Background()
	setup := func(t *testing.T) (worlds.RunID, worlds.EventResult, worlds.EventResult) {
		runID := sdkRun(t, world, "sdk", []byte(`[]`))
		started := conformAppend(t, world, runID, worlds.EventTypeRunStarted, "", nil)
		time.Sleep(2 * time.Millisecond)
		sdkStep(t, world, runID, "corr_step_1", "test-step", []byte(`[]`))
		time.Sleep(2 * time.Millisecond)
		stepStarted := conformAppend(t, world, runID, worlds.EventTypeStepStarted, "corr_step_1", nil)
		return runID, started, stepStarted
	}
	t.Run("ascending", func(t *testing.T) {
		runID, started, stepStarted := setup(t)
		page, err := world.EventsList(ctx, worlds.EventsListInput{RunID: runID, Pagination: worlds.Pagination{SortOrder: worlds.SortOrderAscending}})
		if err != nil || len(page.Data) != 4 {
			t.Fatalf("events = %+v err = %v", page.Data, err)
		}
		if page.Data[0].EventType != worlds.EventTypeRunCreated || page.Data[1].EventID != started.EventID || page.Data[2].EventType != worlds.EventTypeStepCreated || page.Data[3].EventID != stepStarted.EventID || page.Data[3].CreatedAt.Before(page.Data[2].CreatedAt) {
			t.Fatalf("ascending events = %+v", page.Data)
		}
	})
	t.Run("descending", func(t *testing.T) {
		runID, started, stepStarted := setup(t)
		page, err := world.EventsList(ctx, worlds.EventsListInput{RunID: runID, Pagination: worlds.Pagination{SortOrder: worlds.SortOrderDescending}})
		if err != nil || len(page.Data) != 4 {
			t.Fatalf("events = %+v err = %v", page.Data, err)
		}
		if page.Data[0].EventID != stepStarted.EventID || page.Data[1].EventType != worlds.EventTypeStepCreated || page.Data[2].EventID != started.EventID || page.Data[3].EventType != worlds.EventTypeRunCreated || page.Data[0].CreatedAt.Before(page.Data[1].CreatedAt) {
			t.Fatalf("descending events = %+v", page.Data)
		}
	})
	t.Run("pagination", func(t *testing.T) {
		runID := sdkRun(t, world, "sdk", []byte(`[]`))
		for i := range 5 {
			id := fmt.Sprintf("corr_%d", i)
			sdkStep(t, world, runID, id, fmt.Sprintf("step-%d", i), []byte(`[]`))
			conformAppend(t, world, runID, worlds.EventTypeStepCompleted, id, worlds.DataStepResult{Result: []byte(fmt.Sprint(i))})
		}
		first, err := world.EventsList(ctx, worlds.EventsListInput{RunID: runID, Pagination: worlds.Pagination{Limit: 2}})
		if err != nil || len(first.Data) != 2 || first.Cursor == "" {
			t.Fatalf("first page = %+v err = %v", first, err)
		}
		second, err := world.EventsList(ctx, worlds.EventsListInput{RunID: runID, Pagination: worlds.Pagination{Limit: 2, Cursor: first.Cursor}})
		if err != nil || len(second.Data) != 2 || second.Data[0].EventID == first.Data[0].EventID {
			t.Fatalf("second page = %+v err = %v", second, err)
		}
	})
	t.Run("no limit", func(t *testing.T) {
		runID := sdkRun(t, world, "sdk", []byte(`[]`))
		conformAppend(t, world, runID, worlds.EventTypeRunStarted, "", nil)
		page, err := world.EventsList(ctx, worlds.EventsListInput{RunID: runID, Pagination: worlds.Pagination{SortOrder: worlds.SortOrderAscending}})
		if err != nil || len(page.Data) != 2 || page.HasMore {
			t.Fatalf("events = %+v err = %v", page, err)
		}
	})
	t.Run("no ceiling for a missing run", func(t *testing.T) {
		page, err := world.EventsList(ctx, worlds.EventsListInput{RunID: worlds.NewRunID()})
		if err != nil || page.MaxEvents != 0 {
			t.Fatalf("missing run's page = %+v err = %v, want no MaxEvents", page, err)
		}
	})
}

// SDK: packages/world-local/src/storage.test.ts > Storage > events > listByCorrelationId > should list all events with a specific correlation ID
// SDK: packages/world-local/src/storage.test.ts > Storage > events > listByCorrelationId > returns only the named run when two runs share a correlation ID
// SDK: packages/world-local/src/storage.test.ts > Storage > events > listByCorrelationId > should return empty list for non-existent correlation ID
// SDK: packages/world-local/src/storage.test.ts > Storage > events > listByCorrelationId > should respect pagination parameters
// SDK: packages/world-local/src/storage.test.ts > Storage > events > listByCorrelationId > should filter event data when resolveData is "none"
// SDK: packages/world-local/src/storage.test.ts > Storage > events > listByCorrelationId > should return events in ascending order by default
// SDK: packages/world-local/src/storage.test.ts > Storage > events > listByCorrelationId > should support descending order
// SDK: packages/world-local/src/storage.test.ts > Storage > events > listByCorrelationId > should handle hook lifecycle events
func conformSDKEventsByCorrelation(t *testing.T, world worlds.World) {
	ctx := context.Background()
	list := func(t *testing.T, runID worlds.RunID, correlationID string, pagination worlds.Pagination, resolve worlds.EventsResolveData) worlds.Page[worlds.Event] {
		t.Helper()
		page, err := world.EventsListByCorrelationID(ctx, worlds.EventsListByCorrelationIDInput{RunID: runID, CorrelationID: correlationID, Pagination: pagination, ResolveData: resolve})
		if err != nil {
			t.Fatal(err)
		}
		return page
	}
	t.Run("one correlation ID", func(t *testing.T) {
		runID := sdkRun(t, world, "sdk", []byte(`[]`))
		id := "step-abc123"
		sdkStep(t, world, runID, id, "test-step", []byte(`[]`))
		sdkStep(t, world, runID, "different-step", "different-step", []byte(`[]`))
		started := conformAppend(t, world, runID, worlds.EventTypeStepStarted, id, nil)
		time.Sleep(2 * time.Millisecond)
		completed := conformAppend(t, world, runID, worlds.EventTypeStepCompleted, id, worlds.DataStepResult{Result: []byte(`1`)})
		conformAppend(t, world, runID, worlds.EventTypeStepStarted, "different-step", nil)
		conformAppend(t, world, runID, worlds.EventTypeRunCompleted, "", worlds.DataRunCompleted{Output: []byte(`2`)})
		page := list(t, runID, id, worlds.Pagination{}, "")
		if len(page.Data) != 3 || page.Data[0].EventType != worlds.EventTypeStepCreated || page.Data[1].EventID != started.EventID || page.Data[2].EventID != completed.EventID {
			t.Fatalf("correlated events = %+v", page.Data)
		}
		for _, event := range page.Data {
			if event.CorrelationID != id {
				t.Fatalf("correlated event = %+v", event)
			}
		}
	})
	t.Run("two runs", func(t *testing.T) {
		runID := sdkRun(t, world, "sdk", []byte(`[]`))
		other := sdkRun(t, world, "sdk-2", []byte(`[]`))
		id := "hook-xyz789"
		created := conformAppend(t, world, runID, worlds.EventTypeHookCreated, id, worlds.DataHookCreated{Token: "test-token-" + id + string(runID)})
		time.Sleep(2 * time.Millisecond)
		waited := conformAppend(t, world, other, worlds.EventTypeWaitCreated, id, worlds.DataWaitCreated{ResumeAt: time.Now().UTC()})
		time.Sleep(2 * time.Millisecond)
		disposed := conformAppend(t, world, runID, worlds.EventTypeHookDisposed, id, nil)
		page := list(t, runID, id, worlds.Pagination{}, "")
		if !slices.Equal(sdkIDs(page.Data), []worlds.EventID{created.EventID, disposed.EventID}) {
			t.Fatalf("events of the run = %+v", page.Data)
		}
		for _, event := range page.Data {
			if event.RunID != runID {
				t.Fatalf("event of another run = %+v", event)
			}
		}
		if theirs := list(t, other, id, worlds.Pagination{}, ""); !slices.Equal(sdkIDs(theirs.Data), []worlds.EventID{waited.EventID}) {
			t.Fatalf("events of the other run = %+v", theirs.Data)
		}
	})
	t.Run("missing correlation ID", func(t *testing.T) {
		runID := sdkRun(t, world, "sdk", []byte(`[]`))
		sdkStep(t, world, runID, "existing-step", "existing-step", []byte(`[]`))
		conformAppend(t, world, runID, worlds.EventTypeStepStarted, "existing-step", nil)
		page := list(t, runID, "non-existent-correlation-id", worlds.Pagination{}, "")
		if len(page.Data) != 0 || page.HasMore || page.Cursor != "" {
			t.Fatalf("events of a missing correlation ID = %+v", page)
		}
	})
	t.Run("pagination", func(t *testing.T) {
		runID := sdkRun(t, world, "sdk", []byte(`[]`))
		id := "step-paginated"
		sdkStep(t, world, runID, id, "test-step", []byte(`[]`))
		conformAppend(t, world, runID, worlds.EventTypeStepStarted, id, nil)
		time.Sleep(2 * time.Millisecond)
		conformAppend(t, world, runID, worlds.EventTypeStepRetrying, id, worlds.DataStepResult{Error: []byte(`"retry error"`)})
		time.Sleep(2 * time.Millisecond)
		conformAppend(t, world, runID, worlds.EventTypeStepStarted, id, nil)
		time.Sleep(2 * time.Millisecond)
		conformAppend(t, world, runID, worlds.EventTypeStepCompleted, id, worlds.DataStepResult{Result: []byte(`1`)})
		first := list(t, runID, id, worlds.Pagination{Limit: 2}, "")
		if len(first.Data) != 2 || !first.HasMore || first.Cursor == "" {
			t.Fatalf("first page = %+v", first)
		}
		second := list(t, runID, id, worlds.Pagination{Limit: 3, Cursor: first.Cursor}, "")
		if len(second.Data) != 3 || second.HasMore {
			t.Fatalf("second page = %+v", second)
		}
	})
	t.Run("without data", func(t *testing.T) {
		runID := sdkRun(t, world, "sdk", []byte(`[]`))
		id := "step-with-data"
		conformAppend(t, world, runID, worlds.EventTypeStepCreated, id, []byte(`{"stepName":"test-step","input":"W10="}`))
		conformAppend(t, world, runID, worlds.EventTypeStepCompleted, id, []byte(`{"result":"MQ=="}`))
		page := list(t, runID, id, worlds.Pagination{}, worlds.EventsResolveDataNone)
		if len(page.Data) != 2 || page.Data[0].CorrelationID != id {
			t.Fatalf("events = %+v", page.Data)
		}
		// A Go World records a step_created's inline flag, metadata it keeps.
		var created map[string]any
		if err := json.Unmarshal(page.Data[0].Data, &created); err == nil && created["inline"] == false {
			delete(created, "inline")
		}
		if len(created) != 1 || created["stepName"] != "test-step" {
			t.Fatalf("step_created data without payloads = %s", page.Data[0].Data)
		}
		if page.Data[1].Data != nil {
			t.Fatalf("step_completed data without payloads = %s, want none", page.Data[1].Data)
		}
	})
	ordered := func(t *testing.T) (worlds.RunID, string, worlds.EventResult, worlds.EventResult) {
		runID := sdkRun(t, world, "sdk", []byte(`[]`))
		id := "step-ordering"
		sdkStep(t, world, runID, id, "test-step", []byte(`[]`))
		time.Sleep(2 * time.Millisecond)
		started := conformAppend(t, world, runID, worlds.EventTypeStepStarted, id, nil)
		time.Sleep(2 * time.Millisecond)
		completed := conformAppend(t, world, runID, worlds.EventTypeStepCompleted, id, worlds.DataStepResult{Result: []byte(`1`)})
		return runID, id, started, completed
	}
	t.Run("ascending by default", func(t *testing.T) {
		runID, id, started, completed := ordered(t)
		page := list(t, runID, id, worlds.Pagination{}, "")
		if len(page.Data) != 3 || page.Data[0].EventType != worlds.EventTypeStepCreated || page.Data[1].EventID != started.EventID || page.Data[2].EventID != completed.EventID || page.Data[1].CreatedAt.Before(page.Data[0].CreatedAt) {
			t.Fatalf("default order = %+v", page.Data)
		}
	})
	t.Run("descending", func(t *testing.T) {
		runID, id, started, completed := ordered(t)
		page := list(t, runID, id, worlds.Pagination{SortOrder: worlds.SortOrderDescending}, "")
		if len(page.Data) != 3 || page.Data[0].EventID != completed.EventID || page.Data[1].EventID != started.EventID || page.Data[2].EventType != worlds.EventTypeStepCreated || page.Data[0].CreatedAt.Before(page.Data[1].CreatedAt) {
			t.Fatalf("descending order = %+v", page.Data)
		}
	})
	t.Run("hook lifecycle", func(t *testing.T) {
		runID := sdkRun(t, world, "sdk", []byte(`[]`))
		id := "hook_test123"
		created := conformAppend(t, world, runID, worlds.EventTypeHookCreated, id, worlds.DataHookCreated{Token: "test-token-" + string(runID)})
		time.Sleep(2 * time.Millisecond)
		first := conformAppend(t, world, runID, worlds.EventTypeHookReceived, id, worlds.DataHookReceived{Payload: []byte(`1`)})
		time.Sleep(2 * time.Millisecond)
		second := conformAppend(t, world, runID, worlds.EventTypeHookReceived, id, worlds.DataHookReceived{Payload: []byte(`2`)})
		time.Sleep(2 * time.Millisecond)
		disposed := conformAppend(t, world, runID, worlds.EventTypeHookDisposed, id, nil)
		page := list(t, runID, id, worlds.Pagination{}, "")
		want := []worlds.EventID{created.EventID, first.EventID, second.EventID, disposed.EventID}
		types := []worlds.EventType{worlds.EventTypeHookCreated, worlds.EventTypeHookReceived, worlds.EventTypeHookReceived, worlds.EventTypeHookDisposed}
		if !slices.Equal(sdkIDs(page.Data), want) || !slices.Equal(sdkTypes(page.Data), types) {
			t.Fatalf("hook events = %+v", page.Data)
		}
	})
}

// SDK: packages/world-local/src/storage/slot-identity.test.ts > slot event ids > numbers a new run densely from the first slot, in log order
// SDK: packages/world-local/src/storage/slot-identity.test.ts > slot event ids > stays dense when writers race for the same slot
// SDK: packages/world-local/src/storage/slot-identity.test.ts > slot event ids > leaves no hole behind writes that are rejected
// SDK: packages/world-local/src/storage/slot-identity.test.ts > slot event ids > leaves no hole when a rejected write is overtaken by another
// SDK: packages/world-local/src/storage/slot-identity.test.ts > slot event ids > orders a terminal event after every event it raced
func conformSDKSlotIDs(t *testing.T, world worlds.World) {
	create := func(runID worlds.RunID, id, name string) (worlds.EventResult, error) {
		return sdkTry(world, runID, worlds.EventTypeStepCreated, id, worlds.DataStepCreated{StepName: name, Input: []byte(`[]`)})
	}
	t.Run("dense", func(t *testing.T) {
		runID := sdkStartedRun(t, world)
		for i := range 5 {
			if _, err := create(runID, fmt.Sprintf("step_%d", i), fmt.Sprintf("step%d", i)); err != nil {
				t.Fatal(err)
			}
		}
		events := conformAllEvents(t, world, runID)
		if slots := sdkSlots(t, events); !slices.Equal(slots, sdkDense(7)) {
			t.Fatalf("slots = %v", slots)
		}
	})
	t.Run("racing writers", func(t *testing.T) {
		runID := sdkStartedRun(t, world)
		const width = 20
		errs := make([]error, width)
		var group sync.WaitGroup
		for i := range width {
			group.Go(func() { _, errs[i] = create(runID, fmt.Sprintf("step_%d", i), fmt.Sprintf("step%d", i)) })
		}
		group.Wait()
		if err := errors.Join(errs...); err != nil {
			t.Fatal(err)
		}
		if slots := sdkSlots(t, conformAllEvents(t, world, runID)); !slices.Equal(slots, sdkDense(width+2)) {
			t.Fatalf("slots = %v", slots)
		}
	})
	t.Run("rejected writes", func(t *testing.T) {
		runID := sdkStartedRun(t, world)
		const width = 20
		errs := make([]error, width)
		var group sync.WaitGroup
		for i := range width {
			group.Go(func() { _, errs[i] = create(runID, "step_contended", "contended") })
		}
		group.Wait()
		accepted := 0
		for _, err := range errs {
			if err == nil {
				accepted++
			}
		}
		if accepted != 1 {
			t.Fatalf("accepted = %d of %d: %v", accepted, width, errors.Join(errs...))
		}
		if _, err := create(runID, "step_after", "after"); err != nil {
			t.Fatal(err)
		}
		if slots := sdkSlots(t, conformAllEvents(t, world, runID)); !slices.Equal(slots, sdkDense(4)) {
			t.Fatalf("slots = %v", slots)
		}
	})
	t.Run("rejected writes overtaken", func(t *testing.T) {
		runID := sdkStartedRun(t, world)
		const width = 8
		start := func(i int) (worlds.EventResult, error) {
			return sdkTry(world, runID, worlds.EventTypeStepStarted, fmt.Sprintf("step_%d", i), worlds.DataStepStarted{StepName: fmt.Sprintf("step%d", i), Attempt: 1, Input: []byte(`[]`), Inline: true})
		}
		for i := range width {
			if _, err := start(i); err != nil {
				t.Fatal(err)
			}
		}
		errs := make([]error, width)
		var group sync.WaitGroup
		for i := range width {
			group.Go(func() { _, errs[i] = start(i) })
		}
		group.Wait()
		for i, err := range errs {
			if err == nil {
				t.Fatalf("repeated creating step_started %d was accepted", i)
			}
		}
		if _, err := create(runID, "step_after", "after"); err != nil {
			t.Fatal(err)
		}
		if slots := sdkSlots(t, conformAllEvents(t, world, runID)); !slices.Equal(slots, sdkDense(width+3)) {
			t.Fatalf("slots = %v", slots)
		}
	})
	t.Run("terminal event last", func(t *testing.T) {
		runID := sdkStartedRun(t, world)
		if _, err := create(runID, "step_a", "a"); err != nil {
			t.Fatal(err)
		}
		conformAppend(t, world, runID, worlds.EventTypeRunCompleted, "", worlds.DataRunCompleted{Output: []byte(`"done"`)})
		events := conformAllEvents(t, world, runID)
		if last := events[len(events)-1]; last.EventID != worlds.EventIDForSlot(uint64(len(events))) || last.EventType != worlds.EventTypeRunCompleted {
			t.Fatalf("last event = %+v of %d", last, len(events))
		}
	})
}

// SDK: packages/world-local/src/storage/slot-identity.test.ts > skipped-slot report > hands back the events occupying the slots the write skipped
// SDK: packages/world-local/src/storage/slot-identity.test.ts > skipped-slot report > reports nothing when the write lands on the slot it asked for
// SDK: packages/world-local/src/storage/slot-identity.test.ts > skipped-slot report > reports nothing when the writer sends no count
// SDK: packages/world-local/src/storage/slot-identity.test.ts > skipped-slot report > lets the sinceCursor delta answer when the writer asks for both
// SDK: packages/world-local/src/storage/slot-identity.test.ts > skipped-slot report > gives every racing writer the events it was decided without
func conformSDKSkippedSlotReport(t *testing.T, world worlds.World) {
	ctx := context.Background()
	fill := func(t *testing.T, runID worlds.RunID, count int) []worlds.EventID {
		t.Helper()
		ids := make([]worlds.EventID, count)
		for i := range count {
			ids[i] = sdkStep(t, world, runID, fmt.Sprintf("filler_%d", i), fmt.Sprintf("filler%d", i), []byte(`[]`)).EventID
		}
		return ids
	}
	wait := func(t *testing.T, runID worlds.RunID, options worlds.EventsCreateOptions) worlds.EventResult {
		t.Helper()
		result, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: conformEvent(t, worlds.EventTypeWaitCreated, "wait_a", worlds.DataWaitCreated{ResumeAt: time.Unix(0, 0).UTC()}), Options: options})
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	stale := 2
	t.Run("skipped slots", func(t *testing.T) {
		runID := sdkStartedRun(t, world)
		filled := fill(t, runID, 3)
		result := wait(t, runID, worlds.EventsCreateOptions{EventCount: &stale})
		if result.EventID != worlds.EventIDForSlot(uint64(stale+len(filled)+1)) || !slices.Equal(sdkIDs(result.Events), filled) || result.MoreEvents {
			t.Fatalf("report = %+v, want %v", result, filled)
		}
	})
	t.Run("no skipped slot", func(t *testing.T) {
		runID := sdkStartedRun(t, world)
		result := wait(t, runID, worlds.EventsCreateOptions{EventCount: &stale})
		if result.EventID != worlds.EventIDForSlot(3) || len(result.Events) != 0 {
			t.Fatalf("report = %+v", result)
		}
	})
	t.Run("no count", func(t *testing.T) {
		runID := sdkStartedRun(t, world)
		fill(t, runID, 2)
		if result := wait(t, runID, worlds.EventsCreateOptions{}); len(result.Events) != 0 {
			t.Fatalf("report = %+v", result.Events)
		}
	})
	t.Run("count and cursor", func(t *testing.T) {
		// EventCount takes precedence over SinceCursor. Here both name the
		// same position, so the answer is every event after it, the write's
		// own being the result.
		runID := sdkStartedRun(t, world)
		filled := fill(t, runID, 3)
		result := wait(t, runID, worlds.EventsCreateOptions{EventCount: &stale, SinceCursor: string(worlds.EventIDForSlot(uint64(stale)))})
		if result.EventID != worlds.EventIDForSlot(uint64(stale+len(filled)+1)) || !slices.Equal(sdkIDs(result.Events), filled) || result.MoreEvents {
			t.Fatalf("report = %+v, want %v", result, filled)
		}
	})
	t.Run("racing writers", func(t *testing.T) {
		runID := sdkStartedRun(t, world)
		const width = 8
		results := make([]worlds.EventResult, width)
		errs := make([]error, width)
		var group sync.WaitGroup
		for i := range width {
			group.Go(func() {
				event := conformEvent(t, worlds.EventTypeStepCreated, fmt.Sprintf("racer_%d", i), worlds.DataStepCreated{StepName: fmt.Sprintf("racer%d", i), Input: []byte(`[]`)})
				results[i], errs[i] = world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: event, Options: worlds.EventsCreateOptions{EventCount: &stale}})
			})
		}
		group.Wait()
		if err := errors.Join(errs...); err != nil {
			t.Fatal(err)
		}
		for _, result := range results {
			landed, err := worlds.EventSlot(result.EventID)
			if err != nil {
				t.Fatal(err)
			}
			span := int(landed) - stale - 1
			if len(result.Events) > span || result.MoreEvents != (len(result.Events) < span) {
				t.Fatalf("writer at %d reported %d of %d, more %v", landed, len(result.Events), span, result.MoreEvents)
			}
			for _, slot := range sdkSlots(t, result.Events) {
				if slot <= uint64(stale) || slot >= landed {
					t.Fatalf("writer at %d reported slot %d", landed, slot)
				}
			}
		}
	})
}

// SDK: packages/world-local/src/storage/in-band-fence.test.ts > in-band fence (world-local) > does not advance the count when the World refuses an in-band write with a 4xx
// SDK: packages/world-local/src/storage/in-band-fence.test.ts > in-band fence (world-local) > does not advance the count when an in-band write fails: nothing was written
// SDK: packages/world-local/src/storage/in-band-fence.test.ts > in-band fence (world-local) > keeps the counters on every page of a paginated load
// SDK: packages/world-local/src/storage/in-band-fence.test.ts > in-band fence (world-local) > allocates nothing for an in-band hook_received that converges on its committed resume
func conformSDKInBandFence(t *testing.T, world worlds.World) {
	ctx := context.Background()
	slots := func(t *testing.T, runID worlds.RunID) (worlds.EventsSnapshot, []uint64) {
		t.Helper()
		return conformSnapshot(t, world, runID), sdkSlots(t, conformAllEvents(t, world, runID))
	}
	inBand := func(runID worlds.RunID, event worlds.EventsCreateEvent, count int, options worlds.EventsCreateOptions) (worlds.EventResult, error) {
		options.InBandCount = &count
		return world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: event, Options: options})
	}
	t.Run("refused write", func(t *testing.T) {
		runID := sdkRun(t, world, "fenceWorkflow", []byte(`[]`))
		created := conformEvent(t, worlds.EventTypeStepCreated, "step_1", worlds.DataStepCreated{StepName: "add", Input: []byte(`[1]`)})
		if _, err := inBand(runID, created, 1, worlds.EventsCreateOptions{}); err != nil {
			t.Fatal(err)
		}
		snapshot, before := slots(t, runID)
		_, err := inBand(runID, created, snapshot.InBandCount, worlds.EventsCreateOptions{})
		conformExpect(t, err, worlds.ErrEntityConflict, "repeated in-band step_created")
		if is[*worlds.InBandSupersededError](err) {
			t.Fatalf("repeated in-band step_created = %v, want no fence refusal", err)
		}
		if after, now := slots(t, runID); after != snapshot || !slices.Equal(now, before) {
			t.Fatalf("after the refusal: %+v %v, want %+v %v", after, now, snapshot, before)
		}
	})
	t.Run("failed write", func(t *testing.T) {
		runID := sdkRun(t, world, "fenceWorkflow", []byte(`[]`))
		snapshot, before := slots(t, runID)
		_, err := inBand(runID, conformEvent(t, worlds.EventTypeStepStarted, "step_missing", worlds.DataStepStarted{StepName: "add", Attempt: 1}), snapshot.InBandCount, worlds.EventsCreateOptions{})
		if err == nil {
			t.Fatal("step_started of a missing step was accepted")
		}
		if after, now := slots(t, runID); after.InBandCount != snapshot.InBandCount || !slices.Equal(now, before) {
			t.Fatalf("after the failure: %+v %v, want %+v %v", after, now, snapshot, before)
		}
	})
	t.Run("every page", func(t *testing.T) {
		runID := sdkRun(t, world, "fenceWorkflow", []byte(`[]`))
		for i := range 3 {
			value := fmt.Sprint(i)
			conformAppend(t, world, runID, worlds.EventTypeAttrSet, "", worlds.DataAttrSet{Changes: []worlds.RunsAttributeChange{{Key: "k", Value: &value}}, Writer: worlds.AttributeWriter{Type: worlds.AttributeWriterWorkflow}})
		}
		first, err := world.EventsList(ctx, worlds.EventsListInput{RunID: runID, Pagination: worlds.Pagination{Limit: 2}})
		if err != nil {
			t.Fatal(err)
		}
		second, err := world.EventsList(ctx, worlds.EventsListInput{RunID: runID, Pagination: worlds.Pagination{Limit: 2, Cursor: first.Cursor}})
		if err != nil || first.Snapshot != second.Snapshot || first.Snapshot.EventCount != 4 {
			t.Fatalf("snapshots %+v and %+v err = %v, want equal with 4 events", first.Snapshot, second.Snapshot, err)
		}
	})
	t.Run("converging resume", func(t *testing.T) {
		runID := sdkRun(t, world, "fenceWorkflow", []byte(`[]`))
		if _, err := inBand(runID, worlds.EventsCreateEvent{EventType: worlds.EventTypeRunStarted}, 1, worlds.EventsCreateOptions{}); err != nil {
			t.Fatal(err)
		}
		token := "tok-" + string(runID)
		if _, err := inBand(runID, conformEvent(t, worlds.EventTypeHookCreated, "hook_1", worlds.DataHookCreated{Token: token}), 2, worlds.EventsCreateOptions{}); err != nil {
			t.Fatal(err)
		}
		sdkRequireResumeDedup(t, world, token)
		payload := []byte(`{"n":1}`)
		data, _ := worlds.EncodeHookReceived(payload, "fenceWorkflow")
		resume := worlds.EventsCreateEvent{EventType: worlds.EventTypeHookReceived, CorrelationID: "hook_1", Data: data}
		dedup := worlds.EventsCreateOptions{ResumeID: "resume_1", ResumePayloadDigest: worlds.ResumeDigest(payload)}
		first, err := inBand(runID, resume, 3, dedup)
		if err != nil || first.AllocatedEventCount != 1 {
			t.Fatalf("resume = %+v err = %v", first, err)
		}
		replay, err := inBand(runID, resume, 4, dedup)
		if err != nil || replay.EventID != first.EventID || replay.AllocatedEventCount != 0 {
			t.Fatalf("replayed resume = %+v err = %v, want the first resume's event", replay, err)
		}
		if snapshot := conformSnapshot(t, world, runID); snapshot != (worlds.EventsSnapshot{EventCount: 4, InBandCount: 4}) {
			t.Fatalf("snapshot = %+v, want 4 events, 4 in-band", snapshot)
		}
	})
}

// conformSDKEventData checks events a World takes without Data, a
// step_started and a run_cancelled, and that it keeps a run_cancelled's
// reason.
//
// SDK: packages/world/src/events.test.ts > step_started ownerMessageId > accepts a bare step_started with no eventData (legacy contract)
// SDK: packages/world/src/events.test.ts > run_cancelled cancelReason > retains cancelReason when reading back a stored run_cancelled event (not stripped)
func conformSDKEventData(t *testing.T, world worlds.World) {
	runID := conformCreateRun(t, world, "sdk_bare")
	conformAppend(t, world, runID, worlds.EventTypeRunStarted, "", nil)
	conformAppend(t, world, runID, worlds.EventTypeStepCreated, "step_bare", worlds.DataStepCreated{StepName: "bare", Input: []byte(`"input"`)})
	conformAppend(t, world, runID, worlds.EventTypeStepStarted, "step_bare", nil)
	conformAppend(t, world, runID, worlds.EventTypeRunCancelled, "", nil)

	reasoned := conformCreateRun(t, world, "sdk_cancel_reason")
	data, err := worlds.EncodeRunCancelled("operator cancelled")
	if err != nil {
		t.Fatal(err)
	}
	conformAppend(t, world, reasoned, worlds.EventTypeRunCancelled, "", data)
	events := conformAllEvents(t, world, reasoned)
	if last := events[len(events)-1]; last.EventType != worlds.EventTypeRunCancelled || worlds.RunCancelReason(last.Data) != "operator cancelled" {
		t.Fatalf("run_cancelled = %s %s, want its cancel reason", last.EventType, last.Data)
	}
}
