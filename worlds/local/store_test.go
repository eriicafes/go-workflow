package localworld

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

// storageWorld returns a prepared World that delivers nothing.
func storageWorld(t *testing.T, config Config) *World {
	t.Helper()
	return queueWorld(t, config, func(context.Context, worlds.QueueInput) error { return nil })
}

// startedRun creates a run and starts it.
func startedRun(t *testing.T, world *World) worlds.RunID {
	t.Helper()
	runID := worlds.NewRunID()
	if _, err := world.EventsCreateRun(context.Background(), worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "storage"}); err != nil {
		t.Fatal(err)
	}
	appendEvent(t, world, runID, worlds.EventTypeRunStarted, "", nil)
	return runID
}

func appendEvent(t *testing.T, world *World, runID worlds.RunID, eventType worlds.EventType, correlationID string, data []byte) worlds.EventResult {
	t.Helper()
	result, err := world.EventsCreate(context.Background(), worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: eventType, CorrelationID: correlationID, Data: data}})
	if err != nil {
		t.Fatalf("%s %s: %v", eventType, correlationID, err)
	}
	return result
}

// claimToken appends a hook_created for token to the run.
func claimToken(world *World, runID worlds.RunID, hookID, token string, created worlds.DataHookCreated, options worlds.EventsCreateOptions) (worlds.EventResult, error) {
	created.Token = token
	data, _ := json.Marshal(created)
	return world.EventsCreate(context.Background(), worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeHookCreated, CorrelationID: hookID, Data: data}, Options: options})
}

// scalar returns the single value query selects from the World's database.
func scalar[T any](t *testing.T, world *World, query string, args ...any) T {
	t.Helper()
	var value T
	if err := world.db.read.QueryRowContext(context.Background(), query, args...).Scan(&value); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return value
}

// TestWaitRowsAreKeyedByCorrelationID checks a wait's row is keyed by its
// correlation ID, while its doc holds the wait ID that also names the run.
func TestWaitRowsAreKeyedByCorrelationID(t *testing.T) {
	t.Parallel()
	world := storageWorld(t, Config{})
	runID := startedRun(t, world)
	resumeAt := time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano)
	appendEvent(t, world, runID, worlds.EventTypeWaitCreated, "wait_a", []byte(`{"resumeAt":"`+resumeAt+`"}`))
	if key := scalar[string](t, world, "SELECT wait_id FROM waits WHERE run_id = ?", string(runID)); key != "wait_a" {
		t.Fatalf("wait_id = %q, want the correlation ID", key)
	}
	doc := scalar[string](t, world, "SELECT doc FROM waits WHERE run_id = ?", string(runID))
	if !strings.Contains(doc, `"`+string(runID)+`-wait_a"`) {
		t.Fatalf("wait doc = %s, want its wait ID", doc)
	}
	// Completing the wait updates the same row.
	appendEvent(t, world, runID, worlds.EventTypeWaitCompleted, "wait_a", nil)
	if count := scalar[int](t, world, "SELECT count(*) FROM waits WHERE run_id = ?", string(runID)); count != 1 {
		t.Fatalf("wait rows = %d", count)
	}
}

// TestEventPagesEndAtTheByteCap checks a page of a run's events ends early,
// with more to read, once it holds the cap of stored bytes, and always holds
// one event.
func TestEventPagesEndAtTheByteCap(t *testing.T) {
	ctx := context.Background()
	world := storageWorld(t, Config{})
	runID := startedRun(t, world)
	step := func(id string, size int) {
		data, _ := json.Marshal(worlds.DataStepCreated{StepName: "sized", Input: []byte(strings.Repeat("x", size))})
		appendEvent(t, world, runID, worlds.EventTypeStepCreated, id, data)
	}
	for _, id := range []string{"step_a", "step_b", "step_c", "step_d", "step_e"} {
		step(id, 200)
	}
	step("step_large", 2000)
	appendEvent(t, world, runID, worlds.EventTypeStepStarted, "step_large", nil)

	previous := maxEventPageBytes
	maxEventPageBytes = 600
	defer func() { maxEventPageBytes = previous }()

	for _, order := range []worlds.SortOrder{worlds.SortOrderAscending, worlds.SortOrderDescending} {
		var slots []int
		var sizes []int
		cursor := ""
		for {
			page, err := world.EventsList(ctx, worlds.EventsListInput{RunID: runID, Pagination: worlds.Pagination{Cursor: cursor, SortOrder: order}})
			if err != nil {
				t.Fatal(err)
			}
			if len(page.Data) == 0 {
				t.Fatalf("%s: empty page after %v", order, slots)
			}
			for _, event := range page.Data {
				if event.CorrelationID == "step_large" && event.EventType == worlds.EventTypeStepCreated && len(page.Data) != 1 {
					t.Fatalf("%s: the event over the cap shares a page of %d", order, len(page.Data))
				}
				slots = append(slots, eventSlot(event))
			}
			sizes = append(sizes, len(page.Data))
			if !page.HasMore {
				break
			}
			cursor = page.Cursor
		}
		want := []int{1, 2, 3, 4, 5, 6, 7, 8, 9}
		if order == worlds.SortOrderDescending {
			want = []int{9, 8, 7, 6, 5, 4, 3, 2, 1}
		}
		if len(slots) != len(want) {
			t.Fatalf("%s: slots = %v", order, slots)
		}
		for index := range want {
			if slots[index] != want[index] {
				t.Fatalf("%s: slots = %v", order, slots)
			}
		}
		if len(sizes) < 4 {
			t.Fatalf("%s: pages of %v, want the cap to split them", order, sizes)
		}
	}
	// A correlation ID's events are capped the same way.
	maxEventPageBytes = 1
	page, err := world.EventsListByCorrelationID(ctx, worlds.EventsListByCorrelationIDInput{RunID: runID, CorrelationID: "step_large"})
	if err != nil || len(page.Data) != 1 || !page.HasMore || page.Data[0].EventType != worlds.EventTypeStepCreated {
		t.Fatalf("capped correlated page = %#v err=%v", page, err)
	}
	next, err := world.EventsListByCorrelationID(ctx, worlds.EventsListByCorrelationIDInput{RunID: runID, CorrelationID: "step_large", Pagination: worlds.Pagination{Cursor: page.Cursor}})
	if err != nil || len(next.Data) != 1 || next.HasMore || next.Data[0].EventType != worlds.EventTypeStepStarted {
		t.Fatalf("second correlated page = %#v err=%v", next, err)
	}
	// Under the cap the log is one page.
	maxEventPageBytes = previous
	whole, err := world.EventsList(ctx, worlds.EventsListInput{RunID: runID})
	if err != nil || len(whole.Data) != 9 || whole.HasMore {
		t.Fatalf("uncapped page = %d events more=%v err=%v", len(whole.Data), whole.HasMore, err)
	}
}
