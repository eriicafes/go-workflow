package localworld

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

// SDK: packages/world-local/src/storage.test.ts > Storage > path traversal prevention (VULN-916) > rejects traversal runId on events.create: ${JSON.stringify(runId)}
// SDK: packages/world-local/src/storage.test.ts > Storage > path traversal prevention (VULN-916) > rejects traversal runId on runs.get: ${JSON.stringify(runId)}
// SDK: packages/world-local/src/storage.test.ts > Storage > path traversal prevention (VULN-916) > rejects traversal runId on steps.list: ${JSON.stringify(runId)}
// SDK: packages/world-local/src/storage.test.ts > Storage > path traversal prevention (VULN-916) > rejects traversal runId on events.list: ${JSON.stringify(runId)}
// SDK: packages/world-local/src/storage.test.ts > Storage > path traversal prevention (VULN-916) > rejects traversal stepId on steps.get
// SDK: packages/world-local/src/storage.test.ts > Storage > path traversal prevention (VULN-916) > rejects traversal correlationId on events.create
// SDK: packages/world-local/src/storage.test.ts > Storage > path traversal prevention (VULN-916) > rejects empty correlationId on step_created
// SDK: packages/world-local/src/storage.test.ts > Storage > path traversal prevention (VULN-916) > rejects empty correlationId on hook_created
// SDK: packages/world-local/src/storage.test.ts > Storage > path traversal prevention (VULN-916) > rejects empty correlationId on wait_created
// SDK: packages/world-local/src/storage.test.ts > Storage > path traversal prevention (VULN-916) > rejects traversal hookId on hooks.get
func TestSDKTraversalIDsAreRefused(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	world := storageWorld(t, Config{})
	refused := func(t *testing.T, label string, err error) {
		t.Helper()
		if !errors.Is(err, worlds.ErrEventInvalid) || !strings.Contains(err.Error(), "unsafe") {
			t.Errorf("%s = %v, want an unsafe ID refused as ErrEventInvalid", label, err)
		}
	}
	for _, id := range []worlds.RunID{"../../../package", "../runs/wrun_01ARZ3NDEKTSV4RRFFQ69G5FAV", "../nonexistent/wrun_01ARZ3NDEKTSV4RRFFQ69G5FAV", "a/b", `a\b`, ".hidden", ".locks"} {
		_, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: id, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeRunStarted}})
		refused(t, fmt.Sprintf("run_started of %q", id), err)
		_, err = world.RunsGet(ctx, worlds.RunsGetInput{RunID: id})
		refused(t, fmt.Sprintf("RunsGet %q", id), err)
		_, err = world.StepsList(ctx, worlds.StepsListInput{RunID: id})
		refused(t, fmt.Sprintf("StepsList %q", id), err)
		_, err = world.EventsList(ctx, worlds.EventsListInput{RunID: id})
		refused(t, fmt.Sprintf("EventsList %q", id), err)
	}
	runID := startedRun(t, world)
	_, err := world.StepsGet(ctx, worlds.StepsGetInput{RunID: runID, StepID: "../escape"})
	refused(t, "StepsGet ../escape", err)
	_, err = world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeStepStarted, CorrelationID: "../escape"}})
	refused(t, "step_started of ../escape", err)
	resumeAt, _ := json.Marshal(worlds.DataWaitCreated{ResumeAt: time.Now().Add(time.Second)})
	for eventType, data := range map[worlds.EventType][]byte{
		worlds.EventTypeStepCreated: []byte(`{"stepName":"step","input":""}`),
		worlds.EventTypeHookCreated: []byte(`{"token":"tok"}`),
		worlds.EventTypeWaitCreated: resumeAt,
	} {
		_, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: eventType, Data: data}})
		refused(t, string(eventType)+" without a correlation ID", err)
	}
	_, err = world.HooksGet(ctx, worlds.HooksGetInput{HookID: "../escape"})
	refused(t, "HooksGet ../escape", err)
}

// sdkLargeSteps appends count step_created events whose input is size bytes.
func sdkLargeSteps(t *testing.T, world *World, runID worlds.RunID, count, size int) {
	t.Helper()
	input := []byte(strings.Repeat("1", size))
	for i := range count {
		data, _ := json.Marshal(worlds.DataStepCreated{StepName: "add", Input: input})
		appendEvent(t, world, runID, worlds.EventTypeStepCreated, fmt.Sprintf("step_%d", i), data)
	}
}

// SDK: packages/world-local/src/storage/events-page-bytes.test.ts > ends a page of large events early, and the cursor reaches every event
func TestSDKLargeEventPagesEndEarly(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	world := storageWorld(t, Config{})
	runID := worlds.NewRunID()
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "wf", DeploymentID: "dep-1"}); err != nil {
		t.Fatal(err)
	}
	sdkLargeSteps(t, world, runID, 300, 64<<10)
	var pages []int
	var seen []worlds.EventID
	cursor := ""
	for {
		page, err := world.EventsList(ctx, worlds.EventsListInput{RunID: runID, Pagination: worlds.Pagination{Limit: 1000, Cursor: cursor}})
		if err != nil {
			t.Fatal(err)
		}
		pages = append(pages, len(page.Data))
		for _, event := range page.Data {
			seen = append(seen, event.EventID)
		}
		if !page.HasMore {
			break
		}
		cursor = page.Cursor
	}
	unique := slices.Clone(seen)
	slices.Sort(unique)
	if len(seen) != 301 || !slices.Equal(unique, seen) || len(slices.Compact(unique)) != 301 {
		t.Fatalf("events = %d, in order and once: %v", len(seen), slices.Equal(unique, seen))
	}
	if len(pages) <= 2 || slices.Max(pages) >= 150 {
		t.Fatalf("pages = %v, want several, none of 150 events", pages)
	}
}

// SDK: packages/world-local/src/storage/events-page-bytes.test.ts > returns small events in one page, as before
func TestSDKSmallEventsListInOnePage(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	world := storageWorld(t, Config{})
	runID := worlds.NewRunID()
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "wf", DeploymentID: "dep-1"}); err != nil {
		t.Fatal(err)
	}
	sdkLargeSteps(t, world, runID, 200, 8)
	page, err := world.EventsList(ctx, worlds.EventsListInput{RunID: runID, Pagination: worlds.Pagination{Limit: 1000}})
	if err != nil || len(page.Data) != 201 || page.HasMore {
		t.Fatalf("page of %d events, more %v, err = %v", len(page.Data), page.HasMore, err)
	}
}
