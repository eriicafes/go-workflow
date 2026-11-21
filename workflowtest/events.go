package workflowtest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

func conformEventDeltas(t *testing.T, world worlds.World) {
	ctx := context.Background()
	runID := conformCreateRun(t, world, "delta")
	zero := 0
	started, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeRunStarted}, Options: worlds.EventsCreateOptions{EventCount: &zero}})
	if err != nil {
		t.Fatal(err)
	}
	if started.EventID != worlds.EventIDForSlot(2) || started.AllocatedEventCount != 1 || len(started.Events) != 1 || started.Events[0].EventType != worlds.EventTypeRunCreated {
		t.Fatalf("started = %#v", started)
	}
	plain := conformAppend(t, world, runID, worlds.EventTypeWaitCreated, "wait_a", nil)
	if plain.EventID != worlds.EventIDForSlot(3) || len(plain.Events) != 0 {
		t.Fatalf("delta without options = %#v", plain)
	}
	// Each result carries the appended event's stored creation time.
	listed, err := world.EventsList(ctx, worlds.EventsListInput{RunID: runID})
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range []worlds.EventResult{started, plain} {
		slot, _ := worlds.EventSlot(result.EventID)
		if result.CreatedAt.IsZero() || int(slot) > len(listed.Data) || !result.CreatedAt.Equal(listed.Data[slot-1].CreatedAt) {
			t.Fatalf("result %s CreatedAt = %v, want the stored event's", result.EventID, result.CreatedAt)
		}
	}
	since, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeWaitCreated, CorrelationID: "wait_b"}, Options: worlds.EventsCreateOptions{SinceCursor: string(worlds.EventIDForSlot(1))}})
	if err != nil {
		t.Fatal(err)
	}
	if len(since.Events) != 2 || since.Events[0].EventID != worlds.EventIDForSlot(2) || since.Events[1].EventID != worlds.EventIDForSlot(3) {
		t.Fatalf("SinceCursor delta = %#v", since.Events)
	}
	two := 2
	none, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeWaitCreated, CorrelationID: "wait_c", Data: []byte(`{"x":1}`)}, Options: worlds.EventsCreateOptions{EventCount: &two, ResolveData: worlds.EventsResolveDataNone}})
	if err != nil {
		t.Fatal(err)
	}
	if len(none.Events) != 2 || none.Events[0].EventID != worlds.EventIDForSlot(3) || none.Events[0].Data != nil {
		t.Fatalf("EventCount delta with ResolveDataNone = %#v", none.Events)
	}
	large := 100
	beyond, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeWaitCreated, CorrelationID: "wait_d"}, Options: worlds.EventsCreateOptions{EventCount: &large}})
	if err != nil || len(beyond.Events) != 0 {
		t.Fatalf("EventCount beyond log = %#v err=%v", beyond, err)
	}

	_, err = world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeRunCreated}})
	conformExpect(t, err, worlds.ErrEventInvalid, "run_created append")
	_, err = world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: "bogus"}})
	conformExpect(t, err, worlds.ErrEventInvalid, "unknown event type")
	_, err = world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: "wrun_missing", Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeRunStarted}})
	conformExpect(t, err, worlds.ErrRunNotFound, "missing run")
}

// conformRunDeployment checks that a run keeps the deployment it was created
// with, which the Runtime uses to pin the run to a deployment.
func conformRunDeployment(t *testing.T, world worlds.World) {
	ctx := context.Background()
	runID := worlds.RunID("wrun_conformance_deployment")
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "conformance", Input: []byte(`"input"`), DeploymentID: "dpl_conformance"}); err != nil {
		t.Fatal(err)
	}
	for _, resolve := range []worlds.ResolveData{worlds.ResolveDataAll, worlds.ResolveDataNone} {
		run, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: runID, ResolveData: resolve})
		if err != nil || run.DeploymentID != "dpl_conformance" {
			t.Fatalf("run deployment with ResolveData %q = %q err=%v", resolve, run.DeploymentID, err)
		}
	}
	page, err := world.RunsList(ctx, worlds.RunsListInput{WorkflowName: "conformance"})
	if err != nil {
		t.Fatal(err)
	}
	for _, run := range page.Data {
		if run.RunID == runID && run.DeploymentID != "dpl_conformance" {
			t.Fatalf("listed run deployment = %q", run.DeploymentID)
		}
	}
	unpinned := conformCreateRun(t, world, "unpinned")
	run, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: unpinned})
	if err != nil || run.DeploymentID != "" {
		t.Fatalf("unpinned run deployment = %q err=%v", run.DeploymentID, err)
	}
}

// conformInBandFence checks the in-band writer fence: run_created counts as
// one in-band position, an in-band write is accepted only at the run's
// current InBandCount and advances it by the positions it allocates, a stale
// one appends nothing, and an out-of-band write is never refused and leaves
// the count alone.
func conformInBandFence(t *testing.T, world worlds.World) {
	ctx := context.Background()
	runID := conformCreateRun(t, world, "fence")
	inBandCount := func() int {
		t.Helper()
		run, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: runID})
		if err != nil {
			t.Fatal(err)
		}
		snapshot := conformSnapshot(t, world, runID)
		if snapshot.InBandCount != run.InBandCount {
			t.Fatalf("snapshot InBandCount %d, run InBandCount %d", snapshot.InBandCount, run.InBandCount)
		}
		return run.InBandCount
	}
	eventCount := func() int {
		t.Helper()
		return conformSnapshot(t, world, runID).EventCount
	}
	if count := inBandCount(); count != 1 {
		t.Fatalf("new run InBandCount = %d, want 1", count)
	}

	zero, one, two, four := 0, 1, 2, 4
	first, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeWaitCreated, CorrelationID: "wait_fence_a"}, Options: worlds.EventsCreateOptions{InBandCount: &one}})
	if err != nil || first.InBandCount != 2 || first.AllocatedEventCount != 1 || inBandCount() != 2 {
		t.Fatalf("fenced append = %#v err=%v, run InBandCount %d", first, err, inBandCount())
	}

	before := eventCount()
	for _, stale := range []int{zero, one} {
		_, err = world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeWaitCreated, CorrelationID: "wait_fence_stale"}, Options: worlds.EventsCreateOptions{InBandCount: &stale}})
		conformExpect(t, err, worlds.ErrInBandSuperseded, "stale in-band append")
	}
	if eventCount() != before || inBandCount() != 2 {
		t.Fatalf("stale in-band append wrote events %d -> %d, InBandCount %d", before, eventCount(), inBandCount())
	}

	if _, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeWaitCreated, CorrelationID: "wait_fence_out"}}); err != nil || inBandCount() != 2 || eventCount() != before+1 {
		t.Fatalf("out-of-band append err=%v, InBandCount %d, events %d", err, inBandCount(), eventCount())
	}

	batch, err := world.EventsCreateBatch(ctx, worlds.EventsCreateBatchInput{RunID: runID, InBandCount: &two, Events: []worlds.EventsCreateEvent{
		{EventType: worlds.EventTypeWaitCreated, CorrelationID: "wait_fence_b"},
		{EventType: worlds.EventTypeWaitCreated, CorrelationID: "wait_fence_c"},
	}})
	if err != nil || batch.InBandCount != 4 || batch.AllocatedEventCount != 2 || inBandCount() != 4 {
		t.Fatalf("fenced batch = %#v err=%v, run InBandCount %d", batch, err, inBandCount())
	}

	before = eventCount()
	_, err = world.EventsCreateBatch(ctx, worlds.EventsCreateBatchInput{RunID: runID, InBandCount: &two, Events: []worlds.EventsCreateEvent{{EventType: worlds.EventTypeWaitCreated, CorrelationID: "wait_fence_d"}}})
	conformExpect(t, err, worlds.ErrInBandSuperseded, "stale in-band batch")
	if eventCount() != before {
		t.Fatalf("stale in-band batch wrote events %d -> %d", before, eventCount())
	}

	token := "fence:" + string(runID)
	_, err = world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: conformEvent(t, worlds.EventTypeHookCreated, "hook_fence", conformHookData(token, false, 0)), Options: worlds.EventsCreateOptions{InBandCount: &two}})
	conformExpect(t, err, worlds.ErrInBandSuperseded, "stale in-band hook claim")
	claimed, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: conformEvent(t, worlds.EventTypeHookCreated, "hook_fence", conformHookData(token, false, 0)), Options: worlds.EventsCreateOptions{InBandCount: &four}})
	if err != nil || claimed.InBandCount != 5 || inBandCount() != 5 {
		t.Fatalf("fenced hook claim = %#v err=%v, run InBandCount %d", claimed, err, inBandCount())
	}
	five := 5
	repeat, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: conformEvent(t, worlds.EventTypeHookDisposed, "hook_fence", nil), Options: worlds.EventsCreateOptions{InBandCount: &five}})
	if err != nil || repeat.AllocatedEventCount != 1 || inBandCount() != 6 {
		t.Fatalf("fenced disposal = %#v err=%v, run InBandCount %d", repeat, err, inBandCount())
	}
	// A repeated disposal finds no hook and allocates nothing, so the count
	// stays.
	six := 6
	_, err = world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: conformEvent(t, worlds.EventTypeHookDisposed, "hook_fence", nil), Options: worlds.EventsCreateOptions{InBandCount: &six}})
	conformExpect(t, err, worlds.ErrHookNotFound, "repeated fenced disposal")
	if inBandCount() != 6 {
		t.Fatalf("repeated fenced disposal moved run InBandCount to %d", inBandCount())
	}
}

// conformSnapshot returns the snapshot of a listing of runID's log, after
// checking that every page of it carries the same one.
func conformSnapshot(t *testing.T, world worlds.World, runID worlds.RunID) worlds.EventsSnapshot {
	t.Helper()
	ctx := context.Background()
	first, err := world.EventsList(ctx, worlds.EventsListInput{RunID: runID})
	if err != nil {
		t.Fatal(err)
	}
	if first.Snapshot.EventCount != len(first.Data) || first.Snapshot.InBandCount < 1 || first.Snapshot.InBandCount > first.Snapshot.EventCount {
		t.Fatalf("snapshot %+v over %d events", first.Snapshot, len(first.Data))
	}
	paged, err := world.EventsList(ctx, worlds.EventsListInput{RunID: runID, Pagination: worlds.Pagination{Limit: 1}})
	for err == nil {
		if paged.Snapshot != first.Snapshot {
			t.Fatalf("page snapshot %+v, want %+v", paged.Snapshot, first.Snapshot)
		}
		if !paged.HasMore {
			break
		}
		paged, err = world.EventsList(ctx, worlds.EventsListInput{RunID: runID, Pagination: worlds.Pagination{Limit: 1, Cursor: paged.Cursor}})
	}
	if err != nil {
		t.Fatal(err)
	}
	return first.Snapshot
}

// conformSkippedSlots checks an in-band write's skipped-slot report: the
// events between the position it was decided from and its own, and that
// in-band writes are stored at the time they occurred.
func conformSkippedSlots(t *testing.T, world worlds.World) {
	ctx := context.Background()
	runID := conformCreateRun(t, world, "skipped")
	if run, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: runID}); err != nil || run.SpecVersion != worlds.SpecVersion {
		t.Fatalf("run SpecVersion = %d err=%v, want %d", run.SpecVersion, err, worlds.SpecVersion)
	}
	held := 1
	// Two out-of-band writes land after the position the writer holds.
	conformAppend(t, world, runID, worlds.EventTypeWaitCreated, "wait_skipped_a", nil)
	conformAppend(t, world, runID, worlds.EventTypeWaitCreated, "wait_skipped_b", nil)
	occurred := time.Now().Add(-time.Hour).Truncate(time.Millisecond).UTC()
	one := 1
	written, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeWaitCreated, CorrelationID: "wait_skipped_c", OccurredAt: occurred}, Options: worlds.EventsCreateOptions{InBandCount: &one, EventCount: &held}})
	if err != nil {
		t.Fatal(err)
	}
	if written.EventID != worlds.EventIDForSlot(4) || written.MoreEvents || len(written.Events) != 2 || written.Events[0].CorrelationID != "wait_skipped_a" || written.Events[1].CorrelationID != "wait_skipped_b" {
		t.Fatalf("skipped-slot report = %#v", written)
	}
	if !written.CreatedAt.Equal(occurred) {
		t.Fatalf("in-band CreatedAt = %v, want its OccurredAt %v", written.CreatedAt, occurred)
	}
	stored, err := world.EventsGet(ctx, worlds.EventsGetInput{RunID: runID, EventID: written.EventID})
	if err != nil || !stored.CreatedAt.Equal(occurred) {
		t.Fatalf("stored in-band CreatedAt = %v err=%v, want %v", stored.CreatedAt, err, occurred)
	}
	// An out-of-band write takes the World's clock.
	out := conformAppend(t, world, runID, worlds.EventTypeWaitCreated, "wait_skipped_d", nil)
	if time.Since(out.CreatedAt) > time.Minute {
		t.Fatalf("out-of-band CreatedAt = %v", out.CreatedAt)
	}

	// A batch decided from position 4 reports the out-of-band write after it,
	// and stores each item at its own OccurredAt.
	four, two := 4, 2
	batch, err := world.EventsCreateBatch(ctx, worlds.EventsCreateBatchInput{RunID: runID, InBandCount: &two, EventCount: &four, Events: []worlds.EventsCreateEvent{
		{EventType: worlds.EventTypeWaitCreated, CorrelationID: "wait_skipped_e", OccurredAt: occurred},
		{EventType: worlds.EventTypeWaitCreated, CorrelationID: "wait_skipped_f", OccurredAt: occurred.Add(time.Millisecond)},
	}})
	if err != nil || len(batch.Results) != 2 || batch.MoreEvents || len(batch.Events) != 1 || batch.Events[0].CorrelationID != "wait_skipped_d" {
		t.Fatalf("batch report = %#v err=%v", batch, err)
	}
	for index, item := range batch.Results {
		if item.Err != nil || !item.CreatedAt.Equal(occurred.Add(time.Duration(index)*time.Millisecond)) {
			t.Fatalf("batch item %d = %#v", index, item)
		}
	}
}

func conformBatch(t *testing.T, world worlds.World) {
	ctx := context.Background()
	runID := conformCreateRun(t, world, "batch")
	empty, err := world.EventsCreateBatch(ctx, worlds.EventsCreateBatchInput{RunID: runID})
	if err != nil || len(empty.Results) != 0 {
		t.Fatalf("empty batch = %#v err=%v", empty, err)
	}
	result, err := world.EventsCreateBatch(ctx, worlds.EventsCreateBatchInput{RunID: runID, Events: []worlds.EventsCreateEvent{
		{EventType: worlds.EventTypeRunStarted},
		conformEvent(t, worlds.EventTypeStepCreated, "step_a", worlds.DataStepCreated{StepName: "a"}),
		conformEvent(t, worlds.EventTypeStepStarted, "step_a", worlds.DataStepStarted{StepName: "a", Attempt: 1}),
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Results) != 3 || result.AllocatedEventCount != 3 || result.Results[0].EventID != worlds.EventIDForSlot(2) || result.Results[2].EventID != worlds.EventIDForSlot(4) || result.Results[2].EventType != worlds.EventTypeStepStarted {
		t.Fatalf("batch result = %#v", result)
	}
	for _, item := range result.Results {
		if item.Err != nil || item.AllocatedEventCount != 1 {
			t.Fatalf("batch item = %#v", item)
		}
	}
	if result.Results[1].Step == nil || !result.Results[1].StepCreated {
		t.Fatalf("batch step_created result = %#v", result.Results[1])
	}
	step, err := world.StepsGet(ctx, worlds.StepsGetInput{RunID: runID, StepID: "step_a"})
	if err != nil || step.Status != worlds.StatusRunning {
		t.Fatalf("batch step = %#v err=%v", step, err)
	}

	// An invalid batch appends nothing.
	before := len(conformAllEvents(t, world, runID))
	_, err = world.EventsCreateBatch(ctx, worlds.EventsCreateBatchInput{RunID: runID, Events: []worlds.EventsCreateEvent{{EventType: worlds.EventTypeWaitCreated, CorrelationID: "w"}, {EventType: worlds.EventTypeRunCreated}}})
	conformExpect(t, err, worlds.ErrEventInvalid, "batch with run_created")
	if events := conformAllEvents(t, world, runID); len(events) != before {
		t.Fatalf("events after invalid batch = %d, want %d", len(events), before)
	}
	_, err = world.EventsCreateBatch(ctx, worlds.EventsCreateBatchInput{RunID: "wrun_missing", Events: []worlds.EventsCreateEvent{{EventType: worlds.EventTypeRunStarted}}})
	conformExpect(t, err, worlds.ErrRunNotFound, "batch for missing run")

	// A refused event refuses the later events of its correlation ID, and
	// leaves the others to their own outcomes.
	mixed, err := world.EventsCreateBatch(ctx, worlds.EventsCreateBatchInput{RunID: runID, Events: []worlds.EventsCreateEvent{
		{EventType: worlds.EventTypeWaitCreated, CorrelationID: "wait_batch"},
		{EventType: worlds.EventTypeRunStarted},
		conformEvent(t, worlds.EventTypeStepCompleted, "step_a", worlds.DataStepResult{StepName: "a", Result: []byte(`"out"`)}),
		{EventType: worlds.EventTypeRunStarted},
	}})
	if err != nil || len(mixed.Results) != 4 || mixed.AllocatedEventCount != 2 {
		t.Fatalf("mixed batch = %#v err=%v", mixed, err)
	}
	if mixed.Results[0].Err != nil || !errors.Is(mixed.Results[1].Err, worlds.ErrEventConflict) || mixed.Results[2].Err != nil || mixed.Results[3].Err == nil {
		t.Fatalf("mixed batch outcomes = %v, %v, %v, %v", mixed.Results[0].Err, mixed.Results[1].Err, mixed.Results[2].Err, mixed.Results[3].Err)
	}

	conformAppend(t, world, runID, worlds.EventTypeStepCreated, "step_x", worlds.DataStepCreated{StepName: "x"})
	conformAppend(t, world, runID, worlds.EventTypeStepStarted, "step_x", worlds.DataStepStarted{StepName: "x", Attempt: 1})
	completed, err := world.EventsCreateBatch(ctx, worlds.EventsCreateBatchInput{RunID: runID, Events: []worlds.EventsCreateEvent{
		{EventType: worlds.EventTypeRunCompleted, Data: []byte(`{"output":"Im91dCI="}`)},
		{EventType: worlds.EventTypeWaitCreated, CorrelationID: "late"},
	}})
	if err != nil || completed.Results[0].Err != nil || !errors.Is(completed.Results[1].Err, worlds.ErrRunTerminal) {
		t.Fatalf("batch appending after its own terminal event = %#v err=%v", completed, err)
	}
	// The finished run refuses another attempt, and so the failure behind
	// it, which alone it accepts.
	ordered, err := world.EventsCreateBatch(ctx, worlds.EventsCreateBatchInput{RunID: runID, Events: []worlds.EventsCreateEvent{
		conformEvent(t, worlds.EventTypeStepStarted, "step_x", worlds.DataStepStarted{StepName: "x", Attempt: 2}),
		conformEvent(t, worlds.EventTypeStepFailed, "step_x", worlds.DataStepResult{StepName: "x", Attempt: 1, Error: []byte(`{"message":"x"}`)}),
	}})
	if err != nil || !errors.Is(ordered.Results[0].Err, worlds.ErrRunTerminal) || ordered.Results[1].Err == nil || ordered.AllocatedEventCount != 0 {
		t.Fatalf("batch after a refused event of the same step = %#v err=%v", ordered, err)
	}
	conformAppend(t, world, runID, worlds.EventTypeStepFailed, "step_x", worlds.DataStepResult{StepName: "x", Attempt: 1, Error: []byte(`{"message":"x"}`)})
}

func conformEventsList(t *testing.T, world worlds.World) {
	ctx := context.Background()
	runID := conformCreateRun(t, world, "events")
	conformAppend(t, world, runID, worlds.EventTypeRunStarted, "", nil)
	conformAppend(t, world, runID, worlds.EventTypeWaitCreated, "wait_a", worlds.DataWaitCreated{ResumeAt: time.Date(2030, time.January, 1, 0, 0, 0, 0, time.UTC)})
	conformAppend(t, world, runID, worlds.EventTypeWaitCreated, "wait_b", nil)
	conformAppend(t, world, runID, worlds.EventTypeWaitCompleted, "wait_a", nil)

	var ascending []worlds.EventID
	cursor := ""
	for {
		page, err := world.EventsList(ctx, worlds.EventsListInput{RunID: runID, Pagination: worlds.Pagination{Limit: 2, Cursor: cursor}})
		if err != nil {
			t.Fatal(err)
		}
		for _, event := range page.Data {
			ascending = append(ascending, event.EventID)
		}
		if !page.HasMore {
			break
		}
		cursor = page.Cursor
	}
	if len(ascending) != 5 {
		t.Fatalf("ascending events = %v", ascending)
	}
	for i, id := range ascending {
		if id != worlds.EventIDForSlot(uint64(i+1)) {
			t.Fatalf("ascending events = %v", ascending)
		}
	}
	var descending []worlds.EventID
	cursor = ""
	for {
		page, err := world.EventsList(ctx, worlds.EventsListInput{RunID: runID, Pagination: worlds.Pagination{Limit: 2, Cursor: cursor, SortOrder: worlds.SortOrderDescending}})
		if err != nil {
			t.Fatal(err)
		}
		for _, event := range page.Data {
			descending = append(descending, event.EventID)
		}
		if !page.HasMore {
			break
		}
		cursor = page.Cursor
	}
	if len(descending) != 5 || descending[0] != worlds.EventIDForSlot(5) || descending[4] != worlds.EventIDForSlot(1) {
		t.Fatalf("descending events = %v", descending)
	}

	event, err := world.EventsGet(ctx, worlds.EventsGetInput{RunID: runID, EventID: worlds.EventIDForSlot(3)})
	if err != nil || event.EventType != worlds.EventTypeWaitCreated || event.CorrelationID != "wait_a" || len(event.Data) == 0 {
		t.Fatalf("EventsGet = %#v err=%v", event, err)
	}
	// A wait's Data is metadata, so omitting payloads keeps it.
	metadata, err := world.EventsGet(ctx, worlds.EventsGetInput{RunID: runID, EventID: worlds.EventIDForSlot(3), ResolveData: worlds.EventsResolveDataNone})
	if err != nil || !bytes.Equal(metadata.Data, event.Data) {
		t.Fatalf("EventsGet ResolveDataNone = %#v err=%v", metadata, err)
	}
	_, err = world.EventsGet(ctx, worlds.EventsGetInput{RunID: runID, EventID: worlds.EventIDForSlot(99)})
	conformExpect(t, err, worlds.ErrEventNotFound, "missing event")
	_, err = world.EventsGet(ctx, worlds.EventsGetInput{RunID: "wrun_missing", EventID: worlds.EventIDForSlot(1)})
	conformExpect(t, err, worlds.ErrRunNotFound, "event of missing run")
	_, err = world.EventsList(ctx, worlds.EventsListInput{RunID: "wrun_missing"})
	conformExpect(t, err, worlds.ErrRunNotFound, "events of missing run")

	stripped, err := world.EventsList(ctx, worlds.EventsListInput{RunID: runID, ResolveData: worlds.EventsResolveDataNone})
	if err != nil || len(stripped.Data) != 5 || !bytes.Equal(stripped.Data[2].Data, event.Data) {
		t.Fatalf("EventsList ResolveDataNone = %#v err=%v", stripped, err)
	}
	// Omitting payloads drops the run input and keeps the workflow name.
	var created map[string]json.RawMessage
	if err := json.Unmarshal(stripped.Data[0].Data, &created); err != nil {
		t.Fatalf("run_created Data without payloads = %q: %v", stripped.Data[0].Data, err)
	}
	if _, ok := created["input"]; ok || string(created["workflowName"]) != `"conformance"` {
		t.Fatalf("run_created Data without payloads = %s", stripped.Data[0].Data)
	}
	correlated, err := world.EventsListByCorrelationID(ctx, worlds.EventsListByCorrelationIDInput{RunID: runID, CorrelationID: "wait_a"})
	if err != nil || len(correlated.Data) != 2 || correlated.Data[0].EventID != worlds.EventIDForSlot(3) || correlated.Data[1].EventID != worlds.EventIDForSlot(5) {
		t.Fatalf("EventsListByCorrelationID = %#v err=%v", correlated, err)
	}
	firstCorrelated, err := world.EventsListByCorrelationID(ctx, worlds.EventsListByCorrelationIDInput{RunID: runID, CorrelationID: "wait_a", Pagination: worlds.Pagination{Limit: 1}})
	if err != nil || len(firstCorrelated.Data) != 1 || !firstCorrelated.HasMore {
		t.Fatalf("first correlated page = %#v err=%v", firstCorrelated, err)
	}
	nextCorrelated, err := world.EventsListByCorrelationID(ctx, worlds.EventsListByCorrelationIDInput{RunID: runID, CorrelationID: "wait_a", Pagination: worlds.Pagination{Limit: 1, Cursor: firstCorrelated.Cursor}})
	if err != nil || len(nextCorrelated.Data) != 1 || nextCorrelated.Data[0].EventID != worlds.EventIDForSlot(5) || nextCorrelated.HasMore {
		t.Fatalf("second correlated page = %#v err=%v", nextCorrelated, err)
	}
}

func conformWaits(t *testing.T, world worlds.World) {
	ctx := context.Background()
	runID := conformCreateRun(t, world, "waits")
	// A wait completes once it was created, as in the Workflow SDK's Worlds.
	_, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeWaitCompleted, CorrelationID: "wait_a"}})
	conformExpect(t, err, worlds.ErrEventNotFound, "wait_completed of a wait never created")
	resumeAt := time.Now().Add(time.Hour).UTC()
	conformAppend(t, world, runID, worlds.EventTypeWaitCreated, "wait_a", worlds.DataWaitCreated{ResumeAt: resumeAt})
	conformAppend(t, world, runID, worlds.EventTypeWaitCreated, "wait_b", worlds.DataWaitCreated{ResumeAt: resumeAt})
	conformAppend(t, world, runID, worlds.EventTypeWaitCompleted, "wait_a", nil)
	// A wait completes once, as in the Workflow SDK's Worlds.
	_, err = world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeWaitCompleted, CorrelationID: "wait_a"}})
	conformExpect(t, err, worlds.ErrEventConflict, "repeated wait_completed")
	if events := conformAllEvents(t, world, runID); len(events) != 4 {
		t.Fatalf("events = %d, want 4", len(events))
	}
	// A run's waits end with it.
	conformAppend(t, world, runID, worlds.EventTypeRunCompleted, "", nil)
	_, err = world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeWaitCompleted, CorrelationID: "wait_b"}})
	conformExpect(t, err, worlds.ErrEventNotFound, "wait_completed after the run finished")
}

// conformEventsSubscribe checks a World that pushes a run's events: each
// event after the slot subscribed from, in slot order and once, and none
// after unsubscribe returns.
func conformEventsSubscribe(t *testing.T, world worlds.World) {
	subscriber, ok := world.(worlds.EventsSubscriber)
	if !ok {
		t.Skip("the World does not push events")
	}
	ctx := context.Background()
	runID := conformCreateRun(t, world, "subscribe")
	conformAppend(t, world, runID, worlds.EventTypeWaitCreated, "wait_before", nil)
	var mu sync.Mutex
	var slots []uint64
	stopped := false
	pushed := make(chan struct{}, 16)
	unsubscribe := subscriber.EventsSubscribe(ctx, runID, 2, func(event worlds.Event) {
		slot, _ := worlds.EventSlot(event.EventID)
		mu.Lock()
		if stopped {
			t.Errorf("event %s pushed after unsubscribe", event.EventID)
		}
		slots = append(slots, slot)
		mu.Unlock()
		pushed <- struct{}{}
	}, func(err error) { t.Errorf("feed stopped: %v", err) })
	for _, id := range []string{"wait_a", "wait_b", "wait_c"} {
		conformAppend(t, world, runID, worlds.EventTypeWaitCreated, id, nil)
	}
	deadline := time.After(10 * time.Second)
	for range 3 {
		select {
		case <-pushed:
		case <-deadline:
			mu.Lock()
			t.Fatalf("pushed slots = %v, want 3, 4, 5", slots)
			mu.Unlock()
		}
	}
	unsubscribe()
	mu.Lock()
	stopped = true
	if len(slots) != 3 || slots[0] != 3 || slots[1] != 4 || slots[2] != 5 {
		t.Fatalf("pushed slots = %v, want 3, 4, 5", slots)
	}
	mu.Unlock()
	conformAppend(t, world, runID, worlds.EventTypeWaitCreated, "wait_after", nil)
}

// conformEventShapes checks the shapes the Workflow SDK gives events and the
// entities writes return: every event and entity carries its spec version, a
// hook its run's, a wait write returns the wait, a run's attributes are never
// nil, and no writer may append a noop.
func conformEventShapes(t *testing.T, world worlds.World) {
	ctx := context.Background()
	runID := conformCreateRun(t, world, "shapes")
	run, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: runID})
	if err != nil || run.Attributes == nil || run.SpecVersion != worlds.SpecVersion {
		t.Fatalf("run = %+v err = %v, want attributes and spec version %d", run, err, worlds.SpecVersion)
	}
	step := conformAppend(t, world, runID, worlds.EventTypeStepCreated, "step_shape", worlds.DataStepCreated{StepName: "shape", Input: []byte(`"x"`)})
	if step.Step == nil || step.Step.SpecVersion != worlds.SpecVersion {
		t.Fatalf("step_created step = %+v", step.Step)
	}
	if got, err := world.StepsGet(ctx, worlds.StepsGetInput{RunID: runID, StepID: "step_shape"}); err != nil || got.SpecVersion != worlds.SpecVersion {
		t.Fatalf("step = %+v err = %v", got, err)
	}
	hook := conformAppend(t, world, runID, worlds.EventTypeHookCreated, "hook_shape", conformHookData("shape:"+string(runID), false, 0))
	if hook.Hook == nil || hook.Hook.SpecVersion != worlds.SpecVersion || hook.Hook.ResumeContext.RunSpecVersion != worlds.SpecVersion {
		t.Fatalf("hook_created hook = %+v", hook.Hook)
	}
	if got, err := world.HooksGet(ctx, worlds.HooksGetInput{HookID: "hook_shape"}); err != nil || got.SpecVersion != worlds.SpecVersion || got.ResumeContext.RunSpecVersion != worlds.SpecVersion {
		t.Fatalf("hook = %+v err = %v", got, err)
	}
	resumeAt := time.Now().Add(time.Hour).UTC().Truncate(time.Millisecond)
	created := conformAppend(t, world, runID, worlds.EventTypeWaitCreated, "wait_shape", worlds.DataWaitCreated{ResumeAt: resumeAt})
	wait := created.Wait
	if wait == nil || wait.WaitID != worlds.WaitIDOf(runID, "wait_shape") || wait.RunID != runID || wait.Status != worlds.WaitStatusWaiting || wait.ResumeAt == nil || !wait.ResumeAt.Equal(resumeAt) || wait.CompletedAt != nil || wait.SpecVersion != worlds.SpecVersion {
		t.Fatalf("wait_created wait = %+v", wait)
	}
	completed := conformAppend(t, world, runID, worlds.EventTypeWaitCompleted, "wait_shape", worlds.DataWaitCompleted{ResumeAt: &resumeAt})
	wait = completed.Wait
	if wait == nil || wait.Status != worlds.WaitStatusCompleted || wait.CompletedAt == nil || wait.ResumeAt == nil || !wait.ResumeAt.Equal(resumeAt) {
		t.Fatalf("wait_completed wait = %+v", wait)
	}
	batch, err := world.EventsCreateBatch(ctx, worlds.EventsCreateBatchInput{RunID: runID, Events: []worlds.EventsCreateEvent{conformEvent(t, worlds.EventTypeWaitCreated, "wait_batch", worlds.DataWaitCreated{ResumeAt: resumeAt})}})
	if err != nil || len(batch.Results) != 1 || batch.Results[0].Wait == nil || batch.Results[0].Wait.Status != worlds.WaitStatusWaiting {
		t.Fatalf("batch = %+v err = %v", batch, err)
	}
	for _, event := range conformAllEvents(t, world, runID) {
		if event.SpecVersion != worlds.SpecVersion {
			t.Fatalf("event %s %s spec version = %d, want %d", event.EventID, event.EventType, event.SpecVersion, worlds.SpecVersion)
		}
	}
	_, err = world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeNoop}})
	conformExpect(t, err, worlds.ErrEventInvalid, "noop append")
}

// conformRepeats checks that a World refuses an event that repeats a
// creation the run's log holds, as the Workflow SDK's Worlds refuse it with
// EntityConflictError: a wait_created, a hook_created, and an attr_set of
// the workflow.
func conformRepeats(t *testing.T, world worlds.World) {
	ctx := context.Background()
	runID := conformCreateRun(t, world, "repeats")
	conformAppend(t, world, runID, worlds.EventTypeRunStarted, "", nil)
	resumeAt := time.Now().Add(time.Hour).UTC()
	conformAppend(t, world, runID, worlds.EventTypeWaitCreated, "wait_a", worlds.DataWaitCreated{ResumeAt: resumeAt})
	_, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: conformEvent(t, worlds.EventTypeWaitCreated, "wait_a", worlds.DataWaitCreated{ResumeAt: resumeAt})})
	conformExpect(t, err, worlds.ErrEventConflict, "repeated wait_created")

	token := fmt.Sprintf("repeat-%d", time.Now().UnixNano())
	if _, err := conformClaim(world, runID, "hook_a", token, false); err != nil {
		t.Fatal(err)
	}
	_, err = conformClaim(world, runID, "hook_a", token, false)
	conformExpect(t, err, worlds.ErrEventConflict, "repeated hook_created")

	attributes := worlds.DataAttrSet{Changes: []worlds.RunsAttributeChange{{Key: "tier", Value: new("gold")}}, Writer: worlds.AttributeWriter{Type: worlds.AttributeWriterWorkflow}}
	conformAppend(t, world, runID, worlds.EventTypeAttrSet, "attr_a", attributes)
	_, err = world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: conformEvent(t, worlds.EventTypeAttrSet, "attr_a", attributes)})
	conformExpect(t, err, worlds.ErrEventConflict, "repeated attr_set")
	if events := conformAllEvents(t, world, runID); len(events) != 5 {
		t.Fatalf("events after refused repeats = %d, want 5", len(events))
	}
}
