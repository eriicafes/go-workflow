package localworld

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

// recoveryDeliveries records the runs of the deliveries a World makes.
type recoveryDeliveries struct {
	mu   sync.Mutex
	runs []worlds.RunID
}

func (d *recoveryDeliveries) record(runID worlds.RunID) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.runs = append(d.runs, runID)
}

func (d *recoveryDeliveries) list() []worlds.RunID {
	d.mu.Lock()
	defer d.mu.Unlock()
	return slices.Clone(d.runs)
}

// waitFor waits until count deliveries were made, and a moment more for any
// that should not be, then returns them.
func (d *recoveryDeliveries) waitFor(t *testing.T, count int) []worlds.RunID {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for len(d.list()) < count {
		if time.Now().After(deadline) {
			t.Fatalf("deliveries = %v, want %d", d.list(), count)
		}
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	return d.list()
}

// openSDKWorld opens a World on dir that hands its deliveries to execute
// instead of executing them.
func openSDKWorld(t *testing.T, config Config, execute func(context.Context, worlds.QueueInput) error) *World {
	t.Helper()
	return openSDKWorldWith(t, config, func(*World) func(context.Context, worlds.QueueInput) error { return execute })
}

// openSDKWorldWith opens a World whose deliveries go to the function
// executor returns for it.
func openSDKWorldWith(t *testing.T, config Config, executor func(*World) func(context.Context, worlds.QueueInput) error) *World {
	t.Helper()
	world, err := Open(config, nil)
	if err != nil {
		t.Fatal(err)
	}
	world.execute = executor(world)
	world.queue.retry = func(int) time.Duration { return 10 * time.Millisecond }
	if err := world.Prepare(context.Background()); err != nil {
		_ = world.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = world.Close() })
	return world
}

// writer opens a World on dir for writing runs, which delivers nothing.
func writer(t *testing.T, dir, tag string) *World {
	t.Helper()
	return openSDKWorld(t, Config{Dir: dir, Tag: tag, SkipRecovery: true}, func(context.Context, worlds.QueueInput) error { return nil })
}

// restart opens a World on dir as a restarted process: bootedAt moves past
// everything written so far, so the steps started until now were started by
// a process that is gone.
func restart(t *testing.T, config Config, execute func(context.Context, worlds.QueueInput) error) *World {
	t.Helper()
	return restartWith(t, config, func(*World) func(context.Context, worlds.QueueInput) error { return execute })
}

func restartWith(t *testing.T, config Config, executor func(*World) func(context.Context, worlds.QueueInput) error) *World {
	t.Helper()
	time.Sleep(2 * time.Millisecond)
	previous := bootedAt
	bootedAt = time.Now()
	t.Cleanup(func() { bootedAt = previous })
	return openSDKWorldWith(t, config, executor)
}

func sdkAppend(t *testing.T, world *World, runID worlds.RunID, eventType worlds.EventType, correlationID string, data any) {
	t.Helper()
	var encoded []byte
	if data != nil {
		var err error
		if encoded, err = json.Marshal(data); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := world.EventsCreate(context.Background(), worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: eventType, CorrelationID: correlationID, Data: encoded}}); err != nil {
		t.Fatalf("%s %s: %v", eventType, correlationID, err)
	}
}

// createRun creates a pending run.
func createRun(t *testing.T, world *World, workflowName string) worlds.RunID {
	t.Helper()
	runID := worlds.NewRunID()
	if _, err := world.EventsCreateRun(context.Background(), worlds.EventsCreateRunInput{RunID: runID, WorkflowName: workflowName, DeploymentID: "dpl_1", Input: []byte{1}}); err != nil {
		t.Fatal(err)
	}
	return runID
}

// startRun creates a running run.
func startRun(t *testing.T, world *World) worlds.RunID {
	t.Helper()
	runID := createRun(t, world, "myWorkflow")
	sdkAppend(t, world, runID, worlds.EventTypeRunStarted, "", nil)
	return runID
}

// startInlineStep starts a step whose body a flow of this process runs
// inline.
func startInlineStep(t *testing.T, world *World, runID worlds.RunID, stepID string) {
	t.Helper()
	sdkAppend(t, world, runID, worlds.EventTypeStepStarted, stepID, worlds.DataStepStarted{StepName: "myStep", Inline: true, Input: []byte{1}})
}

// stepEventTypes returns the types of a step's events, in order.
func stepEventTypes(t *testing.T, world *World, runID worlds.RunID, stepID string) []worlds.EventType {
	t.Helper()
	var types []worlds.EventType
	for _, event := range stepEvents(t, world, runID, stepID) {
		types = append(types, event.EventType)
	}
	return types
}

func stepEvents(t *testing.T, world *World, runID worlds.RunID, stepID string) []worlds.Event {
	t.Helper()
	var events []worlds.Event
	var cursor worlds.Pagination
	for {
		page, err := world.EventsList(context.Background(), worlds.EventsListInput{RunID: runID, Pagination: cursor})
		if err != nil {
			t.Fatal(err)
		}
		for _, event := range page.Data {
			if event.CorrelationID == stepID {
				events = append(events, event)
			}
		}
		if !page.HasMore {
			return events
		}
		cursor = worlds.Pagination{Cursor: page.Cursor}
	}
}

func expectTypes(t *testing.T, label string, got []worlds.EventType, want ...worlds.EventType) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("%s = %v, want %v", label, got, want)
	}
}

// SDK: packages/world-local/src/orphaned-steps.test.ts > orphaned inline steps on restart > writes step_retrying for an inline step before re-enqueueing its run
func TestSDKRecoveryReleasesAnInlineStepBeforeQueueingItsRun(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	previous := writer(t, dir, "")
	runID := startRun(t, previous)
	startInlineStep(t, previous, runID, "step_inline")
	if err := previous.Close(); err != nil {
		t.Fatal(err)
	}

	atDelivery := make(chan []worlds.EventType, 1)
	var once sync.Once
	world := restartWith(t, Config{Dir: dir}, func(world *World) func(context.Context, worlds.QueueInput) error {
		return func(_ context.Context, input worlds.QueueInput) error {
			if input.RunID == runID {
				once.Do(func() { atDelivery <- stepEventTypes(t, world, runID, "step_inline") })
			}
			return nil
		}
	})
	select {
	case types := <-atDelivery:
		expectTypes(t, "step events at delivery", types, worlds.EventTypeStepStarted, worlds.EventTypeStepRetrying)
	case <-time.After(20 * time.Second):
		t.Fatal("run not delivered")
	}
	events := stepEvents(t, world, runID, "step_inline")
	var retrying worlds.DataStepResult
	if err := json.Unmarshal(events[len(events)-1].Data, &retrying); err != nil || retrying.StepName != "myStep" {
		t.Fatalf("step_retrying data = %s err=%v", events[len(events)-1].Data, err)
	}
	step, err := world.StepsGet(ctx, worlds.StepsGetInput{RunID: runID, StepID: "step_inline"})
	// The dead attempt counts: the next start makes it attempt 2.
	if err != nil || step.Status != worlds.StatusPending || step.Attempt != 1 {
		t.Fatalf("released step = %#v err=%v", step, err)
	}
	const payload = `devl[["Error",1],{"name":2,"message":3},"Error","The process running this step exited before it finished."]`
	if string(step.Error) != payload {
		t.Fatalf("released step error = %q", step.Error)
	}
}

// SDK: packages/world-local/src/orphaned-steps.test.ts > orphaned inline steps on restart > leaves queue-owned and finished steps alone
func TestSDKRecoveryLeavesQueuedAndFinishedStepsAlone(t *testing.T) {
	dir := t.TempDir()
	previous := writer(t, dir, "")
	runID := startRun(t, previous)
	// Started by a queued step message.
	sdkAppend(t, previous, runID, worlds.EventTypeStepCreated, "step_queued", worlds.DataStepCreated{StepName: "myStep", Input: []byte{1}})
	sdkAppend(t, previous, runID, worlds.EventTypeStepStarted, "step_queued", nil)
	startInlineStep(t, previous, runID, "step_completed")
	sdkAppend(t, previous, runID, worlds.EventTypeStepCompleted, "step_completed", worlds.DataStepResult{Result: []byte{1}})
	startInlineStep(t, previous, runID, "step_failed")
	sdkAppend(t, previous, runID, worlds.EventTypeStepFailed, "step_failed", worlds.DataStepResult{Error: []byte{1}})
	// Retried once already, so the step is the queue's from then on.
	startInlineStep(t, previous, runID, "step_retried")
	sdkAppend(t, previous, runID, worlds.EventTypeStepRetrying, "step_retried", worlds.DataStepResult{Error: []byte{1}})
	sdkAppend(t, previous, runID, worlds.EventTypeStepStarted, "step_retried", nil)
	if err := previous.Close(); err != nil {
		t.Fatal(err)
	}

	var deliveries recoveryDeliveries
	world := restart(t, Config{Dir: dir}, func(_ context.Context, input worlds.QueueInput) error {
		deliveries.record(input.RunID)
		return nil
	})
	deliveries.waitFor(t, 1)
	expectTypes(t, "queued step", stepEventTypes(t, world, runID, "step_queued"), worlds.EventTypeStepCreated, worlds.EventTypeStepStarted)
	expectTypes(t, "completed step", stepEventTypes(t, world, runID, "step_completed"), worlds.EventTypeStepStarted, worlds.EventTypeStepCompleted)
	expectTypes(t, "failed step", stepEventTypes(t, world, runID, "step_failed"), worlds.EventTypeStepStarted, worlds.EventTypeStepFailed)
	expectTypes(t, "retried step", stepEventTypes(t, world, runID, "step_retried"), worlds.EventTypeStepStarted, worlds.EventTypeStepRetrying, worlds.EventTypeStepStarted)
}

// SDK: packages/world-local/src/orphaned-steps.test.ts > orphaned inline steps on restart > releases only the restarting world's tag
func TestSDKRecoveryReleasesOnlyItsOwnTag(t *testing.T) {
	dir := t.TempDir()
	runs := map[string]worlds.RunID{}
	for _, tag := range []string{"vitest-0", "vitest-1", ""} {
		previous := writer(t, dir, tag)
		runs[tag] = startRun(t, previous)
		startInlineStep(t, previous, runs[tag], "step_inline")
		if err := previous.Close(); err != nil {
			t.Fatal(err)
		}
	}
	var deliveries recoveryDeliveries
	world := restart(t, Config{Dir: dir, Tag: "vitest-0"}, func(_ context.Context, input worlds.QueueInput) error {
		deliveries.record(input.RunID)
		return nil
	})
	deliveries.waitFor(t, 1)
	expectTypes(t, "own tag's step", stepEventTypes(t, world, runs["vitest-0"], "step_inline"), worlds.EventTypeStepStarted, worlds.EventTypeStepRetrying)
	for _, tag := range []string{"vitest-1", ""} {
		reader := writer(t, dir, tag)
		expectTypes(t, "step of tag "+tag, stepEventTypes(t, reader, runs[tag], "step_inline"), worlds.EventTypeStepStarted)
	}
}

// SDK: packages/world-local/src/orphaned-steps.test.ts > orphaned inline steps on restart > leaves recovery to the embedder when recoverActiveRuns is false
func TestSDKReleaseOrphanedStepsWithoutRecovery(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	previous := writer(t, dir, "")
	runID := startRun(t, previous)
	startInlineStep(t, previous, runID, "step_inline")
	if err := previous.Close(); err != nil {
		t.Fatal(err)
	}
	world := restart(t, Config{Dir: dir, SkipRecovery: true}, func(context.Context, worlds.QueueInput) error { return nil })
	expectTypes(t, "step before the release", stepEventTypes(t, world, runID, "step_inline"), worlds.EventTypeStepStarted)
	// A released step is the queue's from then on, so a second release does
	// nothing.
	for range 2 {
		if err := world.releaseOrphanedSteps(ctx, runID); err != nil {
			t.Fatal(err)
		}
	}
	expectTypes(t, "step after the release", stepEventTypes(t, world, runID, "step_inline"), worlds.EventTypeStepStarted, worlds.EventTypeStepRetrying)
}

// SDK: packages/world-local/src/orphaned-steps.test.ts > orphaned inline steps on restart > leaves a step started since this realm booted alone
func TestSDKRecoveryLeavesAStepStartedSinceBootAlone(t *testing.T) {
	dir := t.TempDir()
	previous := writer(t, dir, "")
	runID := startRun(t, previous)
	startInlineStep(t, previous, runID, "step_dead")
	if err := previous.Close(); err != nil {
		t.Fatal(err)
	}
	world := restart(t, Config{Dir: dir, SkipRecovery: true}, func(context.Context, worlds.QueueInput) error { return nil })
	// A delivery in this process starts another inline step before the run
	// is released.
	startInlineStep(t, world, runID, "step_live")
	if err := world.releaseOrphanedSteps(context.Background(), runID); err != nil {
		t.Fatal(err)
	}
	expectTypes(t, "dead step", stepEventTypes(t, world, runID, "step_dead"), worlds.EventTypeStepStarted, worlds.EventTypeStepRetrying)
	expectTypes(t, "live step", stepEventTypes(t, world, runID, "step_live"), worlds.EventTypeStepStarted)
}

// SDK: packages/world-local/src/orphaned-steps.test.ts > orphaned inline steps on restart > leaves the steps of a finished run alone
func TestSDKRecoveryLeavesTheStepsOfAFinishedRunAlone(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	previous := writer(t, dir, "")
	runID := startRun(t, previous)
	startInlineStep(t, previous, runID, "step_inline")
	sdkAppend(t, previous, runID, worlds.EventTypeRunCancelled, "", nil)
	if err := previous.Close(); err != nil {
		t.Fatal(err)
	}
	world := restart(t, Config{Dir: dir, SkipRecovery: true}, func(context.Context, worlds.QueueInput) error { return nil })
	if err := world.releaseOrphanedSteps(ctx, runID); err != nil {
		t.Fatal(err)
	}
	expectTypes(t, "step", stepEventTypes(t, world, runID, "step_inline"), worlds.EventTypeStepStarted)
	if step, err := world.StepsGet(ctx, worlds.StepsGetInput{RunID: runID, StepID: "step_inline"}); err != nil || step.Status != worlds.StatusRunning {
		t.Fatalf("step = %#v err=%v", step, err)
	}
}

// SDK: packages/world-local/src/orphaned-steps.test.ts > orphaned inline steps on restart > still re-enqueues a run whose steps could not be released
func TestSDKRecoveryQueuesARunWhoseStepsCouldNotBeReleased(t *testing.T) {
	dir := t.TempDir()
	previous := writer(t, dir, "")
	runID := startRun(t, previous)
	startInlineStep(t, previous, runID, "step_inline")
	if err := previous.Close(); err != nil {
		t.Fatal(err)
	}
	// The run's steps cannot be read, so its release fails.
	db, err := sql.Open("sqlite", filepath.Join(dir, "world.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("UPDATE steps SET doc = '{' WHERE run_id = ?", string(runID)); err != nil {
		t.Fatal(err)
	}
	var deliveries recoveryDeliveries
	world := restart(t, Config{Dir: dir}, func(_ context.Context, input worlds.QueueInput) error {
		deliveries.record(input.RunID)
		return nil
	})
	if got := deliveries.waitFor(t, 1); !slices.Equal(got, []worlds.RunID{runID}) {
		t.Fatalf("deliveries = %v, want the run", got)
	}
	expectTypes(t, "step", stepEventTypes(t, world, runID, "step_inline"), worlds.EventTypeStepStarted)
}

// recoveredRuns restarts on dir with config and returns the runs delivered
// once count of them are.
func recoveredRuns(t *testing.T, config Config, count int) []worlds.RunID {
	t.Helper()
	var deliveries recoveryDeliveries
	restart(t, config, func(_ context.Context, input worlds.QueueInput) error {
		deliveries.record(input.RunID)
		return nil
	})
	if count == 0 {
		time.Sleep(50 * time.Millisecond)
		return deliveries.list()
	}
	got := deliveries.waitFor(t, count)
	slices.Sort(got)
	return got
}

func sortedRuns(runs ...worlds.RunID) []worlds.RunID {
	runs = slices.Clone(runs)
	slices.Sort(runs)
	return runs
}

// SDK: packages/world-local/src/reenqueue.test.ts > re-enqueue active runs on start > re-enqueues pending and running runs after restart
func TestSDKRecoveryQueuesPendingAndRunningRuns(t *testing.T) {
	dir := t.TempDir()
	world := writer(t, dir, "")
	pending := createRun(t, world, "myWorkflow")
	running := createRun(t, world, "otherWorkflow")
	sdkAppend(t, world, running, worlds.EventTypeRunStarted, "", nil)
	completed := startRun(t, world)
	sdkAppend(t, world, completed, worlds.EventTypeRunCompleted, "", worlds.DataRunCompleted{Output: []byte{4}})
	failed := startRun(t, world)
	sdkAppend(t, world, failed, worlds.EventTypeRunFailed, "", worlds.DataRunFailed{Error: []byte("boom")})
	if err := world.Close(); err != nil {
		t.Fatal(err)
	}
	if got, want := recoveredRuns(t, Config{Dir: dir}, 2), sortedRuns(pending, running); !slices.Equal(got, want) {
		t.Fatalf("recovered %v, want %v", got, want)
	}
}

// SDK: packages/world-local/src/reenqueue.test.ts > re-enqueue active runs on start > does nothing when there are no active runs
func TestSDKRecoveryQueuesNothingWithoutActiveRuns(t *testing.T) {
	dir := t.TempDir()
	world := writer(t, dir, "")
	runID := startRun(t, world)
	sdkAppend(t, world, runID, worlds.EventTypeRunCompleted, "", worlds.DataRunCompleted{Output: []byte{2}})
	if err := world.Close(); err != nil {
		t.Fatal(err)
	}
	if got := recoveredRuns(t, Config{Dir: dir}, 0); len(got) != 0 {
		t.Fatalf("recovered %v", got)
	}
}

// SDK: packages/world-local/src/reenqueue.test.ts > re-enqueue active runs on start > only re-enqueues runs for the matching tag
func TestSDKRecoveryQueuesOnlyItsTagsRuns(t *testing.T) {
	dir := t.TempDir()
	own := writer(t, dir, "vitest-0")
	run0 := createRun(t, own, "taggedWorkflow0")
	other := writer(t, dir, "vitest-1")
	createRun(t, other, "taggedWorkflow1")
	_, _ = own.Close(), other.Close()
	if got := recoveredRuns(t, Config{Dir: dir, Tag: "vitest-0"}, 1); !slices.Equal(got, []worlds.RunID{run0}) {
		t.Fatalf("recovered %v, want %v", got, run0)
	}
}

// SDK: packages/world-local/src/reenqueue.test.ts > re-enqueue active runs on start > untagged recovery skips tagged runs sharing the data dir
func TestSDKUntaggedRecoverySkipsTaggedRuns(t *testing.T) {
	dir := t.TempDir()
	tagged := writer(t, dir, "vitest-0")
	createRun(t, tagged, "taggedWorkflow")
	untagged := writer(t, dir, "")
	untaggedRun := createRun(t, untagged, "untaggedWorkflow")
	_, _ = tagged.Close(), untagged.Close()
	if got := recoveredRuns(t, Config{Dir: dir}, 1); !slices.Equal(got, []worlds.RunID{untaggedRun}) {
		t.Fatalf("recovered %v, want %v", got, untaggedRun)
	}
}

// SDK: packages/world-local/src/reenqueue.test.ts > re-enqueue active runs on start > keeps tag filtering when recovery paginates across multiple pages
func TestSDKRecoveryKeepsItsTagAcrossManyRuns(t *testing.T) {
	dir := t.TempDir()
	own, other, untagged := writer(t, dir, "vitest-0"), writer(t, dir, "vitest-1"), writer(t, dir, "")
	var want []worlds.RunID
	for range 25 {
		want = append(want, createRun(t, own, "taggedWorkflow"))
	}
	for range 5 {
		createRun(t, other, "otherTaggedWorkflow")
		createRun(t, untagged, "untaggedWorkflow")
	}
	_, _, _ = own.Close(), other.Close(), untagged.Close()
	if got := recoveredRuns(t, Config{Dir: dir, Tag: "vitest-0"}, 25); !slices.Equal(got, sortedRuns(want...)) {
		t.Fatalf("recovered %v, want %v", got, sortedRuns(want...))
	}
}

// SDK: packages/world-local/src/reenqueue.test.ts > re-enqueue active runs on start > skips startup recovery when recoverActiveRuns is false
func TestSDKSkipRecoveryQueuesNothing(t *testing.T) {
	dir := t.TempDir()
	world := writer(t, dir, "")
	createRun(t, world, "myWorkflow")
	if err := world.Close(); err != nil {
		t.Fatal(err)
	}
	if got := recoveredRuns(t, Config{Dir: dir, SkipRecovery: true}, 0); len(got) != 0 {
		t.Fatalf("recovered %v", got)
	}
}

// SDK: packages/world-local/src/reenqueue.test.ts > re-enqueue active runs on start > does nothing on first start with empty data dir
func TestSDKRecoveryOnAnEmptyDirectory(t *testing.T) {
	if got := recoveredRuns(t, Config{Dir: filepath.Join(t.TempDir(), "new")}, 0); len(got) != 0 {
		t.Fatalf("recovered %v", got)
	}
}

// SDK: packages/world-local/src/recovery-wake.test.ts > wakes a recovered wait when it is due
// SDK: packages/world-local/src/recovery-wake.test.ts > wakes an overdue wait at once
// SDK: packages/world-local/src/recovery-wake.test.ts > sends no wake for a wait due after the queue's longest timer
func TestSDKBackstopWakeDelay(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	day := 24 * time.Hour
	for _, test := range []struct {
		resumeAt time.Time
		delay    time.Duration
		ok       bool
	}{
		{now.Add(1500 * time.Millisecond), 1500 * time.Millisecond, true},
		{now.Add(7 * day), 7 * day, true},
		{now.Add(-time.Minute), 0, true},
		// A sleep of 30 days: a plain wake would fire early, after about
		// 24.8 days, and re-arm nothing.
		{now.Add(30 * day), 0, false},
		{now.Add(maxBackstopDelay + time.Millisecond), 0, false},
		{now.Add(maxBackstopDelay), maxBackstopDelay, true},
	} {
		delay, ok := backstopWakeDelay(test.resumeAt, now)
		if ok != test.ok || (ok && delay != test.delay) {
			t.Errorf("backstopWakeDelay(now+%s) = %s %v, want %s %v", test.resumeAt.Sub(now), delay, ok, test.delay, test.ok)
		}
	}
}
