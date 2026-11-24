package localworld

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

// SDK: packages/world-local/src/storage/delete-data-many.test.ts > runs.deleteDataMany (world-local) > answers a retryable failure, deleting nothing, when a payload cannot be scrubbed, and a retry finishes the job
func TestSDKDeleteDataFailureIsRetryable(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	world := storageWorld(t, Config{})
	runID := startedRun(t, world)
	created := appendEvent(t, world, runID, worlds.EventTypeStepCreated, "step_001", []byte(`{"stepName":"step//./mod//doWork","input":"Nzc3"}`))
	appendEvent(t, world, runID, worlds.EventTypeStepCompleted, "step_001", []byte(`{"result":"ODg4"}`))
	appendEvent(t, world, runID, worlds.EventTypeRunCompleted, "", []byte(`{"output":"NDU2"}`))
	slot, _ := worlds.EventSlot(created.EventID)
	doc := scalar[[]byte](t, world, "SELECT doc FROM events WHERE run_id = ? AND seq = ?", string(runID), slot)
	setDoc := func(doc []byte) {
		t.Helper()
		if _, err := world.db.write.ExecContext(ctx, "UPDATE events SET doc = ? WHERE run_id = ? AND seq = ?", doc, string(runID), slot); err != nil {
			t.Fatal(err)
		}
	}
	// An event the deletion cannot read fails it.
	setDoc([]byte{0xff})
	first, err := world.RunsDeleteDataMany(ctx, worlds.RunsDeleteDataManyInput{RunIDs: []worlds.RunID{runID}})
	if err != nil || len(first.Results) != 1 || first.Results[0] != (worlds.DeleteDataResult{RunID: runID, Outcome: worlds.DeleteDataFailed, Code: "internal_error", Retryable: true}) || first.Summary.Failed != 1 {
		t.Fatalf("first deletion = %+v err = %v", first, err)
	}
	// One transaction: the run was not marked deleted either.
	if run, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: runID}); err != nil || run.ExpiredAt != nil || string(run.Output) != "456" {
		t.Fatalf("run after the failure = %+v err = %v", run, err)
	}
	setDoc(doc)
	retry, err := world.RunsDeleteDataMany(ctx, worlds.RunsDeleteDataManyInput{RunIDs: []worlds.RunID{runID}})
	if err != nil || len(retry.Results) != 1 || retry.Results[0] != (worlds.DeleteDataResult{RunID: runID, Outcome: worlds.DeleteDataDeleted}) {
		t.Fatalf("retry = %+v err = %v", retry, err)
	}
	if steps, err := world.StepsList(ctx, worlds.StepsListInput{RunID: runID}); err != nil || steps.Data[0].Output != nil {
		t.Fatalf("steps = %+v err = %v", steps.Data, err)
	}
	if run, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: runID}); err != nil || run.Output != nil {
		t.Fatalf("run = %+v err = %v", run, err)
	}
}

// sdkOnDisk reports whether payload is in the World's database files, as
// stored or as a prefix of its base64 encoding.
func sdkOnDisk(t *testing.T, dir string, payload []byte) bool {
	t.Helper()
	encoded := base64.StdEncoding.EncodeToString(payload)
	needles := [][]byte{payload, []byte(encoded[:min(64, len(encoded))])}
	for _, file := range []string{"world.db", "world.db-wal"} {
		contents, err := os.ReadFile(filepath.Join(dir, file))
		if err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
		for _, needle := range needles {
			if bytes.Contains(contents, needle) {
				return true
			}
		}
	}
	return false
}

// SDK: packages/world-local/src/storage/run-retention.test.ts > run retention (world-local) > $retention: '0' > leaves no trace of the payloads in the database files
func TestSDKZeroRetentionLeavesNoTraceOnDisk(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	world := storageWorld(t, Config{Dir: dir})
	marker := []byte(strings.Repeat("purge-marker-", 40))
	runID := worlds.NewRunID()
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "retention-workflow", DeploymentID: "dpl_test", Input: marker, Attributes: map[string]string{worlds.RetentionAttribute: "0"}, AllowReservedAttributes: true}); err != nil {
		t.Fatal(err)
	}
	appendEvent(t, world, runID, worlds.EventTypeRunStarted, "", nil)
	appendEvent(t, world, runID, worlds.EventTypeRunCompleted, "", []byte(`{"output":"NDU2"}`))
	if sdkOnDisk(t, dir, marker) {
		t.Fatal("the run's input is still in the database files")
	}
}

// SDK: packages/world-local/src/storage/run-retention.test.ts > run retention (world-local) > $retention: '0', a step that finishes after the run ended > scrubs a step_completed that lands after the run was cancelled
// SDK: packages/world-local/src/storage/run-retention.test.ts > run retention (world-local) > $retention: '0', a step that finishes after the run ended > scrubs a step_failed that lands after the run ended
// SDK: packages/world-local/src/storage/run-retention.test.ts > run retention (world-local) > $retention: '0', a step that finishes after the run ended > scrubs a step_retrying that lands after the run ended
// SDK: packages/world-local/src/storage/run-retention.test.ts > run retention (world-local) > $retention: '0', a step that finishes after the run ended > leaves no payload whichever write commits first (${order})
// SDK: packages/world-local/src/storage/run-retention.test.ts > run retention (world-local) > $retention: '0', a step that finishes after the run ended > still scrubs when both writes are in flight at once
//
// The conformance suite checks what a World answers for these runs. Here the
// late step's payloads are looked for in the database files themselves, the
// WAL included.
func TestSDKLateStepPayloadsLeaveNoTraceOnDisk(t *testing.T) {
	t.Parallel()
	input := []byte(strings.Repeat("late-step-input-", 40))
	result := append(bytes.Clone(input), []byte(strings.Repeat("late-step-result-", 40))...)
	stepError := []byte(strings.Repeat("late-step-error-", 40))
	setup := func(t *testing.T) (*World, string, worlds.RunID) {
		t.Helper()
		dir := t.TempDir()
		world := storageWorld(t, Config{Dir: dir})
		runID := worlds.NewRunID()
		if _, err := world.EventsCreateRun(context.Background(), worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "retention-workflow", Input: []byte("123"), Attributes: map[string]string{worlds.RetentionAttribute: "0"}, AllowReservedAttributes: true}); err != nil {
			t.Fatal(err)
		}
		appendEvent(t, world, runID, worlds.EventTypeRunStarted, "", nil)
		data, _ := json.Marshal(worlds.DataStepCreated{StepName: "step//./mod//slowWork", Input: input})
		appendEvent(t, world, runID, worlds.EventTypeStepCreated, "step_late", data)
		appendEvent(t, world, runID, worlds.EventTypeStepStarted, "step_late", nil)
		return world, dir, runID
	}
	cancel := func(world *World, runID worlds.RunID) error {
		_, err := world.EventsCreate(context.Background(), worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeRunCancelled}})
		return err
	}
	settle := func(world *World, runID worlds.RunID, eventType worlds.EventType, payload worlds.DataStepResult) error {
		data, _ := json.Marshal(payload)
		_, err := world.EventsCreate(context.Background(), worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: eventType, CorrelationID: "step_late", Data: data}})
		return err
	}
	absent := func(t *testing.T, dir string, payloads ...[]byte) {
		t.Helper()
		for _, payload := range payloads {
			if sdkOnDisk(t, dir, payload) {
				t.Fatalf("%.32s... is in the database files", payload)
			}
		}
	}
	for _, late := range []struct {
		eventType worlds.EventType
		payload   worlds.DataStepResult
		absent    [][]byte
	}{
		{worlds.EventTypeStepCompleted, worlds.DataStepResult{Result: result}, [][]byte{result, input}},
		{worlds.EventTypeStepFailed, worlds.DataStepResult{Error: stepError}, [][]byte{stepError}},
		{worlds.EventTypeStepRetrying, worlds.DataStepResult{Error: stepError}, [][]byte{stepError}},
	} {
		t.Run(string(late.eventType)+" after a cancel", func(t *testing.T) {
			world, dir, runID := setup(t)
			if err := cancel(world, runID); err != nil {
				t.Fatal(err)
			}
			if err := settle(world, runID, late.eventType, late.payload); err != nil {
				t.Fatal(err)
			}
			absent(t, dir, late.absent...)
		})
	}
	for _, order := range []string{"cancel first", "step first"} {
		t.Run(order, func(t *testing.T) {
			world, dir, runID := setup(t)
			steps := []func() error{
				func() error { return cancel(world, runID) },
				func() error {
					return settle(world, runID, worlds.EventTypeStepCompleted, worlds.DataStepResult{Result: result})
				},
			}
			if order == "step first" {
				steps[0], steps[1] = steps[1], steps[0]
			}
			for _, step := range steps {
				if err := step(); err != nil {
					t.Fatal(err)
				}
			}
			absent(t, dir, result)
		})
	}
	t.Run("both in flight", func(t *testing.T) {
		world, dir, runID := setup(t)
		var cancelErr, settleErr error
		var group sync.WaitGroup
		group.Go(func() { cancelErr = cancel(world, runID) })
		group.Go(func() {
			settleErr = settle(world, runID, worlds.EventTypeStepCompleted, worlds.DataStepResult{Result: result})
		})
		group.Wait()
		if cancelErr != nil || settleErr != nil {
			t.Fatal(cancelErr, settleErr)
		}
		absent(t, dir, result)
	})
}

// SDK: packages/world-local/src/storage/run-retention.test.ts > run retention (world-local) > values this World does not implement > keeps the data for a run that already carries an expiredAt
func TestSDKRetentionKeepsARunsExpiredAt(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	world := storageWorld(t, Config{})
	runID := worlds.NewRunID()
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "retention-workflow", Input: []byte("123")}); err != nil {
		t.Fatal(err)
	}
	appendEvent(t, world, runID, worlds.EventTypeRunStarted, "", nil)
	appendEvent(t, world, runID, worlds.EventTypeStepCreated, "step_001", []byte(`{"stepName":"step//./mod//doWork","input":"Nzc3"}`))
	appendEvent(t, world, runID, worlds.EventTypeStepCompleted, "step_001", []byte(`{"result":"ODg4"}`))
	// A deadline is not a request to delete the data now.
	deadline := time.Now().Add(24 * time.Hour).UTC().Truncate(time.Millisecond)
	if _, err := world.db.write.ExecContext(ctx, "UPDATE runs SET doc = json_set(doc, '$.ExpiredAt', ?) WHERE run_id = ?", deadline.Format(time.RFC3339Nano), string(runID)); err != nil {
		t.Fatal(err)
	}
	appendEvent(t, world, runID, worlds.EventTypeRunCompleted, "", []byte(`{"output":"NDU2"}`))
	run, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: runID})
	if err != nil || run.ExpiredAt == nil || !run.ExpiredAt.Equal(deadline) || string(run.Input) != "123" || string(run.Output) != "456" {
		t.Fatalf("run = %+v err = %v", run, err)
	}
	if steps, err := world.StepsList(ctx, worlds.StepsListInput{RunID: runID}); err != nil || string(steps.Data[0].Output) != "888" {
		t.Fatalf("steps = %+v err = %v", steps.Data, err)
	}
}
