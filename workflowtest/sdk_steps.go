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

// SDK: packages/world-local/src/storage.test.ts > Storage > steps > create > should create a new step
// SDK: packages/world-local/src/storage.test.ts > Storage > steps > get > should retrieve a step with runId and stepId
// SDK: packages/world-local/src/storage.test.ts > Storage > steps > get > should throw error for non-existent step
// SDK: packages/world-local/src/storage.test.ts > Storage > steps > update via events > should update step status to running via step_started event
// SDK: packages/world-local/src/storage.test.ts > Storage > steps > update via events > should update step status to completed via step_completed event
// SDK: packages/world-local/src/storage.test.ts > Storage > steps > update via events > should update step status to failed via step_failed event
func conformSDKSteps(t *testing.T, world worlds.World) {
	ctx := context.Background()
	get := func(t *testing.T, runID worlds.RunID, stepID worlds.StepID) worlds.Step {
		t.Helper()
		step, err := world.StepsGet(ctx, worlds.StepsGetInput{RunID: runID, StepID: stepID})
		if err != nil {
			t.Fatal(err)
		}
		return step
	}
	t.Run("create", func(t *testing.T) {
		runID := sdkRun(t, world, "test-workflow", []byte(`[]`))
		created := sdkStep(t, world, runID, "step_123", "test-step", []byte(`[1,2]`))
		step := created.Step
		if step == nil || step.RunID != runID || step.StepID != "step_123" || step.StepName != "test-step" || step.Status != worlds.StatusPending || string(step.Input) != `[1,2]` {
			t.Fatalf("step = %+v", step)
		}
		if step.Output != nil || step.Error != nil || step.Attempt != 0 || step.StartedAt != nil || step.CompletedAt != nil || step.CreatedAt.IsZero() || step.UpdatedAt.IsZero() {
			t.Fatalf("new step = %+v", step)
		}
	})
	for _, stepID := range []worlds.StepID{"step_123", "unique_step_123"} {
		t.Run("get "+string(stepID), func(t *testing.T) {
			runID := sdkRun(t, world, "test-workflow", []byte(`[]`))
			created := sdkStep(t, world, runID, string(stepID), "test-step", []byte(`[1]`)).Step
			got := get(t, runID, stepID)
			if got.StepID != created.StepID || got.StepName != created.StepName || got.Status != created.Status || string(got.Input) != string(created.Input) || got.Attempt != created.Attempt || !got.CreatedAt.Equal(created.CreatedAt) {
				t.Fatalf("step = %+v, created %+v", got, created)
			}
		})
	}
	t.Run("missing step", func(t *testing.T) {
		runID := sdkRun(t, world, "test-workflow", []byte(`[]`))
		_, err := world.StepsGet(ctx, worlds.StepsGetInput{RunID: runID, StepID: "nonexistent_step"})
		conformExpect(t, err, worlds.ErrStepNotFound, "missing step")
	})
	t.Run("step_started", func(t *testing.T) {
		runID := sdkRun(t, world, "test-workflow", []byte(`[]`))
		sdkStep(t, world, runID, "step_123", "test-step", []byte(`[1]`))
		conformAppend(t, world, runID, worlds.EventTypeStepStarted, "step_123", nil)
		if step := get(t, runID, "step_123"); step.Status != worlds.StatusRunning || step.StartedAt == nil || step.Attempt != 1 {
			t.Fatalf("started step = %+v", step)
		}
	})
	t.Run("step_completed", func(t *testing.T) {
		runID := sdkRun(t, world, "test-workflow", []byte(`[]`))
		sdkStep(t, world, runID, "step_123", "test-step", []byte(`[1]`))
		conformAppend(t, world, runID, worlds.EventTypeStepCompleted, "step_123", worlds.DataStepResult{Result: []byte(`[1]`)})
		if step := get(t, runID, "step_123"); step.Status != worlds.StatusCompleted || string(step.Output) != `[1]` || step.CompletedAt == nil {
			t.Fatalf("completed step = %+v", step)
		}
	})
	t.Run("step_failed", func(t *testing.T) {
		runID := sdkRun(t, world, "test-workflow", []byte(`[]`))
		sdkStep(t, world, runID, "step_123", "test-step", []byte(`[1]`))
		serialized := []byte(`[1,2,3]`)
		conformAppend(t, world, runID, worlds.EventTypeStepFailed, "step_123", worlds.DataStepResult{Error: serialized})
		if step := get(t, runID, "step_123"); step.Status != worlds.StatusFailed || string(step.Error) != string(serialized) || step.CompletedAt == nil {
			t.Fatalf("failed step = %+v", step)
		}
	})
}

// SDK: packages/world-local/src/storage.test.ts > Storage > steps > list > should list all steps for a run
// SDK: packages/world-local/src/storage.test.ts > Storage > steps > list > should support pagination
// SDK: packages/world-local/src/storage.test.ts > Storage > steps > list > should handle pagination when new items are created after getting a cursor
// SDK: packages/world-local/src/storage.test.ts > Storage > steps > list > should handle pagination with cursor after items are added mid-pagination
// SDK: packages/world-local/src/storage.test.ts > Storage > steps > list > should reproduce GitHub issue #298: pagination after reaching the end and creating new items
func conformSDKStepsList(t *testing.T, world worlds.World) {
	ctx := context.Background()
	list := func(t *testing.T, runID worlds.RunID, pagination worlds.Pagination) worlds.Page[worlds.Step] {
		t.Helper()
		page, err := world.StepsList(ctx, worlds.StepsListInput{RunID: runID, Pagination: pagination})
		if err != nil {
			t.Fatal(err)
		}
		return page
	}
	create := func(t *testing.T, runID worlds.RunID, from, to int) {
		t.Helper()
		for i := from; i < to; i++ {
			sdkStep(t, world, runID, fmt.Sprintf("step_%d", i), fmt.Sprintf("step-%d", i), []byte(`[]`))
			// Steps list in creation order, which a World keeps to the
			// millisecond.
			time.Sleep(2 * time.Millisecond)
		}
	}
	ids := func(steps []worlds.Step) []worlds.StepID {
		ids := make([]worlds.StepID, len(steps))
		for index, step := range steps {
			ids[index] = step.StepID
		}
		return ids
	}
	t.Run("newest first", func(t *testing.T) {
		runID := sdkRun(t, world, "test-workflow", []byte(`[]`))
		create(t, runID, 1, 3)
		page := list(t, runID, worlds.Pagination{})
		if !slices.Equal(ids(page.Data), []worlds.StepID{"step_2", "step_1"}) || page.Data[0].CreatedAt.Before(page.Data[1].CreatedAt) {
			t.Fatalf("steps = %+v", page.Data)
		}
	})
	t.Run("pagination", func(t *testing.T) {
		runID := sdkRun(t, world, "test-workflow", []byte(`[]`))
		create(t, runID, 0, 5)
		first := list(t, runID, worlds.Pagination{Limit: 2})
		if len(first.Data) != 2 || first.Cursor == "" {
			t.Fatalf("first page = %+v", first)
		}
		second := list(t, runID, worlds.Pagination{Limit: 2, Cursor: first.Cursor})
		if len(second.Data) != 2 || second.Data[0].StepID == first.Data[0].StepID {
			t.Fatalf("second page = %+v", second)
		}
	})
	t.Run("new items after a cursor", func(t *testing.T) {
		runID := sdkRun(t, world, "test-workflow", []byte(`[]`))
		create(t, runID, 0, 4)
		first := list(t, runID, worlds.Pagination{Limit: 4})
		if len(first.Data) != 4 || first.HasMore || first.Cursor == "" {
			t.Fatalf("first page = %+v", first)
		}
		create(t, runID, 4, 8)
		newest := list(t, runID, worlds.Pagination{Limit: 4})
		if len(newest.Data) != 4 || !newest.HasMore {
			t.Fatalf("newest page = %+v", newest)
		}
		older := list(t, runID, worlds.Pagination{Limit: 4, Cursor: newest.Cursor})
		if len(older.Data) != 4 || older.HasMore {
			t.Fatalf("older page = %+v", older)
		}
		for _, id := range ids(older.Data) {
			if slices.Contains(ids(newest.Data), id) {
				t.Fatalf("step %s on both pages", id)
			}
		}
	})
	t.Run("new items mid-pagination", func(t *testing.T) {
		runID := sdkRun(t, world, "test-workflow", []byte(`[]`))
		create(t, runID, 0, 4)
		first := list(t, runID, worlds.Pagination{Limit: 2})
		if len(first.Data) != 2 || !first.HasMore {
			t.Fatalf("first page = %+v", first)
		}
		second := list(t, runID, worlds.Pagination{Limit: 2, Cursor: first.Cursor})
		if len(second.Data) != 2 || second.HasMore || second.Cursor == "" {
			t.Fatalf("second page = %+v", second)
		}
		create(t, runID, 4, 8)
		if third := list(t, runID, worlds.Pagination{Limit: 2, Cursor: second.Cursor}); len(third.Data) != 0 || third.HasMore {
			t.Fatalf("page after the end = %+v", third)
		}
		if retry := list(t, runID, worlds.Pagination{Limit: 2, Cursor: first.Cursor}); !slices.Equal(ids(retry.Data), ids(second.Data)) {
			t.Fatalf("second page again = %v, want %v", ids(retry.Data), ids(second.Data))
		}
	})
	t.Run("issue 298", func(t *testing.T) {
		runID := sdkRun(t, world, "test-workflow", []byte(`[]`))
		create(t, runID, 0, 4)
		first := list(t, runID, worlds.Pagination{Limit: 4})
		if len(first.Data) != 4 || first.HasMore || first.Cursor == "" {
			t.Fatalf("first page = %+v", first)
		}
		create(t, runID, 4, 8)
		if next := list(t, runID, worlds.Pagination{Limit: 4, Cursor: first.Cursor}); len(next.Data) != 0 || next.HasMore {
			t.Fatalf("page after the end = %+v", next)
		}
		fresh := list(t, runID, worlds.Pagination{Limit: 4})
		if !fresh.HasMore || !slices.Equal(ids(fresh.Data), []worlds.StepID{"step_7", "step_6", "step_5", "step_4"}) {
			t.Fatalf("fresh page = %+v", fresh)
		}
		if second := list(t, runID, worlds.Pagination{Limit: 4, Cursor: fresh.Cursor}); !slices.Equal(ids(second.Data), []worlds.StepID{"step_3", "step_2", "step_1", "step_0"}) {
			t.Fatalf("second page = %v", ids(second.Data))
		}
	})
}

// SDK: packages/world-local/src/storage.test.ts > Storage > steps > lazy step start > creates the step on the fly when step_started carries input
// SDK: packages/world-local/src/storage.test.ts > Storage > steps > lazy step start > records a single-orchestrator step created by its start alone, keeping the input on it
// SDK: packages/world-local/src/storage.test.ts > Storage > steps > lazy step start > still rejects a bare step_started (no input) on a missing step
// SDK: packages/world-local/src/storage.test.ts > Storage > steps > lazy step start > rejects a lazy step_started on a terminal run
// SDK: packages/world-local/src/storage.test.ts > Storage > steps > lazy step start > rejects a second lazy step_started for an existing step (concurrent loser)
// SDK: packages/world-local/src/storage.test.ts > Storage > steps > lazy step start > crash recovery re-starts via a non-lazy step_started on the existing step
// SDK: packages/world-local/src/storage.test.ts > Storage > steps > lazy step start > a lazy step_started followed by step_failed marks the step failed
//
// A Go World creates a step from its step_started when the step_started is
// an inline step's first, whose DataStepStarted has Inline, StepName and
// Input.
func conformSDKLazySteps(t *testing.T, world worlds.World) {
	ctx := context.Background()
	lazy := func(runID worlds.RunID, stepID string, input []byte) (worlds.EventResult, error) {
		return sdkTry(world, runID, worlds.EventTypeStepStarted, stepID, worlds.DataStepStarted{StepName: "lazy-step", Attempt: 1, Input: input, Inline: true})
	}
	get := func(t *testing.T, runID worlds.RunID, stepID worlds.StepID) worlds.Step {
		t.Helper()
		step, err := world.StepsGet(ctx, worlds.StepsGetInput{RunID: runID, StepID: stepID})
		if err != nil {
			t.Fatal(err)
		}
		return step
	}
	t.Run("created on the fly", func(t *testing.T) {
		runID := sdkRun(t, world, "test-workflow", []byte(`[]`))
		result, err := lazy(runID, "lazy_step_1", []byte(`[7,8,9]`))
		if err != nil || result.AllocatedEventCount != 1 {
			t.Fatalf("lazy step_started = %+v err = %v", result, err)
		}
		if step := get(t, runID, "lazy_step_1"); step.StepName != "lazy-step" || step.Status != worlds.StatusRunning || step.Attempt != 1 || string(step.Input) != `[7,8,9]` {
			t.Fatalf("lazy step = %+v", step)
		}
	})
	t.Run("created by its start alone", func(t *testing.T) {
		runID := sdkRun(t, world, "test-workflow", []byte(`[]`))
		if _, err := lazy(runID, "lazy_step_2", []byte(`[1]`)); err != nil {
			t.Fatal(err)
		}
		page, err := world.EventsListByCorrelationID(ctx, worlds.EventsListByCorrelationIDInput{RunID: runID, CorrelationID: "lazy_step_2", ResolveData: worlds.EventsResolveDataAll})
		if err != nil || !slices.Equal(sdkTypes(page.Data), []worlds.EventType{worlds.EventTypeStepStarted}) {
			t.Fatalf("events = %+v err = %v", page.Data, err)
		}
		var data worlds.DataStepStarted
		if json.Unmarshal(page.Data[0].Data, &data) != nil || string(data.Input) != `[1]` || !data.Inline {
			t.Fatalf("step_started data = %s", page.Data[0].Data)
		}
		if step := get(t, runID, "lazy_step_2"); string(step.Input) != `[1]` {
			t.Fatalf("step input = %s", step.Input)
		}
	})
	t.Run("bare step_started of a missing step", func(t *testing.T) {
		runID := sdkRun(t, world, "test-workflow", []byte(`[]`))
		_, err := sdkTry(world, runID, worlds.EventTypeStepStarted, "never_created", worlds.DataStepStarted{StepName: "legacy-step"})
		conformExpect(t, err, worlds.ErrStepNotFound, "bare step_started of a missing step")
	})
	t.Run("terminal run", func(t *testing.T) {
		runID := sdkStartedRun(t, world)
		conformAppend(t, world, runID, worlds.EventTypeRunCompleted, "", worlds.DataRunCompleted{Output: []byte(`[1]`)})
		_, err := lazy(runID, "lazy_on_terminal", []byte(`[1]`))
		conformExpectType[*worlds.RunExpiredError](t, err, "lazy step_started on a finished run")
	})
	t.Run("second lazy step_started", func(t *testing.T) {
		runID := sdkRun(t, world, "test-workflow", []byte(`[]`))
		if first, err := lazy(runID, "lazy_step_3", []byte(`[1]`)); err != nil || first.AllocatedEventCount != 1 {
			t.Fatalf("first lazy step_started = %+v err = %v", first, err)
		}
		if step := get(t, runID, "lazy_step_3"); step.Attempt != 1 {
			t.Fatalf("step = %+v", step)
		}
		_, err := lazy(runID, "lazy_step_3", []byte(`[1]`))
		conformExpect(t, err, worlds.ErrEntityConflict, "second lazy step_started")
	})
	t.Run("restart", func(t *testing.T) {
		runID := sdkRun(t, world, "test-workflow", []byte(`[]`))
		if _, err := lazy(runID, "lazy_step_4", []byte(`[1]`)); err != nil {
			t.Fatal(err)
		}
		conformAppend(t, world, runID, worlds.EventTypeStepStarted, "lazy_step_4", nil)
		if step := get(t, runID, "lazy_step_4"); step.Status != worlds.StatusRunning || step.Attempt != 2 {
			t.Fatalf("restarted step = %+v", step)
		}
	})
	t.Run("lazy step_started then step_failed", func(t *testing.T) {
		runID := sdkRun(t, world, "test-workflow", []byte(`[]`))
		if _, err := sdkTry(world, runID, worlds.EventTypeStepStarted, "lazy_step_fail", worlds.DataStepStarted{StepName: "ghost-step", Attempt: 1, Input: []byte(`[1]`), Inline: true}); err != nil {
			t.Fatal(err)
		}
		conformAppend(t, world, runID, worlds.EventTypeStepFailed, "lazy_step_fail", worlds.DataStepResult{Error: []byte(`[2,3]`)})
		if step := get(t, runID, "lazy_step_fail"); step.Status != worlds.StatusFailed || step.Attempt != 1 {
			t.Fatalf("failed step = %+v", step)
		}
	})
}

// SDK: packages/world-local/src/storage.test.ts > Storage > step terminal state validation > completed step > should reject step_started on completed step
// SDK: packages/world-local/src/storage.test.ts > Storage > step terminal state validation > completed step > should reject step_completed on already completed step
// SDK: packages/world-local/src/storage.test.ts > Storage > step terminal state validation > completed step > should reject step_failed on completed step
// SDK: packages/world-local/src/storage.test.ts > Storage > step terminal state validation > failed step > should reject step_started on failed step
// SDK: packages/world-local/src/storage.test.ts > Storage > step terminal state validation > failed step > should reject step_completed on failed step
// SDK: packages/world-local/src/storage.test.ts > Storage > step terminal state validation > failed step > should reject step_failed on already failed step
// SDK: packages/world-local/src/storage.test.ts > Storage > step terminal state validation > failed step > should reject step_retrying on failed step
// SDK: packages/world-local/src/storage.test.ts > Storage > step terminal state validation > step_retrying validation > should reject step_retrying on completed step
// SDK: packages/world-local/src/storage.test.ts > Storage > step_retrying event handling > should reject step_retrying on completed step
// SDK: packages/world-local/src/storage.test.ts > Storage > step_retrying event handling > should reject step_retrying on failed step
// SDK: packages/world-local/src/storage.test.ts > Storage > concurrent terminal state races > should reject step_started after concurrent step_completed
func conformSDKFinishedSteps(t *testing.T, world worlds.World) {
	finish := map[worlds.EventType]worlds.DataStepResult{
		worlds.EventTypeStepCompleted: {Result: []byte(`[1]`)},
		worlds.EventTypeStepFailed:    {Error: []byte(`"Failed permanently"`)},
	}
	next := map[worlds.EventType]any{
		worlds.EventTypeStepStarted:   nil,
		worlds.EventTypeStepCompleted: worlds.DataStepResult{Result: []byte(`[2]`)},
		worlds.EventTypeStepFailed:    worlds.DataStepResult{Error: []byte(`"Should not work"`)},
		worlds.EventTypeStepRetrying:  worlds.DataStepResult{Error: []byte(`"Retry attempt"`)},
	}
	for _, ended := range []worlds.EventType{worlds.EventTypeStepCompleted, worlds.EventTypeStepFailed} {
		for _, eventType := range []worlds.EventType{worlds.EventTypeStepStarted, worlds.EventTypeStepCompleted, worlds.EventTypeStepFailed, worlds.EventTypeStepRetrying} {
			t.Run(fmt.Sprintf("%s on a step after %s", eventType, ended), func(t *testing.T) {
				runID := sdkStartedRun(t, world)
				sdkStep(t, world, runID, "step_terminal", "test-step", []byte(`[]`))
				conformAppend(t, world, runID, ended, "step_terminal", finish[ended])
				_, err := sdkTry(world, runID, eventType, "step_terminal", next[eventType])
				conformExpect(t, err, worlds.ErrEntityConflict, string(eventType)+" on a finished step")
			})
		}
	}
}

// SDK: packages/world-local/src/storage.test.ts > Storage > step_retrying event handling > should set step status to pending and record error
// SDK: packages/world-local/src/storage.test.ts > Storage > step_retrying event handling > should increment attempt when step_started is called after step_retrying
// SDK: packages/world-local/src/storage.test.ts > Storage > event ordering validation > should reject step_completed before step_created
// SDK: packages/world-local/src/storage.test.ts > Storage > event ordering validation > should reject step_started before step_created
// SDK: packages/world-local/src/storage.test.ts > Storage > event ordering validation > should reject step_failed before step_created
// SDK: packages/world-local/src/storage.test.ts > Storage > event ordering validation > should allow step_completed without step_started (instant completion)
// SDK: packages/world-local/src/storage.test.ts > Storage > event ordering validation > should reject hook_disposed before hook_created
// SDK: packages/world-local/src/storage.test.ts > Storage > event ordering validation > should reject hook_received before hook_created
func conformSDKStepOrdering(t *testing.T, world worlds.World) {
	ctx := context.Background()
	get := func(t *testing.T, runID worlds.RunID, stepID worlds.StepID) worlds.Step {
		t.Helper()
		step, err := world.StepsGet(ctx, worlds.StepsGetInput{RunID: runID, StepID: stepID})
		if err != nil {
			t.Fatal(err)
		}
		return step
	}
	t.Run("step_retrying", func(t *testing.T) {
		runID := sdkRun(t, world, "test-workflow", []byte(`[]`))
		sdkStep(t, world, runID, "step_retry_1", "test-step", []byte(`[]`))
		conformAppend(t, world, runID, worlds.EventTypeStepStarted, "step_retry_1", nil)
		serialized := []byte(`[9,9,9]`)
		retryAfter := time.Now().Add(5 * time.Second).UTC()
		conformAppend(t, world, runID, worlds.EventTypeStepRetrying, "step_retry_1", worlds.DataStepResult{Error: serialized, RetryAfter: &retryAfter})
		if step := get(t, runID, "step_retry_1"); step.Status != worlds.StatusPending || string(step.Error) != string(serialized) || step.RetryAfter == nil {
			t.Fatalf("retrying step = %+v", step)
		}
	})
	t.Run("attempt after a retry", func(t *testing.T) {
		runID := sdkRun(t, world, "test-workflow", []byte(`[]`))
		sdkStep(t, world, runID, "step_retry_2", "test-step", []byte(`[]`))
		conformAppend(t, world, runID, worlds.EventTypeStepStarted, "step_retry_2", nil)
		if step := get(t, runID, "step_retry_2"); step.Attempt != 1 {
			t.Fatalf("first attempt = %d", step.Attempt)
		}
		conformAppend(t, world, runID, worlds.EventTypeStepRetrying, "step_retry_2", worlds.DataStepResult{Error: []byte(`"Temporary failure"`)})
		conformAppend(t, world, runID, worlds.EventTypeStepStarted, "step_retry_2", nil)
		if step := get(t, runID, "step_retry_2"); step.Attempt != 2 {
			t.Fatalf("second attempt = %d", step.Attempt)
		}
	})
	for _, before := range []struct {
		eventType worlds.EventType
		data      any
	}{
		{worlds.EventTypeStepCompleted, worlds.DataStepResult{Result: []byte(`[1]`)}},
		{worlds.EventTypeStepStarted, nil},
		{worlds.EventTypeStepFailed, worlds.DataStepResult{Error: []byte(`"Failed"`)}},
	} {
		t.Run(string(before.eventType)+" before step_created", func(t *testing.T) {
			runID := sdkRun(t, world, "test-workflow", []byte(`[]`))
			_, err := sdkTry(world, runID, before.eventType, "nonexistent_step", before.data)
			conformExpect(t, err, worlds.ErrStepNotFound, string(before.eventType)+" before step_created")
		})
	}
	t.Run("instant completion", func(t *testing.T) {
		runID := sdkRun(t, world, "test-workflow", []byte(`[]`))
		sdkStep(t, world, runID, "instant_complete", "test-step", []byte(`[]`))
		conformAppend(t, world, runID, worlds.EventTypeStepCompleted, "instant_complete", worlds.DataStepResult{Result: []byte(`[1]`)})
		if step := get(t, runID, "instant_complete"); step.Status != worlds.StatusCompleted {
			t.Fatalf("step = %+v", step)
		}
	})
	t.Run("hook events before hook_created", func(t *testing.T) {
		runID := sdkRun(t, world, "test-workflow", []byte(`[]`))
		_, err := sdkTry(world, runID, worlds.EventTypeHookDisposed, "nonexistent_hook", nil)
		conformExpectType[*worlds.HookNotFoundError](t, err, "hook_disposed before hook_created")
		_, err = sdkTry(world, runID, worlds.EventTypeHookReceived, "nonexistent_hook_received", worlds.DataHookReceived{Payload: []byte(`{}`)})
		conformExpectType[*worlds.HookNotFoundError](t, err, "hook_received before hook_created")
	})
}

// SDK: packages/world-local/src/storage.test.ts > Storage > concurrent terminal state races > should reject concurrent step_completed for the same step
// SDK: packages/world-local/src/storage.test.ts > Storage > concurrent terminal state races > should reject concurrent step_failed for the same step
// SDK: packages/world-local/src/storage.test.ts > Storage > concurrent terminal state races > should reject concurrent wait_completed for the same wait
// SDK: packages/world-local/src/storage.test.ts > Storage > concurrent terminal state races > should reject concurrent hook_disposed for the same hook
// SDK: packages/world-local/src/storage.test.ts > Storage > concurrent entity-creation races > should reject concurrent step_created with the same correlationId
// SDK: packages/world-local/src/storage.test.ts > Storage > concurrent entity-creation races > should reject concurrent wait_created with the same correlationId
func conformSDKRaces(t *testing.T, world worlds.World) {
	// race runs two writes at once and returns their errors, the accepted
	// one's nil.
	race := func(first, second func() error) []error {
		errs := make([]error, 2)
		var group sync.WaitGroup
		group.Go(func() { errs[0] = first() })
		group.Go(func() { errs[1] = second() })
		group.Wait()
		return errs
	}
	write := func(runID worlds.RunID, eventType worlds.EventType, correlationID string, data any) func() error {
		return func() error {
			_, err := sdkTry(world, runID, eventType, correlationID, data)
			return err
		}
	}
	// loser returns the error of the one refused write.
	loser := func(t *testing.T, errs []error) error {
		t.Helper()
		var refused []error
		for _, err := range errs {
			if err != nil {
				refused = append(refused, err)
			}
		}
		if len(refused) != 1 {
			t.Fatalf("refused writes = %v, want one", errs)
		}
		return refused[0]
	}
	for _, eventType := range []worlds.EventType{worlds.EventTypeStepCompleted, worlds.EventTypeStepFailed} {
		t.Run("concurrent "+string(eventType), func(t *testing.T) {
			runID := sdkStartedRun(t, world)
			sdkStep(t, world, runID, "step_race", "test-step", []byte(`[]`))
			conformAppend(t, world, runID, worlds.EventTypeStepStarted, "step_race", nil)
			data := func(n int) worlds.DataStepResult {
				if eventType == worlds.EventTypeStepCompleted {
					return worlds.DataStepResult{Result: []byte(fmt.Sprintf("[%d]", n))}
				}
				return worlds.DataStepResult{Error: []byte(fmt.Sprintf(`"err%d"`, n))}
			}
			err := loser(t, race(write(runID, eventType, "step_race", data(1)), write(runID, eventType, "step_race", data(2))))
			conformExpect(t, err, worlds.ErrEntityConflict, "the losing "+string(eventType))
		})
	}
	t.Run("concurrent wait_completed", func(t *testing.T) {
		runID := sdkStartedRun(t, world)
		conformAppend(t, world, runID, worlds.EventTypeWaitCreated, "wait_race_1", worlds.DataWaitCreated{ResumeAt: time.Date(2099, time.January, 1, 0, 0, 0, 0, time.UTC)})
		err := loser(t, race(write(runID, worlds.EventTypeWaitCompleted, "wait_race_1", nil), write(runID, worlds.EventTypeWaitCompleted, "wait_race_1", nil)))
		conformExpect(t, err, worlds.ErrEntityConflict, "the losing wait_completed")
	})
	t.Run("concurrent hook_disposed", func(t *testing.T) {
		runID := sdkStartedRun(t, world)
		conformAppend(t, world, runID, worlds.EventTypeHookCreated, "hook_race_1", worlds.DataHookCreated{Token: "hook-race-token-1:" + string(runID)})
		err := loser(t, race(write(runID, worlds.EventTypeHookDisposed, "hook_race_1", nil), write(runID, worlds.EventTypeHookDisposed, "hook_race_1", nil)))
		if !errors.Is(err, worlds.ErrEntityConflict) && !is[*worlds.HookNotFoundError](err) {
			t.Fatalf("the losing hook_disposed = %v, want a conflict or HookNotFoundError", err)
		}
		if count := sdkCount(t, world, runID, worlds.EventTypeHookDisposed); count != 1 {
			t.Fatalf("hook_disposed events = %d, want 1", count)
		}
	})
	t.Run("concurrent step_created", func(t *testing.T) {
		runID := sdkStartedRun(t, world)
		err := loser(t, race(
			write(runID, worlds.EventTypeStepCreated, "step_dup_1", worlds.DataStepCreated{StepName: "test-step", Input: []byte(`[1]`)}),
			write(runID, worlds.EventTypeStepCreated, "step_dup_1", worlds.DataStepCreated{StepName: "test-step", Input: []byte(`[2]`)}),
		))
		conformExpect(t, err, worlds.ErrEntityConflict, "the losing step_created")
		if count := sdkCount(t, world, runID, worlds.EventTypeStepCreated); count != 1 {
			t.Fatalf("step_created events = %d, want 1", count)
		}
	})
	t.Run("concurrent wait_created", func(t *testing.T) {
		runID := sdkStartedRun(t, world)
		err := loser(t, race(
			write(runID, worlds.EventTypeWaitCreated, "wait_dup_1", worlds.DataWaitCreated{ResumeAt: time.Date(2099, time.January, 1, 0, 0, 0, 0, time.UTC)}),
			write(runID, worlds.EventTypeWaitCreated, "wait_dup_1", worlds.DataWaitCreated{ResumeAt: time.Date(2099, time.January, 2, 0, 0, 0, 0, time.UTC)}),
		))
		conformExpect(t, err, worlds.ErrEntityConflict, "the losing wait_created")
		if count := sdkCount(t, world, runID, worlds.EventTypeWaitCreated); count != 1 {
			t.Fatalf("wait_created events = %d, want 1", count)
		}
	})
}

// SDK: packages/world-local/src/storage.test.ts > Storage > terminal-run step_started fencing > %s > rejects restarting a running step but accepts %s
// SDK: packages/world-local/src/storage.test.ts > Storage > allowed operations on terminal runs > should allow step_completed on completed run for in-progress step
// SDK: packages/world-local/src/storage.test.ts > Storage > allowed operations on terminal runs > should allow step_failed on completed run for in-progress step
// SDK: packages/world-local/src/storage.test.ts > Storage > allowed operations on terminal runs > should auto-delete hooks when run completes (world-local specific behavior)
// SDK: packages/world-local/src/storage.test.ts > Storage > disallowed operations on terminal runs > should reject step_created on completed run
// SDK: packages/world-local/src/storage.test.ts > Storage > disallowed operations on terminal runs > should reject step_started on completed run for pending step
// SDK: packages/world-local/src/storage.test.ts > Storage > disallowed operations on terminal runs > should reject hook_created on completed run
// SDK: packages/world-local/src/storage.test.ts > Storage > disallowed operations on terminal runs > should reject attr_set on completed run
// SDK: packages/world-local/src/storage.test.ts > Storage > disallowed operations on terminal runs > should reject step_created on failed run
// SDK: packages/world-local/src/storage.test.ts > Storage > disallowed operations on terminal runs > should reject step_created on cancelled run
// SDK: packages/world-local/src/storage.test.ts > Storage > disallowed operations on terminal runs > should reject hook_created on failed run
// SDK: packages/world-local/src/storage.test.ts > Storage > disallowed operations on terminal runs > should reject hook_created on cancelled run
// SDK: packages/world-local/src/storage.test.ts > Storage > disallowed operations on terminal runs > should reject hook_received on a completed run
// SDK: packages/world-local/src/storage.test.ts > Storage > run cancellation with in-flight entities > should allow in-progress step to complete after run cancelled
// SDK: packages/world-local/src/storage.test.ts > Storage > run cancellation with in-flight entities > should reject step_created after run cancelled
// SDK: packages/world-local/src/storage.test.ts > Storage > run cancellation with in-flight entities > should reject step_started for pending step after run cancelled
func conformSDKFinishedRunSteps(t *testing.T, world worlds.World) {
	ctx := context.Background()
	ends := map[worlds.EventType]any{
		worlds.EventTypeRunCompleted: worlds.DataRunCompleted{Output: []byte(`[3]`)},
		worlds.EventTypeRunFailed:    worlds.DataRunFailed{Error: []byte(`"run failed"`)},
		worlds.EventTypeRunCancelled: nil,
	}
	endings := []worlds.EventType{worlds.EventTypeRunCompleted, worlds.EventTypeRunFailed, worlds.EventTypeRunCancelled}
	getStep := func(t *testing.T, runID worlds.RunID, stepID worlds.StepID) worlds.Step {
		t.Helper()
		step, err := world.StepsGet(ctx, worlds.StepsGetInput{RunID: runID, StepID: stepID})
		if err != nil {
			t.Fatal(err)
		}
		return step
	}
	for _, end := range endings {
		for _, settle := range []struct {
			eventType worlds.EventType
			data      worlds.DataStepResult
			status    worlds.Status
		}{
			{worlds.EventTypeStepCompleted, worlds.DataStepResult{Result: []byte(`[1]`)}, worlds.StatusCompleted},
			{worlds.EventTypeStepFailed, worlds.DataStepResult{Error: []byte(`"step failed"`)}, worlds.StatusFailed},
		} {
			t.Run(fmt.Sprintf("%s then a restart and %s", end, settle.eventType), func(t *testing.T) {
				runID := sdkStartedRun(t, world)
				sdkStep(t, world, runID, "step_in_progress", "test-step", []byte(`[]`))
				conformAppend(t, world, runID, worlds.EventTypeStepStarted, "step_in_progress", nil)
				started := getStep(t, runID, "step_in_progress")
				if started.Status != worlds.StatusRunning {
					t.Fatalf("started step = %+v", started)
				}
				conformAppend(t, world, runID, end, "", ends[end])
				ended, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: runID})
				if err != nil {
					t.Fatal(err)
				}
				before := conformAllEvents(t, world, runID)
				_, err = sdkTry(world, runID, worlds.EventTypeStepStarted, "step_in_progress", nil)
				conformExpectType[*worlds.RunExpiredError](t, err, "restarting a running step of a finished run")
				if step := getStep(t, runID, "step_in_progress"); step.Status != started.Status || step.Attempt != started.Attempt || !step.UpdatedAt.Equal(started.UpdatedAt) {
					t.Fatalf("step after a refused restart = %+v, want %+v", step, started)
				}
				if after := conformAllEvents(t, world, runID); len(after) != len(before) {
					t.Fatalf("events after a refused restart = %d, want %d", len(after), len(before))
				}
				conformAppend(t, world, runID, settle.eventType, "step_in_progress", settle.data)
				if step := getStep(t, runID, "step_in_progress"); step.Status != settle.status || step.Attempt != started.Attempt {
					t.Fatalf("settled step = %+v", step)
				}
				if run, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: runID}); err != nil || run.Status != ended.Status || !run.UpdatedAt.Equal(ended.UpdatedAt) {
					t.Fatalf("run after the step settled = %+v, want %+v (err %v)", run, ended, err)
				}
				after := conformAllEvents(t, world, runID)
				if last := after[len(after)-1]; len(after) != len(before)+1 || last.EventType != settle.eventType || last.CorrelationID != "step_in_progress" {
					t.Fatalf("events after the step settled = %v", sdkTypes(after))
				}
			})
		}
	}
	for _, settle := range []worlds.EventType{worlds.EventTypeStepCompleted, worlds.EventTypeStepFailed} {
		for _, end := range []worlds.EventType{worlds.EventTypeRunCompleted, worlds.EventTypeRunCancelled} {
			t.Run(fmt.Sprintf("%s of a running step after %s", settle, end), func(t *testing.T) {
				runID := sdkRun(t, world, "test-workflow", []byte(`[]`))
				sdkStep(t, world, runID, "step_in_progress", "test-step", []byte(`[]`))
				conformAppend(t, world, runID, worlds.EventTypeStepStarted, "step_in_progress", nil)
				conformAppend(t, world, runID, end, "", ends[end])
				data := worlds.DataStepResult{Result: []byte(`[1]`)}
				want := worlds.StatusCompleted
				if settle == worlds.EventTypeStepFailed {
					data, want = worlds.DataStepResult{Error: []byte(`"step failed"`)}, worlds.StatusFailed
				}
				conformAppend(t, world, runID, settle, "step_in_progress", data)
				if step := getStep(t, runID, "step_in_progress"); step.Status != want {
					t.Fatalf("step = %+v", step)
				}
			})
		}
	}
	t.Run("hooks end with the run", func(t *testing.T) {
		runID := sdkRun(t, world, "test-workflow", []byte(`[]`))
		conformAppend(t, world, runID, worlds.EventTypeHookCreated, "hook_auto_delete", worlds.DataHookCreated{Token: "test-token-auto-delete:" + string(runID)})
		if _, err := world.HooksGet(ctx, worlds.HooksGetInput{HookID: "hook_auto_delete"}); err != nil {
			t.Fatal(err)
		}
		conformAppend(t, world, runID, worlds.EventTypeRunCompleted, "", ends[worlds.EventTypeRunCompleted])
		_, err := world.HooksGet(ctx, worlds.HooksGetInput{HookID: "hook_auto_delete"})
		conformExpectType[*worlds.HookNotFoundError](t, err, "hook of a completed run")
	})
	for _, end := range endings {
		t.Run("creations after "+string(end), func(t *testing.T) {
			runID := sdkRun(t, world, "test-workflow", []byte(`[]`))
			sdkStep(t, world, runID, "pending_step", "test-step", []byte(`[]`))
			conformAppend(t, world, runID, end, "", ends[end])
			_, err := sdkTry(world, runID, worlds.EventTypeStepCreated, "new_step", worlds.DataStepCreated{StepName: "test-step", Input: []byte(`[]`)})
			conformExpectType[*worlds.RunExpiredError](t, err, "step_created after "+string(end))
			_, err = sdkTry(world, runID, worlds.EventTypeHookCreated, "new_hook", worlds.DataHookCreated{Token: "new-token:" + string(runID)})
			conformExpectType[*worlds.RunExpiredError](t, err, "hook_created after "+string(end))
			_, err = sdkTry(world, runID, worlds.EventTypeStepStarted, "pending_step", nil)
			conformExpectType[*worlds.RunExpiredError](t, err, "step_started of a pending step after "+string(end))
			phase := "too-late"
			_, err = sdkTry(world, runID, worlds.EventTypeAttrSet, "attr_after_complete", worlds.DataAttrSet{Changes: []worlds.RunsAttributeChange{{Key: "phase", Value: &phase}}, Writer: worlds.AttributeWriter{Type: worlds.AttributeWriterWorkflow}})
			conformExpectType[*worlds.RunExpiredError](t, err, "attr_set after "+string(end))
		})
	}
	t.Run("hook_received after the run completed", func(t *testing.T) {
		runID := sdkRun(t, world, "test-workflow", []byte(`[]`))
		conformAppend(t, world, runID, worlds.EventTypeHookCreated, "hook_before_complete", worlds.DataHookCreated{Token: "token-before-complete:" + string(runID)})
		conformAppend(t, world, runID, worlds.EventTypeRunCompleted, "", ends[worlds.EventTypeRunCompleted])
		_, err := sdkTry(world, runID, worlds.EventTypeHookReceived, "hook_before_complete", worlds.DataHookReceived{Payload: []byte(`{}`)})
		conformExpectType[*worlds.HookNotFoundError](t, err, "hook_received after the run completed")
	})
}
