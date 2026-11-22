package localworld

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

// bootedAt is when this process loaded the package. A step started before
// it was started by a process that is gone.
var bootedAt = time.Now()

// orphanedStepMessage is the error a released step's attempt records.
const orphanedStepMessage = "The process running this step exited before it finished."

// maxBackstopDelay is the furthest wait a recovered run's backstop wake is
// queued for, the longest timer delay. A later wait is re-armed by the run's
// recovery flow alone.
const maxBackstopDelay = 2147483647 * time.Millisecond

// recoverActiveRuns queues every active run of the World's own tag again:
// the queue is in memory, so a restart lost
// every message a run had queued. A run's steps that were running inline in
// a previous process are released first, its recovery flow sends again its
// step messages and wait timers, and each of its open waits due within the
// longest timer gets a backstop wake. Recovery assumes no other live process
// runs workflows on the directory and tag.
func (w *World) recoverActiveRuns(ctx context.Context) error {
	var runs []runRecord
	if err := w.read(ctx, func(s *store) error {
		var err error
		runs, err = s.listRuns(runFilter{active: true, ownTag: true})
		return err
	}); err != nil {
		return err
	}
	for _, run := range runs {
		if err := w.releaseOrphanedSteps(ctx, run.RunID); err != nil {
			log.Printf("workflow/local: release orphaned steps of run %s: %v", run.RunID, err)
		}
		flow := worlds.QueueInput{RunID: run.RunID, WorkflowName: run.WorkflowName, DeploymentID: run.DeploymentID, RunSpecVersion: run.SpecVersion, Recovery: true}
		if _, err := w.Queue(ctx, flow); err != nil {
			return err
		}
		var waits []worlds.Wait
		if err := w.read(ctx, func(s *store) error {
			var err error
			waits, err = s.openWaits(run.RunID)
			return err
		}); err != nil {
			return err
		}
		now := time.Now()
		for _, wait := range waits {
			resumeAt := now
			if wait.ResumeAt != nil {
				resumeAt = *wait.ResumeAt
			}
			if resumeAt.Sub(now) > maxBackstopDelay {
				continue
			}
			wake := worlds.QueueInput{RunID: run.RunID, WorkflowName: run.WorkflowName, DeploymentID: run.DeploymentID, RunSpecVersion: run.SpecVersion, NotBefore: &resumeAt, IdempotencyKey: fmt.Sprintf("recovery-wait:%s:%s:%d", run.RunID, wait.WaitID, resumeAt.UnixMilli())}
			if _, err := w.Queue(ctx, wake); err != nil {
				log.Printf("workflow/local: re-arm wait %s of run %s: %v", wait.WaitID, run.RunID, err)
			}
		}
	}
	return nil
}

// stepOwnership is what a run's events record of a step's inline ownership.
type stepOwnership struct {
	name      string
	attempt   int
	inline    bool
	retried   bool
	finished  bool
	startedAt time.Time
}

// releaseOrphanedSteps hands every step that a previous process was running
// inline back to the queue by recording step_retrying for it: the flow that
// owned it is gone, and a recovery flow sends a step delivery only for a
// step that was retried. A step is released only while it is running, was
// created inline, has no step_retrying or result, and was last started
// before this process loaded.
func (w *World) releaseOrphanedSteps(ctx context.Context, runID worlds.RunID) error {
	var run runRecord
	var running []worlds.StepID
	var events []worlds.Event
	if err := w.read(ctx, func(s *store) error {
		var err error
		if run, err = s.getRun(runID); err != nil {
			return err
		}
		steps, err := s.listSteps(runID)
		if err != nil {
			return err
		}
		for _, step := range steps {
			if step.Status == worlds.StatusRunning {
				running = append(running, step.StepID)
			}
		}
		if len(running) == 0 || run.Status.Terminal() {
			return nil
		}
		events, err = s.readEvents(runID, 1, run.EventCount)
		return err
	}); err != nil || len(running) == 0 || run.Status.Terminal() {
		return err
	}
	owned := map[worlds.StepID]*stepOwnership{}
	for _, event := range events {
		id := worlds.StepID(event.CorrelationID)
		step := owned[id]
		switch event.EventType {
		case worlds.EventTypeStepStarted:
			if step == nil {
				step = &stepOwnership{}
				owned[id] = step
				if worlds.StartCreatesStep(event.Data) {
					var data worlds.DataStepStarted
					_ = json.Unmarshal(event.Data, &data)
					step.name, step.inline = data.StepName, true
				}
			}
			step.attempt++
			step.startedAt = event.CreatedAt
		case worlds.EventTypeStepCreated:
			if step == nil {
				var data worlds.DataStepCreated
				_ = json.Unmarshal(event.Data, &data)
				owned[id] = &stepOwnership{name: data.StepName, inline: data.Inline}
			}
		case worlds.EventTypeStepRetrying:
			if step != nil {
				step.retried = true
			}
		case worlds.EventTypeStepCompleted, worlds.EventTypeStepFailed:
			if step != nil {
				step.finished = true
			}
		}
	}
	payload, err := worlds.EncodeErrorPayload(errors.New(orphanedStepMessage), worlds.PayloadOptions{})
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	for _, id := range running {
		step := owned[id]
		if step == nil || !step.inline || step.retried || step.finished || step.startedAt.IsZero() || !step.startedAt.Before(bootedAt) {
			continue
		}
		data, err := json.Marshal(worlds.DataStepResult{StepName: step.name, Attempt: step.attempt, Error: payload, RetryAfter: &now})
		if err != nil {
			return err
		}
		_, err = w.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeStepRetrying, CorrelationID: string(id), Data: data, SpecVersion: run.SpecVersion}})
		// The step finished between the read and this write: nothing to
		// release.
		if err != nil && !errors.Is(err, worlds.ErrEntityConflict) && !errors.Is(err, worlds.ErrStepNotFound) && !is[*worlds.RunExpiredError](err) {
			return err
		}
	}
	return nil
}

// is reports whether err's chain has an error of type E.
func is[E error](err error) bool {
	_, ok := errors.AsType[E](err)
	return ok
}
