package worlds

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"time"

	"github.com/eriicafes/go-workflow/internal"
)

// Engine consumes workflow-only QueueInput deliveries. It uses event state as
// the sole admission fence.
type Engine struct {
	world   World
	runners RunnerSet
}

func NewEngine(world World, runners RunnerSet) *Engine {
	return &Engine{world: world, runners: maps.Clone(runners)}
}

// Execute handles either a flow or a targeted step when StepID is non-nil.
// A duplicate, stale, or terminal delivery is inert.
func (engine *Engine) Execute(ctx context.Context, input QueueInput) error {
	if input.StepID != nil {
		return engine.executeStep(ctx, input)
	}
	run, eventCount, output, err := engine.executeFlow(ctx, input)
	switch {
	case errors.Is(err, internal.ErrReplayComplete), errors.Is(err, internal.ErrReplaySuspended):
		return nil
	case errors.Is(err, ErrRunCancelled), errors.Is(err, ErrStepCancelled):
		return nil
	case err != nil:
		return engine.failRun(ctx, run.RunID, eventCount, err)
	case run.Status.Terminal():
		return nil
	default:
		return engine.completeRun(ctx, run.RunID, eventCount, output)
	}
}

func (engine *Engine) executeFlow(ctx context.Context, input QueueInput) (run Run, eventCount int, output json.RawMessage, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			if recoveredError, ok := recovered.(error); ok {
				err = recoveredError
				return
			}
			panic(recovered)
		}
	}()

	run, events, err := engine.loadRun(ctx, input.RunID)
	if err != nil {
		return Run{}, 0, nil, err
	}
	if run.Status.Terminal() {
		return run, 0, nil, nil
	}
	eventsCount := len(events)
	started, err := engine.world.EventsCreate(ctx, EventsCreateInput{
		RunID:   input.RunID,
		Event:   EventsCreateEvent{EventType: EventTypeRunStarted},
		Options: EventsCreateOptions{EventCount: &eventsCount},
	})
	if errors.Is(err, ErrRunTerminal) {
		run, getErr := engine.world.RunsGet(ctx, RunsGetInput{RunID: input.RunID})
		if getErr != nil {
			return Run{}, 0, nil, getErr
		}
		return run, 0, nil, nil
	}
	if err != nil {
		return run, 0, nil, err
	}
	execution := executionWithEvents(engine.world, run, events)
	execution.Merge(started)
	runner, ok := engine.runners[run.WorkflowName]
	if !ok {
		return run, 0, nil, fmt.Errorf("%w: %s", ErrWorkflowNotFound, run.WorkflowName)
	}
	ctx, stop := engine.observeCancellation(ctx, run.RunID, nil, len(execution.Events()))
	defer stop()
	ctx = context.WithValue(ctx, executionContextKey{}, execution)
	output, err = runner.Run(ctx, json.RawMessage(run.Input))
	if cause := context.Cause(ctx); cause != nil {
		return run, 0, nil, cause
	}
	return run, len(execution.Events()), output, err
}

func (engine *Engine) executeStep(ctx context.Context, input QueueInput) error {
	if input.StepID == nil {
		return nil
	}
	run, events, err := engine.loadRun(ctx, input.RunID)
	if err != nil || run.Status.Terminal() {
		return err
	}
	step, err := engine.world.StepsGet(ctx, StepsGetInput{RunID: input.RunID, StepID: *input.StepID})
	if errors.Is(err, ErrStepNotFound) || step.Status.Terminal() {
		return nil
	}
	if err != nil {
		return err
	}
	eventsCount := len(events)
	_, err = engine.world.EventsCreate(ctx, EventsCreateInput{
		RunID:   input.RunID,
		Event:   EventsCreateEvent{EventType: EventTypeStepStarted, CorrelationID: string(*input.StepID)},
		Options: EventsCreateOptions{EventCount: &eventsCount},
	})
	if errors.Is(err, ErrRunTerminal) || errors.Is(err, ErrStepNotStartable) {
		return nil
	}
	if err != nil {
		return err
	}
	ctx, stop := engine.observeCancellation(ctx, input.RunID, input.StepID, len(events)+1)
	defer stop()
	if errors.Is(context.Cause(ctx), ErrStepCancelled) {
		return nil
	}
	return err
}

func (engine *Engine) loadRun(ctx context.Context, runID RunID) (Run, []Event, error) {
	run, err := engine.world.RunsGet(ctx, RunsGetInput{RunID: runID})
	if err != nil {
		return Run{}, nil, err
	}
	// Worlds must return the full current prefix when no page size is requested.
	// A bounded event source can still page internally here, but the workflow
	// receives one fully preloaded slice.
	page, err := engine.world.EventsList(ctx, EventsListInput{RunID: runID})
	if err != nil {
		return Run{}, nil, err
	}
	events := page.Data
	for page.HasMore {
		page, err = engine.world.EventsList(ctx, EventsListInput{RunID: runID, Pagination: Pagination{Cursor: page.Cursor}})
		if err != nil {
			return Run{}, nil, err
		}
		events = append(events, page.Data...)
	}
	return run, events, nil
}

func (engine *Engine) completeRun(ctx context.Context, runID RunID, eventCount int, data []byte) error {
	_, err := engine.world.EventsCreate(ctx, EventsCreateInput{
		RunID:   runID,
		Event:   EventsCreateEvent{EventType: EventTypeRunCompleted, Data: data},
		Options: EventsCreateOptions{EventCount: &eventCount},
	})
	if errors.Is(err, ErrRunTerminal) {
		return nil
	}
	return err
}

func (engine *Engine) failRun(ctx context.Context, runID RunID, eventCount int, failure error) error {
	_, err := engine.world.EventsCreate(ctx, EventsCreateInput{
		RunID:   runID,
		Event:   EventsCreateEvent{EventType: EventTypeRunFailed, Data: []byte(failure.Error())},
		Options: EventsCreateOptions{EventCount: &eventCount},
	})
	if errors.Is(err, ErrRunTerminal) {
		return nil
	}
	return err
}

func (engine *Engine) observeCancellation(parent context.Context, runID RunID, stepID *StepID, count int) (context.Context, func()) {
	ctx, cancel := context.WithCancelCause(parent)
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		defer close(done)
		var cursor string
		if count > 0 {
			cursor = string(EventIDForSlot(uint64(count)))
		}
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				page, err := engine.world.EventsList(ctx, EventsListInput{RunID: runID, Pagination: Pagination{Cursor: cursor}})
				if err != nil {
					continue
				}
				for _, event := range page.Data {
					if event.EventType == EventTypeRunCancelled {
						cancel(ErrRunCancelled)
						return
					}
					if stepID != nil && event.EventType == EventTypeStepCancelled && event.CorrelationID == string(*stepID) {
						cancel(ErrStepCancelled)
						return
					}
				}
				if len(page.Data) > 0 {
					cursor = string(page.Data[len(page.Data)-1].EventID)
				} else if page.HasMore {
					cursor = page.Cursor
				}
			}
		}
	}()
	return ctx, func() { cancel(nil); <-done }
}
