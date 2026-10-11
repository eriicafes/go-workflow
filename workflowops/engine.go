package workflowops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"time"

	"github.com/eriicafes/go-workflow/internal"
)

// Engine replays workflow code for a leased root run or operation task.
type Engine struct {
	world   World
	runners RunnerSet
}

// NewEngine creates a worker execution engine for world and runners.
func NewEngine(world World, runners RunnerSet) *Engine {
	set := make(RunnerSet, len(runners))
	maps.Copy(set, runners)
	return &Engine{world: world, runners: set}
}

// Execute processes one World-issued lease.
func (engine *Engine) Execute(ctx context.Context, leased *Lease) error {
	runner, ok := engine.runners[leased.Identity]
	if !ok {
		return engine.world.FailRun(ctx, leased, fmt.Errorf("%w: %s", ErrUnknownWorkflow, leased.Identity))
	}
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	stop := engine.heartbeat(ctx, leased, cancel)
	defer stop()
	execution := &Execution{world: engine.world, lease: leased}
	ctx = context.WithValue(ctx, executionContextKey{}, execution)
	output, err := engine.replay(runner, ctx, execution, leased.Input)
	switch {
	case errors.Is(err, internal.ErrReplayComplete), errors.Is(err, internal.ErrReplaySuspended):
		return nil
	case errors.Is(err, ErrCancelled), errors.Is(err, ErrLeaseLost):
		return nil
	case errors.Is(context.Cause(ctx), ErrCancelled), errors.Is(context.Cause(ctx), ErrLeaseLost):
		return nil
	case err != nil:
		return engine.world.FailRun(ctx, leased, err)
	case leased.Sequence != RootSequence:
		return nil
	default:
		return engine.world.CompleteRun(ctx, leased, output)
	}
}

func (engine *Engine) replay(runner Runner, ctx context.Context, execution *Execution, input json.RawMessage) (output json.RawMessage, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			recoveredError, ok := recovered.(error)
			if ok {
				err = recoveredError
				return
			}
			panic(recovered)
		}
		if execution.lease.Sequence != RootSequence {
			err = fmt.Errorf("workflow: history mismatch: replay did not reach command %d", execution.lease.Sequence)
		}
	}()
	return runner.execute(ctx, input)
}

func (engine *Engine) heartbeat(ctx context.Context, leased *Lease, cancel context.CancelCauseFunc) func() {
	done := make(chan struct{})
	every := max(engine.world.LeaseDuration()/3, time.Second)
	go func() {
		ticker := time.NewTicker(every)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				err := engine.world.HeartbeatRun(ctx, leased)
				if errors.Is(err, ErrCancelled) || errors.Is(err, ErrLeaseLost) {
					cancel(err)
					return
				}
			}
		}
	}()
	return func() { close(done) }
}
