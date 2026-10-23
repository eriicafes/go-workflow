package workflow_test

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/testworld"
)

type cancellationInput struct{}
type cancellationOutput struct {
	Cause     string
	Cancelled bool
}

func (cancellationInput) Workflow(cancellationOutput) {}

func TestCancellationConstructorsReturnErrorsOutsideWorkflowContext(t *testing.T) {
	ctx, cancel, err := workflow.WithCancel(context.Background())
	if ctx != nil || cancel != nil || !errors.Is(err, workflow.ErrNoWorkflow) {
		t.Fatalf("WithCancel: ctx=%v cancelNil=%t err=%v", ctx, cancel == nil, err)
	}
	ctx, cancelCause, err := workflow.WithCancelCause(context.Background())
	if ctx != nil || cancelCause != nil || !errors.Is(err, workflow.ErrNoWorkflow) {
		t.Fatalf("WithCancelCause: ctx=%v cancelNil=%t err=%v", ctx, cancelCause == nil, err)
	}
}

func TestCancellationConstructorsReturnErrorsInStepContext(t *testing.T) {
	world := testworld.New(workflow.Use(func(ctx context.Context, _ cancellationInput) (cancellationOutput, error) {
		return workflow.Step(ctx, func(ctx context.Context, _ struct{}) (cancellationOutput, error) {
			cancelCtx, cancel, err := workflow.WithCancel(ctx)
			if cancelCtx != nil || cancel != nil || !errors.Is(err, workflow.ErrNoWorkflow) {
				return cancellationOutput{}, errors.New("WithCancel did not return ErrNoWorkflow in step context")
			}
			cancelCtx, cancelCause, err := workflow.WithCancelCause(ctx)
			if cancelCtx != nil || cancelCause != nil || !errors.Is(err, workflow.ErrNoWorkflow) {
				return cancellationOutput{}, errors.New("WithCancelCause did not return ErrNoWorkflow in step context")
			}
			return cancellationOutput{}, nil
		}, struct{}{})
	}))
	defer world.Close()
	ctx, stop := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), 2*time.Second)
	defer stop()
	run, err := workflow.Start(ctx, cancellationInput{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := run.Value(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestDurableCancellationFailsStepWithoutRetry(t *testing.T) {
	world := testworld.New(workflow.Use(func(ctx context.Context, _ cancellationInput) (cancellationOutput, error) {
		cancelCtx, cancel, err := workflow.WithCancelCause(ctx)
		if err != nil {
			return cancellationOutput{}, err
		}
		result := workflow.Background(cancelCtx, func(ctx context.Context, _ struct{}) (string, error) {
			<-ctx.Done()
			return context.Cause(ctx).Error(), ctx.Err()
		}, struct{}{})
		if err := cancel(errors.New("stop work")); err != nil {
			return cancellationOutput{}, err
		}
		if err := cancel(errors.New("ignored")); err != nil {
			return cancellationOutput{}, err
		}
		_, err = result.Value(ctx)
		return cancellationOutput{Cause: context.Cause(cancelCtx).Error(), Cancelled: errors.Is(err, context.Canceled) && errors.Is(err, workflow.FatalError(context.Canceled))}, nil
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), 2*time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, cancellationInput{})
	if err != nil {
		t.Fatal(err)
	}
	output, err := run.Value(ctx)
	if err != nil || !output.Cancelled || output.Cause != "stop work" {
		t.Fatalf("output=%#v err=%v", output, err)
	}
	// A replay can deliver the unstarted step again, so the step may start more
	// than once, but cancellation must never schedule a retry.
	events, err := world.EventsList(ctx, worlds.EventsListInput{RunID: worlds.RunID(run.ID())})
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events.Data {
		if event.EventType == worlds.EventTypeStepRetrying {
			t.Fatalf("cancelled step retried: %s", event.Data)
		}
	}
}

func TestStepCanCancelItsDurableContext(t *testing.T) {
	world := testworld.New(workflow.Use(func(ctx context.Context, _ cancellationInput) (cancellationOutput, error) {
		cancelCtx, _, err := workflow.WithCancel(ctx)
		if err != nil {
			return cancellationOutput{}, err
		}
		_, err = workflow.Step(cancelCtx, func(ctx context.Context, _ struct{}) (struct{}, error) {
			cancel, ok := workflow.CancelFunc(ctx)
			if !ok {
				return struct{}{}, errors.New("missing controller")
			}
			if err := cancel(); err != nil {
				return struct{}{}, err
			}
			<-ctx.Done()
			return struct{}{}, ctx.Err()
		}, struct{}{})
		return cancellationOutput{Cancelled: errors.Is(err, context.Canceled) && errors.Is(err, workflow.FatalError(context.Canceled))}, nil
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), 2*time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, cancellationInput{})
	if err != nil {
		t.Fatal(err)
	}
	output, err := run.Value(ctx)
	if err != nil || !output.Cancelled {
		t.Fatalf("output=%#v err=%v", output, err)
	}
}

func TestCancelAnyPreservesBaseValuesAndObservesEverySource(t *testing.T) {
	for sourceIndex := range 3 {
		t.Run([]string{"base", "left", "right"}[sourceIndex], func(t *testing.T) {
			type valueKey struct{}
			observed := make(chan bool, 1)
			world := testworld.New(workflow.Use(func(ctx context.Context, _ cancellationInput) (cancellationOutput, error) {
				base, cancelBase, err := workflow.WithCancelCause(context.WithValue(ctx, valueKey{}, "base value"))
				if err != nil {
					return cancellationOutput{}, err
				}
				left, cancelLeft, err := workflow.WithCancelCause(context.WithValue(ctx, valueKey{}, "left value"))
				if err != nil {
					return cancellationOutput{}, err
				}
				right, cancelRight, err := workflow.WithCancelCause(ctx)
				if err != nil {
					return cancellationOutput{}, err
				}
				combined := workflow.CancelAny(base, left, right)
				if workflow.CancelAny(base) != base {
					return cancellationOutput{}, errors.New("empty combination changed base")
				}
				if err := []func(error) error{cancelBase, cancelLeft, cancelRight}[sourceIndex](errors.New("chosen source")); err != nil {
					return cancellationOutput{}, err
				}
				if combined.Err() == nil {
					return cancellationOutput{}, errors.New("combination missed cancellation")
				}
				for i, source := range []context.Context{base, left, right} {
					if i != sourceIndex && source.Err() != nil {
						return cancellationOutput{}, errors.New("combination cancelled another source")
					}
				}
				_, err = workflow.Step(combined, func(ctx context.Context, _ struct{}) (struct{}, error) {
					_, controller := workflow.CancelFunc(ctx)
					observed <- !controller && workflow.GetWorkflowMetadata(ctx).RunID != "" && ctx.Value(valueKey{}) == "base value" && context.Cause(ctx).Error() == "chosen source"
					return struct{}{}, nil
				}, struct{}{})
				return cancellationOutput{Cause: context.Cause(combined).Error(), Cancelled: errors.Is(err, context.Canceled) && errors.Is(err, workflow.FatalError(context.Canceled))}, nil
			}))
			defer world.Close()
			ctx, stop := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), 3*time.Second)
			defer stop()
			run, err := workflow.Start(ctx, cancellationInput{})
			if err != nil {
				t.Fatal(err)
			}
			output, err := run.Value(ctx)
			if err != nil || !output.Cancelled || output.Cause != "chosen source" {
				t.Fatalf("output=%#v err=%v", output, err)
			}
			select {
			case valid := <-observed:
				if !valid {
					t.Fatal("step lost base metadata, values, or cause")
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		})
	}
}

func TestDeadlineConstructorsReturnWorkflowContextErrors(t *testing.T) {
	for name, constructor := range map[string]func(context.Context) (context.Context, func() error, error){
		"WithTimeout": func(ctx context.Context) (context.Context, func() error, error) {
			return workflow.WithTimeout(ctx, time.Second)
		},
		"WithDeadline": func(ctx context.Context) (context.Context, func() error, error) {
			return workflow.WithDeadline(ctx, time.Now().Add(time.Second))
		},
		"WithTimeoutCause": func(ctx context.Context) (context.Context, func() error, error) {
			return workflow.WithTimeoutCause(ctx, time.Second, errors.New("expired"))
		},
		"WithDeadlineCause": func(ctx context.Context) (context.Context, func() error, error) {
			return workflow.WithDeadlineCause(ctx, time.Now().Add(time.Second), errors.New("expired"))
		},
	} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel, err := constructor(context.Background())
			if ctx != nil || cancel != nil || !errors.Is(err, workflow.ErrNoWorkflow) {
				t.Fatalf("ctx=%v cancelNil=%t err=%v", ctx, cancel == nil, err)
			}
		})
	}
}

func TestDeadlineCancellationStopsWorkWithoutCancellingRun(t *testing.T) {
	for _, test := range []struct {
		name        string
		deadline    bool
		sleep       bool
		duration    time.Duration
		withCause   bool
		cause       error
		cancelEarly bool
	}{
		{name: "timeout", duration: time.Hour},
		{name: "deadline", deadline: true, duration: time.Hour},
		{name: "deadline stops sleep", deadline: true, sleep: true, duration: time.Hour},
		{name: "zero timeout"},
		{name: "past deadline", deadline: true, duration: -time.Hour},
		{name: "timeout cause", withCause: true, cause: errors.New("job timed out"), duration: time.Hour},
		{name: "deadline cause", deadline: true, withCause: true, cause: errors.New("approval expired"), duration: time.Hour},
		{name: "nil timeout cause", withCause: true, duration: time.Hour},
		{name: "nil deadline cause", deadline: true, withCause: true, duration: time.Hour},
		{name: "manual timeout cause", withCause: true, cause: errors.New("unused expiry cause"), duration: time.Hour, cancelEarly: true},
		{name: "manual deadline cause", deadline: true, withCause: true, cause: errors.New("unused expiry cause"), duration: time.Hour, cancelEarly: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			var wantCause error = context.DeadlineExceeded
			if test.cause != nil {
				wantCause = test.cause
			}
			if test.cancelEarly {
				wantCause = context.Canceled
			}
			synctest.Test(t, func(t *testing.T) {
				world := testworld.New(workflow.Use(func(ctx context.Context, _ cancellationInput) (cancellationOutput, error) {
					var cancelCtx context.Context
					var cancel func() error
					var err error
					if test.deadline {
						now, nowErr := workflow.Now(ctx)
						if nowErr != nil {
							return cancellationOutput{}, nowErr
						}
						if test.withCause {
							cancelCtx, cancel, err = workflow.WithDeadlineCause(ctx, now.Add(test.duration), test.cause)
						} else {
							cancelCtx, cancel, err = workflow.WithDeadline(ctx, now.Add(test.duration))
						}
					} else if test.withCause {
						cancelCtx, cancel, err = workflow.WithTimeoutCause(ctx, test.duration, test.cause)
					} else {
						cancelCtx, cancel, err = workflow.WithTimeout(ctx, test.duration)
					}
					if err != nil {
						return cancellationOutput{}, err
					}
					if test.cancelEarly {
						if err := cancel(); err != nil {
							return cancellationOutput{}, err
						}
					}
					if test.sleep {
						err = workflow.Sleep(cancelCtx, 2*time.Hour)
					} else {
						_, err = workflow.Step(cancelCtx, func(ctx context.Context, _ struct{}) (struct{}, error) {
							<-ctx.Done()
							if !errors.Is(context.Cause(ctx), wantCause) {
								t.Errorf("step cancellation cause=%v", context.Cause(ctx))
							}
							return struct{}{}, ctx.Err()
						}, struct{}{})
					}
					if err := cancel(); err != nil {
						return cancellationOutput{}, err
					}
					return cancellationOutput{
						Cause: context.Cause(cancelCtx).Error(),
						Cancelled: errors.Is(err, workflow.FatalError(context.Canceled)) &&
							errors.Is(context.Cause(cancelCtx), wantCause),
					}, nil
				}))
				defer world.Close()
				ctx := workflow.NewContext(context.Background(), workflow.NewClient(world))
				run, err := workflow.Start(ctx, cancellationInput{})
				if err != nil {
					t.Fatal(err)
				}
				output, err := run.Value(ctx)
				if err != nil || !output.Cancelled || output.Cause != wantCause.Error() {
					t.Fatalf("output=%#v err=%v", output, err)
				}
				events, err := world.EventsList(ctx, worlds.EventsListInput{RunID: worlds.RunID(run.ID())})
				if err != nil {
					t.Fatal(err)
				}
				for _, event := range events.Data {
					if event.EventType == worlds.EventTypeRunCancelled {
						t.Fatal("deadline cancelled the run")
					}
				}
			})
		})
	}
}
