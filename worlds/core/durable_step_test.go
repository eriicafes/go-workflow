package core_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/core"
	"github.com/eriicafes/go-workflow/worlds/testworld"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

type durableStepInput struct{ Value int }
type durableStepOutput struct{ Value int }
type fatalStepInput struct{}
type fatalStepOutput struct{}
type panicStepInput struct{}
type panicStepOutput struct{}

func (durableStepInput) Workflow(durableStepOutput) {}
func (fatalStepInput) Workflow(fatalStepOutput)     {}
func (panicStepInput) Workflow(panicStepOutput)     {}

func TestDurableStepRecordsRetryAndCompletionEvents(t *testing.T) {
	var attempts atomic.Int32
	world := testworld.New(workflow.Use(func(ctx context.Context, input durableStepInput) (durableStepOutput, error) {
		value, err := workflow.Step(ctx, func(_ context.Context, value int) (int, error) {
			if attempts.Add(1) == 1 {
				return 0, workflow.RetryableError(errors.New("retry"), time.Millisecond)
			}
			return value + 1, nil
		}, input.Value, workflow.StepOptions{MaxRetries: 1})
		return durableStepOutput{Value: value}, err
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, durableStepInput{Value: 41})
	if err != nil {
		t.Fatal(err)
	}
	output, err := run.Value(ctx)
	if err != nil || output.Value != 42 || attempts.Load() != 2 {
		t.Fatalf("output = %#v, attempts=%d, err=%v", output, attempts.Load(), err)
	}
	events, err := world.EventsList(ctx, worlds.EventsListInput{RunID: worlds.RunID(run.ID())})
	if err != nil {
		t.Fatal(err)
	}
	// The step runs inline, created by its first step_started, and its
	// retry is handed to a step delivery, which starts and completes it.
	var types []worlds.EventType
	for _, event := range events.Data {
		if event.CorrelationID != "" {
			types = append(types, event.EventType)
		}
	}
	want := []worlds.EventType{worlds.EventTypeStepStarted, worlds.EventTypeStepRetrying, worlds.EventTypeStepStarted, worlds.EventTypeStepCompleted}
	if !slices.Equal(types, want) {
		t.Fatalf("step events = %v, want %v", types, want)
	}
}

func TestFatalStepFailureDoesNotScheduleARetry(t *testing.T) {
	var attempts atomic.Int32
	original := errors.New("fatal")
	world := testworld.New(workflow.Use(func(ctx context.Context, _ fatalStepInput) (fatalStepOutput, error) {
		_, err := workflow.Step(ctx, func(context.Context, struct{}) (struct{}, error) {
			attempts.Add(1)
			return struct{}{}, workflow.FatalError(original)
		}, struct{}{}, workflow.StepOptions{MaxRetries: 3})
		return fatalStepOutput{}, err
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, fatalStepInput{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := run.Value(ctx); !errors.Is(err, original) || attempts.Load() != 1 {
		t.Fatalf("fatal step error=%v attempts=%d", err, attempts.Load())
	}
	events, err := world.EventsList(ctx, worlds.EventsListInput{RunID: worlds.RunID(run.ID())})
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events.Data {
		if event.EventType == worlds.EventTypeStepRetrying {
			t.Fatal("fatal step was retried")
		}
		if event.EventType == worlds.EventTypeStepFailed {
			var result worlds.DataStepResult
			if err := json.Unmarshal(event.Data, &result); err != nil {
				t.Fatal(err)
			}
			recorded := worlds.DecodeErrorPayload(result.Error, nil)
			if !errors.Is(recorded, original) {
				t.Fatalf("step event lost error chain: %s", result.Error)
			}
			step, err := world.StepsGet(ctx, worlds.StepsGetInput{RunID: event.RunID, StepID: worlds.StepID(event.CorrelationID)})
			if err != nil {
				t.Fatal(err)
			}
			var fatal worlds.FatalError
			decoded := worlds.DecodeErrorPayload(step.Error, nil)
			if !errors.Is(decoded, original) || !errors.As(decoded, &fatal) {
				t.Fatalf("stored step error lost its cause or fatal marker: %s", step.Error)
			}
		}
	}
}

func TestTargetedStepErrorPanicIsRecordedAsStepFailure(t *testing.T) {
	world := testworld.New(workflow.Use(func(ctx context.Context, _ panicStepInput) (panicStepOutput, error) {
		_, err := workflow.Step(ctx, func(context.Context, struct{}) (struct{}, error) {
			panic(errors.New("step panic"))
		}, struct{}{}, workflow.StepOptions{MaxRetries: 0})
		return panicStepOutput{}, err
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, panicStepInput{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := run.Value(ctx); err == nil {
		t.Fatal("step panic did not fail the run")
	}
	events, err := world.EventsList(ctx, worlds.EventsListInput{RunID: worlds.RunID(run.ID())})
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events.Data {
		if event.EventType == worlds.EventTypeStepFailed {
			return
		}
	}
	t.Fatal("missing step_failed event")
}

type redeliveredStepInput struct{}
type redeliveredStepOutput struct{}

func (redeliveredStepInput) Workflow(redeliveredStepOutput) {}

func TestRedeliveredStepRunsAsANewAttempt(t *testing.T) {
	var executions atomic.Int32
	runner := workflow.Use(func(ctx context.Context, _ redeliveredStepInput) (redeliveredStepOutput, error) {
		_, err := workflow.Step(ctx, func(context.Context, struct{}) (struct{}, error) {
			executions.Add(1)
			return struct{}{}, nil
		}, struct{}{})
		return redeliveredStepOutput{}, err
	})
	world := testworld.NewManual(runner)
	defer world.Close()
	ctx := workflow.NewContext(queuedSteps(), workflow.NewClient(world))
	run, err := workflow.Start(ctx, redeliveredStepInput{})
	if err != nil {
		t.Fatal(err)
	}
	runtime := core.NewRuntime(world, worlds.Runners(runner))
	if err := runtime.Execute(ctx, world.QueuedMessages()[0].Input); err != nil {
		t.Fatal(err)
	}
	var delivery worlds.QueueInput
	for _, message := range world.QueuedMessages() {
		if message.Input.StepID != nil {
			delivery = message.Input
		}
	}
	if delivery.StepID == nil {
		t.Fatal("no step delivery queued")
	}
	// A delivery started the step and ended before recording its result, so
	// the queue redelivers the step's message.
	if _, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: worlds.RunID(run.ID()), Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeStepStarted, CorrelationID: string(*delivery.StepID)}}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Execute(ctx, delivery); err != nil {
		t.Fatal(err)
	}
	step, err := world.StepsGet(ctx, worlds.StepsGetInput{RunID: worlds.RunID(run.ID()), StepID: *delivery.StepID})
	if err != nil || executions.Load() != 1 || step.Status != worlds.StatusCompleted || step.Attempt != 2 {
		t.Fatalf("step = %#v executions=%d err=%v", step, executions.Load(), err)
	}
}

type tracedStepInput struct{}
type tracedStepOutput struct{}

func (tracedStepInput) Workflow(tracedStepOutput) {}

func tracedStep(context.Context, int) (int, error) {
	return 0, workflow.RetryableError(errors.New("try again"), time.Millisecond)
}

type startTracedInput struct{}

func (startTracedInput) Workflow(tracedStepOutput) {}

func startTracedStep(context.Context, int) (int, error) { return 0, nil }

func TestDurableStepContinuesTheTraceTheRunWasStartedIn(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	previousProvider, previousPropagator := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter)))
	otel.SetTextMapPropagator(propagation.TraceContext{})
	defer otel.SetTracerProvider(previousProvider)
	defer otel.SetTextMapPropagator(previousPropagator)

	starter := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{7},
		SpanID:     trace.SpanID{8},
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})
	world := testworld.New(workflow.Use(func(ctx context.Context, _ startTracedInput) (tracedStepOutput, error) {
		_, err := workflow.Step(ctx, startTracedStep, 1)
		return tracedStepOutput{}, err
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	// The run records the trace context it is started in.
	run, err := workflow.Start(trace.ContextWithRemoteSpanContext(ctx, starter), startTracedInput{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := run.Value(ctx); err != nil {
		t.Fatal(err)
	}
	spans := exporter.GetSpans()
	if len(spans) != 1 || spans[0].SpanContext.TraceID() != starter.TraceID() || spans[0].Parent.SpanID() != starter.SpanID() {
		t.Fatalf("spans = %+v", spans)
	}
}

func TestDurableStepSpansEachAttemptUnderTheRunnersSpan(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter)))
	defer otel.SetTracerProvider(previous)

	parent := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{1},
		SpanID:     trace.SpanID{2},
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})
	world := testworld.New(workflow.Use(func(ctx context.Context, _ tracedStepInput) (tracedStepOutput, error) {
		// The runner's ctx reaches the step, so the step's span is a child
		// of the span context the runner set.
		ctx = trace.ContextWithRemoteSpanContext(ctx, parent)
		_, err := workflow.Step(ctx, tracedStep, 1, workflow.StepOptions{MaxRetries: 1})
		return tracedStepOutput{}, err
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, tracedStepInput{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := run.Value(ctx); err == nil {
		t.Fatal("expected the step to fail")
	}

	// One span per attempt, not per replay.
	spans := exporter.GetSpans()
	if len(spans) != 2 {
		t.Fatalf("got %d spans", len(spans))
	}
	for i, span := range spans {
		if span.Name != "step core_test.tracedStep" || span.Parent.SpanID() != parent.SpanID() || span.SpanContext.TraceID() != parent.TraceID() {
			t.Fatalf("span = %q with parent %v", span.Name, span.Parent)
		}
		if span.Status.Code != codes.Error {
			t.Fatalf("status = %v", span.Status)
		}
		attributes := map[attribute.Key]attribute.Value{}
		for _, kv := range span.Attributes {
			attributes[kv.Key] = kv.Value
		}
		if attributes["workflow.run.id"].AsString() != run.ID() || attributes["workflow.step.attempt"].AsInt64() != int64(i+1) {
			t.Fatalf("attributes = %v", span.Attributes)
		}
	}
}

type unencodableStepInput struct{}
type unencodableStepOutput struct{}

func (unencodableStepInput) Workflow(unencodableStepOutput) {}

func TestStepWhoseInputDoesNotEncodeIsCreatedWithThePlaceholderAndFailed(t *testing.T) {
	world := testworld.New(workflow.Use(func(ctx context.Context, _ unencodableStepInput) (unencodableStepOutput, error) {
		_, err := workflow.Step(ctx, func(context.Context, chan int) (int, error) { return 0, nil }, make(chan int))
		if err == nil {
			return unencodableStepOutput{}, errors.New("the step ran")
		}
		return unencodableStepOutput{}, nil
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), 5*time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, unencodableStepInput{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := run.Value(ctx); err != nil {
		t.Fatal(err)
	}
	steps, err := world.StepsList(ctx, worlds.StepsListInput{RunID: worlds.RunID(run.ID())})
	if err != nil || len(steps.Data) != 1 || steps.Data[0].Status != worlds.StatusFailed {
		t.Fatalf("steps = %+v err = %v", steps.Data, err)
	}
	var input struct {
		Args []string `json:"args"`
		Flag bool     `json:"__workflowUnserializableStepInput"`
	}
	if err := worlds.DecodePayload(steps.Data[0].Input, nil, &input); err != nil || !input.Flag || len(input.Args) != 1 || !strings.Contains(input.Args[0], "input unavailable") {
		t.Fatalf("input = %+v err = %v", input, err)
	}
}

type retryDelayInput struct{ After time.Duration }
type retryDelayOutput struct{}

func (retryDelayInput) Workflow(retryDelayOutput) {}

func retryDelayStep(_ context.Context, after time.Duration) (struct{}, error) {
	if after > 0 {
		return struct{}{}, workflow.RetryableError(errors.New("later"), after)
	}
	return struct{}{}, errors.New("again")
}

func retryDelayRunner() worlds.Runner {
	return workflow.Use(func(ctx context.Context, input retryDelayInput) (retryDelayOutput, error) {
		_, err := workflow.Background(ctx, retryDelayStep, input.After, workflow.StepOptions{MaxRetries: 1}).Value(ctx)
		return retryDelayOutput{}, err
	})
}

func TestFailedStepRetriesAfterASecondByDefault(t *testing.T) {
	runner := retryDelayRunner()
	world := testworld.NewManual(runner)
	defer world.Close()
	ctx := workflow.NewContext(context.Background(), workflow.NewClient(world))
	run, err := workflow.Start(ctx, retryDelayInput{})
	if err != nil {
		t.Fatal(err)
	}
	runtime := core.NewRuntime(world, worlds.Runners(runner))
	for range 2 {
		// The flow creates the step, and the step's delivery runs its
		// first attempt, which asks for the message again when the retry
		// is due.
		message := world.QueuedMessages()[0]
		err := runtime.Execute(ctx, message.Input)
		if redelivery, ok := worlds.RedeliveryOf(err); ok {
			if redelivery.After < 900*time.Millisecond || redelivery.After > time.Second {
				t.Fatalf("redelivery after %s, want a second", redelivery.After)
			}
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := world.Acknowledge(ctx, message.ID); err != nil {
			t.Fatal(err)
		}
	}
	events, err := world.EventsList(ctx, worlds.EventsListInput{RunID: worlds.RunID(run.ID())})
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events.Data {
		if event.EventType != worlds.EventTypeStepRetrying {
			continue
		}
		var data worlds.DataStepResult
		if err := json.Unmarshal(event.Data, &data); err != nil || data.RetryAfter == nil {
			t.Fatalf("step_retrying data = %s, %v", event.Data, err)
		}
		if wait := data.RetryAfter.Sub(event.CreatedAt); wait < 900*time.Millisecond || wait > 1100*time.Millisecond {
			t.Fatalf("retry due %s after the failure, want a second", wait)
		}
		return
	}
	t.Fatal("no step_retrying recorded")
}

func TestStepWhoseRetryOutlivesItsMessageFails(t *testing.T) {
	world := testworld.New(retryDelayRunner())
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), 5*time.Second)
	defer cancel()
	// The step's message lives a week, so a retry due later would find it
	// gone.
	run, err := workflow.Start(ctx, retryDelayInput{After: worlds.StepRetention + time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := run.Value(ctx); err == nil || !strings.Contains(err.Error(), "could not be retried") {
		t.Fatalf("run = %v, want the step failed for a retry past its message", err)
	}
	events, err := world.EventsList(ctx, worlds.EventsListInput{RunID: worlds.RunID(run.ID())})
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events.Data {
		if event.EventType == worlds.EventTypeStepRetrying {
			t.Fatal("the step was left retrying")
		}
	}
}

type exhaustedInput struct{ Inline bool }
type exhaustedOutput struct{}

func (exhaustedInput) Workflow(exhaustedOutput) {}

type exhaustedCause struct{ Code int }

func (e exhaustedCause) Error() string { return fmt.Sprintf("refused with %d", e.Code) }

func alwaysRefused(context.Context, struct{}) (struct{}, error) {
	return struct{}{}, exhaustedCause{Code: 7}
}

func TestStepThatRunsOutOfRetriesFailsWithAFatalErrorThatKeepsItsLastError(t *testing.T) {
	for _, inline := range []bool{false, true} {
		t.Run(fmt.Sprintf("inline=%t", inline), func(t *testing.T) {
			var stepErr error
			world := testworld.New(workflow.Use(func(ctx context.Context, input exhaustedInput) (exhaustedOutput, error) {
				if input.Inline {
					_, stepErr = workflow.InlineStep(ctx, alwaysRefused, struct{}{}, workflow.StepOptions{MaxRetries: 0})
				} else {
					_, stepErr = workflow.Step(core.WithMaxInlineSteps(ctx, 0), alwaysRefused, struct{}{}, workflow.StepOptions{MaxRetries: 0})
				}
				return exhaustedOutput{}, stepErr
			}))
			defer world.Close()
			ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), 5*time.Second)
			defer cancel()
			run, err := workflow.Start(ctx, exhaustedInput{Inline: inline})
			if err != nil {
				t.Fatal(err)
			}
			_, err = run.Value(ctx)
			var recorded *workflow.Error
			var fatal worlds.FatalError
			var cause exhaustedCause
			if !errors.As(err, &recorded) || recorded.Type != "FatalError" || !strings.HasPrefix(recorded.Message, `Step "`) ||
				!strings.HasSuffix(recorded.Message, `.alwaysRefused" failed after 0 retries: refused with 7`) {
				t.Fatalf("run = %#v, want the step's retries used up", err)
			}
			// The step's own error is the cause, which errors.As reaches.
			if !errors.As(err, &fatal) || fatal.Cause == nil || fatal.Cause.Error() != "refused with 7" || !errors.As(err, &cause) || cause.Code != 7 {
				t.Fatalf("run = %#v, want the step's last error as its cause", err)
			}
		})
	}
}

// stepSignalWorld records what the runtime tells its World about the steps a
// delivery executes: the events it writes, with their options, and the steps
// whose functions it is about to run, in order.
type stepSignalWorld struct {
	*testworld.World
	mu      sync.Mutex
	creates []worlds.EventsCreateInput
	trace   []string
	batches [][]worlds.QueueInput
}

func (w *stepSignalWorld) EventsCreate(ctx context.Context, input worlds.EventsCreateInput) (worlds.EventResult, error) {
	result, err := w.World.EventsCreate(ctx, input)
	if err == nil {
		w.mu.Lock()
		w.creates, w.trace = append(w.creates, input), append(w.trace, string(input.Event.EventType))
		w.mu.Unlock()
	}
	return result, err
}

func (w *stepSignalWorld) RecordStepExecution(_ context.Context, stepID worlds.StepID) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.trace = append(w.trace, "run "+string(stepID))
}

func (w *stepSignalWorld) QueueBatch(ctx context.Context, input worlds.QueueBatchInput) ([]worlds.QueueResult, error) {
	w.mu.Lock()
	w.batches = append(w.batches, input.Items)
	w.mu.Unlock()
	return w.World.QueueBatch(ctx, input)
}

// written returns the options of the writes of eventType.
func (w *stepSignalWorld) written(eventType worlds.EventType) []worlds.EventsCreateOptions {
	w.mu.Lock()
	defer w.mu.Unlock()
	var options []worlds.EventsCreateOptions
	for _, input := range w.creates {
		if input.Event.EventType == eventType {
			options = append(options, input.Options)
		}
	}
	return options
}

type signalInput struct{ Fail bool }
type signalOutput struct{}

func (signalInput) Workflow(signalOutput) {}

func signalStep(_ context.Context, fail bool) (struct{}, error) {
	if fail {
		return struct{}{}, workflow.FatalError(errors.New("refused"))
	}
	return struct{}{}, nil
}

func signalRunner(ctx context.Context, input signalInput) (signalOutput, error) {
	_, err := workflow.Step(ctx, signalStep, input.Fail, workflow.StepOptions{MaxRetries: 1})
	return signalOutput{}, err
}

func TestStepDeliveryTellsTheWorldWhenItsStepWasCreatedAndThatItRan(t *testing.T) {
	ctx, cancel := context.WithTimeout(queuedSteps(), 5*time.Second)
	defer cancel()
	manual := testworld.NewManual()
	defer manual.Close()
	world := &stepSignalWorld{World: manual}
	runtime := core.NewRuntime(world, worlds.Runners(workflow.Use(signalRunner)))
	if _, err := workflow.Start(workflow.NewContext(ctx, workflow.NewClient(manual)), signalInput{Fail: true}); err != nil {
		t.Fatal(err)
	}
	// The flow creates the step and sends its message in a batch, with the
	// time the World stored its step_created.
	deliverNext(t, ctx, manual, runtime)
	events, err := manual.EventsList(ctx, worlds.EventsListInput{RunID: manual.Queued()[0].RunID})
	if err != nil {
		t.Fatal(err)
	}
	var createdAt time.Time
	for _, event := range events.Data {
		if event.EventType == worlds.EventTypeStepCreated {
			createdAt = event.CreatedAt
		}
	}
	if len(world.batches) != 1 || len(world.batches[0]) != 1 || world.batches[0][0].StepID == nil || createdAt.IsZero() || !world.batches[0][0].StepCreatedAt.Equal(createdAt) {
		t.Fatalf("step messages = %+v, want one batch of the step's message with its creation time %s", world.batches, createdAt)
	}
	// Its delivery starts the first attempt with that time, tells the World
	// right before the function runs, and marks the failure it records as
	// one of a function that ran.
	deliverNext(t, ctx, manual, runtime)
	if started := world.written(worlds.EventTypeStepStarted); len(started) != 1 || !started[0].StepCreatedAt.Equal(createdAt) || started[0].AfterStepBody {
		t.Fatalf("step_started options = %+v, want the step's creation time", started)
	}
	if failed := world.written(worlds.EventTypeStepFailed); len(failed) != 1 || !failed[0].AfterStepBody || !failed[0].StepCreatedAt.IsZero() {
		t.Fatalf("step_failed options = %+v, want it marked as after the step's function", failed)
	}
	stepID := string(*world.batches[0][0].StepID)
	if want := []string{"step_started", "run " + stepID, "step_failed"}; !slices.Equal(world.trace[len(world.trace)-3:], want) {
		t.Fatalf("delivery trace = %v, want %v", world.trace, want)
	}
}

func TestStepDeliveryReadsItsStepsCreationFromTheLogWhenItsMessageHasNone(t *testing.T) {
	ctx, cancel := context.WithTimeout(queuedSteps(), 5*time.Second)
	defer cancel()
	manual := testworld.NewManual()
	defer manual.Close()
	world := &stepSignalWorld{World: manual}
	runtime := core.NewRuntime(world, worlds.Runners(workflow.Use(signalRunner)))
	if _, err := workflow.Start(workflow.NewContext(ctx, workflow.NewClient(manual)), signalInput{}); err != nil {
		t.Fatal(err)
	}
	deliverNext(t, ctx, manual, runtime)
	message := manual.QueuedMessages()[0]
	sent := message.Input.StepCreatedAt
	message.Input.StepCreatedAt = time.Time{}
	if err := runtime.Execute(ctx, message.Input); err != nil {
		t.Fatal(err)
	}
	if started := world.written(worlds.EventTypeStepStarted); len(started) != 1 || sent.IsZero() || !started[0].StepCreatedAt.Equal(sent) {
		t.Fatalf("step_started options = %+v, want the creation time %s read from the log", started, sent)
	}
	// A step that completes needs no mark: only a step_failed is ambiguous.
	if completed := world.written(worlds.EventTypeStepCompleted); len(completed) != 1 || completed[0].AfterStepBody {
		t.Fatalf("step_completed options = %+v", completed)
	}
}

func TestStepFailedWithoutRunningIsNotMarkedAsAfterItsFunction(t *testing.T) {
	ctx, cancel := context.WithTimeout(queuedSteps(), 5*time.Second)
	defer cancel()
	manual := testworld.NewManual()
	defer manual.Close()
	world := &stepSignalWorld{World: manual}
	var calls atomic.Int32
	runtime := core.NewRuntime(world, worlds.Runners(workflow.Use(func(ctx context.Context, _ signalInput) (signalOutput, error) {
		_, err := workflow.Step(ctx, func(context.Context, struct{}) (struct{}, error) {
			calls.Add(1)
			return struct{}{}, nil
		}, struct{}{}, workflow.StepOptions{MaxRetries: 0})
		return signalOutput{}, err
	})))
	if _, err := workflow.Start(workflow.NewContext(ctx, workflow.NewClient(manual)), signalInput{}); err != nil {
		t.Fatal(err)
	}
	deliverNext(t, ctx, manual, runtime)
	// The step's only attempt started in a delivery that stopped, so its
	// next delivery fails it without running it, and with no creation time:
	// that is the first attempt's alone.
	message := manual.QueuedMessages()[0]
	started, _ := json.Marshal(worlds.DataStepStarted{Attempt: 1})
	if _, err := manual.EventsCreate(ctx, worlds.EventsCreateInput{RunID: message.Input.RunID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeStepStarted, CorrelationID: string(*message.Input.StepID), Data: started}}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Execute(ctx, message.Input); err != nil {
		t.Fatal(err)
	}
	if failed := world.written(worlds.EventTypeStepFailed); len(failed) != 1 || failed[0].AfterStepBody || calls.Load() != 0 {
		t.Fatalf("step_failed options = %+v after %d calls, want an unmarked failure of a step that never ran", failed, calls.Load())
	}
	if slices.ContainsFunc(world.trace, func(entry string) bool { return strings.HasPrefix(entry, "run ") }) {
		t.Fatalf("trace = %v, want no step execution recorded", world.trace)
	}
}

func TestInlineStepTellsTheWorldItRanAndMarksItsFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	manual := testworld.NewManual()
	defer manual.Close()
	world := &stepSignalWorld{World: manual}
	runtime := core.NewRuntime(world, worlds.Runners(workflow.Use(signalRunner)))
	run, err := workflow.Start(workflow.NewContext(ctx, workflow.NewClient(manual)), signalInput{Fail: true})
	if err != nil {
		t.Fatal(err)
	}
	drain(t, ctx, manual, runtime)
	steps, err := manual.StepsList(ctx, worlds.StepsListInput{RunID: worlds.RunID(run.ID())})
	if err != nil || len(steps.Data) != 1 {
		t.Fatalf("steps = %+v, %v", steps, err)
	}
	// The flow ran the step itself: the World is told before the function
	// runs, the failure is marked, and a step created inline has no creation
	// time to report.
	world.mu.Lock()
	ran := slices.Index(world.trace, "run "+string(steps.Data[0].StepID))
	failed := slices.Index(world.trace, "step_failed")
	world.mu.Unlock()
	if ran < 0 || failed < ran {
		t.Fatalf("trace = %v, want the step's execution recorded before its failure", world.trace)
	}
	if options := world.written(worlds.EventTypeStepFailed); len(options) != 1 || !options[0].AfterStepBody || options[0].InBandCount == nil {
		t.Fatalf("step_failed options = %+v, want an in-band write marked as after the step's function", options)
	}
	for _, options := range world.written(worlds.EventTypeStepStarted) {
		if !options.StepCreatedAt.IsZero() {
			t.Fatalf("an inline step's start carries a creation time: %+v", options)
		}
	}
	if len(world.batches) != 0 {
		t.Fatalf("an inline step was sent a message: %+v", world.batches)
	}
}
