package workflowtest_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/workflowtest"
)

type sleepInput struct {
	Delay time.Duration
}

type sleepOutput struct {
	Completed bool
}

func (sleepInput) Workflow(sleepOutput) {}

func sleepWorkflow(ctx context.Context, input sleepInput) (sleepOutput, error) {
	if err := workflow.Sleep(ctx, input.Delay); err != nil {
		return sleepOutput{}, err
	}
	return sleepOutput{Completed: true}, nil
}

func timeoutWorkflow(ctx context.Context, input sleepInput) (sleepOutput, error) {
	if err := workflow.Timeout(ctx, input.Delay).Wait(ctx); err != nil {
		return sleepOutput{}, err
	}
	return sleepOutput{Completed: true}, nil
}

func delayedWorkflowStep(ctx context.Context, input sleepInput) (sleepOutput, error) {
	select {
	case <-time.After(input.Delay):
		return sleepOutput{Completed: true}, nil
	case <-ctx.Done():
		return sleepOutput{}, ctx.Err()
	}
}

func selectWithCancelWorkflow(ctx context.Context, _ sleepInput) (sleepOutput, error) {
	fast := workflow.Background(ctx, delayedWorkflowStep, sleepInput{Delay: time.Millisecond})
	slow := workflow.Background(ctx, delayedWorkflowStep, sleepInput{Delay: time.Second})
	_, err := workflow.SelectWithCancel(ctx, fast, slow)
	return sleepOutput{Completed: err == nil}, err
}

type stepInput struct {
	Value int
}

type stepOutput struct {
	Value int
}

func (stepInput) Workflow(stepOutput) {}

func stepWorkflow(ctx context.Context, input stepInput) (stepOutput, error) {
	value, err := workflow.Step(ctx, double, input.Value)
	return stepOutput{Value: value}, err
}

func double(_ context.Context, value int) (int, error) {
	return value * 2, nil
}

type retryInput struct{}
type retryOutput struct {
	Attempts int
	Metadata workflow.StepMetadata
}

func (retryInput) Workflow(retryOutput) {}

var retryCalls atomic.Int32

func retryWorkflow(ctx context.Context, _ retryInput) (retryOutput, error) {
	metadata := workflow.GetStepMetadata(ctx)
	value, err := workflow.Step(ctx, retryStep, struct{}{})
	return retryOutput{Attempts: value, Metadata: metadata}, err
}

func retryStep(ctx context.Context, _ struct{}) (int, error) {
	metadata := workflow.GetStepMetadata(ctx)
	attempt := int(retryCalls.Add(1))
	if metadata.ID == "" || metadata.Name == "" || metadata.Attempt != attempt {
		return 0, errors.New("unexpected step metadata")
	}
	if attempt < 4 {
		return 0, errors.New("try again")
	}
	return attempt, nil
}

func TestWorldExecutesDurableSteps(t *testing.T) {
	world := workflowtest.New(workflow.Use(stepWorkflow))
	defer world.Close()

	run, err := workflow.Start(
		workflow.NewContext(context.Background(), world),
		stepInput{Value: 21},
	)
	if err != nil {
		t.Fatal(err)
	}
	output, err := run.Value(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if output.Value != 42 {
		t.Fatalf("output = %#v, want 42", output)
	}
}

func TestStepRetriesAndMetadata(t *testing.T) {
	retryCalls.Store(0)
	world := workflowtest.New(workflow.Use(retryWorkflow))
	defer world.Close()

	run, err := workflow.Start(workflow.NewContext(context.Background(), world), retryInput{})
	if err != nil {
		t.Fatal(err)
	}
	output, err := run.Value(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if output.Attempts != 4 {
		t.Fatalf("attempts = %d, want 4", output.Attempts)
	}
}

type nowInput struct{}
type nowOutput struct {
	Value time.Time
}

func (nowInput) Workflow(nowOutput) {}

func nowWorkflow(ctx context.Context, _ nowInput) (nowOutput, error) {
	value, err := workflow.Now(ctx)
	return nowOutput{Value: value}, err
}

func TestNowRecordsTimeAsDurableValue(t *testing.T) {
	world := workflowtest.New(workflow.Use(nowWorkflow))
	defer world.Close()

	run, err := workflow.Start(workflow.NewContext(context.Background(), world), nowInput{})
	if err != nil {
		t.Fatal(err)
	}
	output, err := run.Value(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if output.Value.IsZero() {
		t.Fatal("workflow.Now returned zero time")
	}
}

type nowThenStepInput struct {
	Value int
}

type nowThenStepOutput struct {
	Now   time.Time
	Value int
}

func (nowThenStepInput) Workflow(nowThenStepOutput) {}

func nowThenStepWorkflow(ctx context.Context, input nowThenStepInput) (nowThenStepOutput, error) {
	value, err := workflow.Now(ctx)
	if err != nil {
		return nowThenStepOutput{}, err
	}
	doubled, err := workflow.Step(ctx, double, input.Value)
	return nowThenStepOutput{Now: value, Value: doubled}, err
}

func TestNowReplaysBeforeATargetedStep(t *testing.T) {
	world := workflowtest.New(workflow.Use(nowThenStepWorkflow))
	defer world.Close()

	run, err := workflow.Start(
		workflow.NewContext(context.Background(), world),
		nowThenStepInput{Value: 21},
	)
	if err != nil {
		t.Fatal(err)
	}
	output, err := run.Value(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if output.Now.IsZero() || output.Value != 42 {
		t.Fatalf("output = %#v, want a time and 42", output)
	}
}

type emptyDependenciesInput struct{}
type emptyDependenciesOutput struct{}

func (emptyDependenciesInput) Workflow(emptyDependenciesOutput) {}

func emptyDependenciesWorkflow(ctx context.Context, _ emptyDependenciesInput) (emptyDependenciesOutput, error) {
	if err := workflow.Join(ctx); err != nil {
		return emptyDependenciesOutput{}, err
	}
	if err := workflow.JoinSettled(ctx); err != nil {
		return emptyDependenciesOutput{}, err
	}
	if err := workflow.Cancel(ctx); err != nil {
		return emptyDependenciesOutput{}, err
	}
	if winner, err := workflow.Select(ctx); winner != nil || err != nil {
		return emptyDependenciesOutput{}, errors.New("unexpected empty select result")
	}
	if winner, err := workflow.SelectOk(ctx); winner != nil || err != nil {
		return emptyDependenciesOutput{}, errors.New("unexpected empty select-ok result")
	}
	return emptyDependenciesOutput{}, nil
}

func TestEmptyDependenciesReturnImmediately(t *testing.T) {
	world := workflowtest.New(workflow.Use(emptyDependenciesWorkflow))
	defer world.Close()

	run, err := workflow.Start(
		workflow.NewContext(context.Background(), world),
		emptyDependenciesInput{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := run.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
}

type rollbackInput struct{}
type rollbackOutput struct{}

func (rollbackInput) Workflow(rollbackOutput) {}

func rollbackStep(context.Context, string) (struct{}, error) {
	return struct{}{}, nil
}

func TestWorldRollsBackInReverseRegistrationOrder(t *testing.T) {
	var (
		mu    sync.Mutex
		order []string
	)
	compensate := func(_ context.Context, value string) error {
		mu.Lock()
		order = append(order, value)
		mu.Unlock()
		return nil
	}
	workflowFn := func(ctx context.Context, _ rollbackInput) (rollbackOutput, error) {
		if _, err := workflow.Step(ctx, rollbackStep, "first"); err != nil {
			return rollbackOutput{}, err
		}
		if err := workflow.OnRollback(ctx, compensate, "first"); err != nil {
			return rollbackOutput{}, err
		}
		if _, err := workflow.Step(ctx, rollbackStep, "second"); err != nil {
			return rollbackOutput{}, err
		}
		if err := workflow.OnRollback(ctx, compensate, "second"); err != nil {
			return rollbackOutput{}, err
		}
		return rollbackOutput{}, errors.New("workflow failed")
	}

	world := workflowtest.New(workflow.Use(workflowFn))
	defer world.Close()

	run, err := workflow.Start(workflow.NewContext(context.Background(), world), rollbackInput{})
	if err != nil {
		t.Fatal(err)
	}
	if err := run.Wait(context.Background()); err == nil || err.Error() != "workflow failed" {
		t.Fatalf("run error = %v, want workflow failed", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if got, want := order, []string{"second", "first"}; !slices.Equal(got, want) {
		t.Fatalf("rollback order = %v, want %v", got, want)
	}
}

type streamInput struct{}
type streamOutput struct{}

func (streamInput) Workflow(streamOutput) {}

func writeStream(ctx context.Context, _ struct{}) (struct{}, error) {
	writer, release, err := workflow.Writer(ctx, workflow.WriterOption{Namespace: "report"})
	if err != nil {
		return struct{}{}, err
	}
	defer release()
	if _, err := writer.Write([]byte("hello stream")); err != nil {
		return struct{}{}, err
	}
	return struct{}{}, writer.Close()
}

func streamWorkflow(ctx context.Context, _ streamInput) (streamOutput, error) {
	_, err := workflow.Step(ctx, writeStream, struct{}{})
	return streamOutput{}, err
}

func TestWorldStreamsStepOutput(t *testing.T) {
	world := workflowtest.New(workflow.Use(streamWorkflow))
	defer world.Close()

	ctx := context.Background()
	run, err := workflow.Start(workflow.NewContext(ctx, world), streamInput{})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := run.ReaderAt(ctx, -6, workflow.ReaderOption{Namespace: "report"})
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(data), "stream"; got != want {
		t.Fatalf("stream = %q, want %q", got, want)
	}
}

type concurrentStreamInput struct{}
type concurrentStreamOutput struct{}

func (concurrentStreamInput) Workflow(concurrentStreamOutput) {}

var concurrentStreamWriters struct {
	mu      sync.Mutex
	ready   chan struct{}
	release chan struct{}
}

func concurrentStreamStep(ctx context.Context, value string) (struct{}, error) {
	writer, release, err := workflow.Writer(ctx, workflow.WriterOption{Namespace: "parallel"})
	if err != nil {
		return struct{}{}, err
	}
	defer release()
	if _, err := writer.Write([]byte(value)); err != nil {
		return struct{}{}, err
	}
	concurrentStreamWriters.mu.Lock()
	ready := concurrentStreamWriters.ready
	releaseWriters := concurrentStreamWriters.release
	concurrentStreamWriters.mu.Unlock()
	ready <- struct{}{}
	<-releaseWriters
	return struct{}{}, nil
}

func concurrentStreamWorkflow(ctx context.Context, _ concurrentStreamInput) (concurrentStreamOutput, error) {
	results := workflow.Map(ctx, concurrentStreamStep, []string{"first", "second"})
	return concurrentStreamOutput{}, workflow.Join(ctx, workflow.Tasks(results)...)
}

func TestWorldSerializesStepWriters(t *testing.T) {
	world := workflowtest.New(workflow.Use(concurrentStreamWorkflow))
	defer world.Close()

	ready := make(chan struct{}, 2)
	releaseWriters := make(chan struct{})
	concurrentStreamWriters.mu.Lock()
	concurrentStreamWriters.ready = ready
	concurrentStreamWriters.release = releaseWriters
	concurrentStreamWriters.mu.Unlock()
	defer func() {
		if releaseWriters != nil {
			close(releaseWriters)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	run, err := workflow.Start(workflow.NewContext(ctx, world), concurrentStreamInput{})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-ready:
	case <-ctx.Done():
		t.Fatal("first step writer did not acquire the stream")
	}
	select {
	case <-ready:
		t.Fatal("second step writer acquired the stream before the first released it")
	case <-time.After(25 * time.Millisecond):
	}
	close(releaseWriters)
	releaseWriters = nil
	select {
	case <-ready:
	case <-ctx.Done():
		t.Fatal("second step writer did not acquire the stream after release")
	}
	if _, err := run.Value(ctx); err != nil {
		t.Fatal(err)
	}
	reader, err := run.Reader(ctx, workflow.ReaderOption{Namespace: "parallel"})
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(data); !strings.Contains(got, "first") || !strings.Contains(got, "second") {
		t.Fatalf("stream = %q, want both step writes", got)
	}
}

type streamAccessInput struct{}
type streamAccessOutput struct {
	RootWriter bool
	RootReader bool
	StepWriter bool
	StepReader bool
}

func (streamAccessInput) Workflow(streamAccessOutput) {}

func streamAccessStep(ctx context.Context, _ struct{}) (streamAccessOutput, error) {
	writer, release, err := workflow.Writer(ctx)
	if err != nil {
		return streamAccessOutput{}, err
	}
	if _, err := writer.Write([]byte("x")); err != nil {
		return streamAccessOutput{}, err
	}
	if err := release(); err != nil {
		return streamAccessOutput{}, err
	}
	reader, err := workflow.Reader(ctx)
	if err != nil {
		return streamAccessOutput{}, err
	}
	buf := make([]byte, 1)
	_, err = reader.Read(buf)
	return streamAccessOutput{StepWriter: true, StepReader: err == nil && string(buf) == "x"}, err
}

func streamAccessWorkflow(ctx context.Context, _ streamAccessInput) (streamAccessOutput, error) {
	run, err := workflow.Get[streamAccessOutput](ctx, workflow.GetWorkflowMetadata(ctx).RunID)
	if err != nil {
		return streamAccessOutput{}, err
	}
	_, _, writerErr := run.Writer(ctx)
	_, readerErr := run.Reader(ctx)
	value, err := workflow.Step(ctx, streamAccessStep, struct{}{})
	if err != nil {
		return streamAccessOutput{}, err
	}
	value.RootWriter = errors.Is(writerErr, workflow.ErrNoWorkflow)
	value.RootReader = errors.Is(readerErr, workflow.ErrNoWorkflow)
	return value, nil
}

func TestStreamAccessRules(t *testing.T) {
	world := workflowtest.New(workflow.Use(streamAccessWorkflow))
	defer world.Close()
	run, err := workflow.Start(workflow.NewContext(context.Background(), world), streamAccessInput{})
	if err != nil {
		t.Fatal(err)
	}
	value, err := run.Value(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !value.RootWriter || !value.RootReader || !value.StepWriter || !value.StepReader {
		t.Fatalf("stream access = %#v", value)
	}
}

func TestWorldWaitsForStreamWriterLock(t *testing.T) {
	world := workflowtest.New(workflow.Use(sleepWorkflow))
	defer world.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	run, err := workflow.Start(
		workflow.NewContext(ctx, world),
		sleepInput{Delay: time.Hour},
	)
	if err != nil {
		t.Fatal(err)
	}

	_, releaseFirst, err := run.Writer(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer releaseFirst()

	type writerResult struct {
		release func() error
		err     error
	}
	acquired := make(chan writerResult, 1)
	go func() {
		_, release, err := run.Writer(ctx)
		acquired <- writerResult{release: release, err: err}
	}()

	select {
	case result := <-acquired:
		if result.release != nil {
			_ = result.release()
		}
		t.Fatalf("second writer acquired before release: %v", result.err)
	case <-time.After(25 * time.Millisecond):
	}

	if err := releaseFirst(); err != nil {
		t.Fatal(err)
	}

	select {
	case result := <-acquired:
		if result.err != nil {
			t.Fatal(result.err)
		}
		if err := result.release(); err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestWorldSchedulesTimersWithWallClockTime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		world := workflowtest.New(workflow.Use(sleepWorkflow))
		defer world.Close()

		delay := 20 * time.Millisecond
		started := time.Now()
		run, err := workflow.Start(
			workflow.NewContext(context.Background(), world),
			sleepInput{Delay: delay},
		)
		if err != nil {
			t.Fatal(err)
		}

		// workflowtest deliberately uses time.Sleep. synctest advances the
		// standard clock once this test and the worker goroutines are blocked.
		if err := run.Wait(context.Background()); err != nil {
			t.Fatal(err)
		}
		if elapsed := time.Since(started); elapsed < delay {
			t.Fatalf("timer completed after %s, want at least %s", elapsed, delay)
		}
	})
}

type metadataInput struct {
	Delay time.Duration
}

type metadataOutput struct {
	Workflow workflow.WorkflowMetadata
	Step     workflow.WorkflowMetadata
}

func (metadataInput) Workflow(metadataOutput) {}

func metadataStep(ctx context.Context, _ struct{}) (workflow.WorkflowMetadata, error) {
	return workflow.GetWorkflowMetadata(ctx), nil
}

func metadataWorkflow(ctx context.Context, input metadataInput) (metadataOutput, error) {
	value := metadataOutput{Workflow: workflow.GetWorkflowMetadata(ctx)}
	if err := workflow.Sleep(ctx, input.Delay); err != nil {
		return metadataOutput{}, err
	}
	step, err := workflow.Step(ctx, metadataStep, struct{}{})
	if err != nil {
		return metadataOutput{}, err
	}
	value.Step = step
	return value, nil
}

func TestWorldRunMetadataAndWake(t *testing.T) {
	world := workflowtest.New(workflow.Use(metadataWorkflow))
	defer world.Close()
	ctx := workflow.NewContext(context.Background(), world)
	run, err := workflow.Start(ctx, metadataInput{Delay: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	status, err := run.Status()
	if err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 20 && status != workflow.StatusWaiting; attempt++ {
		time.Sleep(10 * time.Millisecond)
		status, err = run.Status()
		if err != nil {
			t.Fatal(err)
		}
	}
	if status != workflow.StatusWaiting {
		t.Fatalf("status = %q, want waiting", status)
	}
	count, err := run.Wake()
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("woken sleeps = %d, want 1", count)
	}
	output, err := run.Value(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(output.Workflow.Name, ".metadataInput") ||
		output.Workflow.RunID != run.ID() || output.Workflow.StartedAt.IsZero() {
		t.Fatalf("workflow metadata = %#v", output.Workflow)
	}
	if output.Step != output.Workflow {
		t.Fatalf("step metadata = %#v, want %#v", output.Step, output.Workflow)
	}
}

func TestCancelRunIsIdempotent(t *testing.T) {
	world := workflowtest.New(workflow.Use(sleepWorkflow))
	defer world.Close()

	ctx := workflow.NewContext(context.Background(), world)
	run, err := workflow.Start(ctx, sleepInput{Delay: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if err := workflow.Cancel(ctx, run); err != nil {
		t.Fatal(err)
	}
	if err := workflow.Cancel(ctx, run); err != nil {
		t.Fatal(err)
	}
	if err := run.Wait(context.Background()); !errors.Is(err, workflow.ErrCancelled) {
		t.Fatalf("Wait error = %v, want ErrCancelled", err)
	}
}

func TestTimeoutCancelsItsRun(t *testing.T) {
	world := workflowtest.New(workflow.Use(timeoutWorkflow))
	defer world.Close()

	run, err := workflow.Start(
		workflow.NewContext(context.Background(), world),
		sleepInput{Delay: 10 * time.Millisecond},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := run.Wait(context.Background()); !errors.Is(err, workflow.ErrCancelled) {
		t.Fatalf("Wait error = %v, want ErrCancelled", err)
	}
}

func TestSelectWithCancelCompletesAfterCancellingLosers(t *testing.T) {
	world := workflowtest.New(workflow.Use(selectWithCancelWorkflow))
	defer world.Close()

	run, err := workflow.Start(
		workflow.NewContext(context.Background(), world),
		sleepInput{},
	)
	if err != nil {
		t.Fatal(err)
	}
	output, err := run.Value(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !output.Completed {
		t.Fatal("select workflow did not complete")
	}
}

type hookInput struct {
	Token     string
	Force     bool
	Retention time.Duration
}

type hookOutput struct {
	Value string
}

func (hookInput) Workflow(hookOutput) {}

func hookWorkflow(ctx context.Context, input hookInput) (hookOutput, error) {
	hook := workflow.NewHook[string](workflow.HookOption{
		Token:        input.Token,
		Force:        input.Force,
		MinRetention: input.Retention,
	})
	value, err := hook.Value(ctx)
	return hookOutput{Value: value}, err
}

func TestWorldResumesHook(t *testing.T) {
	world := workflowtest.New(workflow.Use(hookWorkflow))
	defer world.Close()
	ctx := workflow.NewContext(context.Background(), world)
	run, err := workflow.Start(ctx, hookInput{Token: "test-hook"})
	if err != nil {
		t.Fatal(err)
	}
	if err := workflow.ResumeHookWithRetries(ctx, "test-hook", "resumed", 20); err != nil {
		t.Fatal(err)
	}
	output, err := run.Value(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if output.Value != "resumed" {
		t.Fatalf("value = %q, want resumed", output.Value)
	}
}

func TestUnregisteredHookTaskReturnsError(t *testing.T) {
	hook := workflow.NewHook[string]()
	if _, err := hook.Task(); !errors.Is(err, workflow.ErrHookNotRegistered) {
		t.Fatalf("hook task error = %v, want ErrHookNotRegistered", err)
	}

	webhook := workflow.NewWebhook[string]()
	if _, err := webhook.Task(); !errors.Is(err, workflow.ErrHookNotRegistered) {
		t.Fatalf("webhook task error = %v, want ErrHookNotRegistered", err)
	}
}

type joinedHookInput struct{ Token string }
type joinedHookOutput struct{}

func (joinedHookInput) Workflow(joinedHookOutput) {}

func joinedHookWorkflow(ctx context.Context, input joinedHookInput) (joinedHookOutput, error) {
	hook := workflow.NewHook[string](workflow.HookOption{Token: input.Token})
	if _, _, err := hook.GetConflict(ctx); err != nil {
		return joinedHookOutput{}, err
	}
	return joinedHookOutput{}, workflow.Join(ctx, hook)
}

func TestWorldJoinsRegisteredHook(t *testing.T) {
	world := workflowtest.New(workflow.Use(joinedHookWorkflow))
	defer world.Close()
	ctx := workflow.NewContext(context.Background(), world)
	run, err := workflow.Start(ctx, joinedHookInput{Token: "joined-hook"})
	if err != nil {
		t.Fatal(err)
	}
	if err := workflow.ResumeHookWithRetries(ctx, "joined-hook", "resumed", 20); err != nil {
		t.Fatal(err)
	}
	if err := run.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
}

type generatedHookInput struct{}
type generatedHookOutput struct{ Value string }

func (generatedHookInput) Workflow(generatedHookOutput) {}

func generatedHookWorkflow(ctx context.Context, _ generatedHookInput) (generatedHookOutput, error) {
	value, err := workflow.NewHook[string]().Value(ctx)
	return generatedHookOutput{Value: value}, err
}

func TestWorldGeneratedHookTokenIsReplaySafe(t *testing.T) {
	world := workflowtest.New(workflow.Use(generatedHookWorkflow))
	defer world.Close()
	ctx := workflow.NewContext(context.Background(), world)
	run, err := workflow.Start(ctx, generatedHookInput{})
	if err != nil {
		t.Fatal(err)
	}
	token := "hook_" + run.ID() + "_0"
	if err := workflow.ResumeHookWithRetries(ctx, token, "generated", 20); err != nil {
		t.Fatal(err)
	}
	value, err := run.Value(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if value.Value != "generated" {
		t.Fatalf("value = %q", value.Value)
	}
}

func TestWorldRetainsAndExpiresHook(t *testing.T) {
	world := workflowtest.New(workflow.Use(hookWorkflow))
	defer world.Close()
	ctx := workflow.NewContext(context.Background(), world)
	run, err := workflow.Start(ctx, hookInput{Token: "retained-hook", Retention: 30 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if err := workflow.ResumeHookWithRetries(ctx, "retained-hook", "resumed", 20); err != nil {
		t.Fatal(err)
	}
	if err := run.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, found, err := workflow.GetHook[string](ctx, "retained-hook"); err != nil || !found {
		t.Fatalf("retained hook found = %t, err = %v", found, err)
	}
	time.Sleep(60 * time.Millisecond)
	if _, found, err := workflow.GetHook[string](ctx, "retained-hook"); err != nil || found {
		t.Fatalf("expired hook found = %t, err = %v", found, err)
	}
}

func TestWorldForceClaimsHook(t *testing.T) {
	world := workflowtest.New(workflow.Use(hookWorkflow))
	defer world.Close()
	ctx := workflow.NewContext(context.Background(), world)
	first, err := workflow.Start(ctx, hookInput{Token: "force-hook"})
	if err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 20; attempt++ {
		_, found, getErr := workflow.GetHook[string](ctx, "force-hook")
		if getErr != nil {
			t.Fatal(getErr)
		}
		if found {
			break
		}
		if attempt == 19 {
			t.Fatal("first hook did not register")
		}
		time.Sleep(10 * time.Millisecond)
	}
	second, err := workflow.Start(ctx, hookInput{Token: "force-hook", Force: true})
	if err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 20; attempt++ {
		hook, found, getErr := world.GetHook(context.Background(), "force-hook")
		if getErr != nil {
			t.Fatal(getErr)
		}
		if found && hook.Task.RunID == second.ID() {
			break
		}
		if attempt == 19 {
			t.Fatal("replacement hook did not register")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := workflow.ResumeHookWithRetries(ctx, "force-hook", "replacement", 20); err != nil {
		t.Fatal(err)
	}
	if err := first.Wait(context.Background()); !errors.Is(err, workflow.ErrHookForceClaimed) {
		t.Fatalf("first error = %v, want ErrHookForceClaimed", err)
	}
	output, err := second.Value(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if output.Value != "replacement" {
		t.Fatalf("replacement = %q", output.Value)
	}
}

type webhookInput struct {
	Token string
}

type webhookOutput struct {
	Body string
}

func (webhookInput) Workflow(webhookOutput) {}

func webhookWorkflow(ctx context.Context, input webhookInput) (webhookOutput, error) {
	webhook := workflow.NewWebhook[string](workflow.WebhookOption{Token: input.Token})
	value, err := webhook.Value(ctx)
	if err != nil {
		return webhookOutput{}, err
	}
	return webhookOutput{Body: value}, nil
}

func TestWorldReceivesWebhook(t *testing.T) {
	world := workflowtest.New(workflow.Use(webhookWorkflow))
	defer world.Close()
	ctx := workflow.NewContext(context.Background(), world)
	run, err := workflow.Start(ctx, webhookInput{Token: "test-webhook"})
	if err != nil {
		t.Fatal(err)
	}
	handler := workflow.NewHTTPHandler(world)
	request := httptest.NewRequest(http.MethodPost, workflow.APIPrefix+"/webhook/test-webhook", strings.NewReader(`"payload"`))
	response := httptest.NewRecorder()
	for attempt := 0; attempt < 20; attempt++ {
		handler.ServeHTTP(response, request)
		if response.Code == http.StatusAccepted {
			break
		}
		time.Sleep(10 * time.Millisecond)
		request = httptest.NewRequest(http.MethodPost, workflow.APIPrefix+"/webhook/test-webhook", strings.NewReader(`"payload"`))
		response = httptest.NewRecorder()
	}
	if response.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusAccepted)
	}
	output, err := run.Value(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if output.Body != "payload" {
		t.Fatalf("body = %q, want payload", output.Body)
	}
}

type webhookRequestOutput struct {
	Body   string
	Method string
	Header string
}

type webhookRequestInput struct {
	Token string
}

func (webhookRequestInput) Workflow(webhookRequestOutput) {}

func webhookRequestWorkflow(ctx context.Context, input webhookRequestInput) (webhookRequestOutput, error) {
	webhook := workflow.NewWebhook[struct{}](workflow.WebhookOption{Token: input.Token})
	request, err := webhook.Request(ctx)
	if err != nil {
		return webhookRequestOutput{}, err
	}
	body, err := io.ReadAll(request.Body)
	if err != nil {
		return webhookRequestOutput{}, err
	}
	return webhookRequestOutput{
		Body:   string(body),
		Method: request.Method,
		Header: request.Header.Get("X-Webhook"),
	}, nil
}

func TestWorldWebhookRequest(t *testing.T) {
	world := workflowtest.New(workflow.Use(webhookRequestWorkflow))
	defer world.Close()
	ctx := workflow.NewContext(context.Background(), world)
	run, err := workflow.Start(ctx, webhookRequestInput{Token: "request-webhook"})
	if err != nil {
		t.Fatal(err)
	}
	handler := workflow.NewHTTPHandler(world)
	for attempt := 0; attempt < 20; attempt++ {
		request := httptest.NewRequest(
			http.MethodPatch,
			workflow.APIPrefix+"/webhook/request-webhook",
			strings.NewReader("raw payload"),
		)
		request.Header.Set("X-Webhook", "preserved")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code == http.StatusAccepted {
			break
		}
		if attempt == 19 {
			t.Fatalf("status = %d, want %d: %s", response.Code, http.StatusAccepted, response.Body.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	output, err := run.Value(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if output != (webhookRequestOutput{Body: "raw payload", Method: http.MethodPatch, Header: "preserved"}) {
		t.Fatalf("request = %+v", output)
	}
}

type webhookConflictInput struct {
	Token string
}

type webhookConflictOutput struct {
	RunID string
	Found bool
}

func (webhookConflictInput) Workflow(webhookConflictOutput) {}

func webhookConflictWorkflow(ctx context.Context, input webhookConflictInput) (webhookConflictOutput, error) {
	webhook := workflow.NewWebhook[string](workflow.WebhookOption{Token: input.Token})
	runID, found, err := webhook.GetConflict(ctx)
	return webhookConflictOutput{RunID: runID, Found: found}, err
}

func TestWorldWebhookGetsConflictRunID(t *testing.T) {
	world := workflowtest.New(
		workflow.Use(webhookWorkflow),
		workflow.Use(webhookConflictWorkflow),
	)
	defer world.Close()
	ctx := workflow.NewContext(context.Background(), world)
	first, err := workflow.Start(ctx, webhookInput{Token: "conflict-webhook"})
	if err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 20; attempt++ {
		_, found, err := workflow.GetHook[string](ctx, "conflict-webhook")
		if err != nil {
			t.Fatal(err)
		}
		if found {
			break
		}
		if attempt == 19 {
			t.Fatal("first webhook did not register")
		}
		time.Sleep(10 * time.Millisecond)
	}
	second, err := workflow.Start(ctx, webhookConflictInput{Token: "conflict-webhook"})
	if err != nil {
		t.Fatal(err)
	}
	output, err := second.Value(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !output.Found || output.RunID != first.ID() {
		t.Fatalf("conflict = %+v, want %q", output, first.ID())
	}
	if err := workflow.ResumeHook(ctx, "conflict-webhook", "complete"); err != nil {
		t.Fatal(err)
	}
	if err := first.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
}

type repeatedHookInput struct {
	Token string
}

type repeatedHookOutput struct {
	First  string
	Second string
}

func (repeatedHookInput) Workflow(repeatedHookOutput) {}

func repeatedHookWorkflow(ctx context.Context, input repeatedHookInput) (repeatedHookOutput, error) {
	hook := workflow.NewHook[string](workflow.HookOption{Token: input.Token})
	first, err := hook.Value(ctx)
	if err != nil {
		return repeatedHookOutput{}, err
	}
	second, err := hook.Value(ctx)
	if err != nil {
		return repeatedHookOutput{}, err
	}
	return repeatedHookOutput{First: first, Second: second}, nil
}

func TestWorldWaitsForTheSameHookTwice(t *testing.T) {
	world := workflowtest.New(workflow.Use(repeatedHookWorkflow))
	defer world.Close()
	ctx := workflow.NewContext(context.Background(), world)
	run, err := workflow.Start(ctx, repeatedHookInput{Token: "repeated-hook"})
	if err != nil {
		t.Fatal(err)
	}
	if err := workflow.ResumeHookWithRetries(ctx, "repeated-hook", "first", 20); err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 20; attempt++ {
		if err := workflow.ResumeHook(ctx, "repeated-hook", "second"); err != nil {
			t.Fatal(err)
		}
		waitCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		output, err := run.Value(waitCtx)
		cancel()
		if err == nil {
			if output != (repeatedHookOutput{First: "first", Second: "second"}) {
				t.Fatalf("output = %+v", output)
			}
			return
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
	}
	t.Fatal("second hook wait did not register")
}

type manualWebhookInput struct {
	Token string
}

type manualWebhookOutput struct {
	Body string
}

func (manualWebhookInput) Workflow(manualWebhookOutput) {}

type webhookResponseInput struct {
	Webhook *workflow.Webhook[string]
	Body    string
}

func respondWebhook(ctx context.Context, input webhookResponseInput) (struct{}, error) {
	return struct{}{}, input.Webhook.ReplyWebhook(ctx, &http.Response{
		StatusCode: http.StatusCreated,
		Header:     http.Header{"X-Workflow": {"responded"}},
		Body:       io.NopCloser(strings.NewReader(input.Body)),
	})
}

func manualWebhookWorkflow(ctx context.Context, input manualWebhookInput) (manualWebhookOutput, error) {
	webhook := workflow.NewWebhook[string](workflow.WebhookOption{Token: input.Token, Manual: true})
	value, err := webhook.Value(ctx)
	if err != nil {
		return manualWebhookOutput{}, err
	}
	if _, err := workflow.Step(ctx, respondWebhook, webhookResponseInput{
		Webhook: webhook,
		Body:    "received " + value,
	}); err != nil {
		return manualWebhookOutput{}, err
	}
	return manualWebhookOutput{Body: value}, nil
}

func TestWorldManuallyRespondsToWebhook(t *testing.T) {
	world := workflowtest.New(workflow.Use(manualWebhookWorkflow))
	defer world.Close()
	ctx := workflow.NewContext(context.Background(), world)
	run, err := workflow.Start(ctx, manualWebhookInput{Token: "manual-webhook"})
	if err != nil {
		t.Fatal(err)
	}
	handler := workflow.NewHTTPHandler(world)
	request := httptest.NewRequest(http.MethodPost, workflow.APIPrefix+"/webhook/manual-webhook", strings.NewReader(`"payload"`))
	response := httptest.NewRecorder()
	for attempt := 0; attempt < 20; attempt++ {
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusNotFound {
			break
		}
		time.Sleep(10 * time.Millisecond)
		request = httptest.NewRequest(http.MethodPost, workflow.APIPrefix+"/webhook/manual-webhook", strings.NewReader(`"payload"`))
		response = httptest.NewRecorder()
	}
	if response.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusCreated)
	}
	if response.Header().Get("X-Workflow") != "responded" {
		t.Fatalf("header = %q, want responded", response.Header().Get("X-Workflow"))
	}
	if response.Body.String() != "received payload" {
		t.Fatalf("body = %q", response.Body.String())
	}
	duplicate := httptest.NewRequest(http.MethodPost, workflow.APIPrefix+"/webhook/manual-webhook", strings.NewReader(`"payload"`))
	duplicateResponse := httptest.NewRecorder()
	handler.ServeHTTP(duplicateResponse, duplicate)
	if duplicateResponse.Code != http.StatusCreated {
		t.Fatalf("duplicate status = %d, want %d", duplicateResponse.Code, http.StatusCreated)
	}
	if duplicateResponse.Body.String() != "received payload" {
		t.Fatalf("duplicate body = %q", duplicateResponse.Body.String())
	}
	if err := run.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
}

type staticWebhookInput struct {
	Token string
}

func (staticWebhookInput) Workflow(struct{}) {}

func staticWebhookWorkflow(ctx context.Context, input staticWebhookInput) (struct{}, error) {
	webhook := workflow.NewWebhook[struct{}](workflow.WebhookOption{
		Token:  input.Token,
		Method: http.MethodPut,
		Response: &http.Response{
			StatusCode: http.StatusNoContent,
			Header:     http.Header{"X-Workflow": {"static"}},
		},
	})
	_, err := webhook.Request(ctx)
	return struct{}{}, err
}

func TestWorldWebhookChecksMethodAndReturnsStaticResponse(t *testing.T) {
	world := workflowtest.New(workflow.Use(staticWebhookWorkflow))
	defer world.Close()
	ctx := workflow.NewContext(context.Background(), world)
	if _, err := workflow.Start(ctx, staticWebhookInput{Token: "static-webhook"}); err != nil {
		t.Fatal(err)
	}
	handler := workflow.NewHTTPHandler(world)
	wrong := httptest.NewRequest(http.MethodPost, workflow.APIPrefix+"/webhook/static-webhook", nil)
	wrongResponse := httptest.NewRecorder()
	for attempt := 0; attempt < 20; attempt++ {
		handler.ServeHTTP(wrongResponse, wrong)
		if wrongResponse.Code != http.StatusNotFound {
			break
		}
		time.Sleep(10 * time.Millisecond)
		wrong = httptest.NewRequest(http.MethodPost, workflow.APIPrefix+"/webhook/static-webhook", nil)
		wrongResponse = httptest.NewRecorder()
	}
	if wrongResponse.Code != http.StatusMethodNotAllowed {
		t.Fatalf("method status = %d, want %d", wrongResponse.Code, http.StatusMethodNotAllowed)
	}
	request := httptest.NewRequest(http.MethodPut, workflow.APIPrefix+"/webhook/static-webhook", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d: %s", response.Code, http.StatusNoContent, response.Body.String())
	}
	if response.Header().Get("X-Workflow") != "static" {
		t.Fatalf("header = %q, want static", response.Header().Get("X-Workflow"))
	}
}
