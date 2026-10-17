# Testing

Test workflows end to end with the [in-memory test World](/worlds/testworld)
inside a [`testing/synctest`](https://pkg.go.dev/testing/synctest) bubble. The
test World runs every delivery in-process, and synctest gives the bubble a fake
clock, so sleeps, timers, timeouts, and retry delays of any length finish
instantly and deterministically.

## Testing workflows end to end

Create the World inside `synctest.Test`, start a run, and await it like in
production:

```go
func TestGenerateReport(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		world := testworld.New(workflow.Use(flows.GenerateReport))
		defer world.Close()

		ctx := workflow.NewContext(t.Context(), workflow.NewClient(world))
		run, err := workflow.Start(ctx, workflows.GenerateReport{Topic: "go"})
		if err != nil {
			t.Fatal(err)
		}

		result, err := run.Value(ctx)
		if err != nil {
			t.Fatal(err)
		}

		if result.Summary != "source for go" {
			t.Fatalf("summary = %q", result.Summary)
		}
	})
}
```

::: warning Create the World inside the bubble
The World's delivery goroutines and timers must belong to the bubble, so call
`testworld.New` and `world.Close` inside the function passed to
`synctest.Test`. A World created outside it uses the real clock.
:::

## Sleeps and timers

Inside the bubble, a runner that sleeps for 30 days completes as soon as every
goroutine is waiting: synctest moves the fake clock straight to the timer's due
time. No `WakeUp`, polling, or short test durations are needed.

```go
func Monthly(ctx context.Context, input workflows.Monthly) (workflows.MonthlyResult, error) {
	if _, err := workflow.Step(ctx, sendInvoice, input.CustomerID); err != nil {
		return workflows.MonthlyResult{}, err
	}

	if err := workflow.Sleep(ctx, 30*24*time.Hour); err != nil {
		return workflows.MonthlyResult{}, err
	}

	if _, err := workflow.Step(ctx, sendInvoice, input.CustomerID); err != nil {
		return workflows.MonthlyResult{}, err
	}

	return workflows.MonthlyResult{Invoices: 2}, nil
}
```

```go
func TestMonthly(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		world := testworld.New(workflow.Use(flows.Monthly))
		defer world.Close()

		ctx := workflow.NewContext(t.Context(), workflow.NewClient(world))
		run, err := workflow.Start(ctx, workflows.Monthly{CustomerID: "c_1"})
		if err != nil {
			t.Fatal(err)
		}

		start := time.Now()
		result, err := run.Value(ctx) // returns at once in real time
		if err != nil {
			t.Fatal(err)
		}

		if elapsed := time.Since(start); elapsed < 30*24*time.Hour {
			t.Fatalf("elapsed = %v", elapsed)
		}

		if result.Invoices != 2 {
			t.Fatalf("invoices = %d", result.Invoices)
		}
	})
}
```

`time.Now` in steps and in the test reads the same fake clock, so you can assert
on durations. `WithTimeout`, `WithDeadline`, `Timer`, and `RetryableError` delays behave the same
way.

To check what a run does at a point in time, sleep the test itself. The fake
clock advances only as far as you ask:

```go
time.Sleep(24 * time.Hour) // the run is one day in
synctest.Wait()            // let it settle

status, err := run.Status(ctx)
if err != nil {
	t.Fatal(err)
}
```

## Resuming hooks

`synctest.Wait` returns once every goroutine in the bubble is blocked, which
means the run has suspended on its hook. Resume it right after:

```go
synctest.Test(t, func(t *testing.T) {
	world := testworld.New(workflow.Use(flows.ReviewOrder))
	defer world.Close()

	ctx := workflow.NewContext(t.Context(), workflow.NewClient(world))
	run, err := workflow.Start(ctx, workflows.ReviewOrder{OrderID: "42"})
	if err != nil {
		t.Fatal(err)
	}

	synctest.Wait() // the run has created its hook and suspended

	if err := workflow.ResumeHook(ctx, "approve:42", Approval{Approved: true}); err != nil {
		t.Fatal(err)
	}

	result, err := run.Value(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if !result.Approved {
		t.Fatal("order was not approved")
	}
})
```

Without the wait, the hook may not exist yet. `ResumeHookWithRetries` also works
in the bubble, since its retry delays run on the fake clock.

## Streams

Readers block until data arrives, which synctest treats as waiting, so a test
can read a run's stream to the end while the run sleeps between writes:

```go
reader, err := run.Reader(ctx, workflow.ReaderOption{Name: "progress"})
if err != nil {
	t.Fatal(err)
}

progress, err := io.ReadAll(reader) // returns once the stream is closed
```

## Testing HTTP handlers

Wrap your handlers with `workflow.Middleware` and the test World's client:

```go
synctest.Test(t, func(t *testing.T) {
	world := testworld.New(workflow.Use(flows.GenerateReport))
	defer world.Close()

	handler := workflow.Middleware(routes(), workflow.NewClient(world))
	request := httptest.NewRequest(http.MethodPost, "/reports", strings.NewReader(`{"topic":"go"}`))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
})
```

`httptest.NewRecorder` works in the bubble. Avoid a real `httptest.Server`
there: synctest doesn't count a goroutine blocked on network I/O as waiting, so
the fake clock can stall.

## Stepping through deliveries

To inspect each delivery the runtime would make, use `testworld.NewManual`. It
records deliveries without executing them, so the test executes and
acknowledges them one at a time:

```go
world := testworld.NewManual(workflow.Use(flows.GenerateReport))
runtime := worlds.NewRuntime(world, worlds.Runners(workflow.Use(flows.GenerateReport)))

// ... start a run ...

for _, message := range world.QueuedMessages() {
	if err := runtime.Execute(ctx, message.Input); err != nil {
		t.Fatal(err)
	}

	// Acknowledge the delivery.
	if err := world.QueueCancel(ctx, worlds.QueueCancelInput{MessageID: message.ID}); err != nil {
		t.Fatal(err)
	}
}
```

`world.Queued()` returns just the inputs of pending deliveries. A timer's
delivery still checks its due time on replay, so executing it early does not
skip the wait; use synctest for that.

## Testing step functions

Step functions are ordinary functions. Test them directly:

```go
sources, err := findSources(context.Background(), "go")
```

`workflow.StepID` and `GetStepMetadata` return zero values outside a step.

## Checking determinism

Run the [workflow analyzer](/tooling/analyzer) in CI:

```sh
go vet -vettool=$(which workflowcheck) ./...
```
