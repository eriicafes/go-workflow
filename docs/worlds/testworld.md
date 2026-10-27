# Test World

```go
import "github.com/eriicafes/go-workflow/worlds/testworld"
```

`testworld` implements a World in memory, for tests. It keeps nothing across
processes and is not meant for production. Its base URL is
`http://localhost/v1/workflow`.

## Automatic delivery

`testworld.New` executes deliveries in-process, each in its own goroutine once
it is due. A failing delivery is tried at most three times and then left
queued.

```go
world := testworld.New(
	workflow.Use(flows.GenerateReport),
	workflow.Use(flows.PublishReport),
)
defer world.Close()

ctx = workflow.NewContext(ctx, workflow.NewClient(world))
run, err := workflow.Start(ctx, workflows.GenerateReport{Topic: "go"})
```

Note that `New` takes runners directly, not a `RunnerSet`.

Waiting for runs and stream chunks blocks on a change notification rather than
polling, and delayed deliveries wait on ordinary timers. Create the World inside
a [`testing/synctest`](https://pkg.go.dev/testing/synctest) bubble and those
timers run on the bubble's fake clock, so a run that sleeps for weeks finishes
instantly. See [Testing](/guide/testing#sleeps-and-timers).

## Manual delivery

`testworld.NewManual` records deliveries without executing them. The test
executes them with a `worlds.Runtime` and acknowledges each with `QueueCancel`.

```go
runners := []worlds.Runner{workflow.Use(flows.GenerateReport)}
world := testworld.NewManual(runners...)
runtime := worlds.NewRuntime(world, worlds.Runners(runners...))

for len(world.QueuedMessages()) > 0 {
	message := world.QueuedMessages()[0]
	if err := runtime.Execute(ctx, message.Input); err != nil {
		t.Fatal(err)
	}

	if err := world.QueueCancel(ctx, worlds.QueueCancelInput{MessageID: message.ID}); err != nil {
		t.Fatal(err)
	}
}
```

| Method | Description |
| --- | --- |
| `QueuedMessages()` | Deliveries not yet acknowledged, in queue order, with their IDs. Each `Input` carries its `MessageID`. |
| `Queued()` | Just the `QueueInput` of each pending delivery. |
| `Close()` | Stops delivering. Running deliveries see their context cancelled. State stays readable. |

See [Testing](/guide/testing) for more patterns.
