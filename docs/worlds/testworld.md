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

## Encryption

`SetEncryptionKey(key)` makes the World encrypt every run's payloads with a key
derived from a 32-byte key, as a World configured with an `EncryptionKey` does,
so tests cover encrypted runs. Set it before starting runs.

## Manual delivery

`testworld.NewManual` records deliveries without executing them. The test
executes them with a `core.Runtime` and acknowledges each with `Acknowledge`.

```go
runners := []core.Runner{workflow.Use(flows.GenerateReport)}
world := testworld.NewManual(runners...)
runtime := core.NewRuntime(world, core.Runners(runners...))

for len(world.QueuedMessages()) > 0 {
	message := world.QueuedMessages()[0]
	if err := runtime.Execute(ctx, message.Input); err != nil {
		t.Fatal(err)
	}

	if err := world.Acknowledge(ctx, message.ID); err != nil {
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
