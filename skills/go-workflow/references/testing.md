# Integration and testing

## Application and worker

Reuse the application's selected World. For a new local setup, open
`worlds/local` with registered runners, call `Prepare(ctx)`, and close it on
shutdown. Persistent Worlds have their own worker/queue setup; opening storage
alone does not necessarily execute deliveries.

Give external callers a client with `workflow.NewContext(ctx, client)` or wrap
HTTP routes in `workflow.Middleware(handler, client)`. Use `NewClient(world)`
for direct access or `NewHTTPClient(endpoint)` for a remote worker. A request
handler can `Start` a shared workflow type without importing runners.

Mount the World's HTTP handler when providing webhooks or remote RPC, and set
BaseURL to its absolute public URL including the path prefix. Follow the
selected adapter's setup for its worker. The RPC handler has no built-in
application authorization, so preserve the application's access controls when
mounting it.

## Streams

Use `workflow.Writer`, `Reader`, and `ReaderAt` inside steps. A writer's
returned release function relinquishes its exclusive session; `Close` ends
the stream for all writers. Readers block until data arrives or the closed
stream reaches EOF. External callers can read or write through a Run handle.
Stream I/O does not belong in the workflow body.

## Workflow tests

Use `testing/synctest` with `worlds/testworld`. Create and close the World
inside the bubble so its workers and timers use the fake clock:

```go
func TestGreeting(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		world := testworld.New(workflow.Use(Greeting))
		defer world.Close()

		ctx := workflow.NewContext(t.Context(), workflow.NewClient(world))
		run, err := workflow.Start(ctx, GreetingInput{Name: "Ada"})
		if err != nil {
			t.Fatal(err)
		}
		got, err := run.Value(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if got != "Hello, Ada" {
			t.Fatalf("greeting = %q", got)
		}
	})
}
```

Import `testing`, `testing/synctest`, the root SDK, and
`github.com/eriicafes/go-workflow/worlds/testworld`. This example assumes a
registered `Greeting` runner and a shared `GreetingInput` whose marker declares
a string output; adapt them to the application's packages.

Use realistic timer and retry durations: synctest advances the fake clock
when the bubble is blocked. Use `synctest.Wait()` before an external hook
resume when the test must let the runner register its hook first. A real
`httptest.Server` can stall fake time through network I/O; prefer
`httptest.NewRecorder` and direct handler calls inside the bubble.

Test step business logic directly. Use `testworld.NewManual` and explicit
runtime deliveries only when verifying replay, redelivery, or delivery
ordering. Check durable outcomes and external idempotency rather than
assuming a step callback is called exactly once.

## Static checks

Run relevant Go tests and `go vet` for the changed packages. Also run the
SDK's `workflowcheck` analyzer to catch replay violations. Use a binary built
from the application's selected SDK version. Install
`github.com/eriicafes/go-workflow/cmd/workflowcheck` at that version, then run:

```sh
go vet -vettool="$(command -v workflowcheck)" ./...
```
