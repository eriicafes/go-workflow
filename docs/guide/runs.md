# Starting and Reading Runs

## Clients

Code outside a run uses a **Client** to start, read, cancel, and resume runs.
The `workflow` package functions find the Client in the context.

| Constructor | Use when |
| --- | --- |
| `workflow.NewClient(world)` | The World is open in the same process. |
| `workflow.NewHTTPClient(endpoint)` | The World is served by its [HTTP handler](/worlds/#http-handlers) in another process. |

Put a Client in a context with `workflow.NewContext`, or in every request
context of an HTTP server with `workflow.Middleware`:

```go
ctx = workflow.NewContext(ctx, workflow.NewClient(world))
```

```go
client := workflow.NewHTTPClient("http://workflow-worker:8081/v1/workflow")
log.Fatal(http.ListenAndServe(":8080", workflow.Middleware(mux, client)))
```

`NewHTTPClient` takes an absolute URL and panics on anything else. It uses
`http.DefaultClient` unless you pass your own `*http.Client`, for example one
that adds authentication:

```go
client := workflow.NewHTTPClient(endpoint, &http.Client{Transport: authTransport})
```

The endpoint is the URL prefix the World's handler serves, including
`/v1/workflow`. The client posts to `<endpoint>/rpc`, so for a worker serving
`HTTP(world)` at the root of port 8081, the endpoint is
`http://workflow-worker:8081/v1/workflow`.

## Starting a run

`workflow.Start` starts a run of a workflow and returns a handle without
waiting. It needs only the workflow type.

```go
run, err := workflow.Start(ctx, workflows.GenerateReport{Topic: "workflows"})
if err != nil {
	return err
}

fmt.Println(run.ID())
```

It returns `workflow.ErrNoClient` when the context has no Client and does not
belong to a run. In workflow context, `Start` starts a
[child run](/guide/composition).

## Getting an existing run

`workflow.Get` returns a handle to a run by ID. The type parameter is the
workflow's output type. `Get` does not check that the run exists; use
`run.Exists`.

```go
run, err := workflow.Get[workflows.GenerateReportResult](ctx, runID)
```

## The Run handle

| Method | Description |
| --- | --- |
| `ID()` | Run ID. |
| `Value(ctx)` | Output once the run finishes. Blocks outside a run until it finishes or `ctx` is done. |
| `Status(ctx)` | Current `Status`. |
| `Exists(ctx)` | Whether the run is recorded in its World. |
| `WorkflowName(ctx)` | Name of the workflow the run executes. |
| `Cancel(ctx)` | Cancels the run. No-op if it already finished. |
| `WakeUp(ctx, ...WakeOptions)` | Settles pending timers early; see [Sleep and Timers](/guide/sleep#waking-a-run-early). |
| `Reader(ctx, ...ReaderOption)` | Reads a stream from the start; see [Streams](/guide/streams). |
| `ReaderAt(ctx, offset, ...ReaderOption)` | Reads a stream from an offset. |
| `Writer(ctx, ...WriterOption)` | Writes a stream from outside the run. |

`Value` returns a failed run's error as a
[`*workflow.Error`](/guide/errors) and a cancelled run's as
`workflow.ErrRunCancelled`. `Status` and `WorkflowName` return
`worlds.ErrRunNotFound` for a missing run.

### Statuses

| Status | Meaning |
| --- | --- |
| `workflow.StatusPending` | Recorded but not started. |
| `workflow.StatusRunning` | Started and not finished. |
| `workflow.StatusCompleted` | Finished with an output. |
| `workflow.StatusFailed` | Finished with an error. |
| `workflow.StatusCancelled` | Cancelled before it finished. |

Steps use pending, running, completed, and failed statuses; cancellation is a fatal step failure.

### In workflow context

Inside a runner, `Status`, `Exists`, `WorkflowName`, `Value`, `Cancel`,
and `WakeUp` each run once as an [inline step](/guide/steps#inline-steps), and
replay returns the recorded result. They never wait: `Value` returns
`workflow.ErrRunNotFinished` for a run that hadn't finished. `WakeUp` records
which waits it will wake before waking them, so a retry never wakes a timer the
other run created later. `Reader`, `ReaderAt`, and `Writer` are I/O and return
`workflow.ErrNotInStep` there; use them in a step.

## Resuming hooks

`workflow.ResumeHook`, `ResumeHookWithRetries`, and `GetHook` also use the
Client in the context. See [Hooks](/guide/hooks).

## Lower-level access

`workflow.Client` is an alias of `worlds.Client`, which exposes the full API:
listing runs and steps, setting attributes, reading streams by cursor, and
analytics. See the [`worlds` reference](/reference/worlds#client).

```go
client := workflow.NewClient(world)
page, err := client.RunsList(ctx, worlds.RunsListInput{})
```

`Run` exposes `Status` for readiness and is not a promise. Its lifetime is independent of managed context cancellation; call `Run.Cancel` explicitly to cancel it. In workflow context, its methods record inline steps without inheriting managed context cancellation. Cancelling a run prevents new work and permits running steps to finish and record their outcomes. It does not cancel their contexts. Managed cancellation contexts passed explicitly to steps control cooperative step cancellation.
