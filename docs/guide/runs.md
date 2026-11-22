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

### Start options

`workflow.StartOptions` sets how a run is created. Its zero value is the same as passing none.

```go
run, err := workflow.Start(ctx, workflows.GenerateReport{Topic: "workflows"}, workflow.StartOptions{
	Retention:  workflow.RetainNone,
	Attributes: map[string]string{"tenant": "acme"},
})
```

| Field | Effect |
| --- | --- |
| `Deployment` | The deployment the run starts on and stays pinned to: `workflow.LatestDeployment` or `workflow.DeploymentWithID(id)`. It needs a World with deployment affinity. See [moving chains to the latest deployment](/guide/replay#moving-chains-to-the-latest-deployment). |
| `Retention` | `workflow.RetainNone` deletes the run's data as soon as it finishes, recorded as `$retention: "0"`. `Run.Value` then returns a `*workflow.RunExpiredError`, so return a result you need through storage you control. The zero value, `workflow.RetainDefault`, leaves it to the World. |
| `Attributes` | Plain-text attributes set on the run as it is created: at most 64, keys up to 256 characters, values up to 256 bytes. Otherwise `Start` returns `worlds.ErrAttributeInvalid`. |
| `AllowReservedAttributes` | Permits keys starting with `$`, which are reserved for framework and library code. |
| `Hook` | Creates a hook with the run, claiming its token: `&workflow.StartHook{Token, MinRetention, Metadata}`. A second start with the token creates no run and returns a `*worlds.HookConflictError` naming the run that holds it, so the token makes the start idempotent. A start that could not confirm the run was admitted or queued returns a `*workflow.StartError`, after which starting again with the token is safe. The hook takes payloads as soon as `Start` returns. The first `workflow.CreateHook` the workflow declares with the token opens it, and a later one with the token conflicts with it. The opening hook takes no `MinRetention` or `Force`, which return `workflow.ErrStartHookOptions`; set the retention on `StartHook`. It needs a World with `Capabilities().StartHook`. |
| `Region` | The region the run's data lives in and its deliveries go to, one of the World's regions. Only a World that implements `worlds.RegionalRunIDs` uses it; others ignore it. |
| `ReplayedFromRunID` | The run this run replays, `wrun_` and a ULID. It is recorded in the run's `ExecutionContext`, so the Workflow web UI links the replay to its source. Otherwise `Start` returns `workflow.ErrInvalidStartOptions`. |
| `NoArgumentCompression` | Stores the run's input uncompressed, for input that does not compress, such as media or encrypted bytes. |

A child run takes its options beside the attributes that link it to its
parent.

## Setting attributes

`workflow.SetAttributes` changes a running run's attributes as one `attr_set`
event. A nil value deletes its key, and changes are recorded in key order.

```go
err := workflow.SetAttributes(ctx, map[string]*string{"phase": &phase, "draft": nil})
```

In workflow context the write takes the ID `attr_` and the run's next
correlation ULID, drawn from the run's seed like step IDs, and returns once it
is recorded. Replay matches it by ID and checks the changes. In step context
it records the write for the step's attempt at once. Attributes outside the
limits return a `FatalError` wrapping `worlds.ErrAttributeInvalid`, and
`SetAttributesOptions.AllowReservedAttributes` permits keys starting with `$`.

## Getting an existing run

`workflow.Get` returns a handle to a run by ID. The type parameter is the
workflow's output type. `Get` does not check that the run exists. Call
`run.Exists` to check it.

```go
run, err := workflow.Get[workflows.GenerateReportResult](ctx, runID)
```

## The Run handle

| Method | Description |
| --- | --- |
| `ID` | Run ID. |
| `Value` | Output once the run finishes. Blocks outside workflow context until it finishes or `ctx` is done. |
| `Status` | Current `Status`. |
| `Exists` | Whether the run is recorded in its World. |
| `WorkflowName` | Name of the workflow the run executes. |
| `Cancel` | Cancels the run. No-op if it already finished. |
| `WakeUp` | Settles pending timers early. See [Sleep and Timers](/guide/sleep#waking-a-run-early). |
| `Reader` | Reads a stream from a chunk index. See [Streams](/guide/streams). |
| `Writer` | Writes a stream from outside the run. |

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

Steps use the same statuses except for `cancelled`. A cancelled step records
a fatal failure.

### In workflow context

Inside a runner, methods that read or change the run use
[inline steps](/guide/inline-steps), and replay returns the recorded result.
They never wait: `Value` returns `workflow.ErrRunNotFinished` for a run that
hadn't finished. `WakeUp` records which waits it will wake in one inline step
before waking them in another, so a retry never wakes a timer the other run
created later. The exceptions are `Reader` and `Writer`, which are I/O and
return `workflow.ErrNotInStep` there. Use them in a step.

## Cancelling a run

`Run.Cancel` stops new work and leaves already-running steps to finish.
Reading a cancelled run returns `workflow.ErrRunCancelled`. A managed context
passed to `Start` does not control the run's lifetime. See
[Cancellation](/guide/cancellation#cancelling-a-run) for examples and step
cancellation behavior.

## Deleting run data

`workflow.DeleteRuns` deletes the data of up to 100 finished runs: their
inputs, outputs and errors, the payloads of their steps, events and hooks, and
their streams' chunks. It cannot be undone.

```go
result, err := workflow.DeleteRuns(ctx, runA.ID(), runB.ID())
// result.Results[i].Outcome: deleted, already_deleted, skipped, not_found, failed
```

The runs stay listed, with their steps, events and hooks, IDs, statuses,
timestamps and attributes. Their `ExpiredAt` is set, the Workflow web UI shows
their data as expired, and `Run.Value` returns a `*workflow.RunExpiredError`. A run
that has not finished is skipped; cancel it first. Called in a step,
`DeleteRuns` sends retryable failures again once and then fails the step with a
`RetryableError`, and a World's rate limit (`*worlds.ThrottleError`) retries the
step after the wait the World asked for. A World without `Capabilities().DeleteData` returns an error
wrapping `errors.ErrUnsupported`.

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
