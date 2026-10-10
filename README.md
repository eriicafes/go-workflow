# go-workflow

`go-workflow` is a replay-based workflow runtime. Workflows and steps are
ordinary named Go functions using `context.Context`. There are no generated
registries, workflow interfaces, or SDK-specific context type.

`workflow.NewContext` is the boundary between application code and scheduling.
Clients communicate only with a worker HTTP endpoint. They never receive an
World or database handle. The Postgres worker inserts root runs and sends
`pg_notify` in the same transaction. Durable primitives create immutable
ordered commands and queued tasks. A worker leases one task, then replays the
parent function privately until it reaches that exact command. This is also
the external Dev UI/proxy contract.

## Definitions

Definitions are pure types, so request handlers can import them without
importing workflow implementations.

```go
package workflowdefs

type GenerateReport struct { Topic string }
type GenerateReportResult struct { Summary string }

func (GenerateReport) Workflow(GenerateReportResult) {}
```

## Local World

`worlds/local` executes the same replay model in memory. It starts runs and
durable operations automatically, so local development needs no worker, queue,
or database. Its state is lost when the process exits.

```go
// import localworld "github.com/eriicafes/go-workflow/worlds/local"
runners := workflow.Runners(workflow.Use(flows.GenerateReport))

world, err := localworld.Open(".workflow-data", runners)
if err != nil { log.Fatal(err) }
defer world.Close()
if err := world.Start(context.Background()); err != nil { log.Fatal(err) }

run, err := workflow.Start(
    workflow.NewContext(context.Background(), world),
    workflowdefs.GenerateReport{Topic: "workflows"},
)
if err != nil { log.Fatal(err) }
result, err := run.Value(context.Background())
```

## Implementations and steps

Workflow functions and steps are ordinary functions. `Step` and `Background`
accept either named functions or inline closures. The analyzer requires a
closure to receive workflow-relevant values through its explicit input.
Calling a step function directly is normal Go and repeats during replay.

```go
package flows

func GenerateReport(ctx context.Context, input workflowdefs.GenerateReport) (workflowdefs.GenerateReportResult, error) {
    sources, err := workflow.Step(ctx, steps.FindSources, input.Topic)
    if err != nil { return workflowdefs.GenerateReportResult{}, err }

    fmt.Fprintf(workflow.Writer(ctx), "found %d sources\n", len(sources))
    return workflowdefs.GenerateReportResult{Summary: strings.Join(sources, ", ")}, nil
}

package steps

func FindSources(ctx context.Context, topic string) ([]string, error) {
    // I/O belongs in a durable operation.
    // workflow.IdempotencyKey(ctx) is stable for this operation sequence.
    return query(ctx, topic)
}
```

## Application and worker

The application schedules by importing only workflow definition types and an
HTTP client. The worker is a separate entry point that imports the
implementation package, owns the world, and exposes the workflow HTTP API.

```go
// cmd/app/main.go
client := workflow.NewHTTPClient("http://workflow-worker:8081")

type app struct { workflows workflow.Client }

func (a app) createReport(w http.ResponseWriter, r *http.Request) {
    var input workflowdefs.GenerateReport
    if err := json.NewDecoder(r.Body).Decode(&input); err != nil { /* 400 */ }

    run, err := workflow.Start(workflow.NewContext(r.Context(), a.workflows), input)
    if err != nil { /* 500 */ }
    // Reply 202 with run.ID(). The request does not execute workflow code.
}

server := app{workflows: client}
http.ListenAndServe(":8080", http.HandlerFunc(server.createReport))
```

```go
// cmd/worker/main.go
world, err := postgresworld.Open(ctx, postgresworld.Config{URL: os.Getenv("DATABASE_URL")})
if err != nil { log.Fatal(err) }
defer world.Close()

 runners := workflow.Runners(
    workflow.Use(flows.GenerateReport),
)

go postgresworld.Run(ctx, world, runners)
http.ListenAndServe(":8081", postgresworld.HTTP(world))
```

Run these independently with `go run ./examples/basic/cmd/app` and `go run
./examples/basic/cmd/worker/pg`. Only the worker needs `DATABASE_URL`. Point the
application's `WORKFLOW_URL` at the worker HTTP endpoint. The complete Postgres
example can also run through `go run ./examples/basic/cmd/dev`; its workflow
checks run before the application and worker are built.

## World runtime entry points

World packages own deployment-specific entry points:

```go
// Postgres: a long-running poller.
go postgresworld.Run(ctx, postgresWorld, runners)
http.ListenAndServe(":8081", postgresworld.HTTP(postgresWorld))

// AWS: install this as the Lambda SQS handler.
lambda.Start(awsworld.Queue(awsWorld, runners))

// AWS: a separate Lambda Function URL provides the app-facing API.
lambda.Start(lambdaurl.Wrap(awsworld.HTTP(awsWorld)))

// Vercel: deploy separate Queue-consumer and app-facing API functions.
http.ListenAndServe(":8081", vercelworld.Queue(vercelWorld, runners))
http.ListenAndServe(":8082", vercelworld.HTTP(vercelWorld))
```

The examples are split accordingly:

- `cmd/worker/aws/queue` and `cmd/worker/aws/api`
- `cmd/worker/vercel/queue` and `cmd/worker/vercel/api`

Only the queue entrypoints register workflow implementations.

The AWS and Vercel queues publish a `workflow.Task` after each root-run or
command task is persisted. Claiming is lease-fenced, so a duplicate queue
delivery is a successful no-op.

`Workflow[T]` and `Result[T]` both implement `Wait`, `Value`, and `Unwrap`.
`Background` creates a durable operation. `Map`, `All`, `Join`, `JoinSettled`,
`Race`, `Timeout`, and `Sleep` compose them. Each operation is written as attempted
before user code runs and receives an idempotency key. Command input, function
identity, and callsite fingerprint must match during replay. Completion writes
are fenced by task lease tokens, and task leases heartbeat while code runs.

`Map` records one background-step command per input. `Join` records one
fail-fast barrier, `JoinSettled` records one wait-all barrier, and `Race`
records one winner-selection command. `All` is `Map` followed by `Join`: on a
failure it returns the result slice and the first completion failure without
waiting for unfinished mapped work.

`workflow.Writer(ctx)` stores replay-safe ordered `log` commands and events.
The Go package deliberately serves no Dev UI. `Workflow.Events` and the Postgres
operation tables are the contract for `go-dev` or a production proxy.

`workflow.Start` is also valid inside workflows and steps. It records a nested
run command and returns the same child `Workflow` ID on replay.

## go-dev static checks

Use `github.com/eriicafes/go-workflow/workflowdev` to run the analyzer before
every `go-dev` rebuild:

```go
session := dev.New()
session.Catch(session.Run(dev.Cmd{
    Run: dev.Package("./cmd/api"),
    Prepare: dev.Hooks(workflowdev.Check()),
    Watch: dev.Values("."),
}))
session.Catch(session.Wait())
```

It validates `Use` bindings and reports direct I/O, clocks, random values,
goroutines, channel selects, map iteration, and closure/wrapper steps in
workflow bodies. Deterministic slice loops are allowed.
