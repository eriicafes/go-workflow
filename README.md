# go-workflow

`go-workflow` is a replay-based workflow runtime. Workflows and steps are
ordinary named Go functions using `context.Context`. There are no generated
registries, workflow interfaces, or SDK-specific context type.

`workflow.NewContext` is the boundary between application code and scheduling.
Clients communicate only with a worker HTTP endpoint. They never receive an
a World or database handle. The Postgres worker inserts root runs and sends
`pg_notify` in the same transaction. Durable primitives create immutable
ordered commands and queued tasks. A worker leases one task, then replays the
parent function privately until it reaches that exact command. This is also
the external Dev UI/proxy contract.

## Definitions

Definitions are pure types, so request handlers can import them without
importing workflow implementations.

```go
package workflowdefs

type GenerateReport struct {
	Topic string
}

type GenerateReportResult struct {
	Summary string
}

func (GenerateReport) Workflow(GenerateReportResult) {}
```

## Local World

`worlds/local` executes the same replay model in a single process and stores
runs, events, hooks, streams, and queued work as JSON files under a directory,
so local development needs no worker, queue, or database and survives restarts.
`Start` takes an exclusive lock on the directory, recovers interrupted writes,
and begins delivering queued work in-process.

```go
// import localworld "github.com/eriicafes/go-workflow/worlds/local"
runners := workflow.Runners(
	workflow.Use(flows.GenerateReport),
)

world, err := localworld.Open(localworld.Config{Dir: ".workflow-data"}, runners)
if err != nil {
	log.Fatal(err)
}
defer world.Close()
if err := world.Start(context.Background()); err != nil {
	log.Fatal(err)
}

run, err := workflow.Start(
	workflow.NewContext(context.Background(), world),
	workflowdefs.GenerateReport{
		Topic: "workflows",
	},
)
if err != nil {
	log.Fatal(err)
}
result, err := run.Value(context.Background())
if err != nil {
	log.Fatal(err)
}
log.Printf("report summary: %s", result.Summary)
```

## Implementations and steps

Workflow functions and steps are ordinary functions. `Step` and `Background`
accept either named functions or inline closures. The analyzer requires a
closure to receive workflow-relevant values through its explicit input.
Calling a step function directly is normal Go and repeats during replay.

## Rollbacks

Register a compensation after the work it reverses. If the workflow later
fails, registered compensations run as durable steps in reverse registration
order. Successful workflows never execute them.

```go
invoice, err := workflow.Step(ctx, steps.CreateInvoice, input)
if err != nil {
	return Report{}, err
}
if err := workflow.OnRollback(ctx, steps.VoidInvoice, invoice.ID); err != nil {
	return Report{}, err
}
```

`OnRollback` compensations return only `error`. They use the same retry and
step metadata behavior as ordinary durable steps.

```go
package flows

func GenerateReport(
	ctx context.Context,
	input workflowdefs.GenerateReport,
) (workflowdefs.GenerateReportResult, error) {
	sources, err := workflow.Step(
		ctx,
		steps.FindSources,
		input.Topic,
	)
	if err != nil {
		return workflowdefs.GenerateReportResult{}, err
	}

	if _, err := workflow.Step(
		ctx,
		steps.WriteReportProgress,
		len(sources),
	); err != nil {
		return workflowdefs.GenerateReportResult{}, err
	}

	return workflowdefs.GenerateReportResult{
		Summary: strings.Join(sources, ", "),
	}, nil
}

package steps

func FindSources(ctx context.Context, topic string) ([]string, error) {
	// I/O belongs in a durable operation.
	// workflow.StepID(ctx) is stable for this operation sequence.
	return query(ctx, topic)
}

func WriteReportProgress(ctx context.Context, count int) (struct{}, error) {
	writer, release, err := workflow.Writer(ctx)
	if err != nil {
		return struct{}{}, err
	}
	defer release()

	_, err = fmt.Fprintf(writer, "found %d sources\n", count)
	return struct{}{}, err
}
```

## Streams

Streams are run-owned byte streams backed by the selected world rather than by
the event log. Only steps can acquire a writer or reader. A stream has one
active writer at a time. Another writer waits until the current writer releases
its lock or closes the stream. Closing is visible to every reader. Runs close
any remaining streams when they reach a terminal state.

```go
func WriteReportProgress(ctx context.Context, count int) (struct{}, error) {
	writer, release, err := workflow.Writer(
		ctx,
		workflow.WriterOption{Name: "progress"},
	)
	if err != nil {
		return struct{}{}, err
	}
	defer release()

	_, err = fmt.Fprintf(writer, "found %d sources\n", count)
	return struct{}{}, err
}

reader, err := run.ReaderAt(
	ctx,
	-128,
	workflow.ReaderOption{Name: "progress"},
)
if err != nil {
	return err
}
// io.ReadAll(reader) waits for new data until the stream closes.
```

`workflow.Writer` and `workflow.Reader` are step-only. `run.Writer` and
`run.Reader` are for application handlers or durable steps; a root workflow
body receives `ErrNoWorkflow`. `ReaderAt` accepts negative offsets relative to
the stream's end. `Release` completes the writer's step-local lifetime without
closing. `Close` closes the stream for every reader. Use `bufio.Writer` when
writes need application-side buffering.

## Hooks and webhooks

`NewHook` records or derives a replay-stable token but does not claim it until
`Value` or `GetConflict` is reached during workflow replay. `ResumeHook` delivers
a typed payload, and `ResumeHookWithRetries` handles a resume racing a newly
started run. Pass an explicit token when an external caller must know the URL
before the workflow reaches the hook.

```go
hook := workflow.NewHook[string](workflow.HookOption{
	Token: input.Token,
})
value, err := hook.Value(ctx)
```

`CreateWebhook[T]` receives and decodes a JSON request body as `T`. Call
`webhook.Request(ctx)` when the workflow needs the reconstructed
`*http.Request` instead. Its URL is the World-configured absolute public base
URL followed by `/webhooks/:token`, served by `worlds.HTTPHandler`. The token is the route's only authorization, so
verify provider signatures before trusting the request payload.

`WebhookOption` can require one HTTP method, return a static
`*http.Response`, or set `Manual: true`. A manual webhook waits until a
durable step calls `webhook.RespondWith(ctx, response)`.

## Application and worker

The application schedules by importing only workflow definition types and an
HTTP client. The worker is a separate entry point that imports the
implementation package, owns the world, and exposes the workflow HTTP API.

```go
// cmd/app/main.go
client := workflow.NewHTTPClient("http://workflow-worker:8081")

type app struct {
	workflows workflow.Client
}

func (a app) createReport(w http.ResponseWriter, r *http.Request) {
	var input workflowdefs.GenerateReport
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		http.Error(w, "invalid report input", http.StatusBadRequest)
		return
	}

	run, err := workflow.Start(
		workflow.NewContext(r.Context(), a.workflows),
		input,
	)
	if err != nil {
		http.Error(w, "start report workflow", http.StatusInternalServerError)
		return
	}

	// Reply 202 with run.ID(). The request does not execute workflow code.
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte(run.ID()))
}

server := app{workflows: client}
if err := http.ListenAndServe(":8080", http.HandlerFunc(server.createReport)); err != nil {
	log.Fatal(err)
}
```

```go
// cmd/worker/main.go
world, err := postgresworld.Open(ctx, postgresworld.Config{
	BaseURL: os.Getenv("WORKFLOW_BASE_URL"), // e.g. https://workflows.example.com/v1/workflow
	URL:     os.Getenv("DATABASE_URL"),
})
if err != nil {
	log.Fatal(err)
}
defer world.Close()

runners := workflow.Runners(
	workflow.Use(flows.GenerateReport),
)

go func() {
	if err := postgresworld.Start(ctx, world, runners); err != nil {
		log.Printf("workflow worker stopped: %v", err)
	}
}()
if err := http.ListenAndServe(":8081", postgresworld.HTTP(world)); err != nil {
	log.Fatal(err)
}
```

Run these independently with `go run ./examples/basic/cmd/app` and `go run
./examples/basic/cmd/worker/pg`. Only the worker needs `DATABASE_URL`. Point the
application's `WORKFLOW_URL` at the worker HTTP endpoint. The complete Postgres
example can also run through `go run ./examples/basic/cmd/dev`; its workflow
checks run before the application and worker are built.

## World runtime entry points

World packages own deployment-specific entry points:

```go
// Postgres: a long-running worker woken by LISTEN/NOTIFY.
go func() {
	if err := postgresworld.Start(ctx, postgresWorld, runners); err != nil {
		log.Printf("workflow worker stopped: %v", err)
	}
}()
if err := http.ListenAndServe(":8081", postgresworld.HTTP(postgresWorld)); err != nil {
	log.Fatal(err)
}

// AWS: install this as the Lambda SQS handler.
lambda.Start(awsworld.Queue(awsWorld, runners))

// AWS: a separate Lambda Function URL provides the app-facing API.
lambda.Start(lambdaurl.Wrap(awsworld.HTTP(awsWorld)))

// Vercel: deploy separate Queue-consumer and app-facing API functions.
if err := http.ListenAndServe(":8081", vercelworld.Queue(vercelWorld, runners)); err != nil {
	log.Fatal(err)
}
if err := http.ListenAndServe(":8082", vercelworld.HTTP(vercelWorld)); err != nil {
	log.Fatal(err)
}
```

`HTTP(world)` serves the RPC and webhook endpoints under `/v1/workflow`.
`RPC(world)` and `Webhook(world)` serve each endpoint on its own, for
deployments that expose only one of them.

The examples are split accordingly:

- `cmd/worker/aws/queue` and `cmd/worker/aws/api`
- `cmd/worker/vercel/queue` and `cmd/worker/vercel/api`

The Vercel HTTP entrypoint also receives the runners so it can serve an
optional signed QStash timer route.

The AWS and Vercel queues publish a `workflow.Task` after each root-run or
command task is persisted. Claiming is lease-fenced, so a duplicate queue
delivery is a successful no-op.

`Run[T]` and `Result[T]` both implement `Wait`, `Value`, and `Unwrap`.
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

Application handlers use run handles for lifecycle queries; workflow bodies
coordinate only through durable operations, while steps perform I/O:

```go
metadata := workflow.GetWorkflowMetadata(ctx) // Current workflow or step.
run, err := workflow.Get[Report](ctx, id)     // Handler context.
if err != nil {
	return err
}

status, err := run.Status()
if err != nil {
	return err
}
if status == workflow.StatusWaiting {
	if _, err := run.Wake(); err != nil {
		return err
	}
}
_ = metadata.RunID
```

The Go package deliberately serves no Dev UI. Streams and the
Postgres operation tables are the contract for `go-dev` or a production proxy.

`workflow.Start` is also valid inside workflows and steps. It records a nested
run command and returns the same child `Run` ID on replay.

## go-dev static checks

Use `github.com/eriicafes/go-workflow/workflowdev` to run the analyzer before
every `go-dev` rebuild:

```go
session := dev.New()
session.Catch(session.Run(dev.Cmd{
	Run:     dev.Package("./cmd/api"),
	Prepare: dev.Hooks(workflowdev.Check()),
	Watch:   dev.Values("."),
}))
session.Catch(session.Wait())
```

It validates `Use` bindings and reports direct I/O, clocks, random values,
goroutines, channel selects, map iteration, and closure/wrapper steps in
workflow bodies. Deterministic slice loops are allowed.
