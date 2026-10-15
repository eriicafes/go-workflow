# go-workflow

`go-workflow` is a replay-based workflow runtime for Go. A workflow is a
struct type whose `Workflow` method declares its output, and its package path
and type name are the workflow name. A runner, an ordinary Go function using
`context.Context`, executes the workflow and calls steps. There are no
generated registries, workflow interfaces, or SDK-specific context type.

Every run is recorded as an append-only event log in a World, which can be
files on disk, Postgres, Redis with Vercel Queues, or DynamoDB with SQS.
A run executes in one of two contexts. In workflow context, the runner's own
code executes as a flow, which replays the runner from the beginning, records
the operations it starts, and queues the steps it needs. In step context, a
step function executes as a step, which replays the runner up to that step
and runs the step function. Operations that are already recorded return their
recorded results, so a completed step does not run again and the run resumes
where it left off. Code outside a run starts and reads runs through a Client,
either in the same process as the World or over its HTTP endpoint.

## Workflows

Workflows are plain struct types in their own package, so request handlers can
import them without importing their runners.

```go
package workflows

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
	workflows.GenerateReport{
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

## Runners and steps

Runners and steps are ordinary functions. Keep each runner in a file with the
steps it calls. `Step` and `Background` accept either named functions or
inline closures. The analyzer requires a
closure to receive workflow-relevant values through its explicit input.
Calling a step function directly is normal Go and repeats during replay.

Steps run at least once. A step whose delivery ends before recording its
result is redelivered by the World's queue and runs again as a new attempt, so
step functions should be idempotent. Keep the queue's visibility timeout or
lease longer than your longest step, or a redelivery can run the step while
the first delivery is still executing it.

`workflow.InlineStep(ctx, fn)` records the result of a function that cannot
fail, such as a generated ID, without a step delivery. When its result cannot
be recorded, the delivery is retried and calls the function again. Work that
can fail belongs in `Step`.

## Rollbacks

Register a compensation after the work it reverses. If the workflow later
fails, registered compensations run as durable steps in reverse registration
order. Successful workflows never execute them.

```go
invoice, err := workflow.Step(ctx, createInvoice, input)
if err != nil {
	return Report{}, err
}
if err := workflow.OnRollback(ctx, voidInvoice, invoice.ID); err != nil {
	return Report{}, err
}
```

`OnRollback` compensations return only `error`. They use the same retry and
step metadata behavior as ordinary durable steps.

```go
package flows

func GenerateReport(
	ctx context.Context,
	input workflows.GenerateReport,
) (workflows.GenerateReportResult, error) {
	sources, err := workflow.Step(
		ctx,
		findSources,
		input.Topic,
	)
	if err != nil {
		return workflows.GenerateReportResult{}, err
	}

	if _, err := workflow.Step(
		ctx,
		writeReportProgress,
		len(sources),
	); err != nil {
		return workflows.GenerateReportResult{}, err
	}

	return workflows.GenerateReportResult{
		Summary: strings.Join(sources, ", "),
	}, nil
}

func findSources(ctx context.Context, topic string) ([]string, error) {
	// I/O belongs in a durable operation.
	// workflow.StepID(ctx) is stable for this operation sequence.
	return query(ctx, topic)
}

func writeReportProgress(ctx context.Context, count int) (struct{}, error) {
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
func writeReportProgress(ctx context.Context, count int) (struct{}, error) {
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
`run.Reader` are for step context and code outside a run. In workflow context
they return `ErrNotInStep`. `ReaderAt` accepts negative offsets relative to
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
runner calls `webhook.RespondWith(ctx, response)`.

## Application and worker

The application schedules by importing only workflow types and an
HTTP client. The worker is a separate entry point that imports the
implementation package, owns the world, and exposes the workflow HTTP API.

```go
// cmd/app/main.go
client := workflow.NewHTTPClient("http://workflow-worker:8081")

func createReport(w http.ResponseWriter, r *http.Request) {
	var input workflows.GenerateReport
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		http.Error(w, "invalid report input", http.StatusBadRequest)
		return
	}

	// Middleware put the client in r.Context().
	run, err := workflow.Start(r.Context(), input)
	if err != nil {
		http.Error(w, "start report workflow", http.StatusInternalServerError)
		return
	}

	// Reply 202 with run.ID(). The request does not execute workflow code.
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte(run.ID()))
}

mux := http.NewServeMux()
mux.HandleFunc("POST /reports", createReport)
if err := http.ListenAndServe(":8080", workflow.Middleware(mux, client)); err != nil {
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

## Composing workflows

A runner can compose another workflow in two ways, as in the Workflow SDK.
Calling the other workflow's runner directly runs its steps in the current run,
which then waits for its result like any other code.

`workflow.Start` in workflow context starts a child run as a step and returns
at once, and replay returns the same child `Run`. Every `Run` method in
workflow context reads the run once as a step, so `Await` returns
`workflow.ErrRunNotFinished` for a child that had not finished. To wait for a
child run, create a hook, pass its token to the child, and have the child
resume the hook when it finishes.

```go
func Parent(ctx context.Context, input workflows.Parent) (workflows.ParentResult, error) {
	hook, err := workflow.CreateHook[string](ctx)
	if err != nil {
		return workflows.ParentResult{}, err
	}
	if _, err := workflow.Start(ctx, workflows.Child{Token: hook.Token()}); err != nil {
		return workflows.ParentResult{}, err
	}
	value, err := hook.Await(ctx)
	return workflows.ParentResult{Value: value}, err
}

func Child(ctx context.Context, input workflows.Child) (workflows.ChildResult, error) {
	value, err := workflow.Step(ctx, work, input)
	if err != nil {
		return workflows.ChildResult{}, err
	}
	_, err = workflow.Step(ctx, resumeParent, completion{Token: input.Token, Value: value})
	return workflows.ChildResult{Value: value}, err
}

func resumeParent(ctx context.Context, c completion) (struct{}, error) {
	return struct{}{}, workflow.ResumeHook(ctx, c.Token, c.Value)
}
```

A child run carries the `$parentRunId` and `$rootRunId` attributes.

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
