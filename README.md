# go-workflow

go-workflow is a durable workflow library for Go, inspired by the
[Workflow SDK](https://workflow-sdk.dev). Workflows are Go functions whose
progress is persisted step by step. A run can suspend on a timer or a hook
without holding a process and resumes when it can continue. Runs can be
inspected with the Workflow SDK web UI.

```sh
go get github.com/eriicafes/go-workflow
```

Agent skills are available for [writing workflows](skills/go-workflow/SKILL.md)
and [implementing World backends](skills/go-workflow-worlds/SKILL.md).

## Workflows

A workflow is a struct type whose `Workflow` method declares its output. Its
package path and type name are the workflow name, and a value of it is the
input of a run. Keep workflows in their own package so request handlers can
start them without importing their runners.

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

## Runners and steps

A runner executes a workflow. It is a plain function that takes the workflow as
input and calls steps, which do the real work and side effects. Keep each runner
in a file with the steps it calls.

```go
package flows

func GenerateReport(ctx context.Context, input workflows.GenerateReport) (workflows.GenerateReportResult, error) {
	sources, err := workflow.Step(ctx, findSources, input.Topic)
	if err != nil {
		return workflows.GenerateReportResult{}, err
	}
	return workflows.GenerateReportResult{Summary: strings.Join(sources, ", ")}, nil
}

func findSources(ctx context.Context, topic string) ([]string, error) {
	return search(ctx, topic)
}
```

Register runners with `workflow.Use` and collect them with `worlds.Runners`.

```go
runners := worlds.Runners(
	workflow.Use(flows.GenerateReport),
)
```

Each step's result is persisted once it completes, so a run continues from its
last completed step and never repeats finished work. Waiting on a step, a
timer, or a hook suspends the run without consuming any resources until it can
continue. For this to work, a runner does side effects only through steps and
reads time only through `workflow.Now`, and the [analyzer](#development) checks
these rules.

If a deployed change makes a runner replay differently from what its run
recorded, the runtime detects the divergence and fails the run with a
`worlds.ErrCorruptedEventLog` instead of matching results to the wrong calls.
Runs can also be pinned to the deployment that started them using DeploymentID,
so in-flight runs keep replaying the code that recorded them.

Steps run at least once and retry on error, three times by default. Set
`workflow.StepOptions{MaxRetries: n}` to change that, return
`workflow.FatalError(err)` to fail without retrying, or return
`workflow.RetryableError(err, after)` to retry after a delay. Step functions
should be idempotent, and `workflow.StepID(ctx)` is a stable idempotency key.

`workflow.InlineStep(ctx, fn, input)` takes the same arguments as `Step` but
runs a short step, such as generating an ID, in the flow itself, without a
separate step delivery. Failed attempts are retried like a step's.

## Running workflows

Every run is recorded in a World. The local World stores runs as JSON files and
delivers work in-process, so it needs no database or worker.

```go
world, err := localworld.Open(localworld.Config{Dir: ".workflows"}, runners)
if err != nil {
	log.Fatal(err)
}
defer world.Close()
if err := world.Prepare(ctx); err != nil {
	log.Fatal(err)
}

ctx = workflow.NewContext(ctx, workflow.NewClient(world))
run, err := workflow.Start(ctx, workflows.GenerateReport{Topic: "workflows"})
if err != nil {
	log.Fatal(err)
}
result, err := run.Value(ctx)
```

`workflow.Start` needs only the workflow type and a Client in ctx. A `Run`
handle reads and controls the run with `Value`, `Status`, `Cancel`, and
`WakeUp`, and `workflow.Get` returns the handle of an existing run by ID.

## Worlds

| Package | Storage and delivery | Entry points |
| --- | --- | --- |
| [`worlds/local`](worlds/local) | JSON files, in-process delivery | `World.Prepare` |
| [`worlds/postgres`](worlds/postgres) | PostgreSQL, LISTEN/NOTIFY workers | `postgresworld.Worker` |
| [`worlds/vercel`](worlds/vercel) | Redis, Vercel Queues, optional QStash | `vercelworld.Queue` |
| [`worlds/aws`](worlds/aws) | DynamoDB, SQS, optional EventBridge Scheduler | `awsworld.Queue` |

Every World package also exports `HTTP(world)`, which serves the RPC endpoint
that clients and the web UI use and the webhook endpoint under `/v1/workflow`.
`HTTPPrefix(world, prefix)` serves them under another prefix, and `RPC(world)`
and `Webhook(world)` serve each endpoint on its own. Set `Config.BaseURL` to
the public URL of that prefix, since webhook URLs are built from it.

### Postgres

Any number of application and worker processes share one database. `Worker`
runs the loop that executes deliveries.

```go
world, err := postgresworld.Open(ctx, postgresworld.Config{BaseURL: os.Getenv("WORKFLOW_BASE_URL"), URL: os.Getenv("DATABASE_URL")})
if err != nil {
	log.Fatal(err)
}
defer world.Close()

go func() {
	if err := postgresworld.Worker(ctx, world, runners); err != nil {
		log.Printf("workflow worker stopped: %v", err)
	}
}()
log.Fatal(http.ListenAndServe(":8081", postgresworld.HTTP(world)))
```

### Vercel

Deploy `Queue` as a function with a Vercel Queues trigger for `Config.Topic`,
and `HTTP` as a separate function for clients. Redis is reached over TCP with
`RedisURL` or over the Upstash REST API with `RedisRESTURL` and
`RedisRESTToken`. `OIDCToken` defaults to reading `VERCEL_OIDC_TOKEN` on every
request.

```go
world, err := vercelworld.New(vercelworld.Config{
	BaseURL:  os.Getenv("WORKFLOW_BASE_URL"),
	RedisURL: os.Getenv("REDIS_URL"),
	QueueURL: os.Getenv("VERCEL_QUEUE_URL"),
	Topic:    os.Getenv("VERCEL_WORKFLOW_QUEUE_TOPIC"),
})
if err != nil {
	log.Fatal(err)
}

// The queue consumer function.
log.Fatal(http.ListenAndServe(":8081", vercelworld.Queue(world, runners)))

// The client-facing function.
log.Fatal(http.ListenAndServe(":8081", vercelworld.HTTP(world)))
```

Vercel Queues delays deliveries by at most seven days. Set `Config.QStash` to
deliver work due later.

### AWS

Install `Queue` as the SQS handler of one Lambda function and `HTTP` behind a
Function URL of another. The [Terraform module](worlds/aws/terraform) creates
the table, queues, scheduler group, and IAM policies, and
`awsworld.CreateTable` creates the table for tests and DynamoDB Local.

```go
world, err := awsworld.New(awsworld.Config{BaseURL: os.Getenv("WORKFLOW_BASE_URL"), TableName: os.Getenv("WORKFLOW_TABLE"), QueueURL: os.Getenv("WORKFLOW_QUEUE_URL")}, dynamodb.NewFromConfig(cfg), sqs.NewFromConfig(cfg))
if err != nil {
	log.Fatal(err)
}

// The queue consumer function.
lambda.Start(awsworld.Queue(world, runners))

// The client-facing function.
lambdaurl.Start(awsworld.HTTP(world))
```

SQS delays deliveries by at most 15 minutes. Set `Config.Scheduler` to deliver
work due later through EventBridge Scheduler.

## Application and worker

An application that only starts and reads runs needs the workflow types and a
Client, not the runners. `NewHTTPClient` talks to a World served by `HTTP`, at
the server's URL followed by the handler's prefix, and `Middleware` puts the
client in every request context.

```go
client := workflow.NewHTTPClient("http://workflow-worker:8081/v1/workflow")

mux := http.NewServeMux()
mux.HandleFunc("POST /reports", func(w http.ResponseWriter, r *http.Request) {
	run, err := workflow.Start(r.Context(), workflows.GenerateReport{Topic: r.FormValue("topic")})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte(run.ID()))
})
log.Fatal(http.ListenAndServe(":8080", workflow.Middleware(mux, client)))
```

[`examples/basic`](examples/basic) has the full application with a worker for
each World.

## Concurrency and time

`workflow.Background` starts a step without waiting and returns a `*Result[T]`.
`Map` starts one step per input. `All` collects outputs in input order and cancels the remaining steps if any fails. `Join`,
`JoinOk`, and the `Select` functions wait on several promises at once.

```go
results, err := workflow.All(ctx, findSources, []string{"go", "rust"})
```

`MapResults` converts a Map result slice for `Join`, `Select`, or group `Add`.
`WaitGroup` waits for every outcome; `ErrGroup` waits for every outcome and
returns the first failure. Use `workflow.Go(ctx, &group, step, input)` to start
and track steps. `workflow.ErrGroupWithContext(ctx)` returns an
ErrGroup and a durable context cancelled on failure or when its Wait returns.

`workflow.Sleep` suspends the run for a duration, `Timer` returns a promise that
settles after one, and `WithTimeout` and `WithDeadline` return a cancellation context and cancel function, using a timer to cancel work that uses that context. A sleeping run consumes no resources, so it can sleep for days or months.

## Errors

A failed step's error is persisted with the step and comes back as a
`*workflow.Error`, and so does a failed run's error from `Run.Value`. It keeps
the message, type, and exported fields of every error in the original chain, so
`errors.Is` and `errors.As` work on it in the runner and across child runs.

```go
var ErrCardDeclined = errors.New("card declined")

type LimitError struct{ Limit int }

func (e *LimitError) Error() string { return fmt.Sprintf("over limit %d", e.Limit) }
```

Inspect the recorded error in the runner:

```go
_, err := workflow.Step(ctx, charge, order)
var limit *LimitError
switch {
case errors.Is(err, ErrCardDeclined):
	// ...
case errors.As(err, &limit):
	// limit.Limit is decoded from the recorded error.
}
```

## Rollbacks

Defer a compensation after the work it reverses. If the run later fails, the
compensations run as durable steps in Go's defer order, with the last defer first.

```go
invoice, err := workflow.Step(ctx, createInvoice, input)
if err != nil {
	return Report{}, err
}
defer workflow.Rollback(ctx, voidInvoice, invoice.ID)
```

## Streams

Steps write run-owned byte streams that readers consume while the run is still
going. A stream has one writer at a time, and another writer waits until the
current one releases or closes it.

```go
func writeProgress(ctx context.Context, count int) (struct{}, error) {
	writer, release, err := workflow.Writer(ctx, workflow.WriterOption{Name: "progress"})
	if err != nil {
		return struct{}{}, err
	}
	defer release()
	_, err = fmt.Fprintf(writer, "found %d sources\n", count)
	return struct{}{}, err
}
```

Outside a run, such as in a route handler, `run.Reader` reads a stream. Each
write is one chunk, and `ReaderOption.StartIndex` picks the first chunk, where a
negative index counts from the end. Reads wait for new data until the stream
closes.

```go
// The last three writes onward.
reader, err := run.Reader(ctx, workflow.ReaderOption{Name: "progress", StartIndex: -3})
```

## Hooks and webhooks

A hook receives repeated typed payloads. Declare reusable hook definitions beside workflow structs in a shared package.
Pass a token when the caller must know it in advance.

```go
hook := workflow.CreateHook[Approval](ctx, workflow.HookOption{Token: "approve:" + input.OrderID})
defer hook.Dispose(ctx)

approval, err := hook.Next(ctx) // registers the hook and waits for a payload
```

Creation is local and returns only a handle. Receiving from or selecting that hook registers it; unrelated waits do not. Use `GetConflict(ctx)` before starting work that requires ownership or exposes the token; it registers without waiting for a payload and returns the conflicting run, or nil. `Dispose` propagates failures by panicking, including when deferred.

```go
err := workflow.ResumeHook(ctx, "approve:"+orderID, Approval{Approved: true})
```

A webhook is a hook resumed by an HTTP request to its URL, with the JSON body
decoded as the payload. `WebhookOption` can require a method, set a static
response, or hold the request until a step calls `WebhookRequest.RespondWith`. The token
in the URL is its only authorization, so verify provider signatures before
trusting the payload.

```go
webhook, err := workflow.CreateWebhook[Payment](ctx)
if err != nil {
	return Result{}, err
}
defer webhook.Dispose(ctx)

if _, err := workflow.Step(ctx, registerCallback, webhook.URL()); err != nil {
	return Result{}, err
}
request, err := webhook.Request(ctx)
if err != nil {
	return Result{}, err
}
payment, err := request.JSON()
```

`WithCancel` and `WithCancelCause` return a durable cancellation context, a cancel function, and a creation error. Their cancel functions also return errors. Pass cancellation contexts to steps; cancel runs explicitly with `Run.Cancel`; use `CancelAny(ctx, sources...)` to combine cancellation while preserving base values. Cancelled steps fail without retrying; cancelled runs let running steps finish naturally. `Then` follows the result returned by `func(T, error) *Result[U]`; use `NewResult(value, err)` for immediate values and failures.

## Composing workflows

A runner can compose another workflow in two ways, as in the Workflow SDK.
Calling the other workflow's runner directly runs its steps in the current run.

```go
report, err := GenerateReport(ctx, workflows.GenerateReport{Topic: input.Topic})
```

`workflow.Start` in workflow context starts a child run, which carries the
`$parentRunId` and `$rootRunId` attributes. In workflow context `Run.Value`
returns `workflow.ErrRunNotFinished` instead of waiting, so to wait for a child,
pass it a hook token and have it resume the hook when it finishes.

```go
func Parent(ctx context.Context, input workflows.Parent) (workflows.ParentResult, error) {
	hook := workflow.CreateHook[string](ctx)
	defer hook.Dispose(ctx)

	if owner, err := hook.GetConflict(ctx); err != nil {
		return workflows.ParentResult{}, err
	} else if owner != nil {
		return workflows.ParentResult{}, workflow.ErrHookConflict
	}
	if _, err := workflow.Start(ctx, workflows.Child{Token: hook.Token()}); err != nil {
		return workflows.ParentResult{}, err
	}
	value, err := hook.Next(ctx)
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

## Development

The analyzer checks that every runner passed to `Use` is a named function whose
input is a workflow, and reports goroutines, `select`, map iteration,
`time.Now`, I/O outside steps, and steps that capture variables instead of
taking them as input. Run it as a vet tool.

```sh
go install github.com/eriicafes/go-workflow/cmd/workflowcheck@latest
go vet -vettool=$(which workflowcheck) ./...
```

The [`workflowdev`](workflowdev) package integrates with
[go-dev](https://github.com/eriicafes/go-dev). `workflowdev.Check` runs the
analyzer before every start or reload, and `workflowdev.Web` runs the Workflow
SDK web UI against a World served by `HTTP`. The web UI needs the Workflow SDK
CLI and this repository's World adapter for it, installed in the directory
go-dev runs from.

```sh
npm install --save-dev workflow @eriicafes/go-workflow
```

```go
session := dev.New()
task := session.NewTask(dev.Cmd{
	Run:        dev.Package("./cmd/app"),
	Watch:      dev.Values("cmd", "flows", "workflows"),
	ServerAddr: ":8080",
	Commands: dev.Commands(
		dev.Cmd{Run: dev.Package("./cmd/worker/pg"), Prepare: dev.Hooks(workflowdev.Check("./...")), Watch: dev.Values("cmd", "flows", "workflows"), Phase: dev.Before},
		workflowdev.Web("http://localhost:8081/v1/workflow"),
	),
})
session.Catch(task.Run())
session.Catch(session.Wait())
```
