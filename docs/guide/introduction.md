# What is go-workflow?

go-workflow runs durable workflows in Go. It is inspired by the
[Workflow SDK](https://workflow-sdk.dev) and follows its event model.

You write a workflow as an ordinary Go function. The work it does with side
effects goes into **steps**, and every step's result is recorded. If the process
crashes, restarts, or is redeployed in the middle of a run, the run is delivered
again, the function replays from the beginning, and recorded steps return their
recorded results instead of running again. The run resumes where it left off.

## Core ideas

| Concept | What it is |
| --- | --- |
| **Workflow** | A struct type whose `Workflow(Out)` method declares its output. Its package path and type name are the workflow name, and a value of it is the input of a run. |
| **Runner** | A function `func(context.Context, In) (Out, error)` that executes a workflow. You register it with `workflow.Use`. |
| **Step** | A function `func(context.Context, In) (Out, error)` that does the real work and side effects. It runs at least once and retries on error. |
| **Run** | One execution of a workflow, recorded as an append-only event log. |
| **World** | The backend that stores runs, queues deliveries, and holds streams: local files, Postgres, Vercel, AWS, or your own. |
| **Client** | The API that code outside a run uses to start, read, cancel, and resume runs. |

## Two contexts

A run executes in one of two contexts, as in the Workflow SDK.

- In **workflow context**, the runner's own code executes as a *flow*. The flow
  replays the runner from the beginning, records the operations it starts
  (steps, timers, hooks, webhooks, and child runs), queues the steps it needs,
  and records the run's result once the runner returns.
- In **step context**, a step function executes as a *step*. The step delivery
  replays the runner up to that step and calls the step function.

Because the runner replays on every delivery, it must be **deterministic**: it
does side effects only through steps and reads time only through
`workflow.Now`. The [analyzer](/tooling/analyzer) reports runners that break
these rules. [How Replay Works](/guide/replay) explains the model in detail.

## What you get

- **Steps** with automatic retries, fatal and delayed-retry errors, and stable
  idempotency keys.
- **Concurrency** with promises: `Background`, `Map`, `All`, `Join`,
  `JoinSettled`, and the `Select` family.
- **Durable time**: `Sleep`, `Timer`, and `Timeout` that hold no process, so
  waiting for days or weeks is fine.
- **Hooks and webhooks** that suspend a run until an external event or HTTP
  request resumes it with a typed payload.
- **Streams** of bytes that steps write and readers consume while the run is
  still going.
- **Rollbacks**: compensations that run in reverse order when a run fails.
- **Typed errors** that survive replay, so `errors.Is` and `errors.As` work on a
  failed step or run.
- **Child runs** and direct composition of runners.
- **Worlds** for local development, PostgreSQL, Vercel, and AWS, plus an
  in-memory World for tests and a conformance suite for your own.
- **Tooling**: a vet analyzer, a [go-dev](https://github.com/eriicafes/go-dev)
  integration, and compatibility with the Workflow SDK web UI.

## Packages

| Package | Purpose |
| --- | --- |
| [`workflow`](/reference/workflow) | The API runners, steps, and applications use. |
| [`worlds`](/reference/worlds) | The World contract, the runtime, the client, and the HTTP server. |
| [`worlds/local`](/worlds/local) | JSON files on disk, in-process delivery. |
| [`worlds/postgres`](/worlds/postgres) | PostgreSQL with LISTEN/NOTIFY workers. |
| [`worlds/vercel`](/worlds/vercel) | Redis, Vercel Queues, optional QStash. |
| [`worlds/aws`](/worlds/aws) | DynamoDB, SQS, optional EventBridge Scheduler. |
| [`worlds/testworld`](/worlds/testworld) | In-memory World for tests. |
| [`workflowtest`](/worlds/custom#conformance) | Conformance suites for World implementations. |
| [`check`](/tooling/analyzer) and `cmd/workflowcheck` | Static analyzer and its vet tool. |
| [`workflowdev`](/tooling/dev) | go-dev integration. |
| [`@eriicafes/go-workflow/world-http`](/tooling/world-http) | JavaScript World adapter the Workflow SDK web UI loads. |
