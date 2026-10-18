---
name: go-workflow
description: Build, review, and test durable Go workflows using github.com/eriicafes/go-workflow. Use for runners, steps, concurrency, hooks, webhooks, cancellation, compensation, and application or worker integration with this SDK.
---

# go-workflow

Use the application's installed SDK version as the API authority. Inspect its
`go.mod`, existing workflow definitions, runners, and World setup before making
changes. These instructions describe the APIs in the repository containing this
skill; confirm signatures against the selected version rather than upgrading it
to match an example.

## Structure the workflow

- Define workflow input structs and their output marker method in a shared
  package: `func (GenerateReport) Workflow(GenerateReportResult) {}`. Put shared
  `workflow.DefineHook[T]` definitions there too. Clients should not need to
  import runner implementations.
- Implement a runner as a named
  `func(context.Context, Input) (Output, error)` and register it with
  `workflow.Use(runner)` in `worlds.Runners(...)`.
- Keep step inputs and outputs serializable. Pass business data explicitly
  instead of capturing it in closures. Prefer named step functions beside the
  runner that uses them.

## Respect replay boundaries

Workflow bodies execute again from the beginning on replay. Put network,
database, filesystem, random, and other external operations in steps. Use
`workflow.Now`, `Sleep`, `Timer`, and the workflow concurrency and cancellation
APIs in the workflow body. Keep native goroutines, channel coordination,
`time.Sleep`, `time.Now`, and standard context cancellation constructors in
step or external application code. Sort map keys before using their order to
schedule durable operations.

`Step` waits for its result. `Background` returns `*workflow.Result[T]` without
waiting; read it with `Value(ctx)` or `Unwrap(ctx)`. `InlineStep` records the
same step lifecycle while executing short work in the flow delivery. It has
retries too; it is not an escape from replay or idempotency requirements.

Step attempts are delivered at least once. Use `workflow.StepID(ctx)` as a
stable external idempotency key where supported. Omitted `StepOptions` allows
three retries; an explicit zero-valued option disables retries. Use
`FatalError(err)` to stop retries and `RetryableError(err, delay)` to choose a
retry delay. Step panics follow the step failure and retry path.

Do not swallow SDK suspension panics with a blanket `recover`. Plain deferred
side effects can repeat on replay; durable cleanup belongs in SDK operations.
If changing an existing deployed runner, consider its in-flight event logs:
reordering durable operations can break replay. Use a separate workflow type
for incompatible changes when old runs still need to execute.

## Choose the relevant pattern

- For waiting, racing, chaining, deadlines, or child runs, read
  [concurrency and cancellation](references/concurrency.md).
- For token ownership, repeated signals, webhook responses, or cleanup, read
  [hooks and compensation](references/hooks.md).
- For client/worker setup, streams, deterministic tests, or analyzer checks,
  read [integration and testing](references/testing.md).

Do not invent a public `Promise` interface, `Done`, `Run.Await`, hook `Value`,
`OnRollback`, `SelectWithCancel`, or `Timeout` API. Current code uses results,
`Run.Value`, `Hook.Next`, deferred `Rollback`, and `WithTimeout`/`WithDeadline`.

## Find authoritative details

When the SDK checkout is available, inspect the relevant implementation:

| Concern | Source |
| --- | --- |
| Step, timer, rollback, and combinators | `steps.go` |
| Result and Then | `promise.go` |
| Managed contexts | `cancellation.go` |
| Hooks and webhooks | `hook.go`, `webhook.go` |
| Runs and runner registration | `workflow.go` |
| Client context and middleware | `context.go` |
| Streams | `stream.go` |

The checkout's `docs/guide/` and `examples/basic/` provide larger examples.
In a consuming application, resolve the module directory with
`go list -m -f '{{.Dir}}' github.com/eriicafes/go-workflow`; treat module-cache
sources as read-only.
