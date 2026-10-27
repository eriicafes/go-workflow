---
name: go-workflow
description: Build, review, and test durable Go workflows using github.com/eriicafes/go-workflow. Use for runners, steps, concurrency, hooks, webhooks, cancellation, compensation, and application or worker integration with this SDK.
---

# go-workflow

Use the application's installed SDK version as the API authority. Inspect its
`go.mod`, existing workflow definitions, runners, and World setup before making
changes. Use `go doc github.com/eriicafes/go-workflow.Step` or the relevant
package symbol to confirm signatures against that version.

## Structure the workflow

- Define workflow input structs in a `workflows` package, embedding
  `workflow.Struct[Out]` to declare the output:
  `type GenerateReport struct { workflow.Struct[GenerateReportResult]; Topic string }`.
  Put shared `workflow.DefineHook[T]` definitions there too. Clients should
  not need to import runner implementations.
- Implement a runner in a `flows` package inside it, `workflows/flows`, as a
  named `func(context.Context, Input) (Output, error)`, and register it with
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
waiting; read it with `Value(ctx)` or `Unwrap(ctx)`. A flow delivery runs up
to three new steps inline at once when the runner waits and queues the rest;
`SetMaxInlineSteps(ctx, n)` changes the limit and 0 turns it off. Use
`InlineStep` for short recorded operations that always run in the flow
delivery, and `InlineBackground` to start one without waiting. Inline steps
follow the same retry and idempotency requirements as other steps.

Step attempts are delivered at least once. Use `workflow.StepID(ctx)` as a
stable external idempotency key where supported. Omitted `StepOptions` allows
three retries; an explicit zero-valued option disables retries. In step
functions, inline steps, and rollback compensations, return
`FatalError(err)` to stop retries and `RetryableError(err, delay)` to choose a
retry delay. They have no effect on a runner's error: runs are never retried,
and a runner that returns an error fails its run, so return it unwrapped. Step
panics follow the step failure and retry path.

Do not swallow SDK suspension panics with a blanket `recover`. Plain deferred
side effects can repeat on replay; durable cleanup belongs in SDK operations.
If changing an existing deployed runner, consider its in-flight event logs:
reordering durable operations can break replay. Use a separate workflow type
for incompatible changes when old runs still need to execute.

## Deferred cleanup

- In the workflow runner, put `defer hook.Dispose(ctx)` immediately after hook
  creation. For a webhook, check the creation error first, then put
  `defer webhook.Dispose(ctx)` directly after the error check.
- Put `defer workflow.Rollback(ctx, undo, input)` immediately after the
  successful step it compensates and its error check. Compensations execute on
  workflow failure, in reverse acquisition order.
- Keep these defers in the runner so cleanup follows the runner's lifetime.
  SDK cleanup handles replay suspension. A defer in a helper runs when that
  helper exits instead.

Dispose releases the token when the workflow exits successfully or fails. Omit
it when deliberately retaining the token with `MinRetention`. Calling Dispose
directly releases the hook immediately; calling Rollback directly only
registers compensation. See the bundled hooks reference for examples and exit
behavior.

## Choose the relevant pattern

- For waiting, racing, chaining, deadlines, or child runs, read
  [concurrency and cancellation](references/concurrency.md).
- For token ownership, repeated signals, webhook responses, or cleanup, read
  [hooks and compensation](references/hooks.md).
- For client/worker setup, streams, deterministic tests, or analyzer checks,
  read [integration and testing](references/testing.md).
