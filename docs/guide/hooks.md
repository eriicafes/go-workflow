# Hooks

A hook lets a workflow wait for external input without keeping a process
alive. Producers send typed payloads to its token, and the workflow receives
them in recorded order. Each token belongs to one active hook.

## Creating a hook

Define reusable hooks beside workflow types so runners and payload producers
can share the definition:

```go
package workflows

import "github.com/eriicafes/go-workflow"

type Approval struct {
	Approved bool
	Comment  string
}

var Approvals = workflow.DefineHook[Approval](workflow.HookOption{
	Token: "approvals",
})
```

Create a handle in the runner and wait for a payload:

```go
hook := workflows.Approvals.Create(ctx)
defer hook.Dispose(ctx)

approval, err := hook.Next(ctx)
if err != nil {
	return Output{}, err
}

return Output{Approved: approval.Approved}, nil
```

A fixed token allows one owner at a time. For a token tied to an order, create
the hook directly:

```go
hook := workflow.CreateHook[workflows.Approval](ctx, workflow.HookOption{
	Token: "approve:" + input.OrderID,
})
defer hook.Dispose(ctx)
```

Omit `Token` to generate one. The token is available immediately and remains
stable across replay, even if a delivery stops before registration.

## Registering before starting work

Creating a handle returns no error and does not claim its token. Registration
happens when receiving from or selecting that specific hook, including through
`Next`, `Values`, `Select`, or `Join`. Unrelated waits and ownership checks on
other hooks do not register it. Its ID becomes available after registration.

A handle created outside workflow context reports `ErrNoWorkflow` when used.

Call `GetConflict` before passing a token to work that might resume it
immediately. This commits registration without waiting for a payload:

```go
owner, err := hook.GetConflict(ctx)
if err != nil {
	return Output{}, err
}

if owner != nil {
	return Output{DuplicateOf: owner.ID()}, nil
}

// The hook is registered before the step hands its token to a producer.
if _, err := workflow.Step(ctx, requestApproval, hook.Token()); err != nil {
	return Output{}, err
}

approval, err := hook.Next(ctx)
if err != nil {
	return Output{}, err
}

return Output{Approved: approval.Approved}, nil
```

Without this check, a step can start before registration or before a conflict
is discovered. Use the check before work that must not run for duplicate
requests as well.

This follows the [Workflow SDK registration lifecycle](https://workflow-sdk.dev/docs/api-reference/workflow/create-hook#registering-a-hook-before-a-step-uses-it).
[Webhooks](/guide/webhooks) register during creation instead.

## Receiving values

Each `Next` call consumes one payload, suspending until it is available. Use
`Values` to receive repeatedly in a range loop:

```go
for approval, err := range hook.Values(ctx) {
	if err != nil {
		return Output{}, err
	}

	if approval.Approved {
		return Output{Approved: true}, nil
	}
}
```

The iterator and `Next` share a receive cursor. Breaking the loop leaves
unread payloads available and keeps the hook active. Creating an iterator
does not receive anything. An error is yielded once before iteration ends,
and a zero value is an ordinary payload.

Hooks work with the [concurrency helpers](/guide/concurrency). Selecting or
joining a hook waits for its next payload without consuming it. Call `Next`
to receive that payload afterward.

## Sending a payload

Call `ResumeHook` from a step or external handler with a Client in its context:

```go
if err := workflow.ResumeHook(ctx, "approve:"+orderID, workflows.Approval{
	Approved: true,
}); err != nil {
	return err
}
```

The payload is recorded and the owning run is queued. When a producer may
arrive before registration, `ResumeHookWithRetries` retries while the token
has no active hook:

```go
err := workflow.ResumeHookWithRetries(ctx, token, approval, 5, time.Second)
if err != nil {
	return err
}
```

This makes up to five attempts, starting immediately and waiting one second
between attempts. Context cancellation interrupts the wait.

Use `GetHook` to look up an active hook by token. The returned handle exposes
identity and prior ownership but cannot receive workflow payloads. Lookup
and resume operations return `ErrNotInStep` in workflow context.

## Conflicts and forced ownership

`GetConflict` returns nil after a successful claim or the owning run on a
conflict. Repeated calls use the recorded result. Receiving without a prior
check registers automatically and returns `ErrHookConflict` if another run
owns the token. Storage errors retry the delivery.

Set `HookOption.Force` to take a token from its owner. The new hook's
`ClaimedFrom` identifies the previous owner through `RunID` and `HookID`.
The displaced hook yields its already-recorded payloads before returning
`ErrHookForceClaimed`.

A resume resolves the current owner once. If ownership changes during the
request, the caller receives an error and the payload is not redirected.
Webhook requests follow the same rule.

## Disposal and retention

Place `defer hook.Dispose(ctx)` immediately after creation when the token
should be released as the workflow body returns. Replay suspension does not
trigger this disposal.
Disposal stops receiving immediately, including unread buffered payloads,
and releases the token. Disposing an unregistered handle does not claim it
or write a hook event.

`Dispose` returns nothing and panics on failure, so errors from a deferred call
reach the workflow runtime. Storage failures retry the delivery; a terminal
disposal failure fails the workflow and can replace its original error.

To reserve a token after a run ends, set `MinRetention`. The reservation lasts
until creation time plus the duration. An active run keeps its token beyond
that deadline, while a retained terminal hook cannot receive new payloads.

Omit the deferred `Dispose` when relying on retention. Explicit disposal
releases the token immediately and defeats the reservation.
