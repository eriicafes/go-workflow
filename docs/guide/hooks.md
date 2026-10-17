# Hooks

A hook receives repeated typed payloads addressed by an exclusive token. It can wait for approvals, service callbacks, or child-run completion without keeping a process alive.

## Shared definitions

Declare reusable hooks beside workflow input and output structs in a shared package, such as `examples/basic/workflows`. Both workflow handlers and payload producers import that package:

```go
package workflows

import "github.com/eriicafes/go-workflow"

type Approval struct {
    Approved bool
    Comment string
}

var Approvals = workflow.DefineHook[Approval](workflow.HookOption{Token: "approvals"})
```

A fixed token has one owner at a time. For per-order tokens use `CreateHook[workflows.Approval](ctx, workflow.HookOption{Token: "approve:" + input.OrderID})` instead.

```go
hook, err := workflows.Approvals.Create(ctx)
if err != nil {
    return Output{}, err
}
defer hook.Dispose(ctx)

approval, err := hook.Next(ctx)
if err != nil {
    return Output{}, err
}
return Output{Approved: approval.Approved}, nil
```

`Create` makes a local handle without claiming the token. Pending hooks register when the workflow waits, including through `Next`, `Values`, `Join`, `Select`, or another pending result. Reading `ID` or `Token` and declaring work do not register hooks. `Token` is available immediately; `ID` becomes available after registration. Webhooks register during `CreateWebhook` and receive through `Request(ctx)`.

Call `GetConflict(ctx)` to commit registration without waiting for a payload. It returns the owning run on conflict, or nil after a successful claim. Repeated calls use the recorded result. Receiving directly registers automatically and returns `ErrHookConflict` if another run owns the token.

Registration happens alongside work already started before the wait. A step can therefore start before the hook exists, even when the token conflicts. Call `GetConflict` **before** starting duplicate-sensitive work or handing a token to a step or child run that may immediately resume it:

```go
owner, err := hook.GetConflict(ctx)
if err != nil {
    return Output{}, err
}
if owner != nil {
    return Output{DuplicateOf: owner.ID()}, nil
}
if _, err := workflow.Step(ctx, requestApproval, hook.Token()); err != nil {
    return Output{}, err
}
approval, err := hook.Next(ctx)
```

This follows the [Workflow SDK registration lifecycle](https://workflow-sdk.dev/docs/api-reference/workflow/create-hook#registering-a-hook-before-a-step-uses-it).

## Receiving values

`Next(ctx) (T, error)` consumes one payload, suspending until it is available. Repeated calls consume payloads in recorded order. A normal Go loop can call `Next`:

```go
for {
    value, err := hook.Next(ctx)
    if err != nil {
        return Output{}, err
    }
    // Process value and break when finished.
    _ = value
}
```

For range syntax, `Values` is a thin iterator over the same cursor:

```go
for value, err := range hook.Values(ctx) {
    if err != nil {
        return Output{}, err
    }
    _ = value
    break
}
next, err := hook.Next(ctx) // receives the following payload
```

Creating an iterator does not receive anything. Breaking iteration leaves unread payloads available and does not dispose the hook. An error is yielded once, then iteration ends. Receiving preserves JSON decoding, ownership, and disposal errors; a zero value is a valid payload and does not signal end of stream.

Hooks work with `Join`, `JoinOk`, `Select`, and `SelectOk`. Selecting or joining a hook inspects its next receive without consuming it. Use `Next` afterward.

## Resuming and looking up hooks

In step or client context:

```go
err := workflow.ResumeHook(ctx, "approve:"+orderID, workflows.Approval{Approved: true})
hook, found, err := workflow.GetHook[workflows.Approval](ctx, token)
err = workflow.ResumeHookWithRetries(ctx, token, workflows.Approval{Approved: true}, 5, time.Second)
```

`ResumeHook` records a JSON payload and queues the owning run. `ResumeHookWithRetries` accepts a total attempt count and a fixed interval between attempts. The first attempt is immediate; cancellation interrupts the wait. It retries only while no hook has the token, useful just after starting a run. Lookup and resume functions return `ErrNotInStep` in workflow context; call them from a step or external code. Lookup handles expose identity and `ClaimedFrom`; they cannot receive workflow payloads. Leave `HookOption.Token` empty for a generated token available immediately and preserved across replay, including a delivery interrupted before registration.

## Conflicts and forced ownership

A token belongs to one hook. Check ownership lazily inside workflow context:

```go
hook, err := workflow.CreateHook[workflows.Approval](ctx, workflow.HookOption{Token: token})
if err != nil {
    return Output{}, err
}
owner, err := hook.GetConflict(ctx)
if err != nil {
    return Output{}, err
}
if owner != nil {
    return Output{}, fmt.Errorf("token belongs to run %s", owner.ID())
}
defer hook.Dispose(ctx)
```

A successful claim returns a nil owner. A conflict is recorded with its owner in the same storage operation. `Next` also returns `ErrHookConflict` if registration failed. Storage errors retry the delivery rather than becoming an ownership conflict.

`HookOption.Force` takes the token from its current owner. The new hook's `ClaimedFrom` identifies that owner with `RunID` and `HookID`. The displaced hook yields already-recorded payloads, then returns `ErrHookForceClaimed`. Token-based resumes resolve the current owner once. A takeover race returns an error to the caller without redirecting the payload. Webhook requests never redirect to a different webhook.

## Disposal and retention

`Dispose(ctx)` runs as a durable inline step in workflow context. It deactivates registration, stops further payloads and iterator values, and releases the token immediately. Webhook disposal also deactivates its endpoint. The usual idiom is a direct `defer hook.Dispose(ctx)` immediately after creation. Replay suspension does not count as the workflow body returning. Disposing an unregistered handle does not claim its token or write a hook event.

Explicit disposal stops receiving immediately, including unread buffered payloads. Without disposal, an active run retains its token across receives.

`MinRetention` reserves a token until creation time plus the duration. A terminal run keeps the reservation until that deadline; an active run keeps its token beyond the deadline. Retained terminal hooks cannot receive new payloads. **Do not defer `Dispose` when relying on retention:** disposal releases the token when the workflow body returns and defeats the reservation.
