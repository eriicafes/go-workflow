# Hooks and compensation

## Claim before exposing a token

`CreateHook[T](ctx, options...)` and `HookDefinition.Create(ctx)` return a local
hook without an error return. Its token is available immediately, but it is
not yet registered. First use through `Next`, `Values`, a combinator, or
`GetConflict` attempts registration. An unrelated durable operation does not
register the hook.

Use `GetConflict` before a step exposes the token to another system or performs
work that requires exclusive ownership:

```go
hook := workflow.CreateHook[Approval](ctx, workflow.HookOption{
	Token: "approval:" + orderID,
})
defer hook.Dispose(ctx)
owner, err := hook.GetConflict(ctx)
if err != nil {
	return Approval{}, err
}
if owner != nil {
	return Approval{}, workflow.ErrHookConflict
}
if _, err := workflow.Step(ctx, requestApproval, hook.Token()); err != nil {
	return Approval{}, err
}
return hook.Next(ctx)
```

`Approval` is the application's shared payload type; `requestApproval` is a
named step that sends the token. A conflicting owner is a `*Run[any]`, with nil
error from `GetConflict`; the workflow chooses whether to reject or inspect it.
Waiting directly on a conflicting hook instead returns `ErrHookConflict`.

`Next(ctx) (T, error)` consumes one payload. Call it again for the next payload,
or range over `Values(ctx)` and handle its error. Selecting or joining a hook
checks readiness without consuming the payload; follow with Next. Payloads
received before a forced takeover are delivered before `ErrHookForceClaimed`.

`HookOption{Force: true}` displaces the existing token owner atomically.
`ClaimedFrom` records the previous run and hook IDs. Prefer that metadata over
extra lookups. `GetHook`, `ResumeHook`, and `ResumeHookWithRetries` are for
steps or external callers, not the workflow body. The retry helper takes
`attempts` and a fixed `interval`; only hook-not-found errors are retried.

## Disposal and retention

Place `defer hook.Dispose(ctx)` directly after creation, with no intervening
blank line. Dispose is a workflow-only durable inline operation; it returns
nothing and panics on failure. A direct call disposes immediately. It releases
the token, stops further delivery, and makes a generated webhook endpoint
inactive. Disposing a local, unregistered hook needs no durable registration.

Do not defer Dispose when relying on `MinRetention` after the workflow exits:
disposal releases the token immediately. Use an intentional lifecycle instead.

## Webhooks

`CreateWebhook[T](ctx, options...)` returns `(*Webhook[T], error)` and eagerly
registers a random public token. Webhooks expose `Request(ctx)`, not `Next` or
`GetConflict`. Their absolute URL uses the World's public BaseURL.

`Request(ctx)` returns `*WebhookRequest[T]`, which embeds `*http.Request` and
has `JSON() (T, error)`. It is serializable as a step input, including as a
struct field. Pass the request into a step for signature verification and
response work. Use the actual step context for I/O; the reconstructed embedded
HTTP request does not automatically carry it.

For `WebhookOption{Manual: true}`, call `request.RespondWith(ctx, response)`
inside a step and return its error. Repeating an identical response is
idempotent; a different second response fails. Static responses and the default
202 complete at receipt; manual responses complete when recorded. Dispose is
also available to abandon a webhook before normal completion.

## Compensation and deferred cleanup

Register compensation immediately after successful reversible work:

```go
invoice, err := workflow.Step(ctx, createInvoice, orderID)
if err != nil {
	return "", err
}
defer workflow.Rollback(ctx, voidInvoice, invoice.ID)
return workflow.Step(ctx, shipOrder, invoice.ID)
```

The rollback function has signature `func(context.Context, In) error`.
`Rollback` registers work and returns nothing; it does not execute the callback
at the call site. Deferred registrations yield reverse acquisition order.
Compensations run as durable, retrying steps when the workflow returns an
error or has an application panic. They do not run after success or run
cancellation. Exhausted compensation failures are recorded while remaining
compensations continue, preserving the original workflow failure.

SDK suspension unwinds workflow defers before the workflow is finished.
Dispose skips work on those internal exits; Rollback still registers callbacks
so targeted compensation deliveries can reconstruct them. Neither operation
performs final cleanup merely because replay suspended. Arbitrary Go defers
have no such protection, so keep their external side effects in durable steps.
