# Hooks

A hook suspends a run until something outside resumes it with a typed payload:
a person approving a request, another service finishing a job, or a
[child run](/guide/composition) reporting its result.

## Creating and awaiting a hook

```go
type Approval struct {
	Approved bool
	Comment  string
}

func ReviewOrder(ctx context.Context, input workflows.ReviewOrder) (workflows.ReviewOrderResult, error) {
	hook, err := workflow.CreateHook[Approval](ctx, workflow.HookOption{Token: "approve:" + input.OrderID})
	if err != nil {
		return workflows.ReviewOrderResult{}, err
	}

	approval, err := hook.Await(ctx)
	if err != nil {
		return workflows.ReviewOrderResult{}, err
	}

	return workflows.ReviewOrderResult{Approved: approval.Approved}, nil
}
```

`Await` suspends the run until the hook is resumed. The payload is encoded as
JSON, so `T` must be JSON-friendly.

## Resuming a hook

Anything with a Client in its context can resume a hook by its **token**:

```go
err := workflow.ResumeHook(ctx, "approve:"+orderID, Approval{Approved: true})
```

`ResumeHook` records the payload and queues a delivery so the run continues. It
returns `workflow.ErrHookNotFound` when no active hook has the token.

If the run may not have created the hook yet, for example right after `Start`,
use `ResumeHookWithRetries`, which retries with a growing delay while the hook
is not found:

```go
err := workflow.ResumeHookWithRetries(ctx, token, Approval{Approved: true}, 5)
```

## Tokens

A token identifies a hook to the outside world.

- **Deterministic tokens.** Set `HookOption.Token` when the caller must know it
  in advance, like `"approve:" + orderID`.
- **Random tokens.** Leave `Token` empty to get a random 21-character token,
  generated when the hook is created and kept across replays. Read it with
  `hook.Token()` and hand it to whoever will resume the hook.
  `workflow.NewHookToken()` generates the same kind of token.

A token belongs to at most one **active** hook at a time. Creating a hook with a
token another active hook owns fails with `ErrHookConflict`:

```go
hook, err := workflow.CreateHook[Approval](ctx, workflow.HookOption{Token: token})
if errors.Is(err, workflow.ErrHookConflict) {
	owner, _, _ := hook.GetConflict(ctx) // the run that owns the token
	_ = owner
}
```

`HookOption.Force` takes the token over from the other run instead. Awaiting the
displaced hook in that run then returns `ErrHookForceClaimed`.

`HookOption.MinRetention` keeps the token reserved for a while after the hook
settles, so no other hook can take it in the meantime.

## Hook methods

| Method | Description |
| --- | --- |
| `Await(ctx)` | Payload, suspending until resumed. Returns `ErrHookConflict`, `ErrHookForceClaimed`, or `ErrHookNotRegistered` when applicable. |
| `Done(ctx)` | Whether the hook has settled (received a payload, was cancelled, or failed to be created) as of the results [delivered so far](/guide/replay#delivering-results-in-order). Never suspends and never returns the hook's error; `Await` does. |
| `Cancel(ctx)` | Disposes of the hook so it no longer accepts a payload. |
| `Unwrap(ctx)` | `Await`, panicking on error. |
| `ID()` | Hook ID, or `""` before the hook is created. |
| `Token()` | Hook token. |
| `GetConflict(ctx)` | The run that owns the token, after `ErrHookConflict`. |

A `*Hook[T]` implements `Promise[T]`, so it works with `Select` and `Join`.

## Defining a hook up front

`workflow.DefineHook` declares a hook without creating it, and `Create` creates
it in the run. The token is fixed when `DefineHook` is called.

```go
definition := workflow.DefineHook[Approval](workflow.HookOption{Token: token})
hook, err := definition.Create(ctx)
```

`CreateHook` is `DefineHook` followed by `Create`.

## Looking up a hook from outside

`workflow.GetHook` looks up the active hook with a token using the Client in the
context. It reports whether one exists. The returned hook only reports its ID and
token; it cannot be awaited.

```go
hook, ok, err := workflow.GetHook[Approval](ctx, token)
```
