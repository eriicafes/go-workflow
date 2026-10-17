# Concurrency

Start several operations before waiting on them. Background steps run as separate deliveries and can execute in parallel across workers. Workflow code itself follows recorded event order; it must not start goroutines.

## Starting work

`Background` returns `*workflow.Result[T]` immediately. `result.Value(ctx)` waits and returns `(T, error)`; `result.Unwrap(ctx)` waits and panics on failure. `Step` is `Background` followed by `Value`.

Typed results are `*Result[T]`, which exposes `Value` and `Unwrap`. The settlement contract is private to the SDK. Hooks, webhooks, results, timers, and chains can participate in `Join` and `Select`, including inputs with different value types. Runs use `Status` and do not participate in these combinators.

Results become visible in recorded order at waiting calls. See [Delivering results in order](/guide/replay#delivering-results-in-order).

## Waiting

### Join

`Join(ctx, a, b)` waits for all inputs to settle. It ignores their operation errors; read each result or hook to handle those errors. It returns an error if settlement cannot be inspected or awaited.

```go
email := workflow.Background(ctx, sendEmail, input)
sms := workflow.Background(ctx, sendSMS, input)
if err := workflow.Join(ctx, email, sms); err != nil {
    return Output{}, err
}
_, emailErr := email.Value(ctx)
_, smsErr := sms.Value(ctx)
return Output{EmailSent: emailErr == nil, SMSSent: smsErr == nil}, nil
```

### JoinOk

`JoinOk(ctx, a, b)` waits for every input to succeed and returns as soon as any input fails, using recorded settlement order. It leaves the remaining inputs running. Both functions return only an error; neither collects values.

```go
profile := workflow.Background(ctx, fetchProfile, userID)
orders := workflow.Background(ctx, fetchOrders, userID)
if err := workflow.JoinOk(ctx, profile, orders); err != nil {
    return Output{}, err
}
return Output{Profile: profile.Unwrap(ctx), Orders: orders.Unwrap(ctx)}, nil
```

### All

`All(ctx, fn, inputs)` starts one step per input and collects their values in input order. If any step fails, `All` cancels the remaining steps and returns that failure, even when an earlier input is still running. Steps must observe their context to stop cooperatively.

```go
thumbnails, err := workflow.All(ctx, resizeImage, input.Images)
if err != nil {
    return Output{}, err
}
return Output{Thumbnails: thumbnails}, nil
```

When you need individual result handles, `Map(ctx, fn, inputs)` starts one step per input and returns `[]*Result[T]` in input order. Read individual results with `Value`. Go does not allow spreading this typed slice into the combinators' private interface parameter.

## Racing

`Select` returns the first input to settle and its error. `SelectOk` waits for the first success; if all inputs fail, it returns a nil winner and the first error in argument order. They each return only `(winner, error)`.

The winner is the exact input handle. Compare it by identity, then read that handle's typed value:

```go
primary := workflow.Background(ctx, lookupPrimary, query)
replica := workflow.Background(ctx, lookupReplica, query)
winner, err := workflow.Select(ctx, primary, replica)
if err != nil {
    return Output{}, err
}
switch winner {
case primary:
    return Output{Answer: primary.Unwrap(ctx)}, nil
case replica:
    return Output{Answer: replica.Unwrap(ctx)}, nil
}
```

A selected hook's payload remains available for `Next`; selection does not consume it. Other inputs keep running. When several inputs are ready, the earliest recorded settlement wins, with argument order breaking ties. Empty joins succeed; an empty selection returns a nil winner and no error.

## Chaining

`Then` receives both the source value and its error. Its callback returns only a `*Result[U]`, and the chain follows that result's value or error. Use `NewResult(value, err)` for an immediately settled value or failure:

```go
user := workflow.Background(ctx, fetchUser, userID)
name := workflow.Then(ctx, user, func(value User, err error) *workflow.Result[string] {
    if err != nil {
        return workflow.NewResult("Guest", nil)
    }
    return workflow.NewResult(value.Name, nil)
}) // *workflow.Result[string]

welcome := workflow.Then(ctx, name, func(value string, err error) *workflow.Result[Receipt] {
    if err != nil {
        return workflow.NewResult(Receipt{}, err)
    }
    return workflow.Background(ctx, sendWelcome, value)
}) // *workflow.Result[Receipt]
```

A chain stays pending while its source or the returned result is pending. Returning `Background`, `Timer`, or another `Then` lets other `Select` candidates continue to compete. `Value` returns `U` directly, with no nested result to unwrap:

```go
timer := workflow.Timer(ctx, time.Minute)
winner, err := workflow.Select(ctx, welcome, timer)
if err != nil {
    return Output{}, err
}
if winner == welcome {
    receipt := welcome.Unwrap(ctx)
    // Use receipt.
}
```

Callbacks execute synchronously in workflow context during replay. Put side effects in steps, and return pending work without calling `Step` or `Value` inside the callback when the chain needs to participate in a race. Those blocking calls can suspend replay before the outer `Select` resumes. Repeated reads reuse the completed outcome within that replay. Read or join the chain when the workflow must wait for it before returning.

Recorded settlement order accounts for both the callback and its returned result. Returning an already-settled result cannot make the chain win before its callback ran.

## Cancellation

Attach a durable cancellation context to work and invoke its cancellation function yourself:

```go
workCtx, cancel, err := workflow.WithCancel(ctx)
if err != nil {
    return Output{}, err
}
primary := workflow.Background(workCtx, lookupPrimary, query)
replica := workflow.Background(workCtx, lookupReplica, query)
winner, err := workflow.Select(ctx, primary, replica)
if cancelErr := cancel(); cancelErr != nil {
    return Output{}, cancelErr
}
if err != nil {
    return Output{}, err
}
// Compare winner and read primary or replica as above.
_ = winner
```

Cancellation preserves a result already committed by the winner. Pending work observes cancellation through its context. See [Cancellation](/guide/steps#cancellation) for causes, combined contexts, and child runs. Use separate cancellation contexts when branches need independent cancellation.
