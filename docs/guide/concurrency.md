# Concurrency

Start work before waiting for its result to let steps run in parallel across
workers. Keep the runner itself sequential so replay follows recorded event
order.

## Starting work

`workflow.Background` starts a step without waiting. Read its result when you
need it:

```go
profile := workflow.Background(ctx, fetchProfile, userID)
orders := workflow.Background(ctx, fetchOrders, userID)

// Both steps can run while the runner waits for the profile.
profileValue, err := profile.Value(ctx)
if err != nil {
	return Output{}, err
}

orderValues, err := orders.Value(ctx)
if err != nil {
	return Output{}, err
}

return Output{Profile: profileValue, Orders: orderValues}, nil
```

A result's `Unwrap(ctx)` method also waits, but panics on failure. Use `Value`
when you need to handle the error. `workflow.Step` combines `Background` and
`Value` in one call.

## Waiting for a group

### Wait for every outcome

Use `Join` to wait for work to settle, even if some operations fail. Read each
result afterward to handle its error:

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

`Join` only returns an error if it cannot inspect or await settlement.

### Require every operation to succeed

Use `JoinOk` when a failed operation should end the wait immediately:

```go
profile := workflow.Background(ctx, fetchProfile, userID)
orders := workflow.Background(ctx, fetchOrders, userID)

if err := workflow.JoinOk(ctx, profile, orders); err != nil {
	return Output{}, err
}

// JoinOk succeeded, so both values are available.
return Output{Profile: profile.Unwrap(ctx), Orders: orders.Unwrap(ctx)}, nil
```

The first failure in recorded order ends the wait. Other inputs keep running.

### Apply a step to a slice

Use `All` to run a step for each input and collect values in input order:

```go
thumbnails, err := workflow.All(ctx, resizeImage, input.Images)
if err != nil {
	return Output{}, err
}

return Output{Thumbnails: thumbnails}, nil
```

If any step fails, `All` cancels the remaining steps and returns that failure
without waiting for earlier inputs. Steps must observe their context to stop.

For individual result handles, use `Map` and read each result with `Value`.
Use `MapResults` to pass the returned slice to a combinator:

```go
results := workflow.Map(ctx, resizeImage, input.Images)
if err := workflow.JoinOk(ctx, workflow.MapResults(results)...); err != nil {
	return Output{}, err
}
```

## Grouping work

Use a `WaitGroup` to collect work from several calls and wait for every outcome.
`workflow.Go` starts a step, adds it to the group, and returns its result.
`Add` includes existing results, timers, hooks, webhooks, or other groups:

```go
var group workflow.WaitGroup

email := workflow.Go(ctx, &group, sendEmail, input)
sms := workflow.Go(ctx, &group, sendSMS, input)
group.Add(workflow.MapResults(existingResults)...)

if err := group.Wait(ctx); err != nil {
	return Output{}, err
}

_, emailErr := email.Value(ctx)
_, smsErr := sms.Value(ctx)
return Output{EmailSent: emailErr == nil, SMSSent: smsErr == nil}, nil
```

Use `ErrGroup` to collect the first recorded failure. It still waits for every
member to settle. A zero-value ErrGroup does not cancel work. `ErrGroupWithContext`
creates one with a durable cancellation context:

```go
group, groupCtx, err := workflow.ErrGroupWithContext(ctx)
if err != nil {
	return Output{}, err
}

profile := workflow.Go(groupCtx, group, fetchProfile, userID)
orders := workflow.Go(groupCtx, group, fetchOrders, userID)

if err := group.Wait(ctx); err != nil {
	return Output{}, err
}

return Output{Profile: profile.Unwrap(ctx), Orders: orders.Unwrap(ctx)}, nil
```

Groups start observing members when first inspected by Wait, Select, or Join.
After that, the first member failure cancels `groupCtx`, including when the
runner is waiting on other work. Steps must observe their context to stop.
Wait returns the first failure after every member settles and cancels the
context on return. Call Wait after adding the work.

Pass `groupCtx` to work that should stop on group failure, including work
created separately and included with Add.

Both group types can participate directly in `Select`, `Join`, and `Add`.
They settle after their final member settles. An ErrGroup's settled outcome
includes its first failure. Add ignores new members after settlement. Go still
starts a step, but the completed group does not track it. Use a new group to
wait for later work.

## Racing operations

`Select` returns the first input to settle and its error. Compare the winning
handle with the inputs, then read its value:

```go
primary := workflow.Background(ctx, lookupPrimary, query)
replica := workflow.Background(ctx, lookupReplica, query)

winner, err := workflow.Select(ctx, primary, replica)
if err != nil {
	return Output{}, err
}

if winner == primary {
	return Output{Answer: primary.Unwrap(ctx)}, nil
}

return Output{Answer: replica.Unwrap(ctx)}, nil
```

Use `SelectOk` to wait for the first success instead. If every input fails, it
returns a nil winner and the first error in argument order.

Other inputs keep running after a selection. When several inputs are ready,
the earliest recorded settlement wins, with argument order breaking ties.
See [Delivering results in order](/guide/replay#delivering-results-in-order).

Hooks and webhooks can also participate in waits. Selecting or joining a hook
leaves its payload available for `Next`. Runs have an independent lifetime and use
[`Status`](/guide/runs#the-run-handle) to report readiness.

## Chaining work

Use `Then` to choose follow-up work after a result settles. Its callback can
handle the source error and return another result:

```go
user := workflow.Background(ctx, fetchUser, userID)
welcome := workflow.Then(ctx, user, func(value User, err error) *workflow.Result[Receipt] {
	if err != nil {
		return workflow.NewResult(Receipt{}, err)
	}

	return workflow.Background(ctx, sendWelcome, value.Name)
})

// The chain can race while either step is pending.
timer := workflow.Timer(ctx, time.Minute)
winner, err := workflow.Select(ctx, welcome, timer)
if err != nil {
	return Output{}, err
}

if winner == welcome {
	return Output{Receipt: welcome.Unwrap(ctx)}, nil
}

return Output{TimedOut: true}, nil
```

The chain follows the returned result and yields its value directly. Use
`NewResult` when the callback can provide an immediate value or error.

Callbacks run in workflow context during replay. Put side effects in steps
and return their pending results. Calling `Step` or `Value` inside a callback
can suspend replay before an outer `Select` gets to consider other inputs.

Repeated reads reuse the completed outcome within that replay. Read or join
the chain before returning if the workflow must wait for it. The chain's
recorded order accounts for both the callback and its returned result, so an
immediate result cannot win before the callback ran.

## Cancelling remaining work

`Select` and `JoinOk` leave other inputs running. To stop them, start the work
with a shared cancellation context and cancel it after the wait. See
[Cancellation](/guide/cancellation#cancelling-work) for a complete race example.

`All` already cancels remaining steps when one fails. In either case, steps
must observe their context to stop cooperatively.
