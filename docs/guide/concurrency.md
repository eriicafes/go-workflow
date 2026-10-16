# Concurrency

Runners can't start goroutines, because goroutine scheduling isn't replayable.
Instead, operations return **promises**, and you start several before waiting
on any of them. The steps behind the promises run as separate deliveries, so
they execute in parallel across your workers.

## Promises

`workflow.Promise[T]` is the pending result of an operation started in workflow
context: a step, timer, hook, webhook, or child run.

```go
type Promise[T any] interface {
	Done(context.Context) (bool, error) // settled yet? never suspends
	Await(context.Context) (T, error)   // value or error, suspends until settled
	Unwrap(context.Context) T           // Await, panicking on error
	Cancel(context.Context) error       // cancel the operation
}
```

Results are delivered in the order they were recorded, and only where the
runner waits: `Await`, `Join`, and the `Select` functions. See
[Delivering results in order](/guide/replay#delivering-results-in-order).

**`Done`** reports whether the promise has **settled**, whether it succeeded
or failed, as of the results delivered so far. It never suspends the run or
waits for more results, and never returns the error the promise failed with.
Its error is reserved for when settlement can't be determined, such as
`ErrNoWorkflow` outside workflow context. To get a settled promise's error,
call `Await`, which returns immediately once the promise has settled.

Because `Done` only sees delivered results, it is false right after a promise
is created. Check it after waiting on something else. This waits for the
receipt and then checks whether the slower fraud check has finished:

```go
fraud := workflow.Background(ctx, checkFraud, order)

receipt, err := workflow.Step(ctx, chargeCard, order)
if err != nil {
	return Result{}, err
}

done, err := fraud.Done(ctx)
if err != nil {
	return Result{}, err
}

if !done {
	// The fraud check had not finished when the charge did, on every replay.
	if _, err := workflow.Step(ctx, flagForReview, receipt); err != nil {
		return Result{}, err
	}
}
```

Awaiting a promise that has not settled suspends the run until it does.
Cancelling a promise cancels a step, settles a timer immediately, stops a hook
from accepting payloads, or cancels a child run. Cancelling a settled promise
is a no-op.

## Starting steps

### Background

`Background` starts a step and returns its promise without waiting. Here the
profile and orders are fetched in parallel, then awaited in turn.

```go
profilePromise := workflow.Background(ctx, fetchProfile, userID)
ordersPromise := workflow.Background(ctx, fetchOrders, userID)

profile, err := profilePromise.Await(ctx)
if err != nil {
	return Result{}, err
}

orders, err := ordersPromise.Await(ctx)
if err != nil {
	return Result{}, err
}
```

`Background` takes the same `StepOptions` as `Step`. Outside workflow context the
returned promise fails with `ErrNoWorkflow`.

### Map

`Map` starts one step per input and returns their promises in input order, so
you can wait on them however you like. This resizes every image in parallel:

```go
promises := workflow.Map(ctx, resizeImage, input.Images)
thumbnails, err := workflow.Join(ctx, promises...)
if err != nil {
	return Result{}, err
}
```

### All

`All` is `Map` followed by `Join`. This searches two queries in parallel and
gets their results in input order, or the first error:

```go
groups, err := workflow.All(ctx, findSources, []string{input.Topic, input.Topic + " examples"})
if err != nil {
	return Result{}, err
}
```

## Waiting on several promises

Every function below takes promises of the same type `T` and returns
`ErrNoWorkflow` outside workflow context.

### Join

`Join` waits until every promise settles and returns their values in argument
order. If any failed, it returns the first error in argument order. This
creates an invoice and a shipping label in parallel and needs both:

```go
invoice := workflow.Background(ctx, createInvoice, order)
label := workflow.Background(ctx, createShippingLabel, order)

ids, err := workflow.Join(ctx, invoice, label)
if err != nil {
	return Result{}, err
}

invoiceID, labelID := ids[0], ids[1]
```

### JoinSettled

`JoinSettled` waits until every promise settles but never fails because one of
them did; failed promises get the zero value. This notifies every subscriber,
then collects the ones that failed by awaiting each settled promise:

```go
promises := workflow.Map(ctx, notifySubscriber, subscribers)
if _, err := workflow.JoinSettled(ctx, promises...); err != nil {
	return Result{}, err
}

var failed []string
for i, p := range promises {
	if _, err := p.Await(ctx); err != nil {
		failed = append(failed, subscribers[i])
	}
}
```

### Select

`Select` waits until **any** promise settles and returns it, with the error it
settled with. When several have settled, the one whose result was recorded
first wins, so every replay picks the same promise, and the others keep
running. This takes whichever lookup answers first:

```go
primary := workflow.Background(ctx, lookupPrimary, query)
replica := workflow.Background(ctx, lookupReplica, query)

winner, err := workflow.Select(ctx, primary, replica)
if err != nil {
	return Result{}, err // the first lookup to settle failed
}

answer, _ := winner.Await(ctx)
```

### SelectWithCancel

`SelectWithCancel` is `Select` followed by cancelling every other promise. This
is the same race, but the slower lookup is cancelled instead of running to
completion:

```go
winner, err := workflow.SelectWithCancel(ctx, primary, replica)
if err != nil {
	return Result{}, err
}

answer, _ := winner.Await(ctx)
```

### SelectOk

`SelectOk` waits until **any** promise **succeeds**, skipping failures while
another can still succeed. When all fail, it returns a nil promise and the
first error in argument order. This takes the first provider that returns a
quote:

```go
us := workflow.Background(ctx, quoteFromProvider, "us")
eu := workflow.Background(ctx, quoteFromProvider, "eu")

winner, err := workflow.SelectOk(ctx, us, eu)
if err != nil {
	return Result{}, err // every provider failed
}

quote, _ := winner.Await(ctx)
```

### SelectOkWithCancel

`SelectOkWithCancel` is `SelectOk` followed by cancelling every other promise
once one succeeds. This takes the first quote and cancels the other request:

```go
winner, err := workflow.SelectOkWithCancel(ctx, us, eu)
if err != nil {
	return Result{}, err
}

quote, _ := winner.Await(ctx)
```

### Cancel

`Cancel` cancels each promise in argument order and stops at the first error.
Promises that have already settled are left unchanged. This gives cache
warming a minute, then cancels whatever hasn't finished:

```go
promises := workflow.Map(ctx, warmCache, regions)
if err := workflow.Sleep(ctx, time.Minute); err != nil {
	return Result{}, err
}

if err := workflow.Cancel(ctx, promises...); err != nil {
	return Result{}, err
}
```

## Timers and hooks

Timers and hooks are promises too, so they work with every function above when
the types match. [Sleep and Timers](/guide/sleep#timers) races a hook against a
timer, and [`workflow.Timeout`](/guide/sleep#timeouts) bounds how long a whole
run may take.

## Determinism with promises

Promises keep concurrency deterministic because the order in which you start
operations is fixed by your code, not by the scheduler, and the order results
are delivered in is fixed by the event log. Start operations in a stable order:
iterate slices, not maps, when calling `Background` in a loop.

A promise you implement yourself works with `Join` and `Await`. The `Select`
functions check it with `Done` before each wait, so it can't take part in the
ordering by recorded result.
