# How Replay Works

Every run is recorded in a [World](/worlds/) as an append-only **event log**.
The runs, steps, and hooks you read from a World are views derived from that
log. The runtime never keeps a run's state in memory between deliveries:
everything it needs is in the log.

## Deliveries

The World's queue holds **deliveries**. Each delivery makes the runtime execute
either a flow or a step of one run.

- A **flow delivery** replays the runner from the beginning. Every operation
  the runner reaches (a step, timer, hook, webhook, or child run) is looked up
  in the event log. If it is already recorded, its recorded result is returned.
  If it is new, the flow records it, for example with a `step_created` event,
  and queues whatever must happen next. When the runner reaches a promise that
  has not settled, the flow **suspends** and the delivery ends. When the runner
  returns, the flow records `run_completed` or `run_failed`.
- A **step delivery** replays the runner up to the target step, calls the step
  function with its recorded input, and records the result with
  `step_completed`, `step_retrying`, or `step_failed`. A new flow delivery is
  then queued so the runner can continue.

```
Start ──► run_created ──► flow delivery
                            │  replay: Step(findSources) is new
                            │  record step_created, queue step delivery
                            ▼  suspend
                          step delivery
                            │  replay up to findSources, call it
                            │  record step_completed, queue flow delivery
                            ▼
                          flow delivery
                            │  replay: Step(findSources) returns recorded result
                            │  runner returns
                            ▼  record run_completed
```

Whether a delivery may run is decided by appending events, which the World
admits or rejects atomically. Duplicate and stale deliveries are therefore
safe: a delivery for a run or step that is already terminal returns without
running any code. A delivery that dies before recording its result is
redelivered by the queue and runs the step again as a new attempt.

### Concurrent flows

Two flow deliveries of the same run can replay at once, for example when two
steps finish together. Each loaded the log before the other wrote, so each
would declare the next operation on its own. The flow's writes are therefore
**in-band** and fenced: the run keeps a count of them (`Run.InBandCount`), and
every flow write carries the count it last saw. When another flow has written
since, the World refuses the write with `worlds.ErrInBandSuperseded`. The refused flow
stops and queues one follow-up flow, which replays the run and sees the other
flow's declarations. Flows refused by the same write share one
follow-up on a World that deduplicates deliveries. Writes from step deliveries, hook resumes, `WakeUp`, and
cancellation are out-of-band: they are never refused and don't move the count.

## Identifying operations

Operations are identified by **position**: the order in which the runner
reaches them. Each recorded operation stores its position, and replay checks
that the runner reaches a matching operation there.

## Delivering results in order

Replay walks the event log in order, and a result (a step finishing, a timer
firing, a hook receiving its payload) becomes visible to the runner only once
the walk has passed it. The walk advances only where the runner waits: `Value`, `Next`,
`Join`, `JoinOk`, and the `Select` functions. So:

- **`Select` picks the first promise to settle.** When several promises have
  settled, the one whose result was recorded first wins, not the first in
  argument order. Every replay picks the same winner.
- **Timers fire where they are declared.** A flow that replays past a timer's
  declaration once it is due records its `wait_completed`. The runner sees it
  when the walk reaches it, like any other result. `WithTimeout` and
  `WithDeadline` attach a `Then` continuation that cancels their context when
  the timer settles.

Inline steps (`InlineStep`, `Now`, and the `Run` methods in workflow context)
and hook conflicts are the exception: the runner sees them where it declares
them, because the flow that declares them also settles them.

This matches how the Workflow SDK consumes its event log, where a promise
resolves when the walk reaches its result.

## Replay divergence

A replay **diverges** when the runner doesn't reach the operations its log
records. The runtime detects:

- an operation of another kind at a recorded position, such as a step where a
  timer was recorded;
- a step that calls a different step function than the one recorded;
- a hook with a different token than the one recorded;
- a runner that returns without reaching a recorded operation, for example
  after a step was removed.

On divergence, the runtime ends the delivery instead of returning an error to
the runner, so a runner that handles step errors can't carry on with misaligned
results. The runtime acknowledges the delivery and queues a **recovery
delivery** of the same flow or step right away, which replays the run from a
fresh read of its log, possibly on another worker. Each recovery delivery
carries a count in `QueueInput.ReplayDivergenceCount`. After three recovery
deliveries diverge too, the runtime fails the run with an error matching
`worlds.ErrCorruptedEventLog`, whose message names the position and the
mismatch. A step that diverges records a new attempt on each recovery
delivery. Compensations registered with `OnRollback` do **not** run,
because they would replay against the same log.

```go
_, err := run.Value(ctx)
if errors.Is(err, worlds.ErrCorruptedEventLog) {
	log.Printf("run diverged from its event log: %v", err)
}
```

Some changes are not detected: a step called with different input returns its
recorded result, a changed `Sleep` duration keeps its recorded due time, and
calls to the same step function in a different order, such as `Map` over a
reordered slice, match by name. [Pin runs to a deployment](#pinning-runs-to-a-deployment)
to keep in-flight runs on the code that recorded them.

## Determinism rules

A runner replays from the beginning on every delivery, so its code in workflow
context must make the same decisions every time.

- **Do side effects only in steps.** Network calls, database queries, file
  I/O, and anything that sends a message belong in a step function.
- **Read time only through `workflow.Now`.** It records the time the first time
  it runs, so replay returns the same value. `time.Now` is fine inside steps.
- **Don't use randomness directly.** Generate IDs and random choices in a step,
  or with [`workflow.InlineStep`](/guide/steps#inline-steps).
- **Don't start goroutines or `select` on channels.** Use
  [`workflow.Background`](/guide/concurrency) and the `Select` functions.
- **Don't range over maps** in a way that changes the order of operations; Go
  randomizes map iteration order.
- **Pass state to steps as input.** A step function that captures variables
  from the runner hides its real input from the recorded log.

The [analyzer](/tooling/analyzer) reports violations of these rules.

::: warning Suspension uses panics
When a runner awaits a promise that has not settled, the runtime ends the
replay by panicking with a value it recovers. Deferred functions in the runner
run as usual, but a runner must not `recover()` panics it did not cause, or it
will swallow the suspension.
:::

## At-least-once steps

A step can run more than once: when it fails and retries, and when a delivery
dies after the step function returned but before its result was recorded. Step
functions should therefore be **idempotent**. `workflow.StepID(ctx)` returns a
step ID that is stable across attempts and works well as an idempotency key for
the services a step calls.

## Changing running workflows

A run replays against the runner code that handles each delivery. If you
change a runner so that it reaches different operations, or the same operations
in a different order, runs that are already in flight no longer match their
recorded events. Prefer adding new workflow types for incompatible changes, and
let existing runs finish on the old runner.

### Pinning runs to a deployment

On a World with **deployment affinity**, each run is pinned to the deployment
that started it, and only that deployment's code ever replays it. The Vercel
World has affinity: Vercel keeps every deployment reachable, and its queue can
address a delivery to a specific one. Other Worlds deliver to whichever worker
consumes their queue, so they record a run's deployment but don't enforce it.

How it works:

- **Pinned at start.** `workflow.Start` records the World's deployment ID on the
  new run (`Run.DeploymentID`). Runs started from JavaScript through the
  [world-http adapter](/tooling/world-http) get the Go World's deployment ID.
- **Addressed to the run's deployment.** Every delivery for a run is sent to
  its deployment, including the ones queued from outside the run: hook and
  webhook resumes, `WakeUp`, and QStash timers. A newer deployment handling a
  webhook for an older run sends the delivery back to the older one.
- **Re-routed if misdelivered.** A delivery that still reaches another
  deployment is not replayed. It is sent to the run's deployment again, after
  1, 2, then 4 seconds.
- **Failed when unreachable.** After three re-routes, or as soon as Vercel
  reports the run's deployment gone, the run fails with an error matching
  `worlds.ErrDeploymentMismatch`.

```go
_, err := run.Value(ctx)
if errors.Is(err, worlds.ErrDeploymentMismatch) {
	// The run's deployment was deleted, or its deliveries could not reach it.
}
```

Because runs keep using their own deployment, **keep old deployments around
until their runs finish**. On Vercel, set the project's
[deployment retention](https://vercel.com/docs/deployment-retention) longer than
your longest-running workflow. Expired deployments can be restored for 30 days,
which brings their pinned runs back.

| World | Deployment ID | Affinity |
| --- | --- | --- |
| Vercel | `vercelworld.Config.DeploymentID`, defaulting to `VERCEL_DEPLOYMENT_ID` | yes, when the ID is set |
| Local, Postgres, AWS, test | none | no |
| Custom | `World.DeploymentID()` | when `Capabilities().DeploymentAffinity` is true |
