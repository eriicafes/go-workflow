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
  If it is new, the flow records it, for example with a `wait_created` event,
  and queues whatever must happen next. When the runner reaches a promise that
  has not settled and no step the flow runs inline can settle it, the flow
  **suspends**. When the runner returns, the flow
  records `run_completed` or `run_failed`.
- New steps and timers wait until the runner waits for a result that is not
  recorded. The flow then records them with one write. Each step's mode is
  fixed there and never changes: up to
  [the limit](/guide/steps#running-steps-inline) run inline, each created by a
  `step_started` that starts its first attempt, and the rest are recorded with
  `step_created` and queued as step deliveries once the write commits. Instead
  of suspending, the runner waits: as each inline step finishes, the flow
  records its result and the runner continues from where it waited. While it
  waits, the flow completes the timers that fall due and reads what others
  append to the log every two seconds, so a payload or a cancellation reaches
  the runner. The flow suspends once nothing it runs inline is left.
- A **step delivery** reads the log and decides: a finished step or run is
  acknowledged, a retry not yet due asks for its message again when it is,
  and a step whose attempts are all used fails. Otherwise it records
  `step_started`, replays the runner up to the step, calls the step function
  with its recorded input, and records the result with `step_completed`,
  `step_retrying`, or `step_failed`. A retry keeps the step's message: the
  delivery asks the queue for it again when the retry is due. A final result
  queues a flow delivery so the runner can continue.
- A failed inline attempt that may retry is handed to a step delivery: the
  flow records `step_retrying` and queues the step's message for the next
  attempt.

```
Start ──► run_created ──► flow delivery
                            │  record run_started
                            │  replay: Step(findSources) is new, the runner waits
                            │  record step_started, creating the inline step
                            │  call findSources, record step_completed
                            │  the runner continues with the result
                            │  runner returns
                            ▼  record run_completed
```

With inline steps turned off, or past the limit, the step runs in its own
delivery:

```
Start ──► run_created ──► flow delivery
                            │  replay: Step(findSources) is new
                            │  record step_created, queue step delivery, suspend
                            ▼
                          step delivery
                            │  record step_started
                            │  replay up to findSources, call it
                            │  record step_completed, queue flow delivery
                            ▼
                          flow delivery
                            │  replay: Step(findSources) returns recorded result
                            │  runner returns
                            ▼  record run_completed
```

Whether a delivery may run is decided from the event log, so duplicate and
stale deliveries are safe: a delivery for a run or step that is already
terminal returns without running runner or step code. A step delivery that
dies before recording its result is redelivered by the queue and runs the step
again as a new attempt. A step that ran inline in a flow that died runs again
in the next flow that finds its attempt without a result. A flow that is not
its message's first delivery queues again every step delivery a delivery that
died may have recorded and not queued, under the step's stable key, so the
queue keeps one message per step.

### Concurrent flows

A run's flow is its only orchestrator, and its writes are **in-band**: the
run counts the positions they take (`Run.InBandCount`, `run_created`
included), a flow reads the count with its log, and every write it makes
carries the count. When another flow has written in-band since, the World
refuses the write with `worlds.ErrInBandSuperseded` and appends nothing. The
refused flow stops writing and starting steps, and asks the queue for its
message again a second later, when it replays the run with the other flow's
writes. Since every event of an inline step is in-band too, two flows never
both record one. Worlds deliver one flow of a run at a time where their queue
can (`Capabilities().MaxConcurrency`), which makes such an overlap rare; the
fence is what keeps it safe. Writes from step deliveries, hook resumes,
cancellation, and clients are out-of-band: they are never fenced and don't
move the count. Replay ignores any event of a step, hook, or timer after its
final one, so the first writer of a step's result wins.

### Running ahead

A flow hands an inline step's result to the runner as soon as the step
returns, and writes it while the runner goes on, so consecutive inline steps
overlap their writes instead of each waiting for its own. Up to 10 results
(`core.WithRunAheadDepth` on `core.NewRuntime`, 0 turns it off) may be
unconfirmed at once.

A flow that runs ahead decides from the log plus its own writes in flight, and
another writer's event can land between them, where a later replay reads it.
So a flow runs ahead only where no such event can change a decision: while the
runner waits on no hook, has no cancellation context, no step runs in another
delivery, and no timer can fall due within the delivery's inline window.
Every other write of the flow, and its suspension, first waits for the writes
ahead of it to commit. An event another writer appends below one of its
writes, reported by the write's answer or read by the live feed, is handled
by what it is:

- A hook event for a hook nobody waits on, or an attribute write, is folded in
  after the flow's own writes, since a replay that reads it earlier decides
  the same.
- A hook event for a hook the runner waits on or for a cancellation context, a
  step event of a step this delivery does not run, or a `wait_completed` stops
  the delivery's decisions. It still writes the results of the steps whose
  start committed, starts again from a fresh read of the log with run-ahead
  off, and hands its message on if that stops too.
- Any other event, an incomplete report, or a write that committed below the
  position the flow placed it at, makes the delivery replay again from the
  log once its writes ahead settle.

A World that stores a write at a time other than the one the flow sent, which
the runner's clock already read, stops the delivery the same way and is not
run ahead of again in that process. A run whose writes keep being refused by
the fence is logged as a warning.

## Identifying operations

Operations are identified by **ID**, and every ID comes from a generator
seeded with the run: `<run ID>:<workflow name>:<deployment ID>` seeds
seedrandom's ARC4, and steps, timers, and hooks take the run's next
correlation ULID, `step_`, `wait_`, or `hook_` and a ULID at the run's
creation time, each one the previous plus one. A hook created without a
token draws a 21-character Nano ID token from the same generator. Each replay
draws the same IDs in the same order, and replay checks that the operation
the runner reaches matches the one recorded with its ID. This is why a runner
must declare its operations in the same order on every replay.

## Delivering results in order

Replay walks the event log in order, and a result (a step finishing, a timer
firing, a hook receiving its payload) becomes visible to the runner only once
the walk has passed it. The walk advances only where the runner waits on a
promise. So:

- **`Select` picks the first promise to settle.** When several promises have
  settled, the one whose result was recorded first wins, not the first in
  argument order. Every replay picks the same winner.
- **Timers fire where they are declared.** A flow that replays past a timer's
  declaration once it is due records its `wait_completed`, and so does a flow
  that waits for an inline step when the timer falls due. The runner sees it
  when the walk reaches it, like any other result. `WithTimeout` and
  `WithDeadline` attach a `Then` continuation that cancels their context when
  the timer settles.

`GetConflict` is the exception: it returns a hook conflict as soon as it is
called, but `Next` and `Select` see it only when the walk reaches it.

This matches how the Workflow SDK consumes its event log, where a promise
resolves when the walk reaches its result.

## Replay divergence

A replay **diverges** when the runner doesn't reach the operations its log
records. The runtime detects:

- an operation of another kind recorded with the drawn ID, such as a step
  where a timer was recorded.
- a step that calls a different step function than the one recorded.
- a hook with a different token than the one recorded.
- a runner that returns without reaching a recorded operation, for example
  after a step was removed.

On divergence, the runtime ends the delivery instead of returning an error to
the runner, so a runner that handles step errors can't carry on with misaligned
results. The runtime acknowledges the delivery and queues a **recovery
delivery** of the same flow or step right away, which replays the run from a
fresh read of its log, possibly on another worker. Each recovery delivery
carries a count in `QueueInput.ReplayDivergenceCount`. After three recovery
deliveries diverge too, the runtime fails the run with an error matching
`worlds.ErrCorruptedEventLog`, whose message names the operation's ID and
the mismatch. A step that diverges records a new attempt on each recovery
delivery. Compensations registered with `Rollback` do **not** run,
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
- **Read time only through `workflow.Now`.** It returns the replay clock, which
  every replay moves to the same times at the same points.
  `time.Now` is fine inside steps.
- **Read randomness only through `workflow.RandReader`.** Its bytes are drawn
  from the run's seed, the generator that draws step, timer, and hook IDs, so
  every replay reads the same bytes in the same order. Wrap it for other
  shapes, such as `uuid.NewRandomFromReader(workflow.RandReader(ctx))`.
  `crypto/rand` is fine inside steps.
- **Don't start goroutines or `select` on channels.** Use
  [`workflow.Background`](/guide/concurrency) and the `Select` functions.
- **Don't range over maps** in a way that changes the order of operations. Go
  randomizes map iteration order.
- **Pass state to steps as input.** A step function that captures variables
  from the runner hides its real input from the recorded log.

The [analyzer](/tooling/analyzer) reports common violations of these rules.

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
address a delivery to a specific one. The other built-in Worlds have no
deployment ID and deliver to whichever worker consumes their queue.

How it works:

- **Pinned at start.** `workflow.Start` records the World's deployment ID on the
  new run (`Run.DeploymentID`). Runs started from JavaScript through the
  [world-http adapter](/tooling/world-http) get the Go World's deployment ID.
- **Addressed to the run's deployment.** Every delivery for a run is sent to
  its deployment, including the ones queued from outside the run: hook and
  webhook resumes, `WakeUp`, and the hops of long timers. A newer deployment handling a
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

#### Moving chains to the latest deployment

A workflow that never ends, such as a daily job that starts tomorrow's run
before it finishes, would otherwise stay on the deployment its first run
started on until that deployment is deleted. Start its next run with
`workflow.LatestDeployment`, as the Workflow SDK's `deploymentId: 'latest'`
does, and it starts on the World's latest deployment instead of the one that
starts it:

```go
next, err := workflow.Start(ctx, DailyDigest{UserID: input.UserID}, workflow.StartOptions{
	Deployment: workflow.LatestDeployment,
})
```

A run never moves once started, so its event log is still only replayed by
the code that wrote it: each run of the chain picks up new code when the
previous one starts it. This applies whether the run is started from a
workflow, a step or a handler, and through an HTTP client too, since the
server picks the deployment. Use it only for runs nothing waits on the output
of, since a newer deployment may return a different output.
`workflow.DeploymentWithID(id)` names a deployment instead.

The option needs a World with deployment affinity, and `LatestDeployment` one
that implements `worlds.LatestDeployments`, such as
the [Vercel World](/worlds/vercel#latest-deployment). On other Worlds it
changes nothing.
