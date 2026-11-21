# Custom Worlds

You can back go-workflow with any storage and queue by implementing
`worlds.World`. The runtime, the client, and the HTTP handler all work against
this interface.

## The World interface

```go
type World interface {
	Storage
	Queue
	Streamer
	// BaseURL returns the absolute public URL where HTTPHandler is mounted,
	// including its workflow path prefix and without a trailing slash.
	BaseURL() string
	// Analytics returns the Analytics interface when analytics are enabled,
	// otherwise nil.
	Analytics() Analytics
	// Capabilities reports the optional behavior this World implements.
	Capabilities() Capabilities
	// DeploymentID returns the deployment whose code this World's deliveries
	// execute, or "" when the World has no deployments.
	DeploymentID() string
}
```

| Part | Responsibility |
| --- | --- |
| [`Storage`](/reference/worlds#storage) | Append-only event logs and the run, step, and hook views derived from them. |
| [`Queue`](/reference/worlds#queue) | Durable deliveries with a not-before time. |
| [`Streamer`](/reference/worlds#streamer) | Named, ordered byte streams that any number of writers append to. |
| [`Analytics`](/reference/worlds#analytics) | Optional reporting queries across runs. |

## Key requirements

- **Atomic admission.** `EventsCreate` must check the transition rules and
  append in one atomic operation. A finished run refuses `step_created`,
  `step_started`, `step_retrying`, `wait_created`, and other new work with
  `ErrRunTerminal`, and still records a step's `step_completed` or
  `step_failed` and a `wait_completed`. Other checks of a step or wait are
  optional: the runtime decides from the log alone, so a World that appends
  every such event is correct. A World that checks steps accepts a step's
  result while the step is not finished, and a `step_started` that creates
  the step it starts (`worlds.StartCreatesStep`): an inline step's first
  attempt, which no `step_created` precedes.
- **Batches.** `EventsCreateBatch` refuses the whole batch only for an
  invalid event, a missing run, or a stale fence. Otherwise each event gets
  its own outcome in `Results`, and once one is refused, every later event
  of the batch with the same correlation ID is refused too.
- **Idempotent repeats.** An event that repeats an admitted transition, such as
  a second `run_started`, `step_created`, or `hook_disposed`, returns the
  earlier event with `AllocatedEventCount` zero instead of appending. A
  `step_started` for a started step is not a repeat: it appends a new attempt.
- **One copy of each payload.** The built-in Worlds keep run, step, and hook
  payloads only in their events. Their views store the slot of the event that
  holds each one, and reads decode it with `worlds.EventPayload`. A payload
  then only has to fit in one event, and results are written once.
- **Exclusive hook tokens.** `EventsCreate(hook_created)` must give a token to exactly one
  active hook, and with `Force` take it over and append `hook_disposed` with
  `ForceClaimedBy` to the previous owner's run. `hook_disposed` needs a hook
  of the same run (`ErrHookNotActive`).
- **Hooks end.** A disposed hook is deleted, as the Workflow SDK's Worlds
  delete it, and so is a finished run's hook once its token retention ends:
  `HooksGet` returns `ErrHookNotFound` and `HooksList` leaves it out. Keep
  the disposal itself, so a repeated `hook_disposed` returns the first and a
  `hook_received` for a force-claimed hook returns `ErrHookForceClaimed`.
- **Start hooks.** With `Capabilities().StartHook`, `EventsCreateRun`
  creates `EventsCreateRunInput.StartHook` with the run, as hook
  `worlds.StartHookID(runID)`, or creates nothing and returns a
  `*HookConflictError` naming the run that owns the token. Record the
  decision for the run ID and keep it: a repeat of an accepted start returns
  the run's `run_created`, and a refused candidate stays refused, with the
  same conflicting run, even after the token is freed. Refuse a candidate
  whose run ID is older than `worlds.StartHookCandidateMaxAge` with
  `ErrStartHookRejected`, refuse a `hook_created` under the start hook's ID
  with `ErrEventInvalid`, and echo the hook on `Run.StartHook` without its
  metadata.
- **Resume deduplication.** With `Capabilities().HookResumeDedup`, a
  `hook_received` carrying `EventsCreateOptions.ResumeID` is appended once
  per run and resume ID. A repeat with the same `ResumePayloadDigest`
  returns the first event with `AllocatedEventCount` zero, and one with a
  different digest is refused with `ErrEventInvalid`. Store the resume ID on
  the event (`Event.ResumeID`). A World can attest it per hook instead, with
  `Hook.ResumeDedup`.
- **One webhook response.** Refuse a `hook_received` carrying a response with
  `ErrHookNotActive` once the webhook has completed, including while
  `TokenRetentionUntil` keeps its token, in the same atomic step as the
  append.
- **The in-band fence.** Keep a per-run `InBandCount` of the positions
  in-band writes take, starting at 1 for `run_created`. An append that
  carries `InBandCount` (in `EventsCreateOptions`, or on
  `EventsCreateBatchInput` for the whole block) is accepted only when it
  equals the run's count, in the same atomic step as the append, and advances
  it by the positions it allocates. A mismatch returns `ErrInBandSuperseded`
  and appends nothing. Out-of-band writes never move it.
- **Snapshots and skipped slots.** Every `EventsList` page carries the run's
  counters in `Snapshot`, read before the page. An in-band write with
  `EventCount` returns in `Events` every event between that count and its own
  position (`MoreEvents` when the list is incomplete), and is stored with its
  `OccurredAt` as `CreatedAt`.
- **Specification version.** Record `worlds.SpecVersion` on every run you
  create (`Run.SpecVersion`).
- **Resilient start.** A run's first flow carries its creation
  (`QueueInput.RunInput`), and so does its `run_started`, since a start queues
  the flow and writes `run_created` at the same time. Call
  `worlds.CreateRunForStart` for every `run_started`, which creates a run
  whose `run_created` never landed, and store the event without its data.
- **Deliveries.** `Queue` must not deliver before `NotBefore`, and must
  redeliver failed deliveries. Store every `QueueInput` field and hand it back
  unchanged, including `ReplayDivergenceCount` and
  `DeploymentMismatchRetryCount`: the runtime counts its recovery deliveries
  in them. Deduplicate by `IdempotencyKey` while the first message with the
  key lives, for at least `worlds.MinDeduplicationWindow`. Set `MessageID`,
  `DeliveryCount`, and `Attempt` on each delivery. Report the furthest
  `NotBefore` your queue accepts in `Capabilities().MaxQueueDelay`, and the
  runtime reaches later timers in hops.
- **Executing deliveries.** Whatever consumes your queue calls
  `core.NewRuntime(world, runners).Execute(ctx, input)` for each delivery and
  acknowledges it when `Execute` returns nil. A `*worlds.Redelivery` asks for
  the same message again after its `After`, with the next `DeliveryCount`,
  counting an `Attempt` unless `After` is zero. Any other error retries it.
- **One flow at a time.** Deliver one flow of a run at a time
  (`QueueInput.SerializedFlow`). The fence keeps overlapping flows safe,
  and this makes them rare.

Pushing a run's events is optional. A World that implements
`worlds.EventsSubscriber` hands a flow waiting on an inline step each event
as it commits. The flow still polls the log's tail, as a safety net every 10
seconds or more while the push is up, and every 2 seconds or more without
one, and takes an event only when it is the next one it expects.
`worlds.FeedEvents` builds the push from a change notification. On such a
World a flow delivery that suspends with nothing left to run lingers while
the run waits on a background step or an open hook (`core.LingerDuration`,
5 seconds), or until a wait due within 5 seconds, and runs another pass when
another writer appends an event or the wait falls due.

Supporting [retention](/worlds/#retention) is optional. A World that supports
it takes `Retention` and `Retentions` in its configuration, resolves each run's
retention with `worlds.RetentionFor` from the run's workflow name, and deletes
everything recorded for a run, including its index entries and its hooks'
tokens, only once the run is terminal, the retention has passed since it
completed, and no hook of the run still retains its token.

See the [Event Log](/reference/events) reference for event types and the
[`worlds` reference](/reference/worlds) for every method's contract.

## Serving it

```go
handler := worlds.HTTPHandler(world, "") // serves /v1/workflow/rpc and /v1/workflow/webhooks/{token}
rpc := worlds.RPCHandler(world)
webhooks := worlds.WebhookHandler(world)
```

## Conformance

The `workflowtest` package runs the World contract against your
implementation. It is the same suite every built-in World passes.

```go
package myworld_test

import (
	"testing"

	"github.com/eriicafes/go-workflow/workflowtest"
	"github.com/eriicafes/go-workflow/worlds"
)

func TestConformance(t *testing.T) {
	workflowtest.Conformance(t, func(t *testing.T) worlds.World {
		return newIsolatedWorld(t) // fresh state per call
	})
}

func TestConformanceShared(t *testing.T) {
	workflowtest.ConformanceShared(t, func(t *testing.T) (worlds.World, worlds.World) {
		a, b := newWorldPairOnSameBackend(t)
		return a, b
	})
}
```

- **`Conformance`** gives each subtest a fresh World from `newWorld`. Worlds
  backed by shared storage must isolate state per call, for example with a
  unique table, prefix, schema, or directory. Its `RunAhead` cases run a flow
  against your World with their own runtime and append another writer's
  events while its writes are in flight, so they check the in-band fence,
  skipped-slot reports, and stored write times end to end.
- **`ConformanceShared`** covers behavior across instances: concurrent appends,
  hook claims, and stream writers. `newPair` returns two Worlds backed by the
  same storage, as two processes would be.

## Using it from JavaScript

Any World served by `worlds.HTTPHandler` can be used by the Workflow SDK through
the [`world-http` adapter](/tooling/world-http).
