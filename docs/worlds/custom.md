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
	// SpecVersion returns the RPC protocol version implemented by this World.
	SpecVersion() int
	// DeploymentID returns the deployment whose code this World's deliveries
	// execute, or "" when the World has no deployments.
	DeploymentID() string
}
```

| Part | Responsibility |
| --- | --- |
| [`Storage`](/reference/worlds#storage) | Append-only event logs and the run, step, and hook views derived from them. |
| [`Queue`](/reference/worlds#queue) | Durable deliveries with a not-before time. |
| [`Streamer`](/reference/worlds#streamer) | Named, ordered byte streams with exclusive writer sessions. |
| [`Analytics`](/reference/worlds#analytics) | Optional reporting queries across runs. |

## Key requirements

- **Atomic admission.** `EventsCreate` and `EventsCreateBatch` must check the
  transition rules and append in one atomic operation. A batch is
  all-or-nothing. This is what makes duplicate deliveries safe.
- **Idempotent repeats.** An event that repeats an admitted transition, such as
  a second `run_started`, `step_created`, or `hook_disposed`, returns the
  earlier event with `AllocatedEventCount` zero instead of appending. A
  `step_started` for a running step is not a repeat: it appends a new attempt.
- **One copy of each payload.** The built-in Worlds keep run, step, and hook
  payloads only in their events. Their views store the slot of the event that
  holds each one, and reads decode it with `worlds.EventPayload`. A payload
  then only has to fit in one event, and results are written once.
- **Step results need a running step.** Refuse `step_completed`, `step_failed`,
  and `step_retrying` with `ErrStepNotStartable` unless the step is running,
  and `hook_disposed` with `ErrHookNotActive` unless the hook belongs to the run.
- **Exclusive hook tokens.** `EventsCreate(hook_created)` must give a token to exactly one
  active hook, and with `Force` take it over and append `hook_disposed` with
  `ForceClaimedBy` to the previous owner's run.
- **One webhook response.** Refuse a `hook_received` carrying a response with
  `ErrHookNotActive` once the webhook has completed, including while
  `TokenRetentionUntil` keeps its token, in the same atomic step as the
  append.
- **The in-band fence.** Keep a per-run `InBandCount`. An append that carries
  `InBandCount` (in `EventsCreateOptions`, or on `EventsCreateBatchInput`) is
  accepted only when it equals the run's count, in the same atomic step as the
  append, and advances it by the events appended. A mismatch returns
  `ErrInBandSuperseded` and appends nothing. This is what keeps two flow
  deliveries of one run from both declaring the same operation.
- **At-least-once delivery.** `Queue` must not deliver before `NotBefore`, and
  must redeliver failed deliveries. Deduplicating by `IdempotencyKey` is
  optional. Store every `QueueInput` field and hand it back unchanged,
  including `ReplayDivergenceCount` and `DeploymentMismatchRetryCount`: the
  runtime counts its recovery deliveries in them.
- **Executing deliveries.** Whatever consumes your queue calls
  `worlds.NewRuntime(world, runners).Execute(ctx, input)` for each delivery,
  acknowledges it when `Execute` returns nil, and retries it otherwise.

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
  unique table, prefix, schema, or directory.
- **`ConformanceShared`** covers behavior across instances: concurrent appends,
  hook claims, and stream writers. `newPair` returns two Worlds backed by the
  same storage, as two processes would be.

## Using it from JavaScript

Any World served by `worlds.HTTPHandler` can be used by the Workflow SDK through
the [`world-http` adapter](/tooling/world-http).
