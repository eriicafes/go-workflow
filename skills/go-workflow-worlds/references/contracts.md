# Backend contracts

## Events and materialized views

Admission checks, event append, and updates to derived run, step, and hook
views form one atomic operation. `EventsCreateBatch` admits the whole batch or
none of it. Do not persist partially updated views or a partially admitted batch.

Repeated admitted lifecycle transitions return the existing event with
`AllocatedEventCount == 0`. A `step_started` event for an already running step
is a new attempt, not an idempotent repeat: another delivery may be taking over
after a worker died. Read the transition rules and tests for the specific
event rather than broadly deduplicating by type and correlation ID.

Preserve event order, correlation IDs, timestamps, pagination, ResolveData,
and catch-up results. `EventsCreateOptions.EventCount` takes precedence over
SinceCursor. Its returned catch-up events exclude the primary returned event;
see the interface for force-claim disposal inclusion.

Use SDK payload types such as `DataStepCreated`, `DataStepResult`,
`DataHookCreated`, and `DataWaitCreated`. `DataStepResult.Error` holds encoded
error JSON. Preserve the recorded representation through storage; do not
replace it with only a message or recreate private retry/fatal wrappers as a
new wire format.

## In-band fencing

Each run has an InBandCount. A supplied expected count fences an orchestrator
append or batch. Compare and append atomically, advance by the number of events
actually appended, and return `ErrInBandSuperseded` without writing on mismatch.
An out-of-band write has no expected count and leaves the in-band count
unchanged. It still must pass lifecycle admission checks.

## Hook ownership and webhooks

Hook claims happen through `hook_created` event admission, not a separate
HooksClaim method. The token index and hook registration must agree atomically;
exactly one active hook owns a token. The runtime handles conflict recording
from the admission result. Read the selected implementation's admission path
and conformance tests before changing this boundary.

A forced claim transfers ownership and appends `hook_disposed` with
ForceClaimedBy to the old owner's run, preserving ClaimedFrom metadata on the
new hook. If the backend cannot update both runs in one transaction, use a
recoverable protocol that preserves the same observable contract. An
uncoordinated read-then-write token lookup is insufficient.

Disposal deactivates registration and releases its token immediately.
MinRetention can preserve token ownership after run termination; it does not
keep a completed webhook available for another request or response.

Static/default webhook receipt and endpoint deactivation must be admitted
together. Manual webhooks remain active for their response after the request
is received; admit only one completed response. Preserve request/response
event identity and enforce duplicate-response rules atomically. Do not add a
separate handler-side disposal that can race with run completion.

## Cancellation and terminal runs

Managed cancellation uses durable hooks for replay state and closed streams
for immediate notification to running steps. Both mechanisms need to work
across World instances. The watcher runs around the step attempt and cancels
its step context; it does not depend on the workflow body continuing to run.

Cancelled steps use `step_failed`, not a `step_cancelled` event. A cancelled
run rejects new work, while already admitted running steps may still record
completion or failure. Do not turn terminal-run admission into a blanket
rejection that discards those step outcomes.

## Queue delivery

Deliver at least once, respect NotBefore, and redeliver failures. Queue
IdempotencyKey deduplication is optional; runtime replay still needs to be safe
without it. QueueBatch can report items published before an error, unlike an
atomic event batch.

Persist every QueueInput field, including StepID, QueueName, DeploymentID,
DeploymentMismatchRetryCount, and ReplayDivergenceCount. Runtime uses these
fields to select work, route pinned runs, and bound recovery. Do not reset
them while wrapping or serializing a message.

## Streams

Maintain ordered chunks and cursor continuation. A write session owns its
stream exclusively. Reject stale session IDs and out-of-sequence writes with
`ErrStreamLocked`. Release relinquishes ownership without closing the stream;
Close marks it done and relinquishes ownership.

For a reader draining data, stop only after processing the returned chunks
and observing `Done && !HasMore`: a closed stream may still have unread
chunks. A closure-only cancellation watcher can act on Done immediately
because it needs no payload data. Keep long-poll cancellation and RetryAfter
behavior consistent with the Streamer contract.
