# Backend contracts

## Events and materialized views

Admission checks, event append, and updates to derived run, step, and hook
views form one atomic operation. `EventsCreateBatch` admits the whole batch or
none of it.

Repeated admitted lifecycle transitions return the existing event with
`AllocatedEventCount == 0`. A `step_started` event for an already running step
is a new attempt, not an idempotent repeat: another delivery may be taking over
after a worker died. A repeated `step_created` or `hook_disposed` is a repeat.
Step results need a running step (`ErrStepNotStartable`), and `hook_disposed`
needs a hook of the same run (`ErrHookNotActive`). Apply the admission rules
for each event type.

Preserve event order, correlation IDs, timestamps, pagination, ResolveData,
and catch-up results. `EventsCreateOptions.EventCount` takes precedence over
SinceCursor. Its returned catch-up events exclude the primary returned event;
see the interface for force-claim disposal inclusion.

Keep run, step, and hook payloads only in their events: views store the
event slots (run result, step created and latest result, hook created; run
input is slot 1) and reads decode them with `worlds.EventPayload`, skipping
input, output, and metadata under `ResolveDataNone` but keeping errors.

Use SDK payload types such as `DataStepCreated`, `DataStepResult`,
`DataRunCreated`, `DataRunCompleted`, `DataRunFailed`, `DataHookCreated`,
`DataHookReceived`, `DataAttrSet`, and `DataWaitCreated` from
`worlds/data.go`. Materialize run output and error with
`worlds.DecodeRunCompleted` and `worlds.DecodeRunFailed`, decode webhook
receipts with `worlds.DecodeWebhookReceived`, and build attr_set data with
`worlds.EncodeAttrSet`. Error fields hold encoded error JSON.
Preserve the complete encoded error through storage. Apply
`worlds.ResolveEventData` for `EventsResolveDataNone`, which drops payload
fields and keeps metadata.

## In-band fencing

Each run has an InBandCount. A supplied expected count fences an orchestrator
append or batch. Compare and append atomically, advance by the number of events
actually appended, and return `ErrInBandSuperseded` without writing on mismatch.
An out-of-band write has no expected count and leaves the in-band count
unchanged. It still must pass lifecycle admission checks.

## Hook ownership and webhooks

Hook claims happen through `hook_created` event admission.
The token index and hook registration must agree atomically;
exactly one active hook owns a token. The runtime handles conflict recording
from the admission result.

A forced claim transfers ownership and appends `hook_disposed` with
ForceClaimedBy to the old owner's run, preserving ClaimedFrom metadata on the
new hook. If the backend cannot update both runs in one transaction, use a
recoverable protocol that preserves the same observable contract. An
uncoordinated read-then-write token lookup is insufficient.

Disposal deactivates registration and releases its token immediately.
A hook's TokenRetentionUntil can preserve token ownership after run
termination; it does not
keep a completed webhook available for another request or response.

Static/default webhook receipt and endpoint deactivation must be admitted
together. Manual webhooks remain active for their response after the request
is received; admit only one completed response. Preserve request/response
event identity and enforce duplicate-response rules atomically.

## Cancellation and terminal runs

Managed cancellation uses durable hooks for replay state and closed streams
for immediate notification to running steps. Both mechanisms need to work
across World instances.

Cancelled steps are recorded as `step_failed`. A cancelled run rejects new
work, while already admitted running steps may still record completion or
failure.

## Queue delivery

Deliver at least once, respect NotBefore, and redeliver failures. Queue
IdempotencyKey deduplication is optional; runtime replay still needs to be safe
without it. QueueBatch can report items published before an error, unlike an
atomic event batch.

Persist every QueueInput field, including StepID, QueueName, DeploymentID,
DeploymentMismatchRetryCount, and ReplayDivergenceCount. Runtime uses these
fields to select work, route pinned runs, and bound recovery. Do not reset them
while wrapping or serializing a message. A World that chooses a queue by
WorkflowName persists it too and reports Capabilities.QueueRouting, so
deliveries queued for a run nobody loaded still carry it. Every World copies
the run's deployment and workflow name onto the hooks it creates
(Hook.ResumeContext), which ResumeHook uses instead of reading the run.

## Streams

Maintain ordered chunks and cursor continuation. A write session owns its
stream exclusively. Reject stale session IDs and out-of-sequence writes with
`ErrStreamLocked`. Release relinquishes ownership without closing the stream;
Close marks it done and relinquishes ownership.

For a reader draining data, stop only after processing the returned chunks
and observing `Done && !HasMore`: a closed stream may still have unread
chunks. Keep long-poll cancellation and RetryAfter behavior consistent with
the Streamer contract.
