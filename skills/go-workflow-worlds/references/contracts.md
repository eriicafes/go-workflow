# Backend contracts

## Events and materialized views

Admission checks, event append, and updates to derived run, step, and hook
views form one atomic operation. `EventsCreateBatch` appends none of its events
when one is invalid, the run is missing, or the fence refuses the block.
Otherwise each event gets its own `EventBatchItem` outcome: refused events
leave the others appended, and once an event is refused, every later event of
the batch with the same correlation ID is refused too.

Repeated admitted lifecycle transitions return the existing event with
`AllocatedEventCount == 0`. A `step_started` for a started step is a new
attempt, not a repeat. A repeated `step_created` or `hook_disposed`, or an
inline step's creating `step_started` (`worlds.StartCreatesStep`), is a repeat.
A finished run refuses `step_created`, `step_started`, `step_retrying`, and
`wait_created` (`ErrRunTerminal`) and accepts `step_completed`, `step_failed`,
and `wait_completed`. Other step and wait checks are optional, and a World
that checks accepts a step's result while the step is not finished and a
`step_started` that creates its step. `hook_disposed` needs a hook of the same
run (`ErrHookNotActive`).

Preserve event order, correlation IDs, timestamps, pagination, ResolveData,
and catch-up results. `EventsCreateOptions.EventCount` takes precedence over
SinceCursor. Its returned catch-up events exclude the primary returned event;
see the interface for force-claim disposal inclusion.

Keep run, step, and hook payloads only in their events: views store the
event slots (run result, step created and latest result, hook created; run
input is slot 1) and reads decode them with `worlds.EventPayload`, skipping
input, output, and metadata under `ResolveDataNone` but keeping errors.

A backend that cannot store an event's whole Data rebuilds what it dropped:
replay matches declarations by ID and reads a wait's cancellation contexts
from the replay, so a wait needs only its resume time.

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

Each run has an InBandCount, 1 once `run_created` is written. A supplied
expected count fences an orchestrator append or batch. Compare and append
atomically, advance by the positions actually allocated, and return
`ErrInBandSuperseded` without writing on mismatch. An out-of-band write has no
expected count and leaves the in-band count unchanged. It still must pass
lifecycle admission checks. Every `EventsList` page carries the counters in
`Snapshot`, read before the page. An in-band write with `EventCount` reports
the events between that count and its own position (`MoreEvents` when
incomplete) and is stored with its `OccurredAt` as `CreatedAt`. Every run
records `worlds.SpecVersion`.

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

Disposal deletes the hook and releases its token immediately, but the World
remembers the disposal: a repeat returns the first, and a delivery to a
force-claimed hook gets ErrHookForceClaimed. A finished run's hooks are
deleted once their token retention ends.
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

Deliver at least once, respect NotBefore, and redeliver failures. Deduplicate
by IdempotencyKey while the first message with the key lives, for at least
`worlds.MinDeduplicationWindow`. QueueBatch can report items published before
an error. Queued deliveries are never cancelled. Set `QueueInput.MessageID`
(stable across redeliveries), `DeliveryCount` (every redelivery), and
`Attempt` (every redelivery but a handoff); the Runtime fails a run or step
whose delivery passes `core.MaxDeliveries` attempts. A `*worlds.Redelivery`
from `Runtime.Execute` asks for the same message again after `After`, a
handoff when zero. Deliver one serialized flow of a run at a time
(`QueueInput.SerializedFlow`); report the furthest NotBefore the queue accepts as
`MaxQueueDelay`, and the runtime reaches later timers in hops.

Persist every QueueInput field, including StepID, QueueName, DeploymentID,
DeploymentMismatchRetryCount, and ReplayDivergenceCount. Runtime uses these
fields to select work, route pinned runs, and bound recovery. Do not reset them
while wrapping or serializing a message. A World that chooses a queue by
WorkflowName persists it too. Every World copies
the run's deployment and workflow name onto the hooks it creates
(Hook.ResumeContext), which ResumeHook uses instead of reading the run.

## Streams

Maintain ordered chunks and cursor continuation. Streams have no exclusive
writers: concurrent writes interleave, each writer's chunks staying in order.
Writes to a closed stream fail with `ErrStreamClosed`, and closing a closed
stream does nothing. A `StreamWriter` from `StreamsOpenWriter` keeps its
writes in order; Release ends the writer without closing the stream, and
CloseStream closes it for every writer.

For a reader draining data, stop only after processing the returned chunks
and observing `Done && !HasMore`: a closed stream may still have unread
chunks. Keep long-poll cancellation and RetryAfter behavior consistent with
the Streamer contract.

## Retention

Retention is optional. A World that supports it takes `Retention` and
`Retentions` in its Config, validates them with `worlds.MaxRetention`, and
resolves a run's retention with `worlds.RetentionFor` from its workflow name.
Zero keeps runs forever, globally or for one workflow.

Delete a run only once it is terminal and its retention has passed since
`CompletedAt`. Never delete a pending or running run, however old. A hook whose
`TokenRetentionUntil` has not passed keeps its run until then. Delete
everything recorded for the run together: its event log, views, hooks and the
tokens those hooks still own, streams, queued deliveries, and index entries,
so a read finds the whole run or none of it. Make the deletion safe to repeat
and to interrupt, since it runs at least once, and keep late writes to a
finished run from extending or escaping its expiry.
