# Event Log

Every run is an append-only log of events. Run, step, and hook views are derived
from it. The event types follow the Workflow SDK, which is why its web UI can
read Go runs.

```go
type Event struct {
	RunID         RunID
	EventID       EventID
	EventType     EventType
	CorrelationID string // step, hook, or wait ID the event belongs to
	Data          []byte
	CreatedAt     time.Time
}
```

Event IDs encode the event's **slot**, its 1-based position in the log.
`worlds.EventSlot(id)` and `worlds.EventIDForSlot(slot)` convert between them.

The runtime and Worlds share these event data types, defined with their
helpers in `worlds/data.go`. Event data has the Workflow SDK's fields, as a
JSON object, and each type keeps its payload in one named field:

| Type | Events | Payload field |
| --- | --- | --- |
| `DataRunCreated` | `run_created` | `input` |
| `DataStepCreated` | `step_created` | `input` |
| `DataStepStarted` | `step_started` | `input`, on the one that creates an inline step |
| `DataStepResult` | `step_completed` | `result` |
| `DataStepResult` | `step_retrying`, `step_failed` | `error` |
| `DataRunCompleted` | `run_completed` | `output` |
| `DataRunFailed` | `run_failed` | `error` |
| `DataHookCreated` | `hook_created`, `hook_conflict` | `metadata` |
| `DataHookReceived` | `hook_received` | `payload` |
| `DataWaitCreated` | `wait_created` | |
| `DataAttrSet` | `attr_set` | |

Metadata fields are the SDK's: every step event records `stepName`, a
`step_created` whether the step is `inline` and the `creatorMessageId` of the
flow that created it, a `step_started` its `attempt`, `step_failed` and
`step_retrying` the `attempt` that failed, a `step_retrying` that hands an
inline step's retry to a step delivery that flow's `creatorMessageId`, and
`step_created`, an inline step's creating `step_started`, `step_completed`, and
`hook_received` the run's `workflowName`. A wait records `resumeAt` and its
`creatorMessageId`,
`attr_set` its `changes` as `key`/`value` pairs and the `writer` that set them,
`run_created` the run's `encryptionPublicKey` when its payloads are encrypted,
and a hook `token`, `isWebhook`, `isSystem` for hooks the runtime creates for
itself, `forceClaimedFrom` for a forced claim, and `tokenRetentionUntil` for a
reserved token. A step's retries and cancellation contexts come from the
replay that declares it. Helpers named `Decode*` read a payload
from event data, such as `worlds.DecodeRunCompleted`, `worlds.DecodeRunFailed`,
and `worlds.DecodeHookReceived`, and helpers named `Encode*` build event data,
such as `worlds.EncodeRunFailed`, `worlds.EncodeHookReceived`, and
`worlds.EncodeAttrSet`.

Payload fields are `[]byte`, base64 in the JSON, holding payloads in the SDK's
format, possibly encrypted (see [Data Format and Encryption](/guide/encryption)).
A run's input is its argument list, a step's input `{args: [input]}`, an error
an SDK `Error`, and a webhook's payload a `worlds.WebhookRequest`, the SDK's
`Request`. `worlds.DecodePayload` and `worlds.DecodeErrorPayload` read them;
Worlds never do.

Reading events with `EventsResolveDataNone` removes each event's payload field
and keeps the rest, such as a step's name.
`worlds.ResolveEventData` applies this rule for a World.

## Event types

Every event carries the `specVersion` of its writer, 9.

### Runs

| Type | Records |
| --- | --- |
| `run_created` | A new run with its workflow name, input, attributes, and the `deploymentId` it is pinned to. Always the first event; its `specVersion`, 9, is the run's. |
| `run_started` | A flow started replaying the runner. |
| `run_completed` | The run's output. |
| `run_failed` | The run's error. |
| `run_cancelled` | The run was cancelled. |
| `attr_set` | Changes to the run's attributes. |

### Steps

| Type | Records |
| --- | --- |
| `step_created` | A step queued as a step delivery, with its name and input, or one whose input did not encode, created `inline` with the SDK's placeholder input and failed at once. |
| `step_started` | A delivery started an attempt. Each start is a new attempt. An inline step's first `step_started` creates it, with its name, input, and `inline` set, and no `step_created` precedes it. |
| `step_completed` | The step's output. |
| `step_retrying` | A failed attempt and when the step is retried. |
| `step_failed` | The step's final error. |

### Hooks

| Type | Records |
| --- | --- |
| `hook_created` | A hook and the token it claims. |
| `hook_received` | A hook's payload, or a webhook's request. |
| `hook_disposed` | The hook released its token, or lost it to a forced claim (`forceClaimedBy` set). |
| `hook_conflict` | The hook could not be created because another run owns its token. |

### Waits

| Type | Records |
| --- | --- |
| `wait_created` | A timer. |
| `wait_completed` | The timer fired or was woken, with the `resumeAt` it was due at. |

### Other

| Type | Records |
| --- | --- |
| `noop` | A position whose writer allocated it and stopped. Only a World writes it, and a replay passes over it. |

## Writers

A run has one orchestrator, its flow, which makes every decision. What a
flow writes is **in-band**: `run_started` on the run's first delivery,
`run_completed`, `run_failed`, `step_created`, every event of a step it runs
inline, `wait_created`, `wait_completed`, `hook_created`, `hook_disposed`, and
`attr_set` from workflow code. Each in-band write is fenced by the run's
in-band count, which a flow reads from the snapshot of its log listing and
which counts `run_created` too: the World refuses it with
`*InBandSupersededError` when another flow has written in-band since. Everything
else is **out-of-band** and never refused by the fence: the events a step
delivery writes, `hook_received` from `ResumeHook` and webhooks, cancellation,
`run_cancelled` from a client, and `RunsSetAttributes`. See
[Concurrent flows](/guide/replay#concurrent-flows).

An in-band write tells the World how many events the flow holds, and the
World answers with the events between those and its own, so the flow never
uses a result of its own before the events in front of it. It also carries
when it occurred, which the World stores as its `CreatedAt`.

## Admission rules

- A finished run refuses a transition or a new step, wait, hook, or attribute
  with `ErrEntityConflict` (also matching a `*RunExpiredError`), another attempt
  or the result of a step that is not running with a `*RunExpiredError`, and a
  `wait_completed` with `ErrEventNotFound`. It still records a running step's
  result or retry.
- A `wait_completed` needs its `wait_created` (`ErrEventNotFound`).
- A step event needs its step (`ErrStepNotFound`), unfinished
  (`ErrEntityConflict`), except an inline step's first `step_started`, which
  creates it. A `step_started` before the step's retry is due is refused with
  a `*worlds.TooEarlyError`, which says how long to wait. Replay ignores any
  event of a step, hook, or wait after its final one.
- A new run's ID must be `wrun_` and a ULID minted within `RunIDMaxAge`
  before and `RunIDMaxLead` after the World's clock (`ErrEventInvalid`, or
  `ErrStartHookRejected` with a start hook), except for a run a first delivery's
  `run_started` creates. Its attributes, and an `attr_set`'s changes, must be
  within the attribute limits (`ErrAttributeInvalid`), with `$` keys only
  when `AllowReservedAttributes` is set.
- A resume's `ResumePayloadDigest` must be its payload's (`ErrEventInvalid`).
- `hook_disposed` only targets a hook of the same run (`*HookNotFoundError`), so
  a repeated disposal is refused.
- `hook_received` only targets an active hook of the same run that still owns
  its token (`*HookNotFoundError`, or `*HookForceClaimedError` after a forced claim).
  A hook, a webhook included, receives repeated payloads, consumed in order with `Next`.
- `EventsCreate(hook_created)` atomically claims the token or records `hook_conflict` with its owning run. A forced claim disposes the previous owner and records `forceClaimedFrom` in its data and `ClaimedFrom` on the new hook.
- An event that repeats a creation the log holds, a second `step_created`,
  `wait_created`, `wait_completed`, `hook_created`, inline step creation, or
  workflow `attr_set` of one correlation ID, is refused with
  `ErrEntityConflict`. A `run_started` of a running run appends nothing, and a
  `run_cancelled` of a cancelled run is recorded and changes nothing.
- A batch is fenced as one block, but not all-or-nothing: each event gets its
  own outcome, and once one is refused, every later event of the batch with
  the same correlation ID is refused too.

Managed cancellation records its cause as hook payload data and uses a stream for live notification. Cancelled attempts record `step_failed` with `FatalError` wrapping `context.Canceled`. The first committed cancellation cause wins.

## A sample run

A run of a workflow that calls one step, which its flow runs inline:

| Slot | Type | Correlation |
| --- | --- | --- |
| 1 | `run_created` | |
| 2 | `run_started` | |
| 3 | `step_started` | step ID |
| 4 | `step_completed` | step ID |
| 5 | `run_completed` | |

The same step queued as a step delivery records `step_created` before its
`step_started`, which the step delivery writes.

Read the log from any World:

```go
page, err := world.EventsList(ctx, worlds.EventsListInput{RunID: runID})
for _, event := range page.Data {
	fmt.Println(event.EventID, event.EventType, event.CorrelationID)
}
```
