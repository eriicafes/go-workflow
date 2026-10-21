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

The runtime and Worlds share these JSON payload types, defined with their
helpers in `worlds/data.go`. As in the
Workflow SDK, event data is a JSON object and each type keeps its payload in
one named field:

| Type | Events | Payload field |
| --- | --- | --- |
| `DataRunCreated` | `run_created` | `input` |
| `DataStepCreated` | `step_created` | `input` |
| `DataStepResult` | `step_completed` | `result` |
| `DataStepResult` | `step_retrying`, `step_failed` | `error` |
| `DataRunCompleted` | `run_completed` | `output` |
| `DataRunFailed` | `run_failed` | `error` |
| `DataHookCreated` | `hook_created`, `hook_conflict` | `metadata` |
| `DataHookReceived` | `hook_received` | `payload` |
| `DataWaitCreated` | `wait_created` | |
| `DataAttrSet` | `attr_set` | |

Metadata fields also follow
the SDK: a wait records `resumeAt`, `attr_set` records its `changes` as
`key`/`value` pairs and the `writer` that set them, and a hook records
`isWebhook`, `isSystem` for hooks the runtime creates for itself, and
`forceClaimedFrom` for a forced claim and `tokenRetentionUntil` for a
reserved token. Go keeps a few extra metadata fields,
such as a declaration's `position` and a hook's `webhook` settings. A webhook's `hook_received`
payload is a `WebhookReceivedEventData`. Helpers named `Decode*` read a
payload from event data, such as `worlds.DecodeRunCompleted`,
`worlds.DecodeRunFailed`, `worlds.DecodeHookReceived`, and
`worlds.DecodeWebhookReceived`, and helpers named `Encode*` build event data,
such as `worlds.EncodeRunFailed`, `worlds.EncodeHookReceived`, and
`worlds.EncodeAttrSet`.

Payload fields are `json.RawMessage`. An error is embedded as a JSON object
containing its message and recorded chain. `worlds.DecodeError` decodes the
bytes in an `error` field or the materialized `Run.Error` and `Step.Error`
fields.

Reading events with `EventsResolveDataNone` removes each event's payload field
and keeps the rest, such as a step's name and position.
`worlds.ResolveEventData` applies this rule for a World.

## Event types

### Runs

| Type | Records |
| --- | --- |
| `run_created` | A new run with its workflow name, input, attributes, and the `deploymentId` it is pinned to. Always the first event. |
| `run_started` | A flow started replaying the runner. |
| `run_completed` | The run's output. |
| `run_failed` | The run's error. |
| `run_cancelled` | The run was cancelled. |
| `attr_set` | Changes to the run's attributes. |

### Steps

| Type | Records |
| --- | --- |
| `step_created` | A step with its name and input. |
| `step_started` | A delivery started an attempt. Each start is a new attempt. |
| `step_completed` | The step's output. |
| `step_retrying` | A failed attempt and when the step is retried. |
| `step_failed` | The step's final error. |

### Hooks

| Type | Records |
| --- | --- |
| `hook_created` | A hook and the token it claims. |
| `hook_received` | A hook's payload, or a webhook's request or response. |
| `hook_disposed` | The hook released its token, or lost it to a forced claim (`forceClaimedBy` set). |
| `hook_conflict` | The hook could not be created because another run owns its token. |

### Waits

| Type | Records |
| --- | --- |
| `wait_created` | A timer. |
| `wait_completed` | The timer fired or was woken. |

## Admission rules

Appending is how the runtime decides what may happen, so Worlds enforce these
rules atomically:

- Terminal runs reject new work (`ErrRunTerminal`). A running step may still record its final completion or failure without reviving the run or scheduling a retry.
- A step can only be started while pending or running (`ErrStepNotStartable`).
  Starting a running step appends a new attempt, so a delivery can take over a
  step whose previous delivery died.
- A webhook's `hook_received` only targets an active webhook that still owns
  its token (`ErrHookNotActive`). A plain hook receives repeated payloads, consumed in order with `Next`.
- A `hook_received` that carries a webhook's response is refused with
  `ErrHookNotActive` once that webhook has responded, because each webhook request has one response.
- `EventsCreate(hook_created)` atomically claims the token or records `hook_conflict` with its owning run. A forced claim disposes the previous owner and records `forceClaimedFrom` in its data and `ClaimedFrom` on the new hook.
- An event that repeats an admitted transition, such as a second
  `run_started`, returns the earlier event without appending.
- A flow's writes are in-band: each is fenced by the run's `InBandCount` and
  refused with `ErrInBandSuperseded` when another flow has written since. See
  [Concurrent flows](/guide/replay#concurrent-flows).

Managed cancellation records its cause as hook payload data and uses a stream for live notification. Cancelled attempts record `step_failed` with `FatalError` wrapping `context.Canceled`. The first committed cancellation cause wins.

## A sample run

A run of `GenerateReport` that calls one step:

| Slot | Type | Correlation |
| --- | --- | --- |
| 1 | `run_created` | |
| 2 | `run_started` | |
| 3 | `step_created` | step ID |
| 4 | `step_started` | step ID |
| 5 | `step_completed` | step ID |
| 6 | `run_completed` | |

Read the log from any World:

```go
page, err := world.EventsList(ctx, worlds.EventsListInput{RunID: runID})
for _, event := range page.Data {
	fmt.Println(event.EventID, event.EventType, event.CorrelationID)
}
```
