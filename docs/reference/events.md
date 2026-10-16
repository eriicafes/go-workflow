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
| `step_cancelled` | The step was cancelled. |

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

- No event is appended to a terminal run (`ErrRunTerminal`).
- A step can only be started while pending or running (`ErrStepNotStartable`).
  Starting a running step appends a new attempt, so a delivery can take over a
  step whose previous delivery died.
- A webhook's `hook_received` only targets an active webhook that still owns
  its token (`ErrHookNotActive`). A plain hook may receive more payloads after
  it settles, but its run keeps the first.
- A `hook_received` that carries a webhook's response is refused with
  `ErrHookNotActive` once that webhook has responded, even while
  `MinRetention` keeps its token: one response per request.
- A `hook_created` whose token another active hook owns is rejected
  (`ErrHookConflict`) unless the claim is forced.
- An event that repeats an admitted transition, such as a second
  `run_started`, returns the earlier event without appending.
- A flow's writes are in-band: each is fenced by the run's `InBandCount` and
  refused with `ErrInBandSuperseded` when another flow has written since. See
  [Concurrent flows](/guide/replay#concurrent-flows).

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
