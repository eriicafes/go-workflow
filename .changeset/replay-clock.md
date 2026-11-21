---
"go-workflow": minor
---

`workflow.Now(ctx)` returns `time.Time` and records nothing. In workflow context it returns the replay clock: the run's creation time, read from its ID, moved forward to the creation time of each step result, timer, hook registration, cancellation, and consumed hook payload delivered to the runner, in whole milliseconds. Elsewhere it returns `time.Now()`. `Sleep`, `Timer`, `WithTimeout`, and hook `MinRetention` measure from the replay clock. `worlds.EventResult` gains `CreatedAt`, the appended event's stored creation time, and `Execution` gains `Now` and `AdvanceClock`.
