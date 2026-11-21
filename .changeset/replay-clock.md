---
"go-workflow": minor
---

`workflow.Now(ctx)` returns a `time.Time` and records nothing. In workflow code it returns the replay clock: the run's creation time, moved forward to each delivered result's creation time. Elsewhere it returns `time.Now()`. `Sleep`, `Timer`, `WithTimeout`, and hook `MinRetention` use the replay clock. `worlds.EventResult` gains `CreatedAt`.
