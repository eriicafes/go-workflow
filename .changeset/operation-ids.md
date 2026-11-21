---
"go-workflow": minor
---

Step, wait, and hook IDs and default hook tokens are drawn from a generator seeded with `<run ID>:<workflow name>:<deployment ID>`: `step_`, `wait_`, or `hook_` and a monotonic ULID at the run's creation time, and 21-character Nano ID tokens. Replay matches declarations by ID. `worlds.NewStepID`, `NewHookID`, `NewWaitID`, `OperationPosition`, `NewRunHookToken`, and `Execution.NextPosition` are replaced by `Execution.NextStepID`, `NextWaitID`, `NextHookID`, and `NextHookToken`.
