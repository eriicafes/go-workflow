---
"go-workflow": minor
---

Step, wait, and hook IDs and default hook tokens are drawn from a generator seeded with `<run ID>:<workflow name>:<deployment ID>`, and replay matches declarations by ID. IDs are `step_<ULID>`, `wait_<ULID>`, and `hook_<ULID>`, and tokens are 21 Nano ID characters. Run IDs must be `wrun_<ULID>`. `Execution.NextStepID`, `NextWaitID`, `NextHookID`, and `NextHookToken` replace `worlds.NewStepID`, `NewHookID`, `NewWaitID`, `OperationPosition`, `NewRunHookToken`, and `Execution.NextPosition`.
