---
"go-workflow": minor
---

`workflow.Now(ctx)` returns the replay clock in workflow code, and `Sleep`, `Timer`, `WithTimeout`, and hook `MinRetention` use it.
