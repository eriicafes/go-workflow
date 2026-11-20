---
"go-workflow": minor
---

Add `RunsDeleteDataMany`, `Run.ExpiredAt`, and `workflow.ErrRunExpired`, and delete a run's data when it finishes with `$retention: "0"`, as the Workflow SDK does.
