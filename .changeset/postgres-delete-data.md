---
"go-workflow": minor
---

The Postgres World deletes a run's data instead of the run when `Config.Retention` passes, and deletes it as the run finishes with `$retention: "0"`, as the Workflow SDK does.
