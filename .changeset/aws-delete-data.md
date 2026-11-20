---
"go-workflow": minor
---

The AWS World deletes a run's data instead of the run when `Config.Retention` passes, and deletes it as the run finishes with `$retention: "0"`, as the Workflow SDK does. The API role needs `dynamodb:BatchWriteItem`.
