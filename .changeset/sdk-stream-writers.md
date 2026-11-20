---
"go-workflow": minor
---

Let stream writers write at once, as in the Workflow SDK: `workflow.Writer` returns a buffered `workflow.StreamWriter`, closed when its step returns.
