---
"go-workflow": minor
---

Let stream writers write at once: `workflow.Writer` returns a buffered `workflow.StreamWriter`, closed when its step returns.
