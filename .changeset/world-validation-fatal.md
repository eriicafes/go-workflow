---
"go-workflow": patch
---

Return a `FatalError` from `SetAttributes` and from a hook the World refuses as invalid, instead of redelivering the flow.
