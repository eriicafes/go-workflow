---
"go-workflow": patch
---

Record errors made with `errors.New` and `fmt.Errorf` as a plain `Error`, and the runtime's own failures under the Workflow SDK's class names.
