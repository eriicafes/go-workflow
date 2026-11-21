---
"go-workflow": minor
---

Refuse a `hook_received` or `hook_disposed` for a missing or disposed hook with `ErrHookNotFound`, as the Workflow SDK does, so a repeated disposal is refused and the runtime skips it.
