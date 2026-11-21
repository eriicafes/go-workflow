---
"go-workflow": minor
---

A cancellation context's hook has the token `abrt_<ID>`, and running steps read its cancellation from the stream `strm_<ID>_system_abort` instead of `cancel:<hook ID>`.
