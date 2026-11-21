---
"go-workflow": minor
---

A cancellation context's system hook has the token `abrt_<ID>`, with the ID drawn from the run's seed before the hook's ID, and running steps read its cancellation from the stream `strm_<ID>_system_abort` instead of `cancel:<hook ID>`.
