---
"go-workflow": minor
---

Deduplicate hook resumes by a resume ID and payload digest, keep each start hook candidate's decision for good, refuse candidates older than a day with `ErrStartHookRejected`, and echo a run's start hook on `Run.StartHook`, as the Workflow SDK's Worlds do.
