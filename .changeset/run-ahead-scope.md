---
"go-workflow": patch
---

Handle events below a write ahead as the Workflow SDK does: fold, stop, or replay again from the log, and warn about repeated fence refusals.
