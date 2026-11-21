---
"go-workflow": minor
---

Validate new run IDs, run and attr_set attributes, resume digests, and step events in every World as the Workflow SDK's Worlds do, with the new `TooEarlyError` for a step started before its retry is due.
