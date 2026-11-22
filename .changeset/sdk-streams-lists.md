---
"go-workflow": minor
---

Drop writes to closed and deleted streams, give lists the Workflow SDK's default order and page size, and refuse events on runs of a newer spec version with a `*RunNotSupportedError`, as the SDK's Worlds do.
