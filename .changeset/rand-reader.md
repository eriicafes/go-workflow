---
"go-workflow": minor
---

`workflow.RandReader(ctx)` returns a reader of random bytes for workflow code, drawn from the run's seed: each byte is the floor of a drawn value times 256. Its `Read` returns `ErrNoWorkflow` outside workflow context. The analyzer suggests it for `crypto/rand` and `math/rand` calls and allows calls that read from it.
