---
"go-workflow": minor
---

Add `workflow.RandReader(ctx)`, a reader of random bytes drawn from the run's seed for workflow code. The analyzer suggests it for `crypto/rand` and `math/rand` calls.
