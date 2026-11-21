---
"go-workflow": minor
---

Add `worlds.DeliveryRetryDelay`, so a delivery that fails with a `*worlds.ThrottleError` waits at least as long as the rate limit asked before the Vercel, AWS, Postgres, and local Worlds retry it.
