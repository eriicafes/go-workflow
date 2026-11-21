---
"go-workflow": minor
---

Record every run as a spec 9 run. The runtime moves to `worlds/core`, and a run's flow is its only in-band writer: each step's mode is fixed at creation, an inline step is created by its first `step_started`, inline retries are handed to step deliveries, a fence-refused flow asks for its message again with `worlds.Redelivery`, and timers are reached in hops within each World's `MaxQueueDelay`. Worlds list a counter snapshot on every events page, report skipped slots, store `OccurredAt`, give batches per-event outcomes, count deliveries, deduplicate by idempotency key, and deliver one flow of a run at a time. The Vercel World writes through the backend's `/v5` routes.
