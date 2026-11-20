---
"go-workflow": patch
---

Default the AWS module's `max_receive_count` to 50, so the runtime fails a run after 48 deliveries before SQS dead-letters it.
