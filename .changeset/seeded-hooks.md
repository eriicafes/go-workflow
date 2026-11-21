---
"go-workflow": minor
---

Webhook tokens are drawn from the run's seed after the hook's ID, like other generated hook tokens, and replay checks a webhook's token. A hook never draws the ID of the run's start hook, `hook_` and the run ID's ULID, and draws again instead.
