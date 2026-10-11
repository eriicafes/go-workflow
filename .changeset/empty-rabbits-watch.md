---
"@eriicafes/go-workflow": patch
---

Return `workflowops.ErrCancelled` when a remote world rejects a cancelled queue task, so queue consumers acknowledge terminal deliveries without retrying.
