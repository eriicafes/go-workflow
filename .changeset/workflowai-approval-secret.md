---
"go-workflow": minor
---

Change `workflowai.WorkflowAgentOptions.ToolApprovalSecret` to a `*workflowai.ToolApprovalSecret` naming the environment variable that holds the secret, and add `CallOptions.ToolApprovalSecret`.
