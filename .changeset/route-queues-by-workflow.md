---
"go-workflow": minor
---

Route workflows to further queues. `worlds.Workflow`, `worlds.WorkflowName`, `worlds.Route` and `worlds.Routes` name workflows and build a World's `Config.Routes`. The AWS World routes to further standard or FIFO SQS queues (`Config.Queues`, provisioned by the Terraform module's `queues`), the Vercel World to further topics (`Config.Topics`), and the Postgres World to named queues that `Worker` claims with `WorkerOptions.Queues`. Deliveries carry `QueueInput.WorkflowName`, Worlds that route report `Capabilities.QueueRouting`, and hooks carry their run's deployment and workflow name in `Hook.ResumeContext`, so resuming a hook reads no run.
