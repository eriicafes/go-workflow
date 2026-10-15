# AWS world Terraform module

Provisions the infrastructure `awsworld` needs:

- the DynamoDB table with its `list` and `list_group` indexes (on-demand, encrypted,
  point-in-time recovery and deletion protection on by default);
- the task queue and its dead-letter queue (SQS-managed encryption);
- optionally, the SQS event source mapping for the queue worker Lambda, with
  `ReportBatchItemFailures`;
- the EventBridge Scheduler group and the role Scheduler assumes to send to
  the queue, for deliveries due more than 15 minutes ahead;
- least-privilege managed policies for the queue worker and the app-facing
  API, optionally attached to their roles.

It does not create the Lambda functions themselves.

## Usage

```hcl
module "workflow" {
  source = "github.com/eriicafes/go-workflow//worlds/aws/terraform"

  name                 = "workflow"
  worker_function_name = aws_lambda_function.worker.function_name
  worker_role_name     = aws_iam_role.worker.name
  api_role_name        = aws_iam_role.api.name
}

resource "aws_lambda_function" "worker" {
  function_name = "workflow-worker"
  role          = aws_iam_role.worker.arn
  timeout       = 300 # below the queue visibility timeout (360 by default)
  # ...

  environment {
    variables = merge(module.workflow.environment, {
      WORKFLOW_BASE_URL = "https://workflows.example.com/v1/workflow"
    })
  }
}
```

`environment` holds the variables the example entrypoints read:
`WORKFLOW_TABLE`, `WORKFLOW_QUEUE_URL`, and, with the scheduler enabled,
`WORKFLOW_QUEUE_ARN`, `WORKFLOW_SCHEDULER_GROUP`, and
`WORKFLOW_SCHEDULER_ROLE_ARN`. The individual values are also outputs for
building `awsworld.Config` directly.

## Inputs

| Name | Default | Description |
|---|---|---|
| `name` | `"workflow"` | Prefix for every resource name. |
| `tags` | `{}` | Tags for every resource. |
| `point_in_time_recovery` | `true` | Table point-in-time recovery. |
| `deletion_protection` | `true` | Table deletion protection. |
| `visibility_timeout_seconds` | `360` | Keep above the worker Lambda timeout. |
| `message_retention_seconds` | `1209600` | Queue and dead-letter retention (14 days, the SQS maximum). |
| `max_receive_count` | `10` | Deliveries before a message moves to the dead-letter queue. |
| `enable_scheduler` | `true` | Create the schedule group and Scheduler role. Without them, deliveries due more than 15 minutes ahead fail. |
| `worker_function_name` | `null` | Worker Lambda; creates the event source mapping when set. |
| `batch_size` | `10` | Messages per worker invocation. |
| `maximum_concurrency` | `null` | Cap on concurrent worker invocations (2-1000). |
| `worker_role_name` | `null` | Attach the worker policy to this role. |
| `api_role_name` | `null` | Attach the API policy to this role. |

## Permissions

The worker policy grants item reads and writes and `Query` on the table and
its indexes, `SendMessage`, `ReceiveMessage`, `DeleteMessage`, and
`GetQueueAttributes` on the task queue, `CreateSchedule` and `DeleteSchedule`
on the group's `go-workflow-*` schedules, and `iam:PassRole` on the Scheduler
role, limited to Scheduler. The API policy grants the same table access,
`SendMessage`, and `DeleteSchedule` for cancelling long timers.
