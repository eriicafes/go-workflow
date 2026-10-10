# AWS world

The AWS workflow world requires these services in one region:

- A DynamoDB table for runs, commands, task leases, and events.
- An SQS queue for task notifications, configured as a Lambda event source.
- EventBridge Scheduler only for durable timers longer than SQS's 15-minute
  delay limit.
- An optional HTTP Lambda behind API Gateway or a Lambda Function URL for
  `awsworld.HTTP(world)` when another service, a UI, or a proxy needs remote
  access to workflow runs.

Construct the world from the AWS clients that already carry your selected
region, credentials, retry policy, and endpoint settings. The queue worker
registers workflow implementations on the world:

```go
world, err := awsworld.New(awsworld.Config{
    TableName: os.Getenv("WORKFLOW_TABLE"),
    QueueURL: os.Getenv("WORKFLOW_QUEUE_URL"),
    Scheduler: &awsworld.SchedulerConfig{
        Client: schedulerClient,
        Group: "workflow",
        TargetARN: os.Getenv("WORKFLOW_QUEUE_ARN"),
        RoleARN: os.Getenv("WORKFLOW_SCHEDULER_ROLE_ARN"),
    },
}, dynamoClient, sqsClient)
if err != nil { return err }

runners := workflow.Runners(workflow.Use(flows.GenerateReport))

// Lambda SQS event-source handler.
lambda.Start(awsworld.Queue(world, runners))
```

Deploy a separate Lambda Function URL only when an app service, UI, or proxy
needs remote access to workflow runs. It does not need to import or register
workflow implementations. `Scheduler` is optional when a World is used
only for `awsworld.HTTP(world)`.

```go
world, err := awsworld.New(
    awsworld.Config{
        TableName: os.Getenv("WORKFLOW_TABLE"),
        QueueURL: os.Getenv("WORKFLOW_QUEUE_URL"),
    },
    dynamoClient,
    sqsClient,
)
if err != nil { return err }

lambda.Start(lambdaurl.Wrap(awsworld.HTTP(world)))
```

The basic example keeps these units in `cmd/worker/aws/queue` and
`cmd/worker/aws/api`.

When the code that starts a workflow already has the same world in process,
pass it directly to `workflow.NewContext`. No HTTP Lambda is needed.

## When Scheduler is required

SQS can delay one task by at most 15 minutes. `Config.Scheduler` is optional
in either of these cases:

- the world is used only for `awsworld.HTTP(world)`
- a queue worker executes workflows whose `Sleep` and `Timeout` calls are at
  most 15 minutes.

In either case, omit `Scheduler` entirely.

When a queue worker can execute a timer longer than 15 minutes, Scheduler is
required. Provide one `SchedulerConfig` with all of the following:

- a non-nil EventBridge Scheduler client
- `TargetARN`, the SQS queue ARN
- `RoleARN`, an IAM role that lets Scheduler send to that queue

`SchedulerConfig.Group` is optional and defaults to `default`. The queue example
passes `WORKFLOW_SCHEDULER_GROUP` because that deployment is the one which
creates long-timer schedules. Its API counterpart deliberately omits it. The
world returns an error instead of silently scheduling an unsupported long
timer when any of the required Scheduler values are missing.

```hcl
resource "aws_dynamodb_table" "workflows" {
  name         = "workflows"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"
  range_key    = "sk"

  attribute { name = "pk" type = "S" }
  attribute { name = "sk" type = "S" }
}

resource "aws_sqs_queue" "workflow_tasks" {
  name                       = "workflow-tasks"
  visibility_timeout_seconds = 300
}

resource "aws_lambda_event_source_mapping" "workflow_tasks" {
  event_source_arn = aws_sqs_queue.workflow_tasks.arn
  function_name    = aws_lambda_function.workflow_worker.arn
  batch_size       = 10
  function_response_types = ["ReportBatchItemFailures"]
}
```

The Lambda execution role needs DynamoDB read/write access to the table, SQS
send/receive/delete access to the queue, and EventBridge Scheduler create and
delete permissions for timer schedules. The Scheduler role needs permission to
send to the SQS queue ARN supplied as `SchedulerConfig.TargetARN`.

## Security

`awsworld.Queue(world, runners)` is an SQS Lambda event-source handler, not a public HTTP
handler. Keep it attached only through an SQS event-source mapping. AWS polls
the queue and invokes the Lambda. Callers do not invoke this function over the
internet.

`awsworld.HTTP(world)` is the separate app-facing API. It does not authenticate
requests or authorize individual workflow starts, run reads, or event reads.
Prefer an internal service-to-service Function URL or API Gateway route. If it
crosses a trust boundary, require IAM, a JWT authorizer, or equivalent
middleware before it reaches `awsworld.HTTP(world)`, and enforce workflow and run
ownership in that layer.

Use least-privilege IAM. The HTTP Lambda needs DynamoDB read/write and SQS send
permissions. The queue Lambda additionally needs SQS receive/delete and, when
long timers are enabled, EventBridge Scheduler permissions. Keep the DynamoDB
table and SQS queue policies scoped to those roles.
