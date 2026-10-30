# AWS World

```go
import awsworld "github.com/eriicafes/go-workflow/worlds/aws"
```

The AWS World stores workflow state in one DynamoDB table, delivers work through
SQS, and uses EventBridge Scheduler for deliveries due more than 15 minutes
ahead. Any number of Lambda functions or processes can share one table: every
append is a DynamoDB transaction that only commits if the run's event count is
unchanged since it was read, so concurrent writers never lose or reorder events.

## Usage

Construct the World from AWS clients that already carry your region,
credentials, retry policy, and endpoints. `New` does not contact AWS.

```go
cfg, err := config.LoadDefaultConfig(ctx)
if err != nil {
	log.Fatal(err)
}

world, err := awsworld.New(awsworld.Config{
	BaseURL:   os.Getenv("WORKFLOW_BASE_URL"), // https://workflows.example.com/v1/workflow
	TableName: os.Getenv("WORKFLOW_TABLE"),
	QueueURL:  os.Getenv("WORKFLOW_QUEUE_URL"),
	Scheduler: &awsworld.SchedulerConfig{ // optional
		Client:    scheduler.NewFromConfig(cfg),
		Group:     os.Getenv("WORKFLOW_SCHEDULER_GROUP"), // empty uses "default"
		TargetARN: os.Getenv("WORKFLOW_QUEUE_ARN"),
		RoleARN:   os.Getenv("WORKFLOW_SCHEDULER_ROLE_ARN"),
	},
}, dynamodb.NewFromConfig(cfg), sqs.NewFromConfig(cfg))
if err != nil {
	log.Fatal(err)
}
```

Deploy two Lambda functions:

```go
// Queue worker: SQS event source mapping, registers runners.
runners := worlds.Runners(workflow.Use(flows.GenerateReport))
lambda.Start(awsworld.Queue(world, runners))
```

```go
// Client-facing API: Function URL, needs no runners.
lambdaurl.Start(awsworld.HTTP(world))
```

## Entry points

| Function | Purpose |
| --- | --- |
| `Queue(world, runners)` | SQS Lambda handler that executes deliveries |
| `HTTP(world)` | RPC and webhook endpoints under `/v1/workflow` |
| `HTTPPrefix(world, prefix)` | the same endpoints under `prefix` |
| `RPC(world)` | the RPC endpoint only |
| `Webhook(world)` | the webhook endpoint only |
| `CreateTable(ctx, client, name)` | creates the table and waits until it is active |

## Configuration

| Field | Meaning |
| --- | --- |
| `BaseURL` | Required. Public URL of `HTTP(world)` including `/v1/workflow`. |
| `TableName` | Required. DynamoDB table, laid out as `CreateTable` creates it. |
| `QueueURL` | Required. SQS standard queue that carries deliveries of unrouted workflows. |
| `Queues` | Optional. Further queues by name, standard or FIFO. See [Queue routing](#queue-routing). |
| `Routes` | Optional. Workflow name to queue name, built with `Routes` and `Route`. |
| `Scheduler` | Optional. Delivers work due more than 15 minutes ahead, and every delayed delivery to a FIFO queue. |

`SchedulerConfig` requires `Client`, `TargetARN` (the queue's ARN), and
`RoleARN` (the role Scheduler assumes to send to the queue). `Group` defaults to
`default`.

## Queue routing

`QueueURL` carries every workflow by default. `Queues` adds further queues by
name, and `Routes` sends the runs of a workflow to one of them, such as a queue
with its own worker concurrency for heavy work:

```go
world, err := awsworld.New(awsworld.Config{
	// BaseURL, TableName, QueueURL, Scheduler...
	Queues: map[string]awsworld.QueueConfig{
		"backfill": {URL: backfillURL, ARN: backfillARN},
		"ordered":  {URL: orderedURL, ARN: orderedARN, FIFO: true},
	},
	Routes: worlds.Routes(
		worlds.Route[workflows.Backfill]("backfill"),
		worlds.Route[workflows.Ledger]("ordered"),
	),
}, dynamoClient, sqsClient)
```

`worlds.Route[W]` names the workflow with `worlds.WorkflowName[W]()`, the name
its runs are recorded under. Every delivery of a run, flow or step, goes to its
workflow's queue: the Runtime sets `QueueInput.WorkflowName`, `ResumeHook`
takes it from the hook's `ResumeContext`, and wake-ups and cancellation read the run for it. The record keeps the
queue name, so a message that arrives early is sent again to the same queue.
The worker reads messages from every queue; give each its own event source
mapping.

A `QueueConfig` needs its `URL`, and its `ARN` when `Scheduler` is set or the
queue is FIFO; the ARN is the Scheduler target for deliveries due later. `New`
rejects a route to a queue not in `Queues`.

A FIFO queue (`FIFO: true`) runs one delivery of a run at a time: each
message's `MessageGroupId` is the run ID and its `MessageDeduplicationId` the
delivery ID. FIFO queues have no per-message delay, so every delayed delivery,
retries and early arrivals included, goes through Scheduler into the run's
message group, and `New` rejects a FIFO queue without `Scheduler`. Scheduler
can't set a deduplication ID, so enable content-based deduplication on the
queue; scheduled bodies carry a unique `publish` field so they never collide.
FIFO groups order a run's messages, not separate runs: two runs of a workflow
still interleave when one waits.

## Queue delivery

`Queue` writes a `QUEUE#<id>` record holding the delivery, then publishes the
message ID:

- due within 15 minutes: `SendMessage` with `DelaySeconds`.
- due later: a one-time EventBridge schedule named `go-workflow-<id>` that sends
  the same body to the queue and is deleted after it runs.

Without `Config.Scheduler`, a delivery due more than 15 minutes ahead returns an
error. SQS standard queues do not deduplicate, so `IdempotencyKey` is not used.
Delivery is at least once and the runtime is idempotent.

The `Queue` handler loads the record for each SQS message. A missing record or
a body that is not a queue message is acknowledged. A message that arrives
early is sent again with the remaining delay. Otherwise it executes the
delivery, extending the message's visibility every 30 seconds while it runs,
and deletes the record on success. On failure it reports the message in
`BatchItemFailures` so SQS redelivers only that message, after a backoff that
doubles from one second with each receive up to 15 minutes. Configure a
dead-letter queue to collect deliveries that keep failing.

Give the queue a message retention period of at least one hour. Each message
waits at most 15 minutes before it is due or sent again, and a retention period
shorter than that drops deliveries.

::: warning ReportBatchItemFailures
The event source mapping must enable `ReportBatchItemFailures`. Without it,
Lambda ignores the reported failures, treats the batch as successful, and
deletes every message, so failed deliveries are not retried.
:::

`Scheduler` can be omitted when the World only serves HTTP requests, or when no
delay is longer than 15 minutes.

## Table

One table with string keys `pk` and `sk` and two global secondary indexes that
project only keys and `status`:

| Item | `pk` | `sk` |
| --- | --- | --- |
| run view and event count | `RUN#<runID>` | `META` |
| event | `RUN#<runID>` | `EVENT#<slot, 26 digits>` |
| step | `RUN#<runID>` | `STEP#<stepID>` |
| completed wait marker | `RUN#<runID>` | `WAIT#<correlationID>` |
| stream state | `RUN#<runID>` | `STREAM#<base64url(name)>` |
| stream chunk | `RUN#<runID>` | `CHUNK#<base64url(name)>#<index, 10 digits>` |
| hook | `HOOK#<hookID>` | `META` |
| token owner | `TOKEN#<token>` | `META` |
| queue record | `QUEUE#<messageID>` | `META` |

| Index | Partition key values | Sort key |
| --- | --- | --- |
| `list` (`list_pk`, `list_sk`) | `RUNS`, `HOOKS` | `<createdAt>#<id>` |
| `list_group` (`list_group_pk`, `list_group_sk`) | `RUNS#<workflow>`, `HOOKS#<runID>` | `<createdAt>#<id>` |

Listings read an index and then fetch the matching items with consistent
`BatchGetItem` calls. Index membership is eventually consistent, so a run or
hook created a moment ago may be missing from a listing. All other reads are
consistent reads of the base table.

Create the table with the [Terraform module](#terraform), or with
`awsworld.CreateTable(ctx, dynamoClient, name)` for tests and DynamoDB Local.

## Terraform

[`worlds/aws/terraform`](https://github.com/eriicafes/go-workflow/tree/main/worlds/aws/terraform)
provisions:

- the DynamoDB table with its indexes (on-demand, encrypted, point-in-time
  recovery and deletion protection on by default).
- the task queue and its dead-letter queue.
- further queues, standard or FIFO, for `Routes`, each with its own
  dead-letter queue (`queues`).
- optionally, the SQS event source mappings for the worker, with
  `ReportBatchItemFailures`.
- the EventBridge Scheduler group and the role Scheduler assumes.
- least-privilege managed policies for the worker and the API, optionally
  attached to their roles.

It does not create the Lambda functions themselves.

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

The `environment` output holds `WORKFLOW_TABLE`, `WORKFLOW_QUEUE_URL`,
`WORKFLOW_QUEUES` (JSON: `{"<name>":{"url":"...","arn":"...","fifo":false}}`),
and, with the scheduler enabled, `WORKFLOW_QUEUE_ARN`, `WORKFLOW_SCHEDULER_GROUP`, and
`WORKFLOW_SCHEDULER_ROLE_ARN`.

| Input | Default | Description |
| --- | --- | --- |
| `name` | `"workflow"` | Prefix for every resource name. |
| `tags` | `{}` | Tags for every resource. |
| `point_in_time_recovery` | `true` | Table point-in-time recovery. |
| `deletion_protection` | `true` | Table deletion protection. |
| `visibility_timeout_seconds` | `360` | Keep above the worker Lambda timeout. |
| `message_retention_seconds` | `1209600` | Queue and dead-letter retention (14 days). |
| `max_receive_count` | `10` | Deliveries before a message moves to the dead-letter queue. |
| `enable_scheduler` | `true` | Create the schedule group and Scheduler role. |
| `worker_function_name` | `null` | Worker Lambda. Setting it creates the event source mapping. |
| `batch_size` | `10` | Messages per worker invocation. |
| `maximum_concurrency` | `null` | Cap on concurrent worker invocations (2–1000). |
| `worker_role_name` | `null` | Attach the worker policy to this role. |
| `api_role_name` | `null` | Attach the API policy to this role. |
| `queues` | `{}` | Routed queues by name: `fifo`, `batch_size` (10), `maximum_concurrency`, `visibility_timeout_seconds`, `max_receive_count`. A FIFO queue gets the `.fifo` suffix and content-based deduplication. |

## IAM

Both roles need, on the table and its indexes (`<table-arn>` and
`<table-arn>/index/*`): `dynamodb:GetItem`, `PutItem`, `UpdateItem`,
`DeleteItem`, `Query`, and `BatchGetItem`. Both need `sqs:SendMessage` on the
queue.

The worker also needs `sqs:ReceiveMessage`, `DeleteMessage`,
`ChangeMessageVisibility`, and `GetQueueAttributes`, and, with `Scheduler`, `scheduler:CreateSchedule` and
`scheduler:DeleteSchedule` on
`arn:aws:scheduler:<region>:<account>:schedule/<group>/go-workflow-*` plus
`iam:PassRole` on the Scheduler role. The API role needs
`scheduler:DeleteSchedule` only if it cancels long timers.

## Limits

- **400 KB per item.** Each event, step, hook, run view, and stream chunk is one
  item. An oversized event append returns an error wrapping
  `worlds.ErrEventInvalid`. Other oversized writes also fail with an error.
  Keep large payloads in S3 and pass references.
- **100 actions per transaction (4 MB).** One append writes the run view, each
  event, and each changed step, hook, token, and wait marker in one transaction.
  A stream write holds at most 99 chunks.
- **15-minute SQS delay.** Longer delays need `Scheduler`.
- **Hot keys.** Appends to one run are serialized by its event count and
  retried with jittered backoff, so heavy concurrent appends to one run trade
  throughput for retries.

## Security

- `Queue` is an SQS event-source handler, not an HTTP handler. Attach it only
  through an event source mapping.
- The HTTP handlers do not authenticate requests. Prefer an internal
  Function URL with `AWS_IAM` auth or an API Gateway route with an IAM or JWT
  authorizer, and enforce workflow and run ownership there.
- Webhooks at `/v1/workflow/webhooks/{token}` are authorized only by the token.
  `Webhook(world)` lets you expose only that route publicly.
- Enable encryption at rest (the default for DynamoDB and SQS) with a customer
  managed KMS key if your data requires it.

## Tests

```sh
docker run -p 8000:8000 amazon/dynamodb-local
go test -race ./worlds/aws/...
```

The tests use DynamoDB Local at `DYNAMODB_ENDPOINT` (default
`http://localhost:8000`) with fixed test credentials. SQS and Scheduler are
faked.
