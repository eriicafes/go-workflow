# AWS world

`awsworld` stores workflow state in one DynamoDB table, delivers queue
messages through SQS, and uses EventBridge Scheduler for messages due more
than 15 minutes in the future. Any number of Lambda functions or processes can
share one table: every append is a DynamoDB transaction that only commits if
the run's event count is unchanged since it was read, so concurrent writers
never lose or reorder events and exactly one concurrent hook claimant wins.

## Usage

Construct the world from AWS clients that already carry your region,
credentials, retry policy, and endpoints.

```go
world, err := awsworld.New(awsworld.Config{
    BaseURL:   os.Getenv("WORKFLOW_BASE_URL"), // https://workflows.example.com/v1/workflow
    TableName: os.Getenv("WORKFLOW_TABLE"),
    QueueURL:  os.Getenv("WORKFLOW_QUEUE_URL"),
    Scheduler: &awsworld.SchedulerConfig{ // optional, see below
        Client:    scheduler.NewFromConfig(cfg),
        Group:     os.Getenv("WORKFLOW_SCHEDULER_GROUP"), // empty uses "default"
        TargetARN: os.Getenv("WORKFLOW_QUEUE_ARN"),
        RoleARN:   os.Getenv("WORKFLOW_SCHEDULER_ROLE_ARN"),
    },
}, dynamodb.NewFromConfig(cfg), sqs.NewFromConfig(cfg))
if err != nil { return err }
```

Entrypoints:

| Function | Purpose |
|---|---|
| `awsworld.Queue(world, runners)` | SQS Lambda handler that executes workflows |
| `awsworld.HTTP(world)` | RPC and webhook endpoints under `/v1/workflow` |
| `awsworld.RPC(world)` | the CBOR RPC endpoint only |
| `awsworld.Webhook(world)` | the token webhook endpoint only |

```go
// Queue worker: registers workflow implementations.
runners := worlds.Runners(workflow.Use(flows.GenerateReport))
lambda.Start(awsworld.Queue(world, runners))

// App-facing API: needs no workflow implementations.
lambda.Start(lambdaurl.Wrap(awsworld.HTTP(world)))
```

The basic example keeps these in `examples/basic/cmd/worker/aws/queue` and
`examples/basic/cmd/worker/aws/api`. Code that already has the world in
process can pass it to `workflow.NewContext` directly without the HTTP API.

### Queue delivery

`Queue` writes a `QUEUE#<id>` record holding the delivery, then publishes the
message ID:

- due within 15 minutes: `SendMessage` with `DelaySeconds`;
- due later: a one-time EventBridge schedule (`at(...)` in UTC, flexible time
  window off, deleted after it runs) that sends the same body to the queue.
  The schedule name, `go-workflow-<id>`, is stored on the record.

If publishing fails the record is deleted and the error returned. Without
`Config.Scheduler`, a delivery due more than 15 minutes ahead returns an
error. `New` rejects a `SchedulerConfig` without a client, target ARN, and
role ARN. SQS standard queues do not deduplicate, so `IdempotencyKey` is not
used; delivery is at least once and the runtime is idempotent.

The `Queue` handler loads the record for each SQS message. A missing record
(cancelled or already done) is acknowledged. A message that arrives before its
due time is sent again with the remaining delay (at most 15 minutes per hop).
Otherwise it runs the workflow, deletes the record on success, and reports the
message in `BatchItemFailures` on failure so SQS redelivers only that message.
Malformed messages are also reported as failures; configure a dead-letter
queue to collect them.

`QueueCancel` deletes the record and its schedule. A schedule that already
fired counts as deleted. A world without `Scheduler` cannot delete schedules;
when such a schedule fires, the consumer finds no record and acknowledges it.

## Table

Single table with string keys `pk` and `sk` and two global secondary indexes
that project only keys and `status`:

| Item | `pk` | `sk` |
|---|---|---|
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
|---|---|---|
| `list` (`list_pk`, `list_sk`) | `RUNS`, `HOOKS` | `<createdAt>#<id>` |
| `list_group` (`list_group_pk`, `list_group_sk`) | `RUNS#<workflow>`, `HOOKS#<runID>` | `<createdAt>#<id>` |

`RunsList` and analytics read `list` (all runs) or `list_group` (one workflow),
bounded by creation time, with status filtered on the index. `HooksList`
reads `HOOKS` or `HOOKS#<runID>`. Matching keys are then read from the table
with consistent `BatchGetItem` calls, so returned items are current; index
membership itself is eventually consistent, so a run created a moment ago may
be missing from a listing. Since only `status` is projected, ordinary appends
do not write to the indexes. All other reads are consistent reads of the base
table. Stream names are base64url-encoded in keys so names containing `#` do
not overlap.

Create the table with the [Terraform module](#infrastructure), or with
`awsworld.CreateTable(ctx, dynamoClient, name)`, which creates the same table
on demand and waits until it is active (handy for DynamoDB Local
and tests).

## Infrastructure

[`terraform/`](terraform) is a Terraform module that provisions the table,
the task queue with a dead-letter queue, the worker's SQS event source
mapping, the Scheduler group and role, and least-privilege IAM policies for
the worker and API roles. Its `environment` output holds the variables the
example entrypoints read.

```hcl
module "workflow" {
  source = "github.com/eriicafes/go-workflow//worlds/aws/terraform"

  worker_function_name = aws_lambda_function.worker.function_name
  worker_role_name     = aws_iam_role.worker.name
  api_role_name        = aws_iam_role.api.name
}
```

The event source mapping must use `ReportBatchItemFailures`, as the module's
does: without it a single failed record makes Lambda retry the whole batch.
The `queue_arn` and `scheduler_role_arn` outputs are
`SchedulerConfig.TargetARN` and `SchedulerConfig.RoleARN`.

`Scheduler` can be omitted when the world is only used for `HTTP`, `RPC` or
`Webhook`, or when no workflow sleeps, times out or retries more than 15
minutes ahead. The queue worker is the component that creates long timers.

## IAM

Both Lambda roles need, on the table and its indexes (`<table-arn>` and
`<table-arn>/index/*`): `dynamodb:GetItem`, `PutItem`, `UpdateItem`,
`DeleteItem`, `Query` and `BatchGetItem` (transactions are authorized per
action). Both need `sqs:SendMessage` on the queue.

The queue worker additionally needs `sqs:ReceiveMessage`, `DeleteMessage`
and `GetQueueAttributes` (for the event source mapping), and, with
`Scheduler`, `scheduler:CreateSchedule` and `scheduler:DeleteSchedule` on
`arn:aws:scheduler:<region>:<account>:schedule/<group>/go-workflow-*` plus
`iam:PassRole` on the Scheduler role. The API role needs
`scheduler:DeleteSchedule` only if it cancels long timers.

## Limits

- **400 KB per item.** Each event, step (input plus output), hook, run view
  (input plus output plus error) and stream chunk is one item. Writes that
  would exceed the limit return an error wrapping `worlds.ErrEventInvalid`
  for appends; nothing is truncated or dropped. Keep large payloads in S3 and
  pass references.
- **100 actions per transaction (4 MB total).** One append writes the run
  view, each event, and each changed step, hook, token and wait marker in one
  transaction. A batch that needs more than 100 actions returns
  `worlds.ErrEventInvalid`. A stream write holds at most 99 chunks.
- **15-minute SQS delay.** Longer delays need `Scheduler`.
- **Hot keys.** Appends to one run are serialized by its event count and
  retried with jittered backoff (up to 64 attempts), so heavy concurrent
  appends to one run trade throughput for retries. Run and hook creation writes to the `RUNS` and
  `HOOKS` index partitions.

## Testing

The tests expect DynamoDB Local at `http://localhost:8000`; set
`DYNAMODB_ENDPOINT` to use another address. Credentials and region are fixed
test values, so no AWS configuration is needed.

```sh
docker run -p 8000:8000 amazon/dynamodb-local
go test -race ./worlds/aws/...
```

Each test creates and deletes its own table and runs the `workflowtest`
conformance suites against it. SQS and Scheduler are faked.

## Security

`awsworld.Queue` is an SQS event-source handler, not an HTTP handler. Attach
it only through an event source mapping; AWS polls the queue and invokes the
Lambda.

`awsworld.HTTP`, `RPC` and `Webhook` do not authenticate requests or authorize
workflow starts, run reads, or event reads. Prefer an internal Function URL
with `AWS_IAM` auth or an API Gateway route with an IAM or JWT authorizer, and
enforce workflow and run ownership in that layer.

Webhooks are served at `/v1/workflow/webhooks/{token}`. The token is the only
authorization for that route: treat the URL as a bearer capability, and verify
the provider's signature, timestamp, and replay protection in the workflow
before acting on a request. `Webhook(world)` lets you expose only that route
publicly while keeping RPC private.

Use least-privilege IAM as above, scope the queue and table resource policies
to these roles, and enable encryption at rest (default for DynamoDB and SQS
with SSE) with a customer managed KMS key if your data requires it.
