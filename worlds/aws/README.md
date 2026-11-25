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
if err != nil {
	return err
}
```

Entrypoints:

| Function | Purpose |
|---|---|
| `awsworld.Queue(world, runners)` | SQS Lambda handler that executes workflows |
| `awsworld.HTTP(world)` | RPC and webhook endpoints under `/v1/workflow` |
| `awsworld.HTTPPrefix(world, prefix)` | RPC and webhook endpoints under `prefix` |
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

### Queue routing

`QueueURL` carries every workflow by default. `Queues` adds further queues by
name, and `Routes` sends the runs of a workflow to one of them, such as a queue
with its own worker concurrency for heavy work:

```go
world, err := awsworld.New(awsworld.Config{
	// BaseURL, TableName, QueueURL, Scheduler, EncryptionKey...
	Queues: map[string]awsworld.QueueConfig{
		"backfill": {URL: backfillURL, ARN: backfillARN},
		"reports":  {URL: reportsURL, ARN: reportsARN},
	},
	Routes: worlds.Routes(
		worlds.Route[workflows.Backfill]("backfill"),
		worlds.Route[workflows.Report]("reports"),
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

A `QueueConfig` is an SQS standard queue. It needs its `URL`, and its `ARN`
when `Scheduler` is set; the ARN is the Scheduler target for deliveries due
later. `New` rejects a route to a queue not in `Queues`. A run never has two
flows running at once on any queue: the flow lease (see below) serializes
them.

### Queue delivery

`Queue` writes a `QUEUE#<id>` record holding the delivery, every
`QueueInput` field included, then publishes the message ID:

- due within 15 minutes: `SendMessage` with `DelaySeconds`;
- due later: a one-time EventBridge schedule (`at(...)` in UTC, flexible time
  window off, deleted after it runs) that sends the message to the queue.
  The schedule name, `go-workflow-<id>`, is stored on the record. A schedule
  takes at most 8 KB of input, so a delivery too large for it (such as a step
  delivery carrying its input) is sent as its ID alone; the record holds the
  delivery either way.

If publishing fails the record is deleted and the error returned. Without
`Config.Scheduler`, a delivery due more than 15 minutes ahead is sent with a
15-minute delay, arrives early, and is sent again for the rest until it is
due, and `Capabilities().MaxQueueDelay` reports 15 minutes, so the Go runtime
reaches a later timer in hops of at most 15 minutes. With `Scheduler`,
`MaxQueueDelay` is zero and any delay is scheduled directly. `New` rejects a
`SchedulerConfig` without a client, target ARN, and role ARN.

**Deduplication.** A delivery with an `IdempotencyKey` claims the key with a
`DEDUPE#<runID>#<sha256(key)>` item that lasts 24 hours
(`worlds.MinDeduplicationWindow`), through a conditional put. Queueing the
same key of the same run again while the claim lasts and its message lives
(its `QUEUE#` record exists) stores and sends nothing, and returns the first
message's ID. A claim whose window has passed, or whose message was
acknowledged or never published, is taken over by the next message. A
delivery without a key is never deduplicated.

**Delivery.** The `Queue` handler loads the record for each SQS message. A
missing record (already done or cancelled) is acknowledged, and so is a
record past its `Retention`, counted from when the message is first due,
without executing it. A message that arrives before its due time is sent
again for the rest of the delay. Otherwise the handler counts the delivery on
the record (`deliveries`) and executes it with a `MessageID` that stays the
same across redeliveries, a `DeliveryCount` of `deliveries`, and an `Attempt`
of `deliveries` minus the handoffs recorded on the record (`handoffs`). On
success it deletes the record. On failure it reports the message in
`BatchItemFailures` and sets its visibility to a backoff that grows with its
receive count, so SQS redelivers only that message, and a redrive policy
still moves one that keeps failing to the dead-letter queue. Malformed
messages are dropped.

**Redelivery.** When the runtime asks for its message again
(`worlds.Redelivery`), the handler records it and sends the same message ID
again as a new SQS message, then acknowledges the old one, so redeliveries
never count toward the redrive policy and are not bound by SQS's 12-hour
visibility limit:

- a handoff (`After` zero) adds one to `handoffs`, so it does not count as an
  attempt, and is sent again at once;
- a redelivery after a delay sets the record's due time and is sent again
  after it: through SQS within 15 minutes, through Scheduler beyond that, or,
  without Scheduler, after 15 minutes, arriving early and waiting again for
  the rest.


**One flow of a run at a time.** Before executing a serialized flow
(`QueueInput.SerializedFlow`), the handler takes the run's flow lease, a
`LEASE#<runID>` item put on condition that no lease exists or the one there
has expired. The lease lasts two minutes and is renewed every 30 seconds
while the flow executes, then deleted. A flow that finds the lease taken is
sent again two seconds later without counting a delivery. Step deliveries,
intermediate hops of a wait's timer, and retention deliveries never take the
lease. The lease makes overlapping flows rare and the run's in-band fence
keeps them safe.

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
| hook, or its disposal once disposed | `HOOK#<hookID>` | `META` |
| token owner | `TOKEN#<token>` | `META` |
| queue record | `QUEUE#<messageID>` | `META` |
| idempotency-key claim | `DEDUPE#<runID>#<base64url(sha256(key))>` | `META` |
| run's flow lease | `LEASE#<runID>` | `META` |

Events are the only copy of payloads. Run, step and hook items hold no
input, output, error or metadata; they name the events that recorded them. A
run's input is its `run_created` event (slot 1) and `resultEventId` names its
`run_completed` or `run_failed` event. A step's `createdEventId` names its
`step_created` event, or the `step_started` that created an inline step, and
`resultEventId` its latest `step_completed`,
`step_failed` or `step_retrying` event. A hook's `createdEventId` names its
`hook_created` event. Reads load these events with one consistent
`BatchGetItem` per page; `ResolveDataNone` skips all but a failure's error.

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

A disposed hook's item is replaced by a record of its disposal, which no
index lists. A finished run's hooks are hidden once their token retention
ends, and their items are replaced when their token is next claimed.

Claims and leases carry a `ttl` attribute (epoch seconds) after which they
are no longer needed. The World checks their expiry itself, so enabling the
table's TTL on `ttl`, as the Terraform module does, only reclaims their space.

Create the table with the [Terraform module](#infrastructure), or with
`awsworld.CreateTable(ctx, dynamoClient, name)`, which creates the same table
on demand and waits until it is active (handy for DynamoDB Local
and tests). `CreateTable` does not enable TTL.

## Retention

When a run's data is deleted, the run's items stay,
the payloads in its `EVENT#` items are removed, and its chunk items are
deleted, leaving each `STREAM#` item empty and closed. `expiredAt` is set
first and `dataPurged` last. `$retention: "0"` deletes the data right after the
commit that finishes the run, `RunsDeleteDataMany` deletes it on demand, and
`Config.Retention` sets `expiredAt` when the run finishes and queues a
`QUEUE#expire-<runID>` delivery due then (through Scheduler beyond 15 minutes).
See the [docs](https://go-workflow.vercel.app/docs/worlds/aws#retention).

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
action). Both need `sqs:SendMessage` on the queue, and `dynamodb:BatchWriteItem` to
delete a finished run's stream chunks. With `Retention`, the API role needs
`scheduler:CreateSchedule` if it cancels runs, since a finished run schedules
its own expiry.

The queue worker additionally needs `sqs:ReceiveMessage`, `DeleteMessage`
and `GetQueueAttributes` (for the event source mapping), and, with
`Scheduler`, `scheduler:CreateSchedule` on
`arn:aws:scheduler:<region>:<account>:schedule/<group>/go-workflow-*` plus
`iam:PassRole` on the Scheduler role.

## Limits

- **400 KB per item.** Each event and stream chunk is one item, so one
  payload must fit an event item. Writes that
  would exceed the limit return an error wrapping `worlds.ErrEventInvalid`
  for appends; nothing is truncated or dropped. Keep large payloads in S3 and
  pass references.
- **100 actions per transaction (4 MB total).** One append writes the run
  view, each event, and each changed step, hook, token and wait marker in one
  transaction. A batch that needs more than 100 actions for its admitted
  events returns `worlds.ErrEventInvalid` and appends none of them. A stream write holds at most 99 chunks.
- **15-minute SQS delay.** Queueing a longer delay needs `Scheduler`.
  Without it, the runtime reaches its timers in 15-minute hops, and a longer
  redelivery waits in 15-minute hops too.
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
