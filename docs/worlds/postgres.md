# Postgres World

```go
import postgresworld "github.com/eriicafes/go-workflow/worlds/postgres"
```

The Postgres World stores workflow state in PostgreSQL and is safe to use from
many processes at once: API servers, workers, or both in one binary.

## Usage

```go
world, err := postgresworld.Open(ctx, postgresworld.Config{
	BaseURL: os.Getenv("WORKFLOW_BASE_URL"), // https://workflows.example.com/v1/workflow
	URL:     os.Getenv("DATABASE_URL"),
})
if err != nil {
	log.Fatal(err)
}
defer world.Close()

runners := core.Runners(workflow.Use(flows.GenerateReport))
go func() {
	if err := postgresworld.Worker(ctx, world, runners); err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("workflow worker stopped: %v", err)
	}
}()
log.Fatal(http.ListenAndServe(":8081", postgresworld.HTTP(world)))
```

`Open` creates and owns a `pgxpool`. `New(ctx, pool, config)` uses an existing
pool and leaves it open on `Close`. Stop `Worker` loops by cancelling their
context before calling `Close`.

## Configuration

| Field | Default | Meaning |
| --- | --- | --- |
| `BaseURL` | required | Public URL of `HTTP(world)`, including `/v1/workflow`. |
| `EncryptionKey` | none | 32 random bytes. Encrypts every run's payloads with a key derived from it. See [Data Format and Encryption](/guide/encryption). |
| `EncryptionScope` | empty | The scope run keys are derived from `EncryptionKey` in. Changing it leaves earlier runs' payloads unreadable. |
| `URL` | `WORKFLOW_POSTGRES_URL`, or `DATABASE_URL` | Connection string, used by `Open`. |
| `Schema` | `workflow` | The schema the World's tables live in, created when missing. |
| `HookRetentionLimit` | `WORKFLOW_POSTGRES_HOOK_RETENTION_LIMIT_DAYS`, or 30 days | The longest token retention a hook may ask for. A longer one is refused with `ErrEventInvalid`, or `ErrStartHookRejected` for a start hook. |
| `PollInterval` | `WORKFLOW_POSTGRES_POLL_INTERVAL_MS`, or 500 ms | Longest sleep of workers and waiters when no notification arrives. |
| `Concurrency` | `WORKFLOW_POSTGRES_WORKER_CONCURRENCY`, or 50 | Deliveries one `Worker` call executes at once. A step that waits for a child run holds its delivery meanwhile, so a low limit starves workflows that fan out. |
| `LeaseDuration` | 1 min | How long a claimed delivery is hidden from other workers. Workers renew it while executing, so it only bounds how quickly a crashed worker's delivery is retried. |
| `Routes` | none | The queue of each routed workflow. See [routing](#routing-workflows-to-queues). |
| `Retention`, `Retentions` | keep forever | How long finished runs keep their data, by default and per workflow. See [retention](#retention). |
| `SkipRecovery` | false | Leaves active runs alone when `Worker` starts. See [recovery](#recovery). |

## Entry points

- **`Worker(ctx, world, runners, options...)`**: the queue worker. It blocks
  until `ctx` is cancelled, waits for in-flight deliveries, and returns
  `ctx.Err()`. `WorkerOptions.Queues` limits the queues it claims, and
  `WorkerOptions.Concurrency` overrides `Config.Concurrency`. The World's first
  `Worker` recovers active runs as it starts (see [recovery](#recovery)), and
  returns an error when it cannot list them.
- **`HTTP(world)`**: RPC and webhook endpoints under `/v1/workflow`.
- **`HTTPPrefix(world, prefix)`**: the same endpoints under `prefix`.
- **`RPC(world)`** and **`Webhook(world)`**: each endpoint alone.

When the application and worker share a process, pass the World to
`workflow.NewClient`. When they are separate, serve `HTTP(world)` from the
worker and use `workflow.NewHTTPClient` in the application, or open the same
database from both.

## Schema and migrations

`Open` and `New` create `Config.Schema` and migrate it. Migrations run in one
transaction under `pg_advisory_xact_lock`, so processes starting together
migrate once, and the applied versions are recorded in
`workflow_schema_version`. A database with a newer version than the library
knows is rejected.

The tables are world-postgres's, with its names and columns:

| Table | Contents |
| --- | --- |
| `workflow_runs` | Runs, keyed by `id`, with `status` as the `status` enum, `name`, `attributes`, `error_code`, `execution_context_cbor`, `start_hook_cbor`, and the slot of the run's result. |
| `workflow_events` | The event log, keyed by `(run_id, id)`, where `id` is the slot's `evnt_` ID. `payload_cbor` holds the event's data. |
| `workflow_event_slots` | Each run's in-band fence, `seq_in_band`. |
| `workflow_steps` | Steps, keyed by `(run_id, step_id)`, with the slots of their input and result. |
| `workflow_hooks` | Hooks. `claims_token` marks the hook that holds its token, unique per token. A hook's row is deleted when it is disposed, and when its run ends unless its token is retained, then when its token is next claimed. |
| `workflow_waits` | A run's waits, deleted when it ends. |
| `workflow_start_hook_admissions` | Each start hook candidate's decision, kept for 25 hours. |
| `workflow_stream_chunks` | Stream chunks, ordered by their `chnk_` ID. An `eof` chunk closes a stream. |
| `workflow_queue` | Live messages: the whole delivery in `payload`, with `not_before`, `deliveries`, `handoffs`, `lease_until`, and `running`. |

They differ from world-postgres's where Go needs it:
- Payloads are stored once, in the events. The run, step, and hook rows name
  the slot of the event holding each payload, and reads join it.
- `payload_cbor` is Go's own CBOR (`worlds.EncodeEventData`), with payloads as
  raw bytes, not cbor-x. There are no deprecated `jsonb` columns.
- Steps are keyed by run and step, since a step ID is unique only within
  its run. That key leads with the run, so there is no
  `workflow_steps_run_id_index`.
- `workflow_runs.spec_version` is an `integer`, and `data_purged` serves
  `Config.Retention`.
- Hooks fill in `resume_context`, which world-postgres leaves empty, with the
  run's workflow, deployment, public key, and spec version, so a resume
  queues its delivery without reading the run. There are no `owner_id`,
  `project_id`, or `environment` columns.
- The queue is Go's own, `workflow_queue`. There are no snapshot or invocation
  tables.

Indexes have world-postgres's names (`workflow_runs_name_index`,
`workflow_hooks_token_claim_unique`, and so on). The World's own are
`workflow_runs_expired_index` and those of `workflow_queue`.

## Lists

`RunsList`, `StepsList`, and `HooksList` order by ID: runs and steps newest
first, hooks oldest first, unless `Pagination.SortOrder` says otherwise. IDs
end in a ULID, so that is the order they were minted in, which for a run
minted before it was stored is not the order of `created_at`. A page's cursor
is its last ID, and the last page has one too. `Analytics().RunsList` pages
the same way, oldest first.

Attributes are `jsonb`, execution contexts and start hooks are CBOR in
world-postgres's `_cbor` columns, a hook's `claimed_from` is CBOR in `bytea`, and timestamps are `timestamptz` at
microsecond precision. Times returned by the World are UTC.

## Consistency

Every event append is one transaction. It locks the run's row and its
`workflow_event_slots` row with `SELECT ... FOR UPDATE`, reads the next slot as
the run's highest event ID plus one, checks the transition rules, inserts the
events, and updates the views. Writers to one run are serialized across
processes, slots are contiguous, and a batch is all-or-nothing.

`EventsCreate(hook_created)` locks the hook claiming the token. A token is held
while its hook's run is live or its retention lasts. A forced claim over a live owner also locks
the owner's run, taking run locks in run ID order to avoid deadlocks. Exactly
one concurrent claimant wins a free token. Transactions are retried on
serialization failures and deadlocks.

## Routing workflows to queues

```go
world, err := postgresworld.Open(ctx, postgresworld.Config{
	// BaseURL, URL...
	Routes: worlds.Routes(worlds.Route[workflows.Backfill]("backfill")),
})

go postgresworld.Worker(ctx, world, runners, postgresworld.WorkerOptions{Queues: []string{postgresworld.DefaultQueue}})
go postgresworld.Worker(ctx, world, runners, postgresworld.WorkerOptions{Queues: []string{"backfill"}, Concurrency: 2})
```

Every delivery of a routed workflow's runs is stored on its queue, and other
workflows' on `DefaultQueue`. A worker without `Queues` claims every queue.

## Worker model

`Queue` inserts a row holding the whole delivery and, once that commits,
sends `NOTIFY workflow_queue`. A delivery with an
`IdempotencyKey` is stored once while its message lives: a unique index on the
run and key makes a `Queue` with the key of a queued or executing message store
nothing and return that message's ID. A delivery without a key is never
deduplicated.

A message does not expire. Its row stays until a delivery of it succeeds or
its job runs out of attempts, since a dropped message would leave its step or
flow with nothing to run it. `QueueInput.Retention` is a limit of queues that
expire messages, and is stored with the delivery but not enforced.

A message's ID is `msg_<ULID>`. `Worker` claims due rows with
`FOR UPDATE SKIP LOCKED`, sets `lease_until`, and increments `deliveries`, which
fences the lease. Every delivery of a message has the row's ID as its
`MessageID`, `deliveries` as its `DeliveryCount`, and `deliveries - handoffs` as
its `Attempt`. The worker executes each:

- success deletes the row.
- a `worlds.Redelivery` keeps the row and makes it due again after its delay, as
  the next job of its message. A handoff (a delay of zero on a spec 9 run's
  message) keeps `Attempt`. A spec 9 run's flow redelivered after a delay is a
  counted redelivery, one more `Attempt`. Any other redelivery, such as a step's
  in-place retry, starts `Attempt` over from where the job started it, so a step
  retried in place many times on one message is not failed by the runtime's
  `MaxDeliveries`.
- failure keeps the row and sets `not_before` to now plus a backoff of
  e<sup>n</sup> seconds after the job's nth failed delivery, growing over the
  first ten (2.7 s, 7.4 s, 20 s, 55 s, … up to about 6 hours), or the wait a
  `*worlds.ThrottleError` asks for when that is longer, and logs the error. Once
  the message's current job has been delivered 73 times (the runtime's
  `MaxDeliveries` failure on delivery 49 plus 24 retries of its terminal write),
  its last failure deletes the row. Those 24 retries span about 6 days. A row a
  crashed worker left with no attempts
  is deleted when it is next claimed. Every `Redelivery` starts a new job with
  all 73.
- shutdown releases the lease so another worker picks the row up immediately.

Flows of one run execute one at a time across every worker. A claimed
serialized flow (`QueueInput.SerializedFlow`) is marked `running`, and the claim skips a
serialized flow while another of its run is running, taking at most one per
run. A unique index on the run's running flow turns a race between two workers
into a retried claim. A running flow whose worker crashed holds the run's other
flows back until its lease expires and it is claimed again. Step deliveries and
intermediate hops of a wait's timer are never held back.

Delivery is at least once. The runtime's event admission and the in-band fence
make duplicate deliveries inert.

## Recovery

The first `Worker` of a World queues a flow for every pending or running run
before it claims anything, so a run whose process exited before it queued the
run's delivery resumes. A process that runs no `Worker` recovers nothing, and
`Config.SkipRecovery` turns recovery off.

Recovery leaves out a running run that is parked: one of spec 2 or later with a
hook or an open wait, no pending or running step, no open wait that is due, and
no flow of its run in the queue that is due or leased. Its next wake is a hook
delivery or a wait timer that queues its own flow. Each recovery flow carries
the run's spec version and is keyed `startup-recovery:<run ID>`, so starting
more workers while it is queued or executing queues nothing more. The queue
persists, so the flow is not a `Recovery` flow: the run's queued messages still
arrive.

## Start hooks

`EventsCreateRun` with a `StartHook` decides its candidate in one transaction:
it creates the hook `hook_<run ULID>`, claims its token, records the
candidate as accepted, and creates the run. When another run holds the token,
the transaction records the candidate as rejected, naming that run, and
commits, and the call returns a `*worlds.HookConflictError` naming it. A
decision is permanent: every later attempt for the candidate gets the same
answer, an accepted one its `run_created`. Decisions older than 25 hours are
deleted, up to 100 at a time, as new ones are made.

A run ID that already names a run created without a start hook is refused
with `ErrStartHookRejected`.

## Retention

When a run's data is deleted, the run, step, event and remaining hook rows stay
with their IDs, statuses, timestamps and attributes, the payloads in the run's
events are removed from their CBOR, keeping the rest of each event's data, and
its streams' chunks are deleted, leaving each stream empty and closed. The run's
`expired_at` is set and `data_purged` records that its rows no longer hold its
data. Reads hide a run's payloads once `expired_at` has passed.

- **`$retention: "0"`.** The append that finishes the run deletes its data in
  the same transaction, before waiters are notified.
- **`RunsDeleteDataMany`.** Deletes a finished run's data on demand, one
  transaction per run.
- **`Config.Retention`.** When a run finishes, its `expired_at` is set to its
  completion time plus its workflow's retention (`Config.Retentions`, else
  `Retention`). Each `Worker` deletes the data of runs past it in the
  background: at start and then every minute, in batches of 100 with
  `FOR UPDATE SKIP LOCKED`, so workers sharing the database do not wait on
  each other. Changing the retention applies to runs that finish afterwards.

Every writer takes the run's row lock first, so a step that settles after its
run's data was deleted has its result scrubbed as it lands, and a stream write
to a deleted run is refused.

## LISTEN/NOTIFY

Each World opens one dedicated connection on first use and listens on
world-postgres's channels, `workflow_run_events` (the run ID of every committed
write), `workflow_run_status` (sent when a run ends), and `workflow_event_chunk`
(each written chunk's `{streamId, chunkId}`), and on `workflow_queue`. Workers,
flows waiting on their inline steps, `RunsWaitForTerminalStatus`, and
`StreamsGetChunks` wake on these notifications and also re-check every
`PollInterval`, so a lost notification only adds latency. A write sends its
notifications after its transaction commits, since a `NOTIFY` inside the
transaction could fail the write and Postgres serializes the commits of
notifying transactions. A write that ends a run or writes a stream chunk returns
once its notifications are sent. Other writes don't wait for them. A failed
notification never fails the write.

::: warning Connection poolers
Poolers in transaction mode, such as PgBouncer, do not support `LISTEN`. Point
the World at Postgres directly or use session pooling.
:::

## Scaling

Run `Worker` loops across as many processes as needed. `SKIP LOCKED`
gives each row to one worker at a time. Size `Concurrency` to the work your
runners do, and the pool to roughly `Concurrency` plus your API traffic. Each
World uses one extra connection for `LISTEN`.

## Security

- Give the database role only the permissions it needs on the World's schema (plus `CREATE` on the schema for migrations, or migrate with a
  separate role), require TLS, and keep the database reachable only from your
  application and worker network.
- `HTTP(world)` performs no authentication or per-run authorization. Prefer a
  private route from the application to the worker, or wrap `RPC(world)` in
  your own auth middleware.
- The webhook route is authorized only by its token.

## Tests

```sh
docker run -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres
go test -race ./worlds/postgres/...
```

The tests use `DATABASE_URL`, defaulting to
`postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable`. Each
database test creates and drops its own schema and passes it as
`Config.Schema`.
