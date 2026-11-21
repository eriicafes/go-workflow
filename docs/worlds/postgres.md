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
| `EncryptionKey` | none | 32 random bytes. Encrypts every run's payloads with a key derived from it, as in the Workflow SDK. See [Data Format and Encryption](/guide/encryption). |
| `URL` | `WORKFLOW_POSTGRES_URL`, or `DATABASE_URL` | Connection string, used by `Open`. |
| `Schema` | `workflow` | The schema the World's tables live in, created when missing. |
| `HookRetentionLimit` | `WORKFLOW_POSTGRES_HOOK_RETENTION_LIMIT_DAYS`, or 30 days | The longest token retention a hook may ask for. A longer one is refused with `ErrEventInvalid`, or `ErrStartHookRejected` for a start hook. |
| `PollInterval` | 250 ms | Longest sleep of workers and waiters when no notification arrives. |
| `Concurrency` | 10 | Deliveries one `Worker` call executes at once. |
| `LeaseDuration` | 1 min | How long a claimed delivery is hidden from other workers. Workers renew it while executing, so it only bounds how quickly a crashed worker's delivery is retried. |
| `Routes` | none | The queue of each routed workflow. See [routing](#routing-workflows-to-queues). |
| `Retention`, `Retentions` | keep forever | How long finished runs keep their data, by default and per workflow. See [retention](#retention). |

## Entry points

- **`Worker(ctx, world, runners, options...)`**: the queue worker. It blocks
  until `ctx` is cancelled, waits for in-flight deliveries, and returns
  `ctx.Err()`. `WorkerOptions.Queues` limits the queues it claims, and
  `WorkerOptions.Concurrency` overrides `Config.Concurrency`.
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
| `workflow_runs` | Runs, keyed by `id`, with `status` as the `status` enum, `name`, `attributes`, `error_code`, `start_hook`, and the slot of the run's result. |
| `workflow_events` | The event log, keyed by `(run_id, id)`, where `id` is the slot's `evnt_` ID. `payload_cbor` holds the event's data. |
| `workflow_event_slots` | Each run's in-band fence, `seq_in_band`. |
| `workflow_steps` | Steps, keyed by `(run_id, step_id)`, with the slots of their input and result. |
| `workflow_hooks` | Hooks. `claims_token` marks the hook that holds its token, unique per token. A hook's row is deleted when it is disposed, and when its run ends unless its token is retained, then when its token is next claimed. |
| `workflow_waits` | A run's waits, deleted when it ends. |
| `workflow_start_hook_admissions` | Each start hook candidate's decision, kept for a day. |
| `workflow_stream_chunks` | Stream chunks, ordered by their `chnk_` ID. An `eof` chunk closes a stream. |
| `workflow_queue` | Live messages: the whole delivery in `payload`, with `not_before`, `deliveries`, `handoffs`, `lease_until`, `running`, and `expires_at`. |

They differ from world-postgres's where Go needs it:
- Payloads are stored once, in the events. The run, step, and hook rows name
  the slot of the event holding each payload, and reads join it.
- `payload_cbor` is Go's own CBOR (`worlds.EncodeEventData`), with payloads as
  raw bytes, not cbor-x. There are no deprecated `jsonb` columns.
- Steps are keyed by run and step, since a step ID is unique only within its
  run, and stream chunks by run as well as stream.
- `workflow_runs.spec_version` is an `integer`, and `data_purged` serves
  `Config.Retention`.
- Hooks keep the run's workflow, deployment, public key, and spec version,
  which wake the run, and when they were last updated and received, which
  the analytics hook reads return.
- The queue is Go's own. There are no graphile-worker, snapshot, or
  invocation tables.

Attributes and execution contexts are `jsonb`, and timestamps are
`timestamptz` at microsecond precision. Times returned by the World are UTC.

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
deduplicated. A delivery with `Retention` expires that long after it was
queued, and workers delete expired rows before claiming.

`Worker` claims due rows with `FOR UPDATE SKIP LOCKED`, sets `lease_until`, and
increments `deliveries`, which fences the lease. Every delivery of a message
has the row's ID as its `MessageID`, `deliveries` as its `DeliveryCount`, and
`deliveries - handoffs` as its `Attempt`. The worker executes each:

- success deletes the row.
- a `worlds.Redelivery` keeps the row and makes it due again after its delay.
  A handoff (a delay of zero) increments `handoffs`, so it is not an attempt.
- failure keeps the row and sets `not_before` to now plus a capped exponential
  backoff by attempt (1 s, 2 s, 4 s, … up to 5 minutes), or the wait a
  `*worlds.ThrottleError` asks for when that is longer, and logs the error.
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

## Start hooks

`EventsCreateRun` with a `StartHook` creates the hook `hook_<run ULID>` and
claims its token in the transaction that creates the run. When another run
holds the token, the transaction rolls back and returns a
`*worlds.HookConflictError` naming it. Each candidate's decision is recorded,
so a refused candidate stays refused, and decisions older than a day are
deleted as new ones are made.

## Retention

A run's data is deleted as the Workflow SDK deletes it: the run, step, event
and remaining hook rows stay with their IDs, statuses, timestamps and attributes, the
payloads in the run's events are removed from their CBOR, keeping the rest of
each event's data, and its streams' chunks are deleted, leaving each stream
empty and closed. The run's `expired_at` is set and
`data_purged` records that its rows no longer hold its data. Reads hide a run's
payloads once `expired_at` has passed.

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
write), `workflow_run_status` (sent when a run ends), and
`workflow_event_chunk` (a written chunk's `{streamId, chunkId}`), and on
`workflow_queue`. Workers, flows waiting on their inline steps,
`RunsWaitForTerminalStatus`, and `StreamsGetChunks` wake on these
notifications and also re-check every `PollInterval`, so a lost notification
only adds latency. As in world-postgres, a write sends its notifications after
its transaction commits, without waiting for them: a `NOTIFY` inside the
transaction could fail the write, and Postgres serializes the commits of
notifying transactions.

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
