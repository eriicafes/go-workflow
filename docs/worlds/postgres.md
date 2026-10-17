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

runners := worlds.Runners(workflow.Use(flows.GenerateReport))
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
| `URL` | required by `Open` | Connection string. |
| `PollInterval` | 250 ms | Longest sleep of workers and waiters when no notification arrives. |
| `Concurrency` | 10 | Deliveries one `Worker` call executes at once. |
| `LeaseDuration` | 1 min | How long a claimed delivery is hidden from other workers. Workers renew it while executing, so it only bounds how quickly a crashed worker's delivery is retried. |

## Entry points

- **`Worker(ctx, world, runners)`**: the queue worker. It blocks until `ctx` is
  cancelled, waits for in-flight deliveries, and returns `ctx.Err()`.
- **`HTTP(world)`**: RPC and webhook endpoints under `/v1/workflow`.
- **`HTTPPrefix(world, prefix)`**: the same endpoints under `prefix`.
- **`RPC(world)`** and **`Webhook(world)`**: each endpoint alone.

When the application and worker share a process, pass the World to
`workflow.NewClient`. When they are separate, serve `HTTP(world)` from the
worker and use `workflow.NewHTTPClient` in the application, or open the same
database from both.

## Schema and migrations

`Open` and `New` migrate the schema. Migrations run in one transaction under
`pg_advisory_xact_lock`, so processes starting together migrate once, and the
applied versions are recorded in `go_workflow_schema_version`. A database with a
newer version than the library knows is rejected.

Tables are created in the connection's `search_path`; set `search_path` in the
connection string or pool config to use a dedicated schema.

| Table | Contents |
| --- | --- |
| `go_workflow_runs` | Run view plus `event_count`; the next slot is `event_count + 1`. |
| `go_workflow_events` | Event log keyed by `(run_id, slot)`. |
| `go_workflow_steps` | Step view keyed by `(run_id, step_id)`. |
| `go_workflow_hooks` | Hook view, including webhook settings and retention. |
| `go_workflow_hook_tokens` | Token ownership; the primary key makes a token single-owner. |
| `go_workflow_queue` | Pending deliveries with `not_before`, `attempts`, and `lease_until`. |
| `go_workflow_streams` | Stream state: `closed`, writer `session`, `sequence`, `chunk_count`. |
| `go_workflow_stream_chunks` | Stream chunks keyed by `(run_id, name, idx)`. |

Payloads are `bytea`, attributes are `jsonb`, and timestamps are `timestamptz`
at microsecond precision; times returned by the World are UTC.

## Consistency

Every event append is one transaction. It locks the run row with
`SELECT ... FOR UPDATE`, checks the transition rules, inserts the events at the
next slots, and updates the views. Writers to one run are serialized across
processes, slots are contiguous, and a batch is all-or-nothing.

`EventsCreate(hook_created)` locks the token row. A forced claim over a live owner also locks
the owner's run, taking run locks in run ID order to avoid deadlocks. Exactly
one concurrent claimant wins a free token. Transactions are retried on
serialization failures and deadlocks.

## Worker model

`Queue` inserts a row and sends `NOTIFY go_workflow_queue` in the same
transaction. An `IdempotencyKey` deduplicates against a pending row of the same
run; once a worker claims the row the key is released, so a message queued
during execution is never dropped.

`Worker` claims due rows with `FOR UPDATE SKIP LOCKED`, sets `lease_until`,
increments `attempts`, and executes each:

- success deletes the row;
- failure keeps it and sets `not_before` to now plus a capped exponential
  backoff (1 s, 2 s, 4 s, … up to 5 minutes), and logs the error;
- shutdown releases the lease so another worker picks the row up immediately.

## LISTEN/NOTIFY

Each World opens one dedicated connection on first use and listens on
`go_workflow_queue`, `go_workflow_runs` (sent when a run turns terminal), and
`go_workflow_streams`. Workers, `RunsWaitForTerminalStatus`, and
`StreamsGetChunks` wake on these notifications and also re-check every
`PollInterval`, so a lost notification only adds latency.

::: warning Connection poolers
Poolers in transaction mode, such as PgBouncer, do not support `LISTEN`. Point
the World at Postgres directly or use session pooling.
:::

## Scaling

Run as many `Worker` loops, in as many processes, as you like; `SKIP LOCKED`
gives each row to one worker at a time. Size `Concurrency` to the work your
runners do, and the pool to roughly `Concurrency` plus your API traffic. Each
World uses one extra connection for `LISTEN`.

## Security

- Give the database role only the permissions it needs on the `go_workflow_`
  tables (plus `CREATE` on the schema for migrations, or migrate with a
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
`postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable`. Each test
creates and drops its own schema.
