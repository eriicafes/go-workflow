# Postgres world

`postgresworld` stores workflow state in PostgreSQL and is safe to use from
many processes at once: API servers, workers, or both in one binary.

## Setup

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
pool and leaves it open on `Close`.

When the application and worker share a process, pass the world directly to
`workflow.NewClient`. When they are separate, serve `HTTP(world)` from the
worker and use `workflow.NewHTTPClient(workerURL)` in the application, or
open the same database from both.

The tables are world-postgres's, in `Config.Schema` (default `workflow`).
See [Postgres World](https://go-workflow.vercel.app/worlds/postgres) for the
configuration, the schema and how it differs from world-postgres's, the
worker model, retention, and notifications.

## Differences from world-postgres

Go never shares a database with a JavaScript World, so the World keeps
world-postgres's tables, columns, and channels except where they exist only
for JavaScript, for its graphile-worker queue, or for data written by older
releases:

- **Payloads are stored once, in the events.** The run, step, and hook rows
  name the slot of the event holding each payload, where world-postgres keeps
  CBOR copies in them. `payload_cbor` is Go's own CBOR with payloads as raw
  bytes, not cbor-x, and the deprecated `jsonb` columns are gone.
- **The queue is Go's own `workflow_queue`.** There is no graphile-worker,
  `workflow_run_delivery_leases`, `workflow_job_lock_holders`, `wake_pending`,
  or `job_prefix`. There are no invocation or snapshot tables and no
  `dynamic_workflow_code_cbor` column, since Go has no request/response
  invocations, QuickJS snapshots, or dynamic workflow code.
- **The schema name is configurable** with `Config.Schema`, which
  world-postgres hardcodes, so tests can each use their own.
- **`workflow_runs.spec_version` is an `integer`**, as world-postgres's model
  declares it. Its migrations still create the column as `varchar`.
- **`workflow_runs` has a `data_purged` column,** which world-postgres does
  not: `Config.Retention`'s background deletion finds the expired runs whose
  data is still there by it.
- **Hooks fill in `resume_context`,** which world-postgres declares but never
  writes, so a resume queues its delivery without reading the run. The empty
  `owner_id`, `project_id`, and `environment` columns, which world-postgres
  keeps for Vercel's accounts, are gone.
- **Steps and stream chunks are keyed by run** as well as by step and stream,
  as world-local keys steps, since a Go step or stream name is unique only
  within its run.
- **A purge removes the payloads from each event's CBOR** and keeps the rest
  of its data, where world-postgres clears the whole column. It cuts each
  stream to an `eof` row, as world-local does, where world-postgres keeps the
  chunk rows with their data emptied.
- **A `workflow_event_chunk` notification names its stream by a hash** of its
  run and name. Postgres refuses a notification of 8000 bytes or more, and a
  stream name has no length limit.
- **It serves analytics,** which world-postgres does not: runs and
  attributes from its tables, and steps, events, hooks, and waits from the
  event log.
- **A failed chunk notification is ignored.** world-postgres fails the stream
  write after the chunk committed, so a writer that retries writes it twice.
  Readers also poll, so a lost notification only delays them.
