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
for JavaScript, for its queue, or for data written by older releases:

- **Payloads are stored once, in the events.** The run, step, and hook rows
  name the slot of the event holding each payload, where world-postgres keeps
  CBOR copies in them. `payload_cbor` is Go's own CBOR with payloads as raw
  bytes, not cbor-x, and the deprecated `jsonb` columns are gone.
- **The queue is Go's own `workflow_queue`.** There is no
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
- **Steps are keyed by run** as well as by step, since a step ID is unique
  only within its run. That key leads with the run, so there is no
  `workflow_steps_run_id_index`. Streams are keyed by name alone, and the run
  is stored with each chunk for `StreamsList`.
- **A purge removes the payloads from each event's CBOR** and keeps the rest
  of its data, where world-postgres clears the whole column. It cuts each
  stream to an `eof` row, where world-postgres keeps the chunk rows with their
  data emptied.
- **A `workflow_event_chunk` notification names its stream by a hash** of its
  name. Postgres refuses a notification of 8000 bytes or more, and a
  stream name has no length limit.
- **A queue row counts its job's attempts** in its payload and its
  `deliveries` and `handoffs` columns, for `Attempt`, `DeliveryCount`, and the
  73 attempts of a job. A failed delivery is retried after world-postgres's
  backoff, e<sup>n</sup> seconds over the job's first ten failures, and, as
  there, a message never expires.
- **`workflow_runs.updated_at` moves only with the run:** on its own events
  and on attribute writes. world-postgres also moves it whenever it sets or
  clears `wake_pending`, which is part of its queue.
- **A late `step_failed` is refused on a finished run** unless its step is
  running, with `*RunExpiredError`, as every Go World and world-local do.
  world-postgres checks only the step and records it.
- **A run ID that names a run created without a start hook is refused** when
  it is created again with one (`ErrStartHookRejected`), as world-local does.
  world-postgres answers with the existing run, for runs created by workers
  that predate start hooks, which Go has none of.
- **A resume of a hook another run took over is redirected**
  (`*HookForceClaimedError`, naming the token's holder) whether or not it
  carries a resume ID. world-postgres redirects only a `hook_received`
  without one. Go's resumes always carry one, so they would lose the redirect.
- **A stream cursor is the index of the next chunk,** in decimal, as in every
  Go World, and the last page has one, so a reader can wait for more.
  world-postgres's is base64 JSON of the last chunk's ID and the next index,
  and is absent on the last page.
- **A closed stream stores nothing more.** A write or a second close on it is
  dropped, where world-postgres appends rows its readers ignore. The first
  write to a new stream of a run whose data was deleted stores an `eof` row,
  so the stream reads as closed, where world-postgres stores nothing and the
  stream reads as open and empty.
- **Startup recovery reads parked runs from the queue.** A due or leased flow
  of the run stands in for world-postgres's `wake_pending` and locked
  orchestrator jobs, and every run is recovered, as there is no `job_prefix`.
  A recovery flow is deduplicated while its message lives.
- **It appends batches** (`EventsCreateBatch`), which world-postgres does
  not, since Go's runtime records a flow's steps and timers in one fenced
  write.
- **It serves analytics,** which world-postgres does not: runs and
  attributes from its tables, and steps, events, hooks, and waits from the
  event log.
- **A failed chunk notification is ignored.** world-postgres fails the stream
  write after the chunk committed, so a writer that retries writes it twice.
  Readers also poll, so a lost notification only delays them.
