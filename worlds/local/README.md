# Local world

`localworld` is the Go implementation of the Workflow SDK's Local World
(`@workflow/world-local`), for development and single-machine deployments. It stores runs, events, steps,
waits, hooks, and stream chunks in a SQLite database, `world.db` in its data
directory, and delivers queued work in-process from an in-memory queue.

```go
runners := worlds.Runners(workflow.Use(flows.GenerateReport))

world, err := localworld.Open(localworld.Config{Dir: ".workflows"}, runners)
if err != nil {
	return err
}
defer world.Close()

if err := world.Prepare(ctx); err != nil {
	return err
}

client := workflow.NewClient(world)
```

`Prepare` opens the database, begins delivering queued work, and queues the
active runs of the World's tag again, since a restart lost their queued
messages. Several processes may share a data directory: the database
serializes their writes.

See [Local World](https://go-workflow.vercel.app/docs/worlds/local) for the
configuration, the schema and how it differs from world-local's, tags, the
queue, and restart recovery.

## Differences from world-local

Go never shares a data directory with a JavaScript World, so the World keeps
world-local's tables and behavior except where they exist only for
JavaScript or for data written by older releases:

- **No `snapshots` table.** world-local keeps images of the QuickJS virtual
  machine its workflows run in, and Go does not run workflows in one.
- **Payloads are stored once, in the events.** The run, step, and hook views
  record the slot of the event holding each payload, where world-local copies
  the payload into every view.
- **Event data is Go's own CBOR**, with payloads as raw bytes rather than
  cbor-x, and view docs are plain JSON without world-local's `__type` tags,
  since no JavaScript reads them.
- **There is one schema version and no legacy data**, so the World writes no
  `version.txt` and prints no legacy notice.
- **A forced hook claim within one run advances `seq_in_band` by two**, one
  for each event it appends, where world-local advances by one: Go's World
  interface counts every position an in-band write allocates.
- **A write returns the events before it**, after the caller's `EventCount`
  or `SinceCursor`, without a page limit. world-local returns a page of 20
  that includes the written event, and the whole log with `run_started`.
- **`StreamsGetChunks` always returns a cursor and reports `Done` whenever
  the stream is closed**, since Go follows a live stream by polling with the
  cursor, where world-local hands out a `ReadableStream`.
- **A resume's digest is checked against its payload**, which world-local
  stores unchecked.
- **Recovery finds an orphaned step by its inline creation**, since Go's
  `step_started` records no owning queue message.
- **It appends batches** (`EventsCreateBatch`), which world-local does not,
  since Go's runtime records a flow's steps and timers in one fenced write.
- **It serves analytics,** which world-local does not: runs and attributes
  from its tables, and steps, events, hooks, and waits from the event log.
