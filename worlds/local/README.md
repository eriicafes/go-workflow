# Local world

`localworld` is a durable, single-process World for local development. It
stores every run, event, step, hook, stream chunk, and queued delivery as its
own JSON file, and delivers queued work in-process, so local work needs no
worker, queue service, or database and survives a restart.

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

`Config.BaseURL` is the public URL where `localworld.HTTP(world)` is mounted;
it defaults to `http://localhost:3000/v1/workflow`. `HTTP` serves the RPC and
webhook endpoints under `/v1/workflow`; `RPC` and `Webhook` return each
endpoint on its own.

## Lifecycle

- `Open(config, runners)` creates `Dir` with `0700` permissions and owns the
  directory handle. `New(root, config, runners)` borrows a caller-owned
  `*os.Root`, which the caller closes after `Close`. Neither reads any state.
- `Prepare(ctx)` takes an exclusive `flock` on `locks/world.lock`, validates
  `version.json`, recovers interrupted writes, releases stream write sessions
  left by the previous process, and begins delivering every persisted queue
  item. It returns `ErrInUse` when another World (in this or another process)
  holds the directory. Cancelling `ctx` stops delivery.
- **Every other method requires a prepared World** and returns `ErrNotPrepared`
  before `Prepare` and `ErrClosed` after `Close`.
- `Close()` stops delivery, waits for in-flight deliveries, releases the lock,
  and closes a directory opened by `Open`. Interrupted deliveries stay queued
  and run again after the next `Prepare`.
- `Clear(ctx)` requires a prepared World. It stops delivery, removes all
  persisted state while holding the lock, writes a fresh `version.json`, and
  closes the World. Open a new World to continue.

The local world supports exactly one process per directory. Use the Postgres,
AWS, or Vercel worlds for shared or production execution. Development tools
should use the workflow client or RPC API rather than reading these files.

## Layout

```
version.json                               storage format version
locks/world.lock                           exclusive flock
runs/<run>.json                            run view + eventCount (commit point)
events/<run>/<eventID>.json                one file per event; IDs are slot-padded
steps/<run>/<step>.json                    step views
hooks/<hook>.json                          hook views
tokens/<token>.json                        {hookId, retainUntil}: token ownership
streams/<run>/<name>/meta.json             {name, closed, session, sequence, count}
streams/<run>/<name>/<%010d>.json          one file per chunk
queue/<messageID>.json                     durable delivery (ULID IDs)
queue-keys/<run+key>.json                  idempotency key -> pending message ID
```

Run, step, and hook IDs, tokens, stream names, and idempotency keys are
user-controlled, so their file names are hex-encoded (names longer than 64
bytes use a SHA-256 digest); each record stores the original value. Hex also
keeps names distinct on case-insensitive file systems.

## Durability and crash consistency

Every file is written to a temporary sibling, fsynced, renamed into place, and
its directory is synced. Reads reject malformed records and unknown fields.

One in-process lock serializes mutations; the `flock` guarantees one process.
Each append, whether a single event or a batch, writes its event files first,
then the step, hook, and token files it changes, and finally `runs/<run>.json`
with the new `eventCount`. That run file is the commit point: readers only
read slots up to `eventCount`, and readers never observe a mutation in
progress.

When `Prepare` finds an event file past a run's `eventCount`, it rebuilds that
run's steps, hooks, and token claims by replaying the committed events, then
deletes the uncommitted event files. A token now owned by another run's hook
is left alone. Event directories without a run file (an interrupted create)
are removed. Batches are therefore all-or-nothing even across crashes. If a
write fails while the process is running, the same repair runs immediately;
if the repair also fails, the World refuses further calls until it is
restarted.

Streams commit through `meta.json`: chunk files at or past `count` are
ignored and overwritten by the next write.

## Queue

`Queue` writes `queue/<id>.json` and schedules it in an in-process timer heap
ordered by `NotBefore`; a single timer tracks the earliest delivery. Each due
delivery runs `worlds.NewRuntime(world, runners).Execute` in its own
goroutine, and its record is deleted on success. A failed delivery is retried
with exponential backoff from 100ms up to 30s. `IdempotencyKey` deduplicates
per run while the delivery is pending; the key is released when delivery
starts, so work queued during a delivery is never dropped. `QueueCancel`
deletes the record and a scheduled timer for it is skipped.

`RunsWaitForTerminalStatus` and `StreamsGetChunks` wait on an in-process
change notification instead of polling.

## Storage versions

The current format is version 2. `Prepare` rejects directories written by older
versions (the version 1 command store, or the unversioned `world.json`
snapshot) and any non-empty directory without `version.json`. Remove the
directory to start over.
