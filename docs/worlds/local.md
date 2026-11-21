# Local World

```go
import localworld "github.com/eriicafes/go-workflow/worlds/local"
```

The local World is a durable, single-process World for development and small
deployments. It stores every run, event, step, hook, stream chunk, and queued
delivery as its own JSON file, and delivers queued work in-process. It needs no
worker, queue service, or database, and survives a restart.

## Usage

```go
runners := core.Runners(workflow.Use(flows.GenerateReport))

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

## Configuration

| Field | Default | Meaning |
| --- | --- | --- |
| `Dir` | required by `Open` | Storage directory. Created with `0700` permissions. |
| `BaseURL` | `http://localhost:3000/v1/workflow` | Public URL of `HTTP(world)`. Webhook URLs are built from it. |
| `EncryptionKey` | none | 32 random bytes. Encrypts every run's payloads with a key derived from it, as in the Workflow SDK. See [Data Format and Encryption](/guide/encryption). |

## Lifecycle

- **`Open(config, runners)`** creates `Dir` and owns the directory handle.
  **`New(root, config, runners)`** borrows a caller-owned `*os.Root`, which the
  caller closes after `Close`. Neither reads any state.
- **`Prepare(ctx)`** takes an exclusive `flock` on `locks/world.lock`, validates
  `version.json`, recovers interrupted writes, and begins delivering every persisted queue
  item. It returns `ErrInUse` when another World, in this or another process,
  holds the directory. Cancelling `ctx` stops delivery.
- **Methods that read or write data require a prepared World.**
  They return `ErrNotPrepared` before `Prepare` and `ErrClosed` after `Close`.
- **`Close()`** stops delivery, waits for in-flight deliveries, releases the
  lock, and closes a directory opened by `Open`. Interrupted deliveries stay
  queued and run again after the next `Prepare`.
- **`Clear(ctx)`** requires a prepared World. It stops delivery, removes all persisted state while holding the
  lock, writes a fresh `version.json`, and closes the World. Open a new World to
  continue.

::: warning One process per directory
The local World supports exactly one process per directory. Use Postgres, AWS,
or Vercel for shared or production execution.
:::

## HTTP

`localworld.HTTP(world)` serves the RPC and webhook endpoints under
`/v1/workflow`, `HTTPPrefix(world, prefix)` serves them under `prefix`, and
`RPC` and `Webhook` return each endpoint on its own. Serve
`HTTP` to use the [web UI](/tooling/web-ui) or a separate application process.

## Layout

```
version.json                               storage format version
locks/world.lock                           exclusive flock
runs/<run>.json                            run view + eventCount (commit point)
events/<run>/<eventID>.json                one file per event; IDs are slot-padded
steps/<run>/<step>.json                    step views
hooks/<hook>.json                          hook views
tokens/<token>.json                        {hookId, tokenRetentionUntil}: token ownership
streams/<run>/<name>/meta.json             {name, closed, count}
streams/<run>/<name>/<%010d>.json          one file per chunk
queue/<messageID>.json                     durable delivery (ULID IDs)
queue-keys/<key>.json                      idempotency key -> live message ID
```

Run, step, and hook IDs, tokens, stream names, and idempotency keys are
user-controlled, so their file names are hex-encoded (names longer than 64
bytes use a SHA-256 digest). Each record stores the original value. Hex also
keeps names distinct on case-insensitive file systems.

Development tools should use the client or RPC API rather than reading these
files.

## Durability

Every record is written to a temporary sibling, fsynced, renamed into place,
and its directory is synced where the file system supports it. Reads reject malformed records and unknown fields.

Each append writes its event files first, then the step, hook, and token files
it changes, and finally `runs/<run>.json` with the new `eventCount`. That run
file is the **commit point**: readers only read slots up to `eventCount`, so
they never observe a mutation in progress.

When `Prepare` finds an event file past a run's `eventCount`, it rebuilds that
run's steps, hooks, and token claims by replaying the committed events, then
deletes the uncommitted event files. Batches are therefore all-or-nothing even
across crashes. If a write fails while the process is running, the same repair
runs immediately. If that repair also fails, the World refuses further calls
until it is restarted.

Streams commit through `meta.json`: chunk files at or past `count` are ignored
and overwritten by the next write.

## Start hooks

`EventsCreateRun` with a `StartHook` writes the hook `hook_<run ULID>` and its
token file before the run file, which commits them with the run, and refuses
with a `*worlds.HookConflictError` when another run holds the token. `Prepare`
removes a start hook and token whose run file was never written.

## Deleting run data

A run created with `$retention: "0"` loses its data when it finishes, and
`RunsDeleteDataMany` deletes a finished run's data on demand, as the Workflow
SDK's local World does. The run, step, event and hook records stay; the
payloads in the run's event files go, and its streams are left empty and
closed.

The append that finishes the run commits its `ExpiredAt` with it, so the data
is unreadable from that commit on. The event and chunk files are scrubbed next,
and the run file records `dataPurged`. If that is interrupted, reads keep
scrubbing what is left, and `Prepare` finishes the purge. A step that settles
after its run's data was deleted has its result scrubbed as it lands, and a
deleted run's streams refuse writes.

## Queue

`Queue` writes `queue/<id>.json` and schedules it in an in-process timer heap
ordered by `NotBefore`. Each due delivery runs in its own goroutine. The
message is deleted when a delivery succeeds and lives until then, keeping its
message ID across redeliveries.

- Every delivery sets `DeliveryCount` (1-based, rising on every redelivery)
  and `Attempt` (the same count without handoffs). Both are persisted when the
  delivery starts, so a delivery interrupted by a crash counts too.
- When the runtime returns a `worlds.Redelivery`, the message is delivered
  again after its delay, which counts as an attempt. A redelivery of zero is a
  handoff, delivered at once without counting as an attempt. Any other error
  is retried with exponential backoff from 100 ms up to 30 s, or after the
  wait a `*worlds.ThrottleError` asks for when that is longer. The next due
  time is stored in the message, so a restart keeps it.
- `IdempotencyKey` deduplicates across the directory: while a message with the
  key lives, `Queue` with the same key stores nothing and returns that
  message's ID. Messages without a key are never deduplicated.
- One serialized flow of a run executes at a time (`QueueInput.SerializedFlow`):
  a later flow of the run waits until the executing one is acknowledged, fails,
  or asks for a redelivery. Step deliveries and intermediate hops of a wait's
  timer are never held. Because only one process uses a directory, this gate
  is in-process.
- A message with a `Retention` is dropped instead of delivered once it is older
  than that. Zero keeps it until it succeeds. `NotBefore` has no limit, so
  `Capabilities().MaxQueueDelay` is zero.

`RunsWaitForTerminalStatus` and `StreamsGetChunks` wait on an in-process change
notification instead of polling.

## Storage versions

The current format is version 1. `Prepare` rejects directories written with
any other version and any non-empty directory without `version.json`. Remove
the directory to start over.

Forced hook takeover across runs first records a `hook-handoff.json` write-ahead decision. Recovery completes both sides before serving reads, including a crash between their commit points. Same-run takeover uses one run commit.
