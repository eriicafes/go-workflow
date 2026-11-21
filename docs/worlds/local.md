# Local World

```go
import localworld "github.com/eriicafes/go-workflow/worlds/local"
```

The local World is a port of the Workflow SDK's `@workflow/world-local` for
development and single-machine deployments. It stores runs, events, steps,
waits, hooks, and stream chunks in a SQLite database, `world.db` in its data
directory, and delivers queued work in-process from an in-memory queue. It
needs no worker, queue service, or database server, and keeps every run
across restarts. The SQLite driver is `modernc.org/sqlite`, which needs no cgo.

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
| `Dir` | `WORKFLOW_LOCAL_DATA_DIR`, or `.workflow-data` | Data directory, created with `0700` permissions. It holds `world.db`. |
| `Tag` | untagged | Scopes the runs, hooks, and streams this World writes, such as a test's. See [Tags](#tags). |
| `BaseURL` | `http://localhost:3000/v1/workflow` | Public URL of `HTTP(world)`. Webhook URLs are built from it. |
| `EncryptionKey` | none | 32 random bytes. Encrypts every run's payloads with a key derived from it, as in the Workflow SDK. See [Data Format and Encryption](/guide/encryption). |
| `Concurrency` | `WORKFLOW_LOCAL_QUEUE_CONCURRENCY`, or 1000 | How many deliveries execute at once. |
| `HookRetentionLimit` | `WORKFLOW_LOCAL_HOOK_RETENTION_LIMIT_DAYS`, or 30 days | The longest token retention a hook may ask for. A longer one is refused with `ErrEventInvalid`, or `ErrStartHookRejected` for a start hook. |
| `SkipRecovery` | false | Leaves the active runs alone when the World is prepared. See [Restart recovery](#restart-recovery). |

## Lifecycle

- **`Open(config, runners)`** returns a World. It reads no state.
- **`Prepare(ctx)`** opens the database, creating its schema in a new
  directory, begins delivering queued work, and queues the active runs of the
  World's tag again. Cancelling `ctx` stops delivery.
- **Methods that read or write data require a prepared World.**
  They return `ErrNotPrepared` before `Prepare` and `ErrClosed` after `Close`.
- **`Close()`** stops delivery, waits for in-flight deliveries, and closes the
  database. Queued messages are lost, and the next `Prepare` queues the
  active runs again.
- **`Clear(ctx)`** requires a prepared World. It cancels the deliveries in
  flight and deletes the World's data: a tagged World's runs, everything they
  own, and its hooks and streams; an untagged World's whole database. The
  World stays open, as world-local's `clear()` leaves it.

Several processes may share a data directory, such as an application and a
script that starts runs. The database serializes their writes. Restart
recovery assumes that no other live process runs workflows on the same
directory and tag.

## HTTP

`localworld.HTTP(world)` serves the RPC and webhook endpoints under
`/v1/workflow`, `HTTPPrefix(world, prefix)` serves them under `prefix`, and
`RPC` and `Webhook` return each endpoint on its own. Serve
`HTTP` to use the [web UI](/tooling/web-ui) or a separate application process.

## Database

The schema is world-local's:

| Table | Holds |
| --- | --- |
| `runs` | One row per run: its tag, workflow, status, creation time, event count (`seq`), in-band fence (`seq_in_band`), and the run as JSON. |
| `events` | One row per event, keyed by run and slot, with its type and correlation ID. |
| `steps`, `waits`, `hooks` | The views the events build. A run's waits are deleted when it ends. |
| `hook_tokens` | Each token's owner, unique across runs, kept past its run's end while the hook's retention lasts. |
| `hook_disposals` | Hooks whose disposal committed, and the claim that took the token from a force-claimed one. The hook's own row is deleted, as is a finished run's hook once its token retention ends. |
| `hook_resumes` | Deduplicated hook resumes, by run and resume ID. |
| `start_hook_rejections` | Start hook candidates refused for good. |
| `stream_chunks` | Stream chunks, numbered from 1, and each closed stream's end marker. |

It differs from world-local's in these ways:
- It has no `snapshots` table, since Go has no dynamic workflow code.
- An event's `doc` holds its data as CBOR (`worlds.EncodeEventData`), with
  payloads as raw bytes. The encoding is Go's own, not cbor-x.
- The run, step, and hook views hold no payloads. Each records the slot of the
  event that holds its payload, and reads load it from there.

The database runs in WAL mode with `synchronous=NORMAL`, `secure_delete`, and
incremental vacuum. Every write is one `BEGIN IMMEDIATE` transaction, which
waits up to 30 seconds for another writer. An append, a batch, a run's
creation with its start hook, and a forced hook claim with the disposal it
records in the previous owner's run each commit at once, so there is nothing
to repair after a crash.

`PRAGMA user_version` holds the schema version, 1. `Prepare` refuses a
database written by a newer release. Development tools should use the client
or RPC API rather than reading the database.

## Tags

A tagged World writes its tag on the runs, hooks, start hook rejections, and
stream chunks it creates. It sees its own tag's rows and untagged ones. An
untagged World sees only untagged rows. A run's steps, waits, and events belong
to its tag. Tests can share one directory with a tag each, and `Clear` deletes
only that tag's data.

## Start hooks

`EventsCreateRun` with a `StartHook` creates the hook `hook_<run ULID>` and
claims its token in the same transaction as the run. It refuses with a
`*worlds.HookConflictError` when another run holds the token, and records the
refusal, so the candidate stays refused. A candidate older than a day is
refused with `ErrStartHookRejected`.

## Deleting run data

A run created with `$retention: "0"` loses its data when it finishes, and
`RunsDeleteDataMany` deletes a finished run's data on demand, as world-local
does. The run, step, event, and remaining hook rows stay. The payloads in the run's
events go, and its streams are cut to their end markers, in the same
transaction that finishes the run or sets its `ExpiredAt`. A step that
settles after its run's data was deleted has its result scrubbed as it lands,
and a deleted run's streams refuse writes. After a deletion the World
checkpoints the WAL and returns the freed pages to the file system.

## Queue

The queue is a port of world-local's, kept in memory as world-local's is.

- **Messages.** IDs are `msg_<ULID>`, kept across redeliveries.
  `IdempotencyKey` deduplicates while the message with the key is in flight.
- **Delays.** A message's `NotBefore` is slept before it takes a delivery
  slot, so a delayed message holds no slot.
- **One flow per run.** One serialized flow of a run
  (`QueueInput.SerializedFlow`) executes at a time, in the order they were
  queued. Step deliveries and intermediate hops of a wait's timer are never
  held.
- **Coalescing.** A flow waiting behind its run's executing flow is coalesced
  into it: when the executing delivery succeeds, it runs again, up to 32
  times, to cover the flows it took, which reads the run's log. A run's first
  flow, a wake that completes waits, and a recovery flow are never coalesced.
- **Handoffs.** A redelivery of zero keeps the run's gate and is delivered at
  once. It does not count as an attempt, up to 10,000 times.
- **Retries.** A positive redelivery releases the gate and is delivered after
  its delay. A failed delivery is retried after 5 seconds for its first 32
  failures, then after a delay that doubles up to a minute, or after the wait
  a `*worlds.ThrottleError` asks for. A message is dropped after 256
  deliveries, handoffs and re-runs aside, or once it is older than its
  `Retention`.
- **Counts.** `DeliveryCount` counts every delivery, and `Attempt` the same
  without handoffs and re-runs.

`NotBefore` has no limit, so `Capabilities().MaxQueueDelay` is zero.

## Restart recovery

The queue is in memory, so a restart loses every message a run had queued.
`Prepare` queues every active run of the World's own tag again:

1. Steps that a previous process was running inline are released with
   `step_retrying`, as world-local's `orphaned-steps.js` does. A released
   step is sent as a step delivery.
2. The run gets a flow with `QueueInput.Recovery`, which sends again the
   messages of its steps and the timers of its waits.
3. Each of its open waits due within about 24.8 days gets a backstop wake,
   keyed `recovery-wait:<run>:<wait>:<resumeAt ms>`.

Set `SkipRecovery` for a process, such as a script, that should not take over
runs.

## Waiting

The local World does not push events, as world-local does not: a flow polls
the run's log, and does not linger. `RunsWaitForTerminalStatus` and
`StreamsGetChunks` wake on this process's commits, and poll every 100 ms for
other processes'.
