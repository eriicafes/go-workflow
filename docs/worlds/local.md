# Local World

```go
import localworld "github.com/eriicafes/go-workflow/worlds/local"
```

The local World is the Go implementation of the Workflow SDK's local World,
`@workflow/world-local`, for development and single-machine deployments. It
stores runs, events, steps, waits, hooks, and stream chunks in a SQLite
database, `world.db` in its data directory, and delivers queued work in-process
from an in-memory queue. It needs no worker, queue service, or database server,
and keeps every run across restarts. The SQLite driver is `modernc.org/sqlite`,
which needs no cgo.

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
| `EncryptionKey` | none | 32 random bytes. Encrypts every run's payloads with a key derived from it. See [Data Format and Encryption](/guide/encryption). |
| `EncryptionScope` | empty | The scope run keys are derived from `EncryptionKey` in. Changing it leaves earlier runs' payloads unreadable. |
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
  own, and its hooks and streams; an untagged World's whole database. The World
  stays open.

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
| `steps`, `waits`, `hooks` | The views the events build. A wait's row is keyed by its correlation ID, and a run's waits are deleted when it ends. |
| `hook_tokens` | Each token's owner, unique across runs, with the retention its hook asked for. The owner holds the token while its run is unfinished, then until the retention ends. |
| `hook_disposals` | Hooks whose disposal committed, and the claim that took the token from a force-claimed one. The hook's own row is deleted, as is a finished run's hook once its token retention ends. |
| `hook_resumes` | Deduplicated hook resumes, by run and resume ID. |
| `start_hook_rejections` | Start hook candidates refused for good. |
| `stream_chunks` | Stream chunks, numbered from 1, and each closed stream's end marker after its last chunk. A stream of a run whose data was deleted has only its end marker, in row 0. |

It differs from world-local's in these ways:
- It has no `snapshots` table, since Go has no dynamic workflow code.
- An event's `doc` holds its data as CBOR (`worlds.EncodeEventData`), with
  payloads as raw bytes. The encoding is Go's own, not cbor-x.
- The run, step, and hook views hold no payloads. Each records the slot of the
  event that holds its payload, and reads load it from there.

Times are recorded to the millisecond. See
[Differences from world-local](#differences-from-world-local) for how the
World's behavior differs.

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
stream chunks it creates. Reading or writing one run is scoped by tag: a
tagged World finds its own tag's runs and untagged ones, and an untagged World
only untagged runs. Nothing else is: `RunsList` lists the runs of every tag,
and steps, events, hooks, and streams are read by ID whatever their tag.
`Prepare` recovers only the World's own tag's runs, and `Clear` deletes only
its tag's data, so tests can share one directory with a tag each.

## Start hooks

`EventsCreateRun` with a `StartHook` creates the hook `hook_<run ULID>` and
claims its token in the same transaction as the run. It refuses with a
`*worlds.HookConflictError` when another run holds the token, and records the
refusal, so the candidate stays refused. A candidate older than a day is
refused with `ErrStartHookRejected`.

## Deleting run data

A run created with `$retention: "0"` loses its data when it finishes, and
`RunsDeleteDataMany` deletes a finished run's data on demand. The run, step,
event, and remaining hook rows stay. The payloads in the run's events go, and
its streams are cut to their end markers, in the same transaction that finishes
the run or sets its `ExpiredAt`. A step that settles after its run's data was
deleted has its result scrubbed as it lands, and a deleted run's streams refuse
writes. After a deletion the World checkpoints the WAL and returns the freed
pages to the file system.

`RunsDeleteDataMany` reports `already_deleted` for a run whose data was deleted
before. It keeps that run's first `ExpiredAt` and deletes again whatever is
left of its data. A run ID that is not safe (see [IDs](#ids-and-pages)) is
reported `not_found`, and a deletion the database fails is reported `failed`
with the code `internal_error`.

## IDs and pages

- **IDs.** A run ID, step ID, hook ID, event ID, or correlation ID that is
  empty or contains `.`, `/`, `\`, or a NUL byte is refused with
  `ErrEventInvalid`, on a write and on a read by ID.
- **Runs, steps, and hooks** are paged by creation time, to the millisecond,
  then ID. The cursor is `<time>|<id>`, such as
  `2026-01-02T03:04:05.006Z|step_01J…`. A cursor that does not parse starts
  from the first page.
- **Events** are paged by position. The cursor is `key:<event ID>`; a bare
  event ID and `<time>|<event ID>` are read too, and a cursor that names no
  event starts from the first page. A page ends early, with `HasMore`, once
  it holds 8 MiB of stored events, and always holds at least one.
- **Stream chunks.** The cursor is base64 of `{"i":<count>}`, the number of
  chunks before the next one. A plain decimal count is read too.

## Queue

The queue is kept in memory.

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
  deliveries, handoffs and re-runs aside. It does not expire: its `Retention`
  is not applied.
- **Counts.** `DeliveryCount` counts every delivery, and `Attempt` the same
  without handoffs and re-runs.

`NotBefore` has no limit, so `Capabilities().MaxQueueDelay` is zero.

## Restart recovery

The queue is in memory, so a restart loses every message a run had queued.
`Prepare` queues every active run of the World's own tag again:

1. Steps that a previous process was running inline are released with
   `step_retrying`, which records the step's name and the error "The process
   running this step exited before it finished." A released step is sent as
   a step delivery.
2. The run gets a flow with `QueueInput.Recovery`, which sends again the
   messages of its steps and the timers of its waits.
3. Each of its open waits due within about 24.8 days gets a backstop wake,
   keyed `recovery-wait:<run>:<wait>:<resumeAt ms>`.

Set `SkipRecovery` for a process, such as a script, that should not take over
runs.

## Waiting

The local World does not push events: a flow polls the run's log, and does
not linger. `RunsWaitForTerminalStatus` and `StreamsGetChunks` wake on this
process's commits, and poll every 100 ms for other processes'.

## Differences from world-local

The World keeps world-local's tables, refusals, and stored values. Besides
the [database's encoding](#database), it differs where Go's `worlds.World`
interface or runtime does:

- **A forced hook claim within one run advances `seq_in_band` by two.** The
  claim appends the previous hook's `hook_disposed` and its own
  `hook_created` to the same run, and Go's interface counts every position
  an in-band write allocates (`AllocatedEventCount` is 2). world-local
  advances by one.
- **The events a write returns.** `EventResult.Events` holds the events
  after `EventCount`, or after `SinceCursor` when `EventCount` is unset, that
  were in the log before the write, without the written event and without a
  page limit. world-local returns a page of 20 from `sinceCursor` that
  includes the written event, gives `sinceCursor` precedence, and returns
  the whole log with `run_started`.
- **Stream reads.** `StreamsGetChunks` always returns a cursor and reports
  `Done` whenever the stream is closed, also on a page with `HasMore`, since
  Go follows a live stream by polling with the cursor. world-local returns a
  cursor only with `hasMore`, reports `done` on the last page, and follows a
  live stream with a `ReadableStream`.
- **A resume's digest is checked.** A `hook_received` whose
  `ResumePayloadDigest` is not the digest of its payload is refused with
  `ErrEventInvalid`. world-local stores the digest unchecked. A correct
  caller never sees the difference.
- **Orphaned steps are found by their creation.** Recovery releases a
  running step that was created inline, which is how a spec 9 run marks a
  step its flow runs. world-local reads `ownerMessageId` from the step's
  latest `step_started`, which only a runtime below spec 9 writes.
- **No legacy data.** There is one schema version, so the World writes no
  `version.txt` and prints no legacy notice.
- **Batches and analytics.** The World appends batches (`EventsCreateBatch`)
  and serves analytics, which world-local does not.
