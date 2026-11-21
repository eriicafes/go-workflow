# Vercel World

```go
import vercelworld "github.com/eriicafes/go-workflow/worlds/vercel"
```

The Vercel World stores runs in Vercel's managed workflow storage, the
backend the [Workflow SDK](https://workflow-sdk.dev) uses on Vercel, and
delivers work through Vercel Queues. There is no database to provision:
storage is scoped to the deployment's project and environment, and the
deployment's OIDC token authenticates every request.

| Service | Role |
| --- | --- |
| Vercel workflow storage | Event log, run, step, and hook views, hook tokens, and streams |
| Vercel Queues | Delivers queued items with delays up to 23 hours, one flow of a run at a time |

## Usage

```go
world, err := vercelworld.New(vercelworld.Config{
	BaseURL: os.Getenv("WORKFLOW_BASE_URL"), // https://workflows.example.com/v1/workflow
	Topic:   os.Getenv("VERCEL_WORKFLOW_QUEUE_TOPIC"),
})
if err != nil {
	log.Fatal(err)
}
```

Deploy two functions:

```go
// The queue consumer function.
runners := core.Runners(workflow.Use(flows.GenerateReport))
log.Fatal(http.ListenAndServe(":8081", vercelworld.Queue(world, runners)))
```

```go
// The client-facing function.
log.Fatal(http.ListenAndServe(":8081", vercelworld.HTTP(world)))
```

Give the consumer a `queue/v2beta` trigger for the topic:

```json
{
  "functions": {
    "api/queue.go": {
      "experimentalTriggers": [{ "type": "queue/v2beta", "topic": "workflow" }]
    }
  }
}
```

When the code that starts workflows has the World in process, pass it to
`workflow.NewClient`. The HTTP function is then needed only for webhooks,
clients in other processes, or `workflow.LatestDeployment`, which needs the
[deployment route](#latest-deployment).

## Configuration

| Field | Meaning |
| --- | --- |
| `BaseURL` | Required. Public URL of `HTTP(world)` including `/v1/workflow`. |
| `Topic` | Required. Topic deliveries of unrouted workflows are published to. |
| `Topics`, `Routes` | Further topics by queue name, and the queue of each routed workflow. See [routing](#routing-workflows-to-topics). |
| `QueueURL` | A Vercel Queues API root including `/api/v3` that every queue request goes to. Empty routes each message to its region's `https://<region>.vercel-queue.com/api/v3`. Through the API proxy (`Token`), requests name the region in `x-vercel-queue-region`. See [regions](#regions). |
| `OIDCToken` | Returns the deployment's OIDC token for each request. Default `OIDCTokenFromEnv(DefaultOIDCTokenEnv)`. |
| `Token`, `ProjectID`, `TeamID`, `Environment` | A Vercel access token and the project it acts in, for using the storage from outside a deployment. See [outside a deployment](#outside-a-deployment). |
| `BackendURL` | Root of the workflow API. Default `https://vercel-workflow.com/api`, or `https://api.vercel.com/v1/workflow` with `Token`. |
| `HTTPClient` | Client for the workflow API and Vercel Queues. Default `http.DefaultClient`. |
| `DeploymentID` | The deployment this World runs in. Default `VERCEL_DEPLOYMENT_ID`. When set, runs are pinned to the deployment that starts them and their deliveries are published there. See [pinning](/guide/replay#pinning-runs-to-a-deployment) and [latest deployment](#latest-deployment). |

`New` does not contact Vercel.

Vercel rotates OIDC tokens, so `OIDCToken` is called for every request.
`vercelworld.OIDCTokenFromEnv(name)` reads the environment variable `name` on
each call. The default reads `VERCEL_OIDC_TOKEN`.

When a deployment ID is set, Queues requests send `Vqs-Deployment-Id`. A
delivery is published to its run's deployment, or to this World's when the run
has none.

### Outside a deployment

A script, a CLI, or the [web UI](/tooling/web-ui) behind
[world-http](/tooling/world-http) can read and write the same storage with a
Vercel access token:

```go
world, err := vercelworld.New(vercelworld.Config{
	BaseURL:     "https://workflows.example.com/v1/workflow",
	Topic:       "workflow",
	Token:       os.Getenv("VERCEL_TOKEN"),
	ProjectID:   "prj_...",
	TeamID:      "team_...",
	Environment: "production",
})
```

Requests then go through Vercel's API proxy for that project and environment.
Starting runs from there still publishes to Vercel Queues, which needs an OIDC
token, such as the one `vercel env pull` writes to `VERCEL_OIDC_TOKEN`.

## Routing workflows to topics

```go
world, err := vercelworld.New(vercelworld.Config{
	// BaseURL...
	Topic:  "workflow",
	Topics: map[string]string{"backfill": "workflow-backfill"},
	Routes: worlds.Routes(worlds.Route[workflows.Backfill]("backfill")),
})
```

Every delivery of a routed workflow's runs is published to its queue's topic,
and other workflows' to `Topic`. `Queue(world, runners)` consumes whichever topics its
function is triggered by, so give each topic its own trigger, with its own
concurrency, or one function several triggers. `New` rejects a route to a queue
without a topic.

## Entry points

- **`Queue(world, runners)`**: the Vercel Queues push consumer. It executes
  workflows and steps.
- **`HTTP(world, options...)`**: the RPC endpoint, token webhooks, and the
  [deployment route](#latest-deployment) `/v1/workflow/deployment`. Options
  such as `worlds.WithClientToken` protect the client endpoints.
- **`HTTPPrefix(world, prefix)`**: the same routes under `prefix`.
- **`RPC(world)`**: the RPC endpoint alone. Mount it at
  `POST <Config.BaseURL>/rpc`.
- **`Webhook(world)`**: the token webhooks alone. Mount it at
  `<Config.BaseURL>/webhooks/{token}`.
- **`Deployment(world)`**: the deployment route alone. Mount it at
  `GET <Config.BaseURL>/deployment`.

## Storage

Each run's event log is kept by Vercel's workflow API. Every run the World
creates is a spec 9 run, written through the backend's `/v5` event routes:
its events have slot-numbered IDs in a sealed log, its flow's writes are
in-band and fenced, hook tokens can be force-claimed, and their retention
reserved. `run_started` attests the spec 9 executor
(`executorSpecVersion`), and a refused in-band write is a
`worlds.InBandSupersededError` with the run's counters. The backend derives the run, step, and hook views and
enforces token ownership across the whole project and environment, every
region and deployment included.

go-workflow's events have the Workflow SDK's shape, so the backend keeps each
as it keeps a JS run's event: its metadata, such as a step's name or a wait's
resume time, and its payload, already in the SDK's format. The runs, steps,
hooks, and streams of Go runs show in Vercel's dashboard as JS ones do.

| Event | Payload | Metadata |
| --- | --- | --- |
| `run_created` | the run's input | workflow name, deployment, attributes, public key |
| `run_completed`, `run_failed` | the output or error | |
| `step_created` | the step's input | step name, workflow name, inline, creator message |
| `step_completed`, `step_failed`, `step_retrying` | the result or error | step name, attempt, workflow name, retry time, creator message |
| `step_started` | an inline step's input, on its creating start | step name, attempt, workflow name, inline |
| `hook_created` | the hook's metadata | token, retention, webhook, system, forced |
| `hook_received` | the payload | workflow name |
| `wait_created` | | resume time, creator message |
| `attr_set` | | changes and writer |

Replay matches declarations by ID, and a step's retries and cancellation
contexts come from the replay that declares it, so a wait needs only its
resume time.

| Contract | On Vercel |
| --- | --- |
| Batches | One request and one fenced allocation. As the contract allows, the backend keeps the events it admitted when another is refused, and the runtime replays whatever the log holds. |
| Repeated transitions | The backend refuses them as conflicts. The World finds and returns the event that admitted the transition, as the contract asks. |
| Event feed | `EventsSubscribe` opens the run's events socket (`wss://…/websockets/v1/runs/{run}`), as world-vercel's live feed does: it subscribes after a slot, takes the backend's pushed events and heartbeats, reads from the log the events a heartbeat says it missed, and ends when the backend ends the feed, closes the socket, or goes silent for three heartbeat intervals. A flow waiting on an inline step, or lingering, takes events from it. Through the API proxy (`Token`) the feed is unavailable and the flow polls. |
| Stream writes | Each writer opens the backend's stream socket (`wss://…/websockets/v1/runs/{run}/streams/{name}`) in the background, as the Workflow SDK does, and writes through HTTP until it is open. A declined socket keeps the World's writers on HTTP for five minutes. Through the API proxy (`Token`), or with `WORKFLOW_STREAMS_TRANSPORT=http`, writers use HTTP only. |
| Stream reads | Cursors are chunk indexes. The World remembers the backend's cursor for the last index each stream was read to, so polling a stream continues where it stopped. The default stream, whose name is empty, is stored as `_default`. |
| `RunsList` by several statuses | The backend filters by one status, so the World reads a page of each and merges them by creation time. |
| Hooks | `HooksGet` reads the hook and its events, for when it received a payload or was disposed. |
| Analytics | Not supported. `Capabilities().Analytics` is false. |
| Retention | `Config.Retention` and `Config.Retentions`, built with `vercelworld.Retentions(vercelworld.Retain[W](...))`, take `worlds.RetainDefault`, which leaves runs to Vercel's plan, or `worlds.RetainNone`, which creates runs with the Workflow SDK's `$retention: "0"` attribute so the backend deletes their data when they finish. The backend supports no other retention. A run created with its own `$retention` attribute keeps it. |
| Start hooks | `run_created` carries the `startHook`, which the backend creates with the run, as world-vercel sends it. A `409 start-hook-conflict` becomes a `*worlds.HookConflictError` naming the token's owner. |
| Deleting run data | `RunsDeleteDataMany` calls the backend's `/v4/runs/purge`, as world-vercel does, and maps its `purged` and `already_purged` outcomes to `deleted` and `already_deleted`. The backend deletes a `$retention: "0"` run's data when it finishes and reports `expiredAt` on runs. |

### Backend contract

The run-scoped event routes come in two versions, with the same CBOR frame
encoding. `/v5` serves runs of every spec version and is the only route that
writes to a spec 9 run. `/v4` serves runs below spec 9. Listing by
correlation ID stays on `/v4/events` for every run. On `/v5`:

| Part | Contract |
| --- | --- |
| Routes | `POST /v5/runs/{run}/events/{type}`, `POST /v5/runs/{run}/events/batch`, `GET /v5/runs/{run}/events`, and `GET /v5/runs/{run}/events/{event}`. A write to a spec 9 run through `/v4` is refused with a 400. |
| Frame meta | A write carries `specVersion`, `occurredAt`, and, from the run's orchestrator, `inBand: true` with `expectedSeqInBand` and `maxSlot`, the number of events it holds. An in-band write without `expectedSeqInBand` is a 400. `run_started` carries `executorSpecVersion`, and `step_created` carries `inline` and `creatorMessageId`. |
| Counters | Each run has `seq`, its number of positions, and `seqInBand`, the positions allocated to in-band writes, with `seqInBand ≤ seq`. `run_created` takes the first position and counts as in-band. An in-band write advances both by the positions it allocates, and an out-of-band write moves only `seq`. |
| Snapshot | A listing reports the counters it read before listing in the `x-wf-snapshot-seq` and `x-wf-snapshot-seq-in-band` headers and in its end frame's `snapshot`. |
| Fence | An in-band write whose `expectedSeqInBand` is not `seqInBand` is refused with a 412 whose body has `error: "in-band-superseded"`, `seq`, `seqInBand`, and optionally the events the writer was missing. Nothing is written. A batch is fenced as one block. |
| Skipped-slot report | The answer to an in-band write has `allocated`, and `events`, the events in the positions after `maxSlot` and before its own, in order, with `hasMore` when that list is incomplete. A batch answers with one result per frame and the report and `allocated` at the top level. |
| Queue | Vercel Queues stores one message per `Vqs-Idempotency-Key` while it lives, delivers one message per `Vqs-Experimental-Concurrency-Key` at a time, and redelivers the same message after a visibility change or a handoff. |

The World also relies on what the client cannot see, which
`TestBackendAssumptionsLive` checks against a real project:

- The backend enforces the fence only on `/v5`.
- An in-band write's stored `createdAt` is the `occurredAt` it was sent with.
- `step_created` keeps `inline` and `creatorMessageId` in the event's data.
- On a spec 9 run, a finished run refuses `step_created` and `step_started`,
  and still accepts `step_completed`.

## Encryption

The World encrypts payloads as the Workflow SDK does on Vercel, with no
configuration. With a project ID, from `Config.ProjectID` or
`VERCEL_PROJECT_ID`, a run's key is:

- in a Vercel Function, for a run of this deployment, derived from the
  deployment's key, `VERCEL_DEPLOYMENT_KEY`, with HKDF-SHA256 and
  `{projectId}|{runId}`;
- otherwise, such as for a run of another deployment, read from Vercel's API,
  `GET https://api.vercel.com/v1/workflow/run-key/{deploymentId}`, with
  `Config.Token`, `VERCEL_TOKEN`, or the deployment's OIDC token, and
  `Config.TeamID`.

Without a deployment key, or a project ID, payloads are not encrypted. See
[Data Format and Encryption](/guide/encryption).

## Long timers

A message published without a retention keeps Vercel Queues' default of 24
hours, so the World delays it by at most 23 hours, leaving it retained for an
hour after it becomes visible. A message with a retention lives for it, within
the 1 minute to 7 days Vercel Queues accepts, and may be delayed by up to an
hour less. The World reports a `MaxQueueDelay` of 23 hours, and the runtime
reaches a later timer in hops of up to 23 hours, each its own message, so the
World publishes no hops of its own.

## Latest deployment

In a production deployment, runs started with
[`workflow.LatestDeployment`](/guide/replay#moving-chains-to-the-latest-deployment)
start on the deployment the production domain serves, instead of the
deployment that starts them.

- **Asked of the production domain.** The World requests
  `GET <BaseURL>/deployment`, which answers with the deployment serving it.
  Vercel routes the request to the current production deployment, so the
  answer follows new deployments and rollbacks.
- **Needs the deployment route.** `BaseURL` must be on the production domain,
  with `HTTP(world)` or `Deployment(world)` mounted there. `Deployment` serves
  the route alone, without exposing the RPC endpoint. Without the route,
  marked runs stay on the deployment that starts them.
- **Reused for an hour.** Each process reuses the answer for an hour, so a
  chain that hands off within that hour may start one more run on the
  previous deployment before it moves.
- **Production only.** Previews and custom environments keep marked runs on
  the deployment that starts them, since their `BaseURL` names another
  deployment or is protected. So does a failed request, which is logged.

## Regions

Run IDs carry the region a run's data lives in, as world-vercel's tagged run
IDs do, and each run's queue messages go to that region:

- **Run IDs.** The World implements `worlds.RegionalRunIDs`: a new run's ID is
  a ULID with its region in the top bits of its randomness, from
  `StartOptions.Region`, else `VERCEL_REGION`, else `iad1`. Child run IDs,
  derived from their start step, are tagged the same way. An untagged ID lives
  in `iad1`, as on the backend.
- **Routing.** A message is sent to the region its run ID carries, else to
  `VERCEL_REGION`'s, else `iad1`. A delivery's lease is renewed and released in
  the region Vercel names in its `ce-vqsregion` header.

## Queue delivery

`Queue` publishes the whole delivery as the message, `POST
{QueueURL}/topic/{Topic}`, and stores nothing for it. The request carries:

| Header | Value |
| --- | --- |
| `Vqs-Idempotency-Key` | The delivery's `IdempotencyKey`, when it has one. Vercel Queues stores one message per key while it lives. |
| `Vqs-Experimental-Concurrency-Key` | The run ID, on a flow other than an intermediate hop of a wait's timer. Vercel Queues delivers one message per key at a time and coalesces the key's other messages into that delivery. Step deliveries carry none. |
| `Vqs-Retention-Seconds` | The delivery's `Retention`, clamped to 60 seconds through 7 days, when it is set. |
| `Vqs-Delay-Seconds` | The time until `NotBefore`, rounded up, and capped as in [long timers](#long-timers). |
| `Vqs-Deployment-Id` | The deployment the delivery is addressed to. |

The consumer handles each delivery as follows:

1. Parses the push request: a binary `com.vercel.queue.v2beta` CloudEvent or a
   structured `v1beta` one.
2. If the delivery is not yet due, hides it until it is, for at most 15
   minutes at a time.
3. Executes the delivery, renewing the lease every minute. The delivery's
   `MessageID` is the World's ID for the message, the same on every
   redelivery. Its `DeliveryCount` is Vercel's delivery count, and its
   `Attempt` that count less the message's handoffs.
4. On success acknowledges the message. When Vercel refuses because messages
   were coalesced into the delivery, executes it again with the next
   `DeliveryCount`, as a redelivery, and acknowledges with
   `Vqs-Experimental-If-Coalesced` covering them. After 32 such runs it hands
   the message off instead.
5. When the runtime asks for a redelivery after a delay, sets the message's
   visibility to that delay, at most the 15 minutes Vercel Queues honors (the
   runtime asks again when it arrives early), and leaves it unacknowledged.
   A redelivery of zero hands the message off: a visibility of zero with
   `Vqs-Experimental-Handoff: 1`, which Vercel Queues redelivers at once and
   does not count as an attempt. A flow message within 4 hours of its expiry,
   or 100 handoffs of the 10,000 Vercel Queues makes, continues on a new
   message instead, carrying its wait's timer and the waits it completes, and
   is acknowledged. So does one on its last coalesced run, before the
   acknowledgment Vercel may refuse.
6. On failure sets the visibility to a backoff, a second doubling with each
   delivery up to 15 minutes with 25% jitter, and at least the 5 seconds
   Vercel Queues accepts, or longer when the backend asks for it, and answers
   `500`. A delivery for a run that no longer exists is dropped. The runtime
   gives up after 48 attempts.

## Security

- **`Queue`** must be deployed only as a queue-triggered function. Vercel does
  not expose such functions publicly, so the handler does not authenticate
  deliveries.
- **The client endpoints** (RPC and the stream socket) are unauthenticated
  unless `HTTP` is given `worlds.WithClientToken` or
  `worlds.WithClientMiddleware`. Without them, keep them internal or put your
  own authentication in front of them.
- **Webhooks** are authorized only by the token in the URL.
- **The deployment route** is public and read-only: it answers with the
  serving deployment's ID and nothing else.
- **Storage** is reachable only with the deployment's OIDC token or a Vercel
  access token for the project.

::: info Undocumented API
Vercel documents its workflow storage only through the Workflow SDK, so the
World follows the protocol of `@workflow/world-vercel`. Vercel Queues is in
public beta. Recheck both when upgrading.
:::

## Tests

```sh
go test -race ./worlds/vercel/...
```

The tests run the conformance suites and end-to-end workflows against an
in-process imitation of the workflow API, which keeps an event only as the
backend does. Set `WORKFLOW_VERCEL_LIVE_OIDC_TOKEN` (a deployment's OIDC
token), or `WORKFLOW_VERCEL_LIVE_TOKEN` with `VERCEL_PROJECT_ID` and
`VERCEL_TEAM_ID`, to also run the end-to-end workflows against Vercel's
storage, the backend contract tests against its `/v5` routes, and the
backend assumptions.
