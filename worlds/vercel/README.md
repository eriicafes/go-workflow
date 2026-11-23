# Vercel world

`vercelworld` stores runs in Vercel's managed workflow storage, the
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
runners := worlds.Runners(workflow.Use(flows.GenerateReport))
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
| `QueueURL` | A Vercel Queues API root including `/api/v3` that every queue request goes to. Empty routes each message to its region's `https://<region>.vercel-queue.com/api/v3`, or with `Token` to the API proxy at `<BackendURL>/queues-proxy/api/v3`, naming the region in `x-vercel-queue-region`. See [regions](#regions). |
| `OIDCToken` | Returns the deployment's OIDC token for each request. Default `OIDCTokenFromEnv(DefaultOIDCTokenEnv)`. |
| `Token`, `ProjectID`, `TeamID`, `Environment` | A Vercel access token and the project it acts in, for using the storage from outside a deployment. See [outside a deployment](#outside-a-deployment). |
| `BackendURL` | Root of the workflow API. Default `https://vercel-workflow.com/api`, or `https://api.vercel.com/v1/workflow` with `Token`. When it is empty, `VERCEL_WORKFLOW_SERVER_URL` names another backend host, used directly with `/api` appended or passed to the proxy in `x-vercel-workflow-api-url`, and `WORKFLOW_VERCEL_BACKEND_URL` another proxy root. |
| `HTTPClient` | Client for the workflow API and Vercel Queues. Default `http.DefaultClient`. Workflow API requests never follow a redirect, so their credentials stay with the backend. |
| `DeploymentID` | The deployment this World runs in. Default `VERCEL_DEPLOYMENT_ID`. When set, runs are pinned to the deployment that starts them and their deliveries are published there. See [pinning](https://go-workflow.vercel.app/guide/replay#pinning-runs-to-a-deployment) and [latest deployment](#latest-deployment). |

`New` does not contact Vercel.

Vercel rotates OIDC tokens, so `OIDCToken` is called for every request.
`vercelworld.OIDCTokenFromEnv(name)` reads the environment variable `name` on
each call. The default reads `VERCEL_OIDC_TOKEN`.

When a deployment ID is set, Queues requests send `Vqs-Deployment-Id`. A
delivery is published to its run's deployment, or to this World's when the run
has none.

### Outside a deployment

A script, a CLI, or the [web UI](https://go-workflow.vercel.app/tooling/web-ui) behind
[world-http](https://go-workflow.vercel.app/tooling/world-http) can read and write the same storage with a
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

Requests then go through Vercel's API proxy for that project and environment,
with the access token: the workflow API, and Vercel Queues under the proxy's
`/queues-proxy` path. No OIDC token is needed. The events and stream sockets
are not reachable through the proxy, so flows poll and streams are written
over HTTP.

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
hooks, and streams of Go runs show in Vercel's dashboard as JS ones do. Workflow and step
names are recorded as Go names them, such as `example.com/app.Checkout`, not
in the SDK's `workflow//` form.

| Event | Payload | Metadata |
| --- | --- | --- |
| `run_created` | the run's input | workflow name, deployment, attributes, public key |
| `run_completed`, `run_failed` | the output or error | |
| `step_created` | the step's input | step name, workflow name, inline, creator message |
| `step_completed`, `step_failed`, `step_retrying` | the result or error | step name, attempt, workflow name, retry time, creator message, and the latencies the runtime measured (`ttfs`, `stso`, `rsfs`, `stepCount`, `eventCount`, `finalSchedulingReplay`, `optimizations`) |
| `run_cancelled` | | the reason, when one was given |
| `step_started` | an inline step's input, on its creating start | step name, attempt, workflow name, inline, and `computeInstanceId`, a `cinst_` ULID made once per process |
| `hook_created` | the hook's metadata | token, retention, webhook, system, forced |
| `hook_received` | the payload | workflow name, and the token it was sent to as `hookToken` |
| `hook_disposed` | | the token it releases as `hookToken` |
| `wait_created` | | resume time, creator message |
| `attr_set` | | changes and writer |

Replay matches declarations by ID, and a step's retries and cancellation
contexts come from the replay that declares it, so a wait needs only its
resume time.

| Contract | On Vercel |
| --- | --- |
| Batches | One request and one fenced allocation. As the contract allows, the backend keeps the events it admitted when another is refused, and the runtime replays whatever the log holds. |
| Repeated transitions | The backend refuses them as conflicts, which the World returns as `ErrEntityConflict`. A `run_started` of a running run is one of them (`run-already-started`). |
| Stream writes | Each writer opens the backend's stream socket (`wss://…/websockets/v1/runs/{run}/streams/{name}`) in the background, and writes through HTTP until it is open. A declined socket keeps the World's writers on HTTP for five minutes. Through the API proxy (`Token`), or with `WORKFLOW_STREAMS_TRANSPORT=http`, writers use HTTP only. An append over HTTP is not idempotent, so it is sent again only when the backend cannot have stored it: up to five times, after a connection that was never made (500ms doubling) or a 429, waited out as a [rate limit](#requests-deadlines-and-retries). A close is sent again after a transport failure or a 429, 500, 502, 503, or 504, up to five times, after the `Retry-After` or a backoff from 500ms doubling to at most 30 seconds. On the socket, a write or close the backend refuses with a 429 did not apply: it is sent again under a new request ID after the refusal's `retryAfter`, at least a second, while the waits of one request add up to 30 seconds or less and stay 15 seconds short of the context's deadline. Past that, and when the backend is draining the socket, the writer leaves the socket and sends the request, and every later one, through HTTP. A close the socket answers with a 5xx is closed again through HTTP. |
| Stream reads | Cursors are chunk indexes, and a cursor that is not one reads from the beginning. The World remembers the backend's cursor for the last index each stream was read to, so polling a stream continues where it stopped. A `410` is a `*RunExpiredError`, and the backend's `stream-expired` is a `*StreamExpiredError`. |
| List cursors | A page carries the backend's cursor, the last page included: it is where an incremental read resumes. `HasMore` tells whether more follow. |
| `RunsList` by several statuses | The backend filters by one status, so `RunsList` with several is refused with `ErrEventInvalid`. |
| Hooks | `HooksGet` reads the hook and its `hook_created` event, for its metadata, webhook and system flags, and forced claim. |
| Entity payloads | A run's, step's, or hook's payload stored under a `gzip` or `zstd` wrapper is returned as the payload inside it. An encrypted payload is returned as stored. |
| Event limit | `EventsList` reports the run's event limit from the backend's `x-wf-max-events` header as `MaxEvents`. |
| Start hooks | `run_created` carries the `startHook`, which the backend creates with the run. A `409 start-hook-conflict` is a `*worlds.HookConflictError` naming the token and its owner, and a `400 start-hook-admission-rejected` is `ErrStartHookRejected`. A `409 start-hook-candidate-cancelled`, a candidate the backend fenced after a cancellation, is `ErrStartHookRejected` on a creation and the conflict of a finished run on a `run_cancelled`. A `400 start-hook-disabled`, admission switched off with nothing recorded, is `ErrStartHookDisabled`: no decision on the candidate. A start sends the creation again after 100ms, 300ms, and 900ms and then returns a `*worlds.StartError`, and the candidate's queued delivery fails and is delivered again, so it is admitted once admission is back on. |
| Analytics | `Analytics()` reads the backend's `/v2/analytics` endpoints. The backend filters analytics runs by one status, so `RunsList` with several is refused with `ErrEventInvalid`, and a time window needs both `StartTime` and `EndTime` or neither. Larger pages than the backend allows (100, or 1000 for a run's steps, events, and waits) come back with `HasMore`. |
| Retention | Vercel's plan decides how long runs are kept. |

### Requests, deadlines, and retries

| Request | Deadline | Sent again |
| --- | --- | --- |
| Runs, steps, hooks, analytics, stream chunks and info | 60 seconds for the whole request, its retries and the reading of its answer included. `WORKFLOW_REQUEST_TIMEOUT_MS` sets another, within 10 seconds to 2 minutes. | A GET, up to five times after a failed connection or a 500, 502, 503, or 504 (after the `Retry-After`, or 500ms doubling, at most 30 seconds), and up to twice more when its answer cannot be read or decoded (100ms, 200ms). A request past its deadline is not. |
| Event reads | 60 seconds for the answer's headers, and 60 seconds of silence within its body. A long log that keeps arriving has no limit. | As a GET above. A read of a run's whole log also waits out a 429 and asks for the same page again, and continues an answer cut off before its end frame after its last whole event, up to twice. A page with a limit, and a correlation listing, return either as an error. |
| Event writes | As event reads. | Up to twice (100ms, 200ms, plus up to 50ms) after a transport failure, a timeout, a 5xx, or an answer that cannot be decoded, for the events a repeat of which the backend refuses as a conflict: not `step_started`, `step_retrying`, a `hook_received` without a resume ID, or any in-band write. A 429 and a `503 start-hook-run-pending` are waited out for every event. |
| Stream writes, closes, and listing | 5 minutes per phase. | See stream writes above. The listing is not. |
| Run key | None of its own. | As a GET above, without following a redirect. |
| Run status long poll | As an entity request. The backend is asked to hold for the deadline less 10 seconds. | When the route is missing, a gateway gives up on the hold (504), or the hold times out or loses its connection, the World polls plain reads instead. |

A rate limit (429) is waited out for its `Retry-After`, at least a second, while
the waits of one request add up to 30 seconds or less. A `step_completed` or
`step_retrying`, or a `step_failed` the runtime marks as recorded after the
step's function ran (`EventsCreateOptions.AfterStepBody`), whose context has a
deadline waits up to 15 minutes instead, since losing it costs the step's work. No wait runs into the last 15 seconds
before the context's deadline. Past the budget the request returns a
`*worlds.ThrottleError`, and the queue's redelivery takes over. A 429 that
Vercel's firewall answers with a challenge (`x-vercel-mitigated: challenge`) is
a transport failure, never waited out.

A 5xx is an error with an `HTTPStatus`, which `worlds.IsRetryableWorldError`
reads, so a start whose `run_created` fails that way still succeeds and the
queued flow creates the run.

### Backend contract

The run-scoped event routes come in two versions, with the same CBOR frame
encoding. `/v5` serves runs of every spec version and is the only route that
writes to a spec 9 run. `/v4` serves runs below spec 9. Listing by
correlation ID stays on `/v4/events` for every run. On `/v5`:

| Part | Contract |
| --- | --- |
| Routes | `POST /v5/runs/{run}/events/{type}`, `POST /v5/runs/{run}/events/batch`, `GET /v5/runs/{run}/events`, and `GET /v5/runs/{run}/events/{event}`. A write to a spec 9 run through `/v4` is refused with a 400. |
| Frame meta | A write carries `specVersion`, `occurredAt`, and, from the run's orchestrator, `inBand: true` with `expectedSeqInBand` and `maxSlot`, the number of events it holds. An in-band write without `expectedSeqInBand` is a 400. `run_started` carries `executorSpecVersion`, and `step_created` carries `inline` and `creatorMessageId`. The first attempt's `step_started` of a step created by a `step_created` carries `stepCreatedAt`, the step's creation time in milliseconds since the epoch, which the backend takes for its start latency metric and does not store. |
| Counters | Each run has `seq`, its number of positions, and `seqInBand`, the positions allocated to in-band writes, with `seqInBand ≤ seq`. `run_created` takes the first position and counts as in-band. An in-band write advances both by the positions it allocates, and an out-of-band write moves only `seq`. |
| Snapshot | A listing reports the counters it read before listing in the `x-wf-snapshot-seq` and `x-wf-snapshot-seq-in-band` headers and in its end frame's `snapshot`. |
| Fence | An in-band write whose `expectedSeqInBand` is not `seqInBand` is refused with a 412 whose body has `error: "in-band-superseded"`, `seq`, `seqInBand`, and optionally the events the writer was missing. Nothing is written. A batch is fenced as one block. |
| Skipped-slot report | The answer to an in-band write has `allocated`, and `events`, the events in the positions after `maxSlot` and before its own, in order, with `hasMore`, or `reportIncomplete` when the backend could not finish the report, when that list is incomplete. A batch answers with one result per frame and the report and `allocated` at the top level. |
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
[Data Format and Encryption](https://go-workflow.vercel.app/guide/encryption).

## Long timers

A message published without a retention keeps Vercel Queues' default of 24
hours, so the World delays it by at most 23 hours, leaving it retained for an
hour after it becomes visible. A message with a retention lives for it, within
the 1 minute to 7 days Vercel Queues accepts, and may be delayed by up to an
hour less. `VERCEL_QUEUE_MAX_DELAY_SECONDS` sets another cap for messages
without a retention. The World reports a `MaxQueueDelay` of 23 hours, or that cap, and the runtime
reaches a later timer in hops of up to 23 hours, each its own message, so the
World publishes no hops of its own.

## Latest deployment

In a production deployment, runs started with
[`workflow.LatestDeployment`](https://go-workflow.vercel.app/guide/replay#moving-chains-to-the-latest-deployment)
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

## Queue delivery

`Queue` publishes the whole delivery as the message, `POST
{QueueURL}/topic/{Topic}`, and stores nothing for it. Its result is the
message ID Vercel Queues answers with, or none when it answers `202` after
failing over to another region. A send that fails with a transport error, its
30-second request deadline (`WORKFLOW_VERCEL_QUEUE_TIMEOUT_MS`), a 5xx, or a
429 whose `Retry-After` is at most 5 seconds is sent again, up to three
attempts, after 100ms and then 500ms plus up to 100ms of jitter. Other 4xx
answers are final. The request carries:

| Header | Value |
| --- | --- |
| `Vqs-Idempotency-Key` | The delivery's `IdempotencyKey`, when it has one. Vercel Queues stores one message per key while it lives. |
| `Vqs-Experimental-Concurrency-Key` | The run ID, on a flow other than an intermediate hop of a wait's timer. Vercel Queues delivers one message per key at a time and coalesces the key's other messages into that delivery. Step deliveries carry none. |
| `Vqs-Retention-Seconds` | The delivery's `Retention`, clamped to 60 seconds through 7 days, when it is set. |
| `Vqs-Delay-Seconds` | The time until `NotBefore`, rounded up, and capped as in [long timers](#long-timers). |
| `Vqs-Deployment-Id` | The deployment the delivery is addressed to. |
| `x-vercel-workflow-run-id`, `x-vercel-workflow-step-id` | The run, and the step of a step delivery, by which Vercel links the message's invocation to them. |
| `Vqs-Client-Ts` | When the request was sent. |

`QueueBatch` uses the batch send, `POST {QueueURL}/topic/{Topic}/batch`: one
`multipart/mixed` request for up to 100 messages that share a topic, a region,
and a deployment, each part carrying one message's headers and payload.
Requests to different destinations are sent at the same time, and each is
retried as a single send is. A message Vercel Queues refuses is reported in
its own result, with whether sending it again may succeed, beside the results
of the others. A request that fails is `QueueBatch`'s error, since the outcome
of its messages is not known.

The consumer handles each delivery as follows:

1. Parses the push request: a binary `com.vercel.queue.v2beta` CloudEvent or a
   structured `v1beta` one. A delivery without a receipt handle is claimed by
   ID, which Vercel Queues answers with the message as one `multipart/mixed`
   part.
2. If the delivery is not yet due, hides it until it is, for at most 15
   minutes at a time.
3. Executes the delivery, renewing the lease every minute. The delivery's
   `MessageID` is Vercel Queues' ID for the message, the same on every
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
   does not count as an attempt.
6. On failure sets the visibility to a backoff, a second doubling with each
   delivery up to 15 minutes less up to 25% jitter, and at least the 5 seconds
   Vercel Queues accepts, or the `Retry-After` of the failure when that is
   longer, and answers `500`. A delivery for a run that no longer exists is dropped. The runtime
   gives up after 48 attempts.

The response to a flow's delivery names the steps whose functions it ran, up
to ten, as a JSON array in `x-vercel-internal-workflow-step-ids`, by which
Vercel links the invocation's logs to those steps. The runtime tells the World
right before it calls a step's function (`worlds.StepExecutions`). A delivery's `CreatedAt` is when Vercel
Queues says the message was first sent.

## Differences from the Workflow SDK's client

The World follows `@workflow/world-vercel` 5.7.1 and `@vercel/queue` 0.11.0.
Where it differs, it is for one of these reasons:

| Difference | Reason |
| --- | --- |
| Workflow and step names are sent as Go names them, not in the SDK's `workflow//` form. | A Go run is resumed only by Go code. Resuming a run across languages is not supported. |
| Events are written over HTTP only. The SDK can also write them over its events socket. | The socket carries the same frames and the same retry policy, so HTTP loses nothing but a connection setup. The World uses the socket for the event feed. |
| `ownerMessageId` is never sent on `step_started`. The SDK's client sends it when the event's data has it. | Only a runtime below spec 9 writes it. A spec 9 run, which is all the Go runtime records, marks an inline step with `inline` on the event that creates it and names a flow's message as `creatorMessageId`. |
| A stream write whose socket fails before its acknowledgment is sent again through HTTP, and the writer stays on HTTP once its socket fails, drains, or is closed. The SDK fails the writer after an unacknowledged write, and reconnects a socket that drained or closed while idle. | The Go writer reads its socket only while a request waits for its answer, so it cannot tell a socket closed while idle from one that lost a write. Sending the write again keeps every chunk, at the cost of a possible duplicate, where failing the writer fails the step that writes. |
| The rate limit and start-hook waits stop 15 seconds before the context's deadline. The SDK reads the function's deadline from the platform. | A Go function's deadline reaches the World as its context's. Without one, the short budgets apply. |
| The queue payload is the delivery as JSON. The SDK sends CBOR in its own wrapper. | A message is read only by the deployment's own Go consumer. |
| A request names its client as `go-workflow/vercelworld <go version> <os> (<arch>) <deployment>`. | The same shape as the SDK's, naming this client. |
| The latest deployment is asked of the production domain's [deployment route](#latest-deployment), not of Vercel's API. | It needs no access token in the deployment. |
| With `Token`, the proxy is used whether or not `TeamID` is set. The SDK needs both a project and a team. | A personal account has no team. |
| Snapshots, dynamic workflow code, OpenTelemetry spans, and the SDK's `skip-step-inputs` event reads are not implemented. | go-workflow has no feature that uses them. |

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

> [!NOTE]
> Vercel documents its workflow storage only through the Workflow SDK, so the
> World follows the protocol of `@workflow/world-vercel` and `@vercel/queue`.
> Vercel Queues is in public beta. Recheck both when upgrading.

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
