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
| Vercel Queues | Delivers queued items with delays up to 23 hours, in hops for longer timers |

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
and other workflows' to `Topic`. The message keeps the topic, so the hops of a
long timer stay on it. `Queue(world, runners)` consumes whichever topics its
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
creates is a Workflow SDK spec 8 run: its events have slot-numbered IDs,
in-band writes are fenced, hook tokens can be force-claimed, and their
retention reserved. The backend derives the run, step, and hook views and
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
| `step_created` | the step's input | step name |
| `step_completed`, `step_failed`, `step_retrying` | the result or error | retry time |
| `step_started` | | owner message |
| `hook_created` | the hook's metadata | token, retention, webhook, system, forced |
| `hook_received` | the payload | |
| `wait_created` | | resume time |
| `attr_set` | | changes and writer |

The runtime mints every step, hook, and wait ID with its replay position in the
ID's entropy (`worlds.NewWaitIDAt` and the like), so a declaration's position
comes back from its ID, and a step's retries and cancellation contexts come from
the replay that declares it.

| Contract | On Vercel |
| --- | --- |
| Batches | One request and one fenced allocation. As the contract allows, the backend keeps the events it admitted when another is refused, and the runtime replays whatever the log holds. |
| Repeated transitions | The backend refuses them as conflicts. The World finds and returns the event that admitted the transition, as the contract asks. |
| Stream writes | Each writer opens the backend's stream socket (`wss://…/websockets/v1/runs/{run}/streams/{name}`) in the background, as the Workflow SDK does, and writes through HTTP until it is open. A declined socket keeps the World's writers on HTTP for five minutes. Through the API proxy (`Token`), or with `WORKFLOW_STREAMS_TRANSPORT=http`, writers use HTTP only. |
| Stream reads | Cursors are chunk indexes. The World remembers the backend's cursor for the last index each stream was read to, so polling a stream continues where it stopped. The default stream, whose name is empty, is stored as `_default`. |
| `RunsList` by several statuses | The backend filters by one status, so the World reads a page of each and merges them by creation time. |
| Hooks | `HooksGet` reads the hook and its events, for when it received a payload or was disposed. |
| Analytics | Not supported. `Capabilities().Analytics` is false. |
| Retention | `Config.Retention` and `Config.Retentions`, built with `vercelworld.Retentions(vercelworld.Retain[W](...))`, take `worlds.RetainDefault`, which leaves runs to Vercel's plan, or `worlds.RetainNone`, which creates runs with the Workflow SDK's `$retention: "0"` attribute so the backend deletes their data when they finish. The backend supports no other retention. A run created with its own `$retention` attribute keeps it. |
| Start hooks | `run_created` carries the `startHook`, which the backend creates with the run, as world-vercel sends it. A `409 start-hook-conflict` becomes a `*worlds.HookConflictError` naming the token's owner. |
| Deleting run data | `RunsDeleteDataMany` calls the backend's `/v4/runs/purge`, as world-vercel does, and maps its `purged` and `already_purged` outcomes to `deleted` and `already_deleted`. The backend deletes a `$retention: "0"` run's data when it finishes and reports `expiredAt` on runs. |

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

The World publishes with Vercel Queues' default retention of 24 hours and, as
the Workflow SDK does, delays of up to 23 hours, so every message is still
retained for an hour after it becomes visible. A delivery due later is
published with a 23-hour delay, and when it arrives more than an hour early the
consumer
publishes a new message for the rest of the delay, then acknowledges the old
one. Each hop's idempotency key is the delivery's message ID and the incoming
Vercel message ID, so a hop retried after a crash is dropped.

When the deployment a delivery is pinned to has been deleted, the next hop
goes to the current deployment instead, whose runtime fails the run with
`worlds.ErrDeploymentMismatch`.

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

`Queue` publishes the whole delivery as the message: `POST
{QueueURL}/topic/{Topic}` with `Vqs-Delay-Seconds`,
the delivery's message ID as
`Vqs-Idempotency-Key`. Nothing is stored for it.

The consumer handles each delivery as follows:

1. Parses the push request: a binary `com.vercel.queue.v2beta` CloudEvent or a
   structured `v1beta` one.
2. If the delivery is due within an hour, extends the lease instead of
   failing. If it is due later, publishes the next hop and acknowledges this
   one.
3. Executes the delivery, renewing the lease every minute.
4. On success acknowledges the message.
5. On failure sets the visibility to the Workflow SDK's backoff, a second
   doubling with each delivery up to 15 minutes with 25% jitter, and at least
   the 5 seconds Vercel Queues accepts, and answers `500`, so Vercel
   redelivers. A delivery for a run that no longer exists is dropped. The
   delivery count is the delivery's `Attempt`, so the runtime gives up after
   48 deliveries.

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
storage.
