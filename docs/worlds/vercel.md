# Vercel World

```go
import vercelworld "github.com/eriicafes/go-workflow/worlds/vercel"
```

The Vercel World stores the event log and its views in Redis and delivers work
through Vercel Queues. QStash is optional and only carries timers longer than
seven days.

| Service | Role |
| --- | --- |
| Redis (Upstash, TCP or REST) | Event log, run/step/hook views, hook tokens, streams, queue records |
| Vercel Queues | Delivers queued items with delays up to 7 days |
| QStash (optional) | Timers longer than 7 days, handed back to Vercel Queues when due |

## Usage

```go
world, err := vercelworld.New(vercelworld.Config{
	BaseURL:  os.Getenv("WORKFLOW_BASE_URL"), // https://workflows.example.com/v1/workflow
	RedisURL: os.Getenv("REDIS_URL"),         // rediss://default:password@host:6379
	QueueURL: os.Getenv("VERCEL_QUEUE_URL"),  // https://iad1.vercel-queue.com/api/v3
	Topic:    os.Getenv("VERCEL_WORKFLOW_QUEUE_TOPIC"),
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
`workflow.NewClient`. This setup needs no HTTP function.

## Configuration

| Field | Meaning |
| --- | --- |
| `BaseURL` | Required. Public URL of `HTTP(world)` including `/v1/workflow`. |
| `RedisURL` | TCP transport. `rediss://` uses TLS (minimum TLS 1.2). `redis://` is plain TCP. |
| `RedisRESTURL`, `RedisRESTToken` | Upstash REST transport, used when `RedisURL` is empty. |
| `QueueURL` | Required. Regional Vercel Queues API root including `/api/v3`. |
| `Topic` | Required. Topic the consumer is subscribed to. |
| `OIDCToken` | Returns the Vercel OIDC token for each Queues request. Default `OIDCTokenFromEnv(DefaultOIDCTokenEnv)`. |
| `Prefix` | Redis key prefix. Default `go-workflow`. |
| `QStash` | Optional long-timer configuration. |
| `HTTPClient` | Client for Redis REST, Vercel Queues, and QStash. Default `http.DefaultClient`. |
| `DeploymentID` | The deployment this World runs in. Default `VERCEL_DEPLOYMENT_ID`. Runs are pinned to the deployment that starts them and always delivered there. See [pinning](/guide/replay#pinning-runs-to-a-deployment). |

`New` does not contact Redis or Vercel. `Close` closes the Redis TCP pool and
does nothing with the REST transport.

Vercel rotates OIDC tokens, so `OIDCToken` is called for every Queues request.
`vercelworld.OIDCTokenFromEnv(name)` reads the environment variable `name` on
each call. The default reads `VERCEL_OIDC_TOKEN`.

When `VERCEL_DEPLOYMENT_ID` is set, Queues requests send it as
`Vqs-Deployment-Id`, so push deliveries return to the deployment that
published them.

## Entry points

- **`Queue(world, runners)`**: the Vercel Queues push consumer. It executes
  workflows and steps.
- **`HTTP(world)`**: the RPC endpoint, token webhooks, and, when QStash is
  configured, the signed timer callback `/v1/workflow/callbacks/qstash`.
- **`HTTPPrefix(world, prefix)`**: the same routes, including the QStash
  callback, under `prefix`.
- **`RPC(world)`** and **`Webhook(world)`**: each endpoint alone.
- **`QStash(world)`**: the signed timer callback alone. Requires `Config.QStash`;
  mount it at `POST <Config.BaseURL>/callbacks/qstash`.

## Long timers with QStash

Vercel Queues delays deliveries by at most seven days. Without QStash, queueing
a delivery due later returns an error. Configure QStash before opening the World:

```go
config.QStash = &vercelworld.QStashConfig{
	Token:             os.Getenv("QSTASH_TOKEN"),
	CurrentSigningKey: os.Getenv("QSTASH_CURRENT_SIGNING_KEY"),
	NextSigningKey:    os.Getenv("QSTASH_NEXT_SIGNING_KEY"),
}
```

The timer is published to `<Config.BaseURL>/callbacks/qstash` with
`Upstash-Not-Before`, so `BaseURL` must be HTTPS. The callback is mounted under
the same prefix as the rest of the handler, so custom prefixes work. When QStash calls it, `HTTP(world)` verifies the `Upstash-Signature`
JWT (HS256 with the current or next signing key, issuer `Upstash`, subject equal
to the URL, within its `nbf`/`exp` window, and a matching body SHA-256), then
publishes the delivery to Vercel Queues with the remaining delay.

## Storage

Every mutation is one Lua script: creating a run, appending events (admission,
the append, and every view update), claiming a hook, putting or deleting a
queue record, and each stream operation. Scripts run atomically, so the World is
equally safe over TCP and REST, and any number of instances can share one
database. Scripts are sent with `EVALSHA` and fall back to `EVAL` on
`NOSCRIPT`.

::: warning No Redis Cluster
Scripts derive their keys from the prefix, so the database must not be a Redis
Cluster. Upstash regional and global databases are fine.
:::

| Key (prefix `p`) | Type | Contents |
| --- | --- | --- |
| `p:run:<run>` | hash | Run view, event count, creation score |
| `p:attrs:<run>` | hash | Run attributes |
| `p:events:<run>` | list | Event records at index = slot − 1 |
| `p:last:<run>` | hash | Last slot per event type, for idempotent replays |
| `p:corr:<run>:<hex(cid)>` | zset | Slots of one correlation ID |
| `p:steps:<run>`, `p:steporder:<run>` | hash, zset | Step JSON and step IDs by creation |
| `p:hook:<hook>`, `p:token:<token>` | string | Hook JSON and token ownership |
| `p:hooks`, `p:runhooks:<run>` | zset | Hook IDs by creation |
| `p:runs`, `p:runs:wf:<name>`, `p:runs:status:<s>`, `p:runs:wfstatus:<s>:<name>` | zset | Run IDs by creation |
| `p:streams:<run>` | set | Stream names |
| `p:stream:<run>:<hex(name)>`, `p:chunks:<run>:<hex(name)>` | hash, list | Stream session state and chunks |
| `p:queue:<id>` | string | Queue record |

Binary payloads are stored as base64 so every value is valid UTF-8 for the REST
API. Event data is parsed in Go, and only the fields admission needs are passed
to Lua. `RunsWaitForTerminalStatus` and `StreamsGetChunks` poll with a backoff
from 50 ms to 500 ms, because REST has no blocking reads.

## Queue delivery

`Queue` writes the record `p:queue:<ULID>`, then publishes `{"id": "<ULID>"}`
to `POST {QueueURL}/topic/{Topic}` with `Vqs-Delay-Seconds`,
`Vqs-Retention-Seconds: 604800`, and the message ID as `Vqs-Idempotency-Key`.
If publishing fails, the record is deleted and the error returned.

The consumer handles each delivery as follows:

1. Parses the push request: a binary `com.vercel.queue.v2beta` CloudEvent or a
   structured `v1beta` one.
2. Loads the record. A missing record (cancelled or done) is acknowledged.
3. If the record is not yet due, extends the lease (at most an hour) instead of
   failing.
4. Executes the delivery, renewing the lease every minute.
5. On success deletes the record and acknowledges the message.
6. On failure sets the visibility to a backoff of 5 s to 5 min and answers
   `500`, so Vercel redelivers.

`QueueCancel` deletes the record and any pending QStash message. A Vercel Queues
message cannot be recalled. Its delivery finds no record and is acknowledged.

## Security

- **`Queue`** must be deployed only as a queue-triggered function. Vercel does
  not expose such functions publicly, so the handler does not authenticate
  deliveries. A delivery only names a Redis record, so a forged one can at most
  run an already-queued item early.
- **The QStash route** is public and rejects requests without a valid QStash
  signature for its exact URL.
- **`HTTP` / `RPC`** perform no authentication. Keep them internal or put your
  own authentication in front of them.
- **Webhooks** are authorized only by the token in the URL.

::: info Vercel Queues is in beta
Vercel Queues is in public beta and its push request format is defined by its
SDK, so recheck these details when upgrading.
:::

## Tests

```sh
docker network create workflow-redis
docker run -d --name redis --network workflow-redis -p 6379:6379 redis
docker run -d --network workflow-redis -p 8079:80 -e SRH_MODE=env \
  -e SRH_TOKEN=local -e SRH_CONNECTION_STRING=redis://redis:6379 \
  hiett/serverless-redis-http
go test -race ./worlds/vercel/...
```

The tests use `REDIS_URL`, `UPSTASH_REDIS_REST_URL`, and
`UPSTASH_REDIS_REST_TOKEN`, and run the conformance suites over both
transports. With `luajit` on `PATH`, every Lua script is also compile-checked.
