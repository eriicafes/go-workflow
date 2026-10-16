# Vercel world

`vercelworld` stores the workflow event log and its views in Redis and delivers
work through Vercel Queues. QStash is optional and only carries timers longer
than seven days.

| Service | Role |
| --- | --- |
| Redis (Upstash, TCP or REST) | Event log, run/step/hook views, hook tokens, streams, queue records |
| Vercel Queues | Delivery of every queued item; delays up to 7 days |
| QStash (optional) | Timers longer than 7 days, handed back to Vercel Queues when due |

## Entrypoints

All are in `world.go`:

- `vercelworld.Queue(world, runners)`: the Vercel Queues push consumer. It
  executes workflows and steps.
- `vercelworld.HTTP(world)`: the RPC endpoint (`/v1/workflow/rpc`), token
  webhooks (`/v1/workflow/webhooks/{token}`), and, when QStash is configured,
  the signed timer route `/v1/workflow/callbacks/qstash`.
- `vercelworld.HTTPPrefix(world, prefix)`: the same routes under `prefix`.
  `Config.BaseURL` must end with the same prefix.
- `vercelworld.RPC(world)` and `vercelworld.Webhook(world)`: the RPC or webhook
  endpoint alone, for callers that mount their own routes.

The basic example deploys `cmd/worker/vercel/queue` (consumer) and
`cmd/worker/vercel/api` (HTTP). When the code that starts workflows has the
World in process, pass it to `workflow.NewContext`; no HTTP function is needed.

## Configuration

```go
world, err := vercelworld.New(vercelworld.Config{
    BaseURL:  os.Getenv("WORKFLOW_BASE_URL"), // https://workflows.example.com/v1/workflow
    RedisURL: os.Getenv("REDIS_URL"),         // rediss://default:password@host:6379
    QueueURL: os.Getenv("VERCEL_QUEUE_URL"),  // https://iad1.vercel-queue.com/api/v3
    Topic:    os.Getenv("VERCEL_WORKFLOW_QUEUE_TOPIC"),
})
```

| Field | Meaning |
| --- | --- |
| `BaseURL` | Required. Public URL of `HTTP(world)` including `/v1/workflow`; webhook URLs are built from it. |
| `RedisURL` | TCP transport. `rediss://` uses TLS (minimum TLS 1.2); `redis://` is plain TCP. |
| `RedisRESTURL`, `RedisRESTToken` | Upstash REST transport, used when `RedisURL` is empty. |
| `QueueURL` | Required. Regional Vercel Queues API root including `/api/v3`. |
| `Topic` | Required. Topic the consumer is subscribed to. |
| `OIDCToken` | Function returning the Vercel OIDC token, called for every Queues request. Default `OIDCTokenFromEnv(DefaultOIDCTokenEnv)`. |
| `DeploymentID` | Deployment this World runs in. Default `VERCEL_DEPLOYMENT_ID`. Runs are pinned to the deployment that starts them, and every delivery is published to the run's deployment. |
| `Prefix` | Redis key prefix, default `go-workflow`. |
| `QStash` | Optional long-timer configuration (below). |
| `HTTPClient` | Client for Redis REST, Vercel Queues and QStash. Default `http.DefaultClient`. |

Vercel rotates OIDC tokens, so `OIDCToken` is called for every Queues request.
`vercelworld.OIDCTokenFromEnv(name)` returns a function that reads the
environment variable `name` on each call; the default reads
`VERCEL_OIDC_TOKEN`. Pass your own function to read the token elsewhere.

Each publish sends `Vqs-Deployment-Id` set to the deployment of the delivery's
run, so a webhook or hook resume handled by a newer deployment still returns
to the run's own. QStash timers keep that deployment in their queue record. A
delivery that reaches another deployment anyway is re-routed up to three times,
and the run fails with `worlds.ErrDeploymentMismatch` after that, or as soon as
Vercel answers `502` with `Vqs-Error-Code: consumer_discovery_failed` for a
deleted deployment.

Deploy the consumer with a `queue/v2beta` trigger for the topic:

```json
{
  "functions": {
    "api/queue.go": {
      "experimentalTriggers": [{ "type": "queue/v2beta", "topic": "workflow" }]
    }
  }
}
```

## Storage

Every mutation is one Lua script. That covers creating a run, appending an
event or a batch (admission, the append and every view update), claiming a
hook (including a forced takeover), putting or deleting a queue record, and
each stream operation. A script runs atomically, so the World is equally safe
over TCP and REST, and any number of instances can share one database. Scripts
are sent with `EVALSHA` and fall back to `EVAL` on `NOSCRIPT`.

Scripts derive their keys from the prefix, so the database must not be a
Redis Cluster. Upstash regional and global databases are fine.

| Key (prefix `p`) | Type | Contents |
| --- | --- | --- |
| `p:run:<run>` | hash | Run view, `count` (event count), creation `score` |
| `p:attrs:<run>` | hash | Run attributes |
| `p:events:<run>` | list | Event records; index = slot − 1 |
| `p:last:<run>` | hash | Last slot per event type (and correlation ID), used for idempotent replays |
| `p:corr:<run>:<hex(cid)>` | zset | Slots of one correlation ID |
| `p:steps:<run>`, `p:steporder:<run>` | hash, zset | Step JSON; step IDs ordered by creation slot |
| `p:hook:<hook>`, `p:token:<token>` | string | Hook JSON; owning hook of a token |
| `p:hooks`, `p:runhooks:<run>` | zset | Hook IDs by creation |
| `p:runs`, `p:runs:wf:<name>`, `p:runs:status:<s>`, `p:runs:wfstatus:<s>:<name>` | zset | Run IDs by creation |
| `p:streams:<run>` | set | Stream names |
| `p:stream:<run>:<hex(name)>`, `p:chunks:<run>:<hex(name)>` | hash, list | Stream session state; base64 chunks |
| `p:queue:<id>` | string | Queue record |

Encoding works as follows:

- Binary payloads are stored as base64, so every value is valid UTF-8 for the
  REST API.
- Event data is parsed in Go. Only the fields admission and the views need are
  passed to Lua, as a JSON descriptor, so Lua never parses user bytes.
- Timestamps are Unix microseconds taken from the writer's clock.
- Creation scores are microsecond timestamps made strictly increasing by
  `p:order`. They are unique, so list cursors (`RunsList`, `StepsList`,
  `HooksList`) are the last score and stay stable.
- Event cursors are event IDs.

Token retention expires lazily: on a claim, on a conflicting `hook_created`, and
in `HooksGetByToken`. Redis also expires the token key a minute after retention
ends, as a backstop.

`RunsWaitForTerminalStatus` and `StreamsGetChunks` poll with a backoff from
50 ms to 500 ms, because REST has no blocking reads.

## Queue and timers

`Queue` writes the record `p:queue:<ULID>` first and then publishes `{"id":
"<ULID>"}`. If the publish fails, the record is deleted and the error is
returned.

- Delays up to 7 days go to Vercel Queues as `POST {QueueURL}/topic/{Topic}`.
  The request sets `Vqs-Delay-Seconds`, rounded up, and
  `Vqs-Retention-Seconds: 604800`, so a delayed or retried message outlives
  its delay. `Vqs-Idempotency-Key` is the message ID. Runtime idempotency keys
  are reused across distinct deliveries, and Vercel would silently drop a
  repeat for up to 24 hours.
- Delays over 7 days go to QStash when `Config.QStash` is set. Otherwise
  `Queue` returns an error. The QStash message ID is stored on the record.

`QueueCancel` deletes the record and any pending QStash message. A Vercel
Queues message cannot be recalled; its delivery finds no record and is
acknowledged.

The consumer handles each delivery as follows:

1. It parses the push request: a binary `com.vercel.queue.v2beta` CloudEvent
   (`ce-vqs*` headers, payload in the body) or a structured `v1beta` one.
   Deliveries without a receipt handle are claimed with `ReceiveMessageById`.
2. It loads the record. A missing record is acknowledged without running.
3. If the record's `NotBefore` is in the future, it extends the lease
   (`PATCH .../lease/{receiptHandle}`, at most an hour) instead of failing.
4. It runs `worlds.NewRuntime(world, runners).Execute`, renewing the lease
   every minute.
5. On success it deletes the record and acknowledges the message
   (`DELETE .../lease/{receiptHandle}`).
6. On failure it sets the visibility to a backoff of 5 s to 5 min and answers
   500, so Vercel redelivers. A delivery for a run that no longer exists is
   dropped.

### Long timers with QStash

```go
QStash: &vercelworld.QStashConfig{
    Token:             os.Getenv("QSTASH_TOKEN"),
    CurrentSigningKey: os.Getenv("QSTASH_CURRENT_SIGNING_KEY"),
    NextSigningKey:    os.Getenv("QSTASH_NEXT_SIGNING_KEY"),
},
```

The timer is published to `<BaseURL>/callbacks/qstash` with
`Upstash-Not-Before`. That URL must be HTTPS. When QStash calls it:

1. `HTTP(world)` verifies the `Upstash-Signature` JWT: HS256 with the current
   or next signing key, issuer `Upstash`, subject equal to that URL, within its
   `nbf`/`exp` window, and a body SHA-256 that matches.
2. It publishes the record to Vercel Queues with the remaining delay.
3. It clears the stored QStash message ID.

## Security

- **`Queue`** must be deployed only as a queue-triggered function. Vercel
  documents these functions as having no public URL: only Vercel's queue
  infrastructure can invoke them. So the handler does not authenticate
  deliveries. It does not compare bearer tokens, because the old check against
  its own OIDC token is not the documented model. A delivery only names a Redis
  record, so a forged one can at most run an already-queued item early.
- **The QStash route** is public, and it rejects any request without a valid
  QStash signature for its exact URL.
- **`HTTP` / `RPC`** perform no authentication or per-run authorization. Keep
  them internal, or put your own authentication in front of them.
- **Webhooks** are authorized only by the token in the URL. Treat that URL as a
  bearer capability. Verify the provider's signature and replay protection
  inside the workflow.

## Verified against the Vercel docs (October 2026)

| Page | What was confirmed |
| --- | --- |
| [Queues API](https://vercel.com/docs/queues/api) | Regional `https://{region}.vercel-queue.com/api/v3`. `POST /topic/{topic}` with `Authorization: Bearer <OIDC token>` and a raw body. Headers `Vqs-Delay-Seconds` (max 7 days, capped at the TTL), `Vqs-Retention-Seconds` (60 s to 7 days, default 24 h), `Vqs-Idempotency-Key`, and `Vqs-Deployment-Id`. Responses 201 `{"messageId"}` or 202 deferred. `ReceiveMessageById`, `DELETE .../lease/{receiptHandle}` to acknowledge, and `PATCH .../lease/{receiptHandle}` with `visibilityTimeoutSeconds` (max 3600). |
| [Queues concepts](https://vercel.com/docs/queues/concepts) | Push consumers use `queue/v2beta` triggers and are not reachable from the internet. Delivery is at least once. The deduplication window is `min(retention, 24 h)`. A failed delivery is retried after its lease expires. |
| [Queues SDK](https://vercel.com/docs/queues/sdk), plus the published `@vercel/queue` source (read only, because the docs do not document the raw push request) | The binary `ce-*` headers. The structured `v1beta` fallback. Acknowledging via `DELETE` lease and retrying via `PATCH` visibility. |
| [OIDC](https://vercel.com/docs/oidc) | In Functions the token arrives in the `x-vercel-oidc-token` header. It is reused for up to 90 minutes and expires after 2 hours. `VERCEL_OIDC_TOKEN_FILE` and `VERCEL_OIDC_TOKEN` are the other sources. |

Vercel Queues is in public beta and the push request format is defined by the
SDK, so recheck these details when upgrading.

## Tests

The tests expect Redis at `redis://localhost:6379` (`REDIS_URL`) and an
Upstash-compatible REST endpoint at `http://localhost:8079` with token `local`
(`UPSTASH_REDIS_REST_URL`, `UPSTASH_REDIS_REST_TOKEN`), and run the
`workflowtest` conformance suites over both transports:

```sh
docker network create workflow-redis
docker run -d --name redis --network workflow-redis -p 6379:6379 redis
docker run -d --network workflow-redis -p 8079:80 -e SRH_MODE=env \
  -e SRH_TOKEN=local -e SRH_CONNECTION_STRING=redis://redis:6379 \
  hiett/serverless-redis-http
go test -race ./worlds/vercel/...
```

Each test uses a unique prefix and deletes its keys afterwards. With `luajit`
on `PATH`, every Lua script is also compile-checked.
