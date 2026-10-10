# Vercel world

The Vercel workflow world requires:

- An Upstash Redis database. Copy its TLS TCP connection string from the
  Upstash console's **Connect → TCP** tab. It starts with `rediss://`.
- A Vercel Queue topic for workflow task notifications.
- A Queue consumer route running `vercelworld.Queue(world, runners)`.
- An optional HTTP route running `vercelworld.HTTP(world)` when another service,
  a UI, or a proxy uses `workflow.NewHTTPClient`.

Configure the queue endpoint, OIDC token, and topic as
`VERCEL_QUEUE_URL`, `VERCEL_OIDC_TOKEN`, and `VERCEL_WORKFLOW_QUEUE_TOPIC`.
The queue endpoint includes `/api/v3`, for example
`https://iad1.vercel-queue.com/api/v3`.

```go
world, err := vercelworld.New(vercelworld.Config{
    RedisURL: os.Getenv("REDIS_URL"), // rediss://default:password@host:6379
    QueueURL: os.Getenv("VERCEL_QUEUE_URL"),
    OIDCToken: os.Getenv("VERCEL_OIDC_TOKEN"),
    Topic: os.Getenv("VERCEL_WORKFLOW_QUEUE_TOPIC"),
})
if err != nil { return err }

runners := workflow.Runners(workflow.Use(flows.GenerateReport))
http.ListenAndServe(":8081", vercelworld.Queue(world, runners))
```

Deploy `vercelworld.HTTP(world)` as a separate app-facing API function only when
another service, a UI, or a proxy needs remote access. It shares the Redis and
Queue configuration but does not need to register workflow implementations.
The basic example keeps the two entrypoints in `cmd/worker/vercel/queue` and
`cmd/worker/vercel/api`.

When the code that starts a workflow already has the same world in process,
pass it directly to `workflow.NewContext`. No HTTP function is needed.

The consumer is at-least-once. Redis task claims are lease-fenced and
idempotent. Vercel Queue delays are limited to 24 hours, so this world
returns an error for a longer durable timer.

## Security

Deploy `vercelworld.Queue(world, runners)` as a Vercel Queue push consumer, not as an
ordinary public route. Vercel queue consumer functions are not externally
reachable. Only Queue delivery can invoke them, so the consumer handler assumes
trusted delivery.

`vercelworld.HTTP(world)` is a separate app-facing API. It performs no
authentication or per-run authorization. Prefer an internal application-to-
workflow route. If it is exposed across a trust boundary, put your
authentication and authorization middleware in front of it and enforce which
callers may start each workflow or read each run.
