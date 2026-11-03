# Worlds

A **World** is the backend a run is recorded in. It provides three things:

- **Storage** for each run's append-only event log and the run, step, and hook
  views derived from it.
- A **queue** of deliveries, each of which makes the runtime execute a flow or
  a step.
- **Streams** of bytes that runs write and readers consume.

Your workflow code is the same on every World. Choose one per environment.

## Choosing a World

| World | Storage and delivery | Processes | Entry points | Best for |
| --- | --- | --- | --- | --- |
| [Local](/worlds/local) | JSON files, in-process delivery | one per directory | `World.Prepare` | development, single-process apps |
| [Vercel](/worlds/vercel) | Redis, Vercel Queues, optional QStash | any number | `vercelworld.Queue` | Vercel Functions |
| [AWS](/worlds/aws) | DynamoDB, SQS, optional EventBridge Scheduler | any number | `awsworld.Queue` | AWS Lambda |
| [Postgres](/worlds/postgres) | PostgreSQL, LISTEN/NOTIFY workers | any number | `postgresworld.Worker` | servers and containers |
| [Test](/worlds/testworld) | memory | one | `testworld.New` | tests |

You can also [implement your own](/worlds/custom).

## HTTP handlers

Every World package except the test World exports the same handler constructors:

| Function | Serves |
| --- | --- |
| `HTTP(world)` | the RPC endpoint and the webhook endpoint under `/v1/workflow` |
| `HTTPPrefix(world, prefix)` | the same endpoints under `prefix`, such as `/internal/workflow` |
| `RPC(world)` | only the RPC endpoint, to mount yourself |
| `Webhook(world)` | only the webhook endpoint, to mount yourself |

`HTTP(world)` serves:

| Route | Purpose |
| --- | --- |
| `POST /v1/workflow/rpc` | CBOR RPC used by `workflow.NewHTTPClient` and the [web UI](/tooling/web-ui) |
| `/v1/workflow/webhooks/{token}` | [webhook](/guide/webhooks) requests |

The Vercel World also serves `/v1/workflow/callbacks/qstash` when QStash is configured.
See [HTTP Endpoints](/reference/http) for details.

### Base URL

Set `Config.BaseURL` to the absolute public URL of the handler's prefix,
**including** `/v1/workflow` (or the prefix you passed to `HTTPPrefix`), for
example `https://workflows.example.com/v1/workflow`. Webhook URLs are built from
it. It is required except on the local World, which defaults to
`http://localhost:3000/v1/workflow`, and the test World, which always uses
`http://localhost/v1/workflow`.

`workflow.NewHTTPClient` takes the same kind of URL: the server's address
followed by the prefix, such as `http://workflow-worker:8081/v1/workflow`.

## Retention

Runs are kept forever by default. A World that supports retention deletes a
run's data `Config.Retention` after the run completes, fails, or is cancelled,
so storage stays bounded. `Config.Retentions` sets it per workflow, by workflow
name, and zero keeps that workflow's runs forever:

```go
config.Retention = 24 * time.Hour
config.Retentions = worlds.Retentions(
	worlds.Retain[workflows.Chat](10 * time.Minute),
	worlds.Retain[workflows.Invoice](0),
)
```

Every World that supports retention follows the same rules:

- Runs that are still pending or running are never deleted, however old.
- A hook whose token retention has not ended keeps its run until it ends.
- Runs that finished before retention was set are kept.

Callers read a run's result and streams right after it finishes, so a few
minutes is the shortest practical retention. Each World's page describes how
and when it deletes.

## Delivery guarantees

Every World delivers **at least once**. The runtime decides whether a delivery
may run by appending events, and every World admits or rejects appends
atomically, so duplicate and stale deliveries are harmless. A failed delivery
is retried with backoff.

| World | Native max delay | Failed delivery backoff |
| --- | --- | --- |
| Local | unlimited | 100 ms up to 30 s |
| Vercel | 6 days (longer with QStash) | 5 s up to 5 min |
| AWS | 15 min (longer with Scheduler) | 1 s up to 15 min |
| Postgres | unlimited | 1 s up to 5 min |

## Security

The RPC and webhook handlers don't authenticate requests.

- **RPC** can start runs and read every run's input, output, and events. Keep it
  on a private network, or [protect it](#protecting-the-rpc-path) with
  `worlds.WithRPCToken`.
- **Webhooks** are authorized only by the token in the URL. Treat webhook URLs
  as bearer capabilities and verify provider signatures in the workflow.
- **Queue consumers** must only be reachable by the queue infrastructure.

### Protecting the RPC path

Pass `worlds.WithRPCToken` to `HTTP(world)` to require a bearer token on the RPC
path. Webhook requests still pass, since webhook callers can't send your
credentials:

```go
handler := postgresworld.HTTP(world, worlds.WithRPCToken(os.Getenv("WORKFLOW_HTTP_TOKEN")))
log.Fatal(http.ListenAndServe(":8081", handler))
```

An empty token refuses every RPC request, so an unset secret never leaves the
endpoint open. For other checks, such as a session or an IP allowlist,
`worlds.WithRPCMiddleware(middleware)` wraps only the RPC path in your own
middleware.

The [world-http adapter](/tooling/world-http), and so the
[web UI](/tooling/web-ui), sends `WORKFLOW_HTTP_TOKEN` as the bearer token.
Go clients send it through their `*http.Client`:

```go
type bearer struct{ token string }

func (b bearer) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("Authorization", "Bearer "+b.token)
	return http.DefaultTransport.RoundTrip(r)
}

client := workflow.NewHTTPClient(
	"http://workflow-worker:8081/v1/workflow",
	&http.Client{Transport: bearer{os.Getenv("WORKFLOW_HTTP_TOKEN")}},
)
```

Alternatively, serve `RPC(world)` and `Webhook(world)` on separate listeners
or routes, and expose only the webhook route publicly.
