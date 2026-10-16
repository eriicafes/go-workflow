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

Every World package exports the same handler constructors:

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
it. It is required on every World except the local World, which defaults to
`http://localhost:3000/v1/workflow`.

`workflow.NewHTTPClient` takes the same kind of URL: the server's address
followed by the prefix, such as `http://workflow-worker:8081/v1/workflow`.

## Delivery guarantees

Every World delivers **at least once**. The runtime decides whether a delivery
may run by appending events, and every World admits or rejects appends
atomically, so duplicate and stale deliveries are harmless. A failed delivery
is retried with backoff.

| World | Native max delay | Failed delivery backoff |
| --- | --- | --- |
| Local | unlimited | 100 ms up to 30 s |
| Vercel | 7 days (longer with QStash) | 5 s up to 5 min |
| AWS | 15 min (longer with Scheduler) | SQS redelivery |
| Postgres | unlimited | 1 s up to 5 min |

## Security

None of the HTTP handlers authenticate requests.

- **RPC** can start runs and read every run's input, output, and events. Keep it
  on a private network, or put your own authentication and authorization in
  front of it.
- **Webhooks** are authorized only by the token in the URL. Treat webhook URLs
  as bearer capabilities and verify provider signatures in the workflow.
- **Queue consumers** must only be reachable by the queue infrastructure.

### Protecting the RPC path

To keep a single handler, wrap `HTTP(world)` itself in middleware that checks
requests to its RPC path and lets webhook requests through, since webhook
callers can't send your credentials:

```go
func requireToken(next http.Handler, token string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/workflow/rpc" &&
			subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+token)) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

handler := requireToken(postgresworld.HTTP(world), os.Getenv("WORKFLOW_RPC_TOKEN"))
log.Fatal(http.ListenAndServe(":8081", handler))
```

Clients then send the token through their `*http.Client`:

```go
type bearer struct{ token string }

func (b bearer) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("Authorization", "Bearer "+b.token)
	return http.DefaultTransport.RoundTrip(r)
}

client := workflow.NewHTTPClient(
	"http://workflow-worker:8081/v1/workflow",
	&http.Client{Transport: bearer{os.Getenv("WORKFLOW_RPC_TOKEN")}},
)
```

Alternatively, serve `RPC(world)` and `Webhook(world)` on separate listeners
or routes, and expose only the webhook route publicly.
