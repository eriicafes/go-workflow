# Application and Worker

go-workflow separates the code that **starts and reads** runs from the code
that **executes** them.

- The **application** needs the workflow types and a Client. It does not
  import runners or their dependencies.
- The **worker** opens a World with the runners and executes deliveries.

They can live in one process or in many.

## One process

The simplest setup opens the World in the application and uses
`workflow.NewClient`. This is how the [local World](/worlds/local) is usually
used, and it also works with Postgres.

```go
runners := worlds.Runners(workflow.Use(flows.GenerateReport))

world, err := localworld.Open(localworld.Config{Dir: ".workflows"}, runners)
if err != nil {
	log.Fatal(err)
}

defer world.Close()

if err := world.Prepare(ctx); err != nil {
	log.Fatal(err)
}

handler := workflow.Middleware(routes(), workflow.NewClient(world))
log.Fatal(http.ListenAndServe(":8080", handler))
```

## Separate application and worker

The worker serves the World's HTTP handler. The application talks to it with
`workflow.NewHTTPClient`.

```go
// cmd/worker/main.go
world, err := postgresworld.Open(ctx, postgresworld.Config{
	BaseURL: os.Getenv("WORKFLOW_BASE_URL"),
	URL:     os.Getenv("DATABASE_URL"),
})
if err != nil {
	log.Fatal(err)
}

defer world.Close()

runners := worlds.Runners(
	workflow.Use(flows.GenerateReport),
	workflow.Use(flows.PublishReport),
)
go func() {
	if err := postgresworld.Worker(ctx, world, runners); err != nil {
		log.Printf("workflow worker stopped: %v", err)
	}
}()
log.Fatal(http.ListenAndServe(":8081", postgresworld.HTTP(world)))
```

```go
// cmd/app/main.go
client := workflow.NewHTTPClient("http://workflow-worker:8081/v1/workflow")

mux := http.NewServeMux()
mux.HandleFunc("POST /reports", func(w http.ResponseWriter, r *http.Request) {
	run, err := workflow.Start(r.Context(), workflows.GenerateReport{Topic: r.FormValue("topic")})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte(run.ID()))
})
log.Fatal(http.ListenAndServe(":8080", workflow.Middleware(mux, client)))
```

### Sharing the World instead

With Postgres, AWS, or Vercel, the application can instead open the same World
itself, since those Worlds are shared by any number of processes. The World
then serves as the application's client through `workflow.NewClient`, with no
network hop to the worker, and the application can mount the World's HTTP
handler next to its own routes for webhooks and the web UI. It still doesn't
need runners: only the processes that execute deliveries do.

```go
// cmd/app/main.go
world, err := postgresworld.Open(ctx, postgresworld.Config{
	BaseURL: os.Getenv("WORKFLOW_BASE_URL"), // https://app.example.com/v1/workflow
	URL:     os.Getenv("DATABASE_URL"),
})
if err != nil {
	log.Fatal(err)
}

defer world.Close()

mux := http.NewServeMux()
mux.HandleFunc("POST /reports", createReport)
mux.Handle("/v1/workflow/", postgresworld.HTTP(world))

log.Fatal(http.ListenAndServe(":8080", workflow.Middleware(mux, workflow.NewClient(world))))
```

The worker then only runs `postgresworld.Worker` with the runners and doesn't
need to serve HTTP at all. Protect the mounted RPC path if the application is
public. See [Protecting the RPC path](/worlds/#protecting-the-rpc-path).

## Serverless

On Vercel and AWS, deliveries arrive through a queue trigger, so the worker is
a queue consumer function and the HTTP handler is a separate function:

| | Queue consumer | Client-facing API |
| --- | --- | --- |
| Vercel | `vercelworld.Queue(world, runners)` | `vercelworld.HTTP(world)` |
| AWS | `awsworld.Queue(world, runners)` (SQS Lambda) | `awsworld.HTTP(world)` (Function URL) |

See [Vercel](/worlds/vercel) and [AWS](/worlds/aws).

## Deploying runner changes

Runs replay against the runner code that handles each delivery. Changing what
a runner does, or the order it does it in, can break runs that are already in
flight: the runtime detects the divergence and fails those runs with a
`worlds.ErrCorruptedEventLog`. Add a new workflow type for incompatible
changes. On Vercel, runs are pinned to the deployment that started them and
keep replaying its code, so keep old deployments until their runs finish. See
[Pinning runs to a deployment](/guide/replay#pinning-runs-to-a-deployment).

## Security checklist

- The RPC endpoint performs **no authentication or authorization**. Keep it on a
  private network, or wrap `RPC(world)` in your own auth middleware that decides
  who may start each workflow and read each run.
- Webhook URLs are bearer capabilities. Serve `Webhook(world)` publicly if you
  need webhooks, and verify provider signatures in the workflow.
- Queue consumers (`vercelworld.Queue`, `awsworld.Queue`, `postgresworld.Worker`)
  must only be reachable by the queue infrastructure.
