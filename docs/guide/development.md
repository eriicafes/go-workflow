# Development

During development, run everything in one process: the application, the
worker that executes deliveries, and the World's HTTP handler mounted next to
your routes. [go-dev](https://github.com/eriicafes/go-dev) runs and reloads
that process, and the [`workflowdev`](/tooling/dev) package adds the Workflow
SDK web UI and the workflow analyzer to it.

```
┌─────────────────────────────────────────────┐
│ cmd/app  :8080                              │
│                                             │
│   /reports        workflow.NewClient(world) │
│   /v1/workflow/*  postgresworld.HTTP(world) │
│   goroutine       postgresworld.Worker(...) │
└──────────────────────▲──────────────────────┘
                       │ /v1/workflow/rpc
            ┌──────────┴──────────┐
            │ npx workflow web    │
            │ Workflow SDK web UI │
            └─────────────────────┘
```

## 1. Install the tools

The web UI needs the Workflow SDK CLI and the go-workflow World adapter,
installed in the directory go-dev runs from:

```sh
go get github.com/eriicafes/go-dev
npm install --save-dev workflow go-workflow
```

## 2. Run the World in the application

The application opens the World, runs the worker in a goroutine, and uses the
World directly through `workflow.NewClient`. No network hop sits between the
application and the World. The World's HTTP handler is mounted on the
application's own mux under `/v1/workflow/`, so the web UI can reach it.

```go
// cmd/app/main.go
package main

func main() {
	ctx := context.Background()

	world, err := postgresworld.Open(ctx, postgresworld.Config{
		BaseURL: "http://localhost:8080/v1/workflow",
		URL:     os.Getenv("DATABASE_URL"),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer world.Close()

	runners := worlds.Runners(workflow.Use(flows.GenerateReport))
	go func() {
		if err := postgresworld.Worker(ctx, world, runners); err != nil {
			log.Printf("workflow worker stopped: %v", err)
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("POST /reports", createReport)
	mux.Handle("/v1/workflow/", postgresworld.HTTP(world))

	handler := workflow.Middleware(mux, workflow.NewClient(world))
	log.Fatal(http.ListenAndServe(":8080", handler))
}

func createReport(w http.ResponseWriter, r *http.Request) {
	run, err := workflow.Start(r.Context(), workflows.GenerateReport{Topic: r.FormValue("topic")})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte(run.ID()))
}
```

For a database-free loop, use the [local World](/worlds/local) the same way:
`localworld.Open`, `world.Prepare(ctx)`, and mount `localworld.HTTP(world)`.
The local World delivers work in-process, so it needs no worker goroutine.

## 3. Run it with go-dev

The dev command runs the application, reloads it when `cmd`, `flows`, or
`workflows` change, and runs the web UI alongside it, connected to the
handler mounted in the application:

```go
// cmd/dev/main.go
package main

import (
	"github.com/eriicafes/go-dev"
	"github.com/eriicafes/go-workflow/workflowdev"
)

func main() {
	session := dev.New()
	err := session.Run(dev.Cmd{
		Run:        dev.Package("./cmd/app"),
		Prepare:    dev.Hooks(workflowdev.Check("./...")),
		Watch:      dev.Values("cmd", "flows", "workflows"),
		ServerAddr: ":8080",
		Commands: dev.Commands(
			workflowdev.Web("http://localhost:8080/v1/workflow"),
		),
	})
	session.Catch(err)
	session.Catch(session.Wait())
}
```

```sh
go run ./cmd/dev
```

- **`workflowdev.Web(baseURL)`** runs `npx workflow web --noBrowser` with
  `WORKFLOW_TARGET_WORLD=go-workflow/world-http` and
  `WORKFLOW_HTTP_BASE_URL=baseURL`, so the web UI reads runs from the
  application's World.
- **`workflowdev.Check("./...")`** runs the analyzer before every start or
  reload (see [below](#_4-check-runners-on-every-reload)).

## 4. Check runners on every reload

The `Prepare` hook above runs the [analyzer](/tooling/analyzer) before go-dev
starts or reloads the application. A runner that starts a goroutine, calls
`time.Now`, or does I/O outside a step stops the reload with the diagnostic,
before the broken code can record events.

Outside go-dev, for example in CI, run the analyzer as a vet tool:

```sh
go install github.com/eriicafes/go-workflow/cmd/workflowcheck@latest
go vet -vettool=$(which workflowcheck) ./...
```

## 5. Inspect runs in the web UI

Start a run through the application, then open the URL the web UI prints. It
lists runs by workflow and status, and for each run shows its steps with inputs,
outputs, errors, and attempts, its hooks and waits, its streams, and the raw
event log.

To run the web UI by hand instead of through go-dev:

```sh
export WORKFLOW_TARGET_WORLD=go-workflow/world-http
export WORKFLOW_HTTP_BASE_URL=http://localhost:8080/v1/workflow
npx workflow web
```

The web UI talks to the World through the [world-http adapter](/tooling/world-http).

::: warning
Mounting `HTTP(world)` exposes the unauthenticated RPC endpoint on the
application's port. That's fine on your machine. In production, keep it off
public routes or [protect the RPC path](/worlds/#protecting-the-rpc-path).
:::

## Splitting later

Nothing here ties you to one process. When you deploy, the same runners and
workflow types work with a separate worker, a queue-triggered function, or
several application instances sharing one World. See
[Application and Worker](/guide/deployment).

## Testing

For automated tests, use the [in-memory test World](/worlds/testworld) instead.
See [Testing](/guide/testing).
