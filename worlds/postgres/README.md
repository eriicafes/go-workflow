# Postgres world

Create a PostgreSQL database reachable by the worker. `postgresworld.Open` creates
the workflow tables automatically. `postgresworld.New` uses an existing `pgxpool`
without taking ownership of it.

```go
world, err := postgresworld.Open(ctx, postgresworld.Config{URL: os.Getenv("DATABASE_URL")})
client := world // same-process app and worker
```

For separate processes, serve `postgresworld.HTTP(world)` in the worker and use
`workflow.NewHTTPClient(workerURL)` in the application. This HTTP handler is
optional. When the application and worker share a process, pass `world`
directly to `workflow.NewContext` instead.

## Security

`postgresworld.Run` is a database worker, not an HTTP endpoint. Give its database
role only the permissions it needs for the workflow tables, require TLS for
database connections where supported, and keep the database reachable only
from the application and worker network.

`postgresworld.HTTP(world)` is an app-facing transport for starting runs and
reading their outputs and events. It performs no authentication or per-run
authorization. Prefer a private service-to-service route from the application
to the worker. If the handler is reachable across a trust boundary, wrap it in
your authentication and authorization middleware before serving it. The
middleware must decide which callers may start each workflow and read each run.

The same handler serves token-authorized webhooks at
`/.well-known/workflow/v1/webhook/:token`. The token is the only authorization
for that route. Treat the URL as a bearer capability and verify the provider's signature,
timestamp, and replay protection in the workflow before acting on a request.
