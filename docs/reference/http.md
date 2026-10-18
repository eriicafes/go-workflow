# HTTP Endpoints

`worlds.HTTPHandler(world, prefix)`, which every World package wraps as
`HTTP(world)`, serves two endpoints under a prefix (default `/v1/workflow`).
It does not execute deliveries.

| Route | Handler | Purpose |
| --- | --- | --- |
| `POST <prefix>/rpc` | `worlds.RPCHandler` | CBOR RPC for clients and tools |
| `<prefix>/webhooks/{token}` | `worlds.WebhookHandler` | webhook requests |

The Vercel World adds `POST <prefix>/callbacks/qstash` when QStash is
configured. `vercelworld.QStash(world)` exposes that callback separately.
Callback routes under `<prefix>/callbacks/` are where a World mounts endpoints
specific to it.

## RPC

The RPC endpoint exposes the World and Client APIs. `workflow.NewHTTPClient`
and the [`world-http` adapter](/tooling/world-http) use it.

- **Request:** `POST` with `Content-Type: application/cbor` (other types get
  `415`), a body of at most 8 MiB:

  ```
  { method: string, input: <cbor>, specVersion?: int }
  ```

- **Response:** always `200` with `Content-Type: application/cbor`:

  ```
  { output?: <cbor>, error?: { code: string, message: string } }
  ```

  Error codes are `not_found`, `invalid_argument`, `conflict`, `cancelled`,
  `deadline_exceeded`, and `internal`.

- **Spec version:** when `specVersion` is set and differs from the World's
  (`worlds.SpecVersion`, currently `1`), the request fails with
  `invalid_argument`. Clients call `client.spec` first, which returns
  `{ specVersion, capabilities }`.

### Methods

| Group | Methods |
| --- | --- |
| World | `world.deploymentId` |
| Runs | `runs.get`, `runs.list`, `runs.waitForTerminalStatus`, `runs.setAttributes` |
| Steps | `steps.get`, `steps.list` |
| Events | `events.create`, `events.createRun`, `events.createBatch`, `events.get`, `events.list`, `events.listByCorrelationID` |
| Hooks | `hooks.get`, `hooks.getByToken`, `hooks.list` |
| Queue | `queue.queue`, `queue.queueBatch`, `queue.cancel` |
| Streams | `streams.write`, `streams.createWriteSession`, `streams.writeSession`, `streams.closeWriteSession`, `streams.writeMulti`, `streams.close`, `streams.getChunks`, `streams.getInfo`, `streams.list` |
| Analytics | `analytics.runs.get`, `analytics.runs.list`, `analytics.attributes.list`, `analytics.steps.get`, `analytics.steps.list`, `analytics.events.get`, `analytics.events.getMany`, `analytics.events.list`, `analytics.events.listByCorrelationId`, `analytics.hooks.get`, `analytics.hooks.list`, `analytics.waits.get`, `analytics.waits.list` |
| Client | `client.spec`, `client.runs.create`, `client.runs.cancel`, `client.runs.wake`, `client.streams.write`, `client.streams.writeMulti`, `client.streams.close`, `client.hooks.resume` |

::: danger Unauthenticated
The RPC endpoint can start runs, append events, and read every payload. Never
expose it publicly without your own authentication and authorization in front
of it.
:::

## Webhooks

`<prefix>/webhooks/{token}` resumes the webhook that owns `token` with the
request.

| Situation | Response |
| --- | --- |
| No active webhook owns the token, or its run finished | `404 Not Found` |
| Method differs from `WebhookOption.Method` | `405 Method Not Allowed` with `Allow` |
| Body over 128 KiB | `413 Request Entity Too Large` |
| Default webhook | `202 Accepted` |
| `WebhookOption.Response` set | that response |
| `WebhookOption.Manual` | Waits for a step to call `WebhookRequest.RespondWith`. |
| Manual response times out | `504 Gateway Timeout` |
| Webhook disposed or run terminated while waiting for a response | `410 Gone` |
| Storage failure while waiting for a response | `500 Internal Server Error` |
| Ownership changes during the request | Retryable `503 Service Unavailable`. The request is not forwarded to the new owner. |

The request's method, URL, headers, and body are recorded in a `hook_received`
event and a delivery is queued so the run resumes.

The token is the only authorization. Serve `WebhookHandler` (or a World's
`Webhook(world)`) publicly on its own if you need webhooks but want to keep RPC
private.

## Mounting under another path

```go
mux := http.NewServeMux()
mux.Handle("/v1/workflow/", postgresworld.HTTP(world))
mux.Handle("/", app)
```

Or with a custom prefix, using the World package's `HTTPPrefix` (or
`worlds.HTTPHandler` for your own World):

```go
handler := postgresworld.HTTPPrefix(world, "/internal/workflow")
```

Whatever you choose, set the World's `Config.BaseURL` to the public URL of that
prefix so webhook URLs are correct, and pass the same URL to
`workflow.NewHTTPClient`.
