# `go-workflow/world-http`

JavaScript World adapter for
[go-workflow](https://go-workflow.vercel.app), a durable workflow library for
Go.

`createWorld` exposes a Go `worlds.HTTPHandler` as a JavaScript World-shaped
object, so the [Workflow SDK](https://workflow-sdk.dev) CLI and web UI can
inspect Go runs. It sends its operations to the handler's CBOR RPC endpoint and
uses bounded polling for stream chunks.

```sh
npm install go-workflow
```

```js
import { createWorld } from "go-workflow/world-http";

const world = await createWorld({
  baseURL: "https://workflows.example.com/v1/workflow",
});
```

`baseURL` is the prefix passed to Go's `worlds.HTTPHandler`. When omitted,
`createWorld()` reads `WORKFLOW_HTTP_BASE_URL`.

To load this package through the Workflow SDK runtime, set
`WORKFLOW_TARGET_WORLD=go-workflow/world-http` alongside
`WORKFLOW_HTTP_BASE_URL`:

```sh
export WORKFLOW_TARGET_WORLD=go-workflow/world-http
export WORKFLOW_HTTP_BASE_URL=https://workflows.example.com/v1/workflow
```

See the [world-http adapter](https://go-workflow.vercel.app/tooling/world-http)
and [Workflow SDK web UI](https://go-workflow.vercel.app/tooling/web-ui) docs
for every option.

## Agent skills

Agent skills are available for
[writing workflows](https://github.com/eriicafes/go-workflow/blob/main/skills/go-workflow/SKILL.md)
and
[implementing World backends](https://github.com/eriicafes/go-workflow/blob/main/skills/go-workflow-worlds/SKILL.md).
