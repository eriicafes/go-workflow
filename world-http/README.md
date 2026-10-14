# `@eriicafes/go-workflow/world-http`

`createWorld` exposes a Go `worlds.HTTPHandler` as a JavaScript World-shaped
object. It sends every operation to the handler's CBOR RPC endpoint and uses
bounded polling for stream chunks.

```js
import { createWorld } from "@eriicafes/go-workflow/world-http";

const world = await createWorld({
  baseURL: "https://workflows.example.com/v1/workflow",
});
```

`baseURL` is the prefix passed to Go's `worlds.HTTPHandler`. When omitted,
`createWorld()` reads `WORKFLOW_HTTP_BASE_URL`.

To load this package through the Workflow SDK runtime, set
`WORKFLOW_TARGET_WORLD=@eriicafes/go-workflow/world-http` alongside
`WORKFLOW_HTTP_BASE_URL`:

```sh
export WORKFLOW_TARGET_WORLD=@eriicafes/go-workflow/world-http
export WORKFLOW_HTTP_BASE_URL=https://workflows.example.com/v1/workflow
```
