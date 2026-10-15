# world-http Adapter

`@eriicafes/go-workflow/world-http` exposes a Go World served by
`HTTP(world)` as a [Workflow SDK](https://workflow-sdk.dev) `World` in
JavaScript. It sends every operation to the handler's CBOR
[RPC endpoint](/reference/http#rpc) and uses bounded polling for stream chunks.

Most projects never import it: the [web UI](/tooling/web-ui) loads it for you
through `WORKFLOW_TARGET_WORLD`. Use it directly when JavaScript or TypeScript
code needs to read or control Go runs with the Workflow SDK's World API, such
as an admin script or a Node service.

## Install

```sh
npm install @eriicafes/go-workflow
```

## Usage

Create the World with the URL of the Go handler, including `/v1/workflow`:

```ts
import { createWorld } from "@eriicafes/go-workflow/world-http";

const world = await createWorld({
  baseURL: "http://localhost:8080/v1/workflow",
});
```

List the most recent failed runs, without their payloads:

```ts
const failed = await world.runs.list({ status: "failed", resolveData: "none" });
```

Read one run with its input and output:

```ts
const run = await world.runs.get(failed.data[0].runId);
```

Wait up to 30 seconds for a run to finish:

```ts
const finished = await world.runs.waitForTerminalStatus(run.runId, { timeoutMs: 30_000 });
```

## Options

| Option | Default | Description |
| --- | --- | --- |
| `baseURL` | `WORKFLOW_HTTP_BASE_URL` | The handler's URL, including `/v1/workflow`. |
| `headers` | none | Headers sent with every RPC request, such as `Authorization`. |
| `fetch` | `globalThis.fetch` | A custom `fetch` implementation. |

`createWorld` reads the World's capabilities over RPC before it returns, so it
fails fast when the handler is unreachable.

If you [protect the RPC path](/worlds/#protecting-the-rpc-path) with a token,
pass it in `headers`:

```ts
const world = await createWorld({
  baseURL: "https://workflows.example.com/v1/workflow",
  headers: { Authorization: `Bearer ${process.env.WORKFLOW_RPC_TOKEN}` },
});
```

## Loading through the Workflow SDK runtime

The Workflow SDK CLI and runtime load a World by module name. Set:

```sh
export WORKFLOW_TARGET_WORLD=@eriicafes/go-workflow/world-http
export WORKFLOW_HTTP_BASE_URL=http://localhost:8080/v1/workflow
```

Loaded this way, the adapter is created without options and reads
`WORKFLOW_HTTP_BASE_URL`, so it cannot send custom headers.

## Limits

The adapter is a client only. Deliveries are executed by your Go process, so it
cannot create queue handlers, and it has no deployment ID of its own.

::: warning
The RPC endpoint is unauthenticated unless you protect it. Point the adapter
only at Worlds on a trusted network, or put authentication in front of the
endpoint.
:::
