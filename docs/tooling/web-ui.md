# Workflow SDK Web UI

go-workflow records runs with the [Workflow SDK](https://workflow-sdk.dev)'s
event model, so the Workflow SDK web UI can inspect Go runs: their steps,
events, hooks, waits, and streams.

## Running the web UI

1. Mount your World's HTTP handler, for example `postgresworld.HTTP(world)`
   under `/v1/workflow/` on your application's mux.
2. Install the Workflow SDK CLI and the go-workflow World adapter:

   ```sh
   npm install --save-dev workflow go-workflow
   ```

3. Point the CLI at the adapter and your handler:

   ```sh
   export WORKFLOW_TARGET_WORLD=go-workflow/world-http
   export WORKFLOW_HTTP_BASE_URL=http://localhost:8080/v1/workflow
   npx workflow web
   ```

With [go-dev](/tooling/dev), `workflowdev.Web(baseURL)` runs this command for
you, with `--noBrowser`. [Development](/guide/development) shows the whole setup.

The CLI loads [`go-workflow/world-http`](/tooling/world-http), which
talks to your handler's RPC endpoint. You don't import it yourself.

::: warning
The RPC endpoint is unauthenticated. Only point the web UI at Worlds on a
trusted network, or put authentication in front of the endpoint.
:::
