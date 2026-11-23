# go-workflow

## 0.10.0

### Minor Changes

- 8db36f5: Move `Runners`, `RunnerSet`, `Runner`, and `NewRunner` from `worlds/core` to `worlds`.

## 0.9.0

### Minor Changes

- 8e22d09: Report runs and waits in the Workflow SDK's analytics shapes, and add `Run.ErrorCode`.
- 13ea9b0: Report steps, events, and hooks in the Workflow SDK's analytics shapes.
- 1e2804d: Store a run's cancellation in the `abrt_` hook and the `system_abort` stream.
- 39cb07e: Add `workflow.CancelOptions`, so `Run.Cancel` can record a reason.
- 11ac7c5: Rename `WithRPCToken` and `WithRPCMiddleware` to `WithClientToken` and `WithClientMiddleware`.
- 1789808: Add `workflow.DeleteRuns` and `worlds.RetainNone` to delete a run's data.
- 99c101d: Wake waiting flows from the events a World pushes.
- 39cb07e: Add `workflow.Version`, and record each run's execution context.
- 08f82eb: Refuse events on a finished run.
- 1103048: Delete disposed hooks, and drop `Hook.DisposedAt` and `Hook.ForceClaimedBy`.
- a65eedb: Deduplicate hook resumes by resume ID and payload digest.
- bad8732: Keep the local World open after `Clear`.
- 98f6fc4: Store the local World in SQLite on world-local's schema.
- 11ac7c5: Fail a run or step after 48 deliveries.
- 23e96ad: Draw step, wait, and hook IDs and tokens from the run's seed.
- 5af35a9: Add `Config.EncryptionKey` to encrypt run data.
- 39cb07e: Default the Postgres World to 50 workers and a 500 ms poll.
- 2a3a6e3: Store the Postgres World in world-postgres's tables.
- 39cb07e: Report each refused message of `QueueBatch` on its result.
- 11ac7c5: Remove `QueueCancel` from Worlds.
- e3dd2ba: Add `workflow.RandReader` for deterministic random bytes in workflow code.
- bd5fa00: Add `QueueInput.Recovery` for a World that lost its queue.
- 5f95359: Refuse a repeated creation event with `ErrEntityConflict`.
- d7da9be: Use the replay clock for `workflow.Now`, sleeps, and timeouts in workflow code.
- d986301: Retention deletes a run's data instead of the run in the Postgres and AWS Worlds.
- d842644: Rename the RPC version to `worlds.RPCVersion`, and remove `World.SpecVersion`.
- d5921b2: Let flows run ahead of their inline steps' writes.
- 39cb07e: Fail runs past 25,000 events or a replay time limit.
- adde901: Match no runs with an empty status filter.
- 3e7fd0e: Replace sentinel errors that have a Workflow SDK class with its error types.
- 77360e3: Record a spec version on every event, step, and hook.
- cdd1d3d: Align World reads, refusals, and retries with the Workflow SDK's Worlds.
- 5af35a9: Record run data in the Workflow SDK's format, readable by its web UI and Vercel's dashboard.
- 6ea9e15: Name streams and compress payloads in the Workflow SDK's formats.
- 11ac7c5: Make `workflow.Writer` return a buffered `workflow.StreamWriter`.
- 822457d: Drop writes to closed streams, and refuse runs of a newer spec version.
- 02331c6: Validate run IDs, attributes, resumes, and step events in every World.
- 5af35a9: Let webhooks take any number of requests, and remove `WebhookOption.Method`.
- 16aa2ea: Add `workflow.SetAttributes` to set a run's attributes.
- 5c22077: Record runs as spec 9 runs, and move the runtime to `worlds/core`.
- 0d8a08e: Replace `workflow.StartOnLatest` with `StartOptions.Deployment`.
- 59c8ad3: Add `StartOptions.Hook` to create a hook with the run.
- fc5f97d: Add `workflow.StartOptions`.
- ca40a1f: Add `StartOptions.Region`, and tag Vercel run IDs with their region.
- b790971: Add `StartOptions.ReplayedFromRunID`.
- 39cb07e: Wait a second between step retries, and name the retry count when a step runs out.
- 11ac7c5: Serve stream writes over a WebSocket.
- cc942a7: Add `*worlds.ThrottleError`, and wait out rate limits before retrying a delivery.
- 1584f15: Support `Analytics()` in the Vercel World.
- 11ac7c5: Store the Vercel World's runs in Vercel's managed workflow storage instead of Redis.
- 39cb07e: Send Vercel queue requests through the API proxy when given an access token.

### Patch Changes

- 39cb07e: Refuse an `attr_set` over 8192 bytes in every World.
- 8006067: Default the AWS module's `max_receive_count` to 50.
- 39cb07e: Write integers beyond 2^53 as `BigInt`, so JavaScript tools read them exactly.
- 39cb07e: Keep queued messages in the local and Postgres Worlds until they are delivered.
- 39cb07e: Record errors under the Workflow SDK's class names.
- 39cb07e: Retry and time out requests to the Vercel backend.
- 39cb07e: Make world-http compatible with the Workflow SDK's spec 9 client.
- 39cb07e: Fail instead of redelivering when the World refuses attributes or a hook as invalid.

## 0.8.0

### Minor Changes

- 3440879: Deliver timers longer than six days in hops of up to six days on Vercel Queues in the Vercel World, so QStash is no longer required for them.

## 0.7.0

### Minor Changes

- 6f133e3: Add `workflow.StartOnLatest` to start a workflow's new runs on the latest deployment.
- 6f133e3: Support `workflow.StartOnLatest` in the Vercel World.

## 0.6.0

### Minor Changes

- cef51ec: Add `worlds.WithRPCToken` and `worlds.WithRPCMiddleware` to protect the RPC endpoint, and send `WORKFLOW_HTTP_TOKEN` as a bearer token from world-http.
- 0060621: Add `Config.Retention` and per-workflow `Config.Retentions` to the Postgres, Vercel, and AWS Worlds, which delete a finished run's data that long after it completes.

## 0.5.2

### Patch Changes

- 9b61bcf: Check the bodies of method runners passed to `workflow.Use`, such as `workflow.Use(f.Report)`, like function runners.

## 0.5.1

### Patch Changes

- 3b5641b: Fix concurrent deliveries of one flow message in a process both running a step the first started inline.

## 0.5.0

### Minor Changes

- e4564ee: Add `SetMaxInlineStepsDuration`, and stop running new steps inline halfway to the delivery's ctx deadline.

### Patch Changes

- e4564ee: Leave the rest of an SQS batch to another invocation once the AWS world's `Queue` handler is halfway to the Lambda deadline.
- 602e63d: Fix an inline step's retry never running when its flow arrives while the failed attempt is still being recorded.

## 0.4.0

### Minor Changes

- 1844cc8: Route workflows to further standard or FIFO SQS queues in the AWS world with `Config.Queues` and `Config.Routes`, provisioned by the Terraform module's `queues`.
- 1844cc8: Add `Hook.ResumeContext` with the run's deployment and workflow, so `ResumeHook` no longer reads the run.
- 1844cc8: Route workflows to named queues in the Postgres world with `Config.Routes`, and claim them with `WorkerOptions.Queues`.
- 1844cc8: Route workflows to further topics in the Vercel world with `Config.Topics` and `Config.Routes`.
- 1844cc8: Add `worlds.WorkflowName`, `worlds.Route`, and `worlds.Routes` to route workflows to queues, and `QueueInput.WorkflowName` for Worlds that route.

## 0.3.0

### Minor Changes

- 2281a0f: Run each step attempt in an OpenTelemetry span, parented under the span context the runner passes to the step.
- 2281a0f: Add the `workflowai` package to run AI SDK `WorkflowAgent` agents as durable runs with Go AI SDK.
- c32cc83: Run new steps inline in the flow delivery up to `SetMaxInlineSteps` (default 3), and add `InlineBackground`.
- c32cc83: Add `workflowai.ExecuteInlineStep` to run a tool's calls as inline steps.

## 0.2.0

### Minor Changes

- 32d039a: Add `worlds.HTTPHandler` with CBOR RPC and webhook endpoints for `World` and
  client operations.
- 7f14818: Add `workflow.Middleware` and context-bound `workflow.Get` run handles.
- 21b843b: Add `Run.InBandCount`, `InBandCount` on event appends, and `worlds.ErrInBandSuperseded` to fence concurrent flows of one run.
- ff939a4: Add `HTTPPrefix(world, prefix)` to every World package to serve the RPC, webhook and callbacks endpoints.
- 4d2fc59: Add `aws.CreateTable` and a Terraform module for the AWS world's DynamoDB, SQS, and EventBridge Scheduler resources.
- 32d039a: Add the event-sourced `worlds.World` interface with `Storage`, `Queue`,
  `Streamer`, `Analytics`, and `Capabilities` APIs.
- 370e7a8: Add typed `Hook` and `Webhook` APIs with lazy hook registration, token ownership checks, repeated payloads, and durable disposal.
- 84095e4: Add deployment affinity to pin runs to the deployment that starts them, with `worlds.ErrDeploymentMismatch` for runs that cannot reach it.
- 4d2fc59: Add `worlds.RPCHandler` and `worlds.WebhookHandler`, and `RPC` and `Webhook` handlers in each world package, to serve the RPC and webhook endpoints separately.
- 4f18970: Add `@eriicafes/go-workflow/world-http`, a CBOR RPC `worlds.World` client for
  `worlds.HTTPHandler`.
- 4d2fc59: Add `World.BaseURL` so webhook URLs are absolute.
- 32d039a: Add `Result[T]` with `Value` and `Unwrap` for durable step and timer outcomes, and `Run.Value` for run outputs.
- 32d039a: Add `workflow.Client` with `workflow.NewClient` and `workflow.NewHTTPClient` constructors.
- 7f14818: Add `workflow.WithCancel`, `WithCancelCause`, `CancelAny`, and `CancelFunc` for durable step and timer cancellation, with explicit `Run.Cancel` for runs.
- 4d2fc59: Add `vercel.OIDCTokenFromEnv` and make `Config.OIDCToken` a function so the Vercel world reads its OIDC token on each request.
- a91564b: Add run streams with `Writer` and `Reader` for steps and `Run.Writer` and `Run.Reader` for use outside a run.
- 1743d44: Add `Client.Spec` to read a World's RPC spec version and capabilities, and `worlds.ResumeHook`.
- 7f14818: Add `workflow.WithTimeout`, `WithDeadline`, and their Cause variants for durable cancellation deadlines.
- 4d2fc59: Add `workflowtest.Conformance` and `workflowtest.ConformanceShared` for testing `worlds.World` implementations.
- e7e679c: Add `MapResults`, `WaitGroup`, `ErrGroup`, and `Go`, with durable cancellation through `ErrGroupWithContext`.
- 4d2fc59: Add `workflow.InlineStep` for durable step outcomes and retries without a separate step delivery.
- 2579e07: Add `workflowdev.Web` for running the Workflow SDK web UI with `npx workflow web --noBrowser`.
- 84095e4: Detect replay divergence and fail runs whose event log no longer matches their runner with `worlds.ErrCorruptedEventLog`.
- 7f14818: Add `Select`, `SelectOk`, `Join`, and `JoinOk` for racing and waiting on durable outcomes.
- 32d039a: Add `workflow.WorkflowMetadata` and `workflow.StepMetadata` for workflow
  execution contexts.
- 9a3d8ab: Make the in-memory test World testable with `testing/synctest`.
- 8270eb7: Add `workflow.Error` so `errors.Is` and `errors.As` match the errors of failed steps and runs after replay.
- 7f14818: Add `Config.RedisURL`, `Config.RedisRESTURL`, and `Config.RedisRESTToken` for TCP-or-REST Redis transport selection in the Vercel world.
- 32d039a: Add `Webhook.Request` and serializable `WebhookRequest[T]` with `JSON` and step-only `RespondWith`.
- 7287812: Add `workflow.Then` to chain result-returning callbacks and `NewResult` for immediately settled outcomes.
- 370e7a8: Add `workflow.Rollback` for deferred durable compensation when a workflow fails or panics.
- 32d039a: Add `workflow.Now` for durable workflow timestamps.

### Patch Changes

- 50de1c1: Decode run and step `input`, `output`, and `error` in the HTTP world, wait for `runs.waitForTerminalStatus` across bounded requests, and report RPC errors with their HTTP status and Go sentinel.
