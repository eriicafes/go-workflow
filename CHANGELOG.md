# go-workflow

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
