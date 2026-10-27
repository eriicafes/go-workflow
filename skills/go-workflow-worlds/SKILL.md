---
name: go-workflow-worlds
description: Implement, debug, or test go-workflow World backends and delivery adapters. Use when changing worlds.World storage, event admission, hook claims, queues, streams, or runtime integration; ordinary workflow authoring does not require this skill.
---

# go-workflow Worlds

Use the application's installed SDK version. A World stores append-only run
event logs, derives run/step/hook views, delivers queued work, and stores
streams. Implement the atomic admission and replay contracts described below.

## Inspect the contract

Use `go doc github.com/eriicafes/go-workflow/worlds.World` and the relevant
symbols (`StorageEvents`, `QueueInput`, `Streamer`, or `Runtime`) to inspect the
installed contract. `go doc github.com/eriicafes/go-workflow/workflowtest`
describes the public conformance test entry points.

`World` embeds `Storage`, `Queue`, and `Streamer` and exposes BaseURL,
Analytics, Capabilities, SpecVersion, and DeploymentID. Report supported
optional capabilities. Unsupported attribute writes are successful no-ops;
unsupported analytics returns nil.

Read [backend contracts](references/contracts.md) for the transaction,
ownership, delivery, and stream invariants. Reuse the SDK's exported `Data*`
event payload types when recording or reading events.

## Validate the implementation

Run `workflowtest.Conformance` with a fresh, isolated backend per callback.
Use a unique schema, prefix, table, or directory where necessary. Register
cleanup for every created resource.

For storage shared between processes, also run
`workflowtest.ConformanceShared` with two independent World instances pointing
at the same isolated data.

Add targeted tests for changed semantics: concurrent fenced appends, contested
hook claims, stale stream writers, and delivery failures after a committed
append are useful boundaries. Verify that retrying a delivery recovers from a
committed write whose acknowledgement was lost. Assert recorded outcomes and
invariants rather than exactly-once callback execution.

Exercise runtime integration with `testworld.NewManual` when precise delivery
control is needed. A consumer sets `input.MessageID` to the message's ID,
calls `Runtime.Execute(ctx, input)`, acknowledges on nil, and retries on error.
Persist and redeliver the original QueueInput including its recovery counters
and deployment metadata, with the same message ID on every redelivery.

Run the adapter's tests and relevant conformance tests; use `-race` for local
concurrency changes. Integration tests that require services must use the
project's configured test infrastructure. State which backend checks ran and
which could not run.
