---
name: go-workflow-worlds
description: Implement, debug, or test go-workflow World backends and delivery adapters. Use when changing worlds.World storage, event admission, hook claims, queues, streams, or runtime integration; ordinary workflow authoring does not require this skill.
---

# go-workflow Worlds

Start from the target SDK version's interfaces and conformance tests. A World
stores append-only run event logs, derives run/step/hook views, delivers queued
work, and stores streams. Correctness depends on atomic admission and replay
contracts, not just matching method signatures.

## Inspect the contract

Resolve the SDK checkout or read-only module directory. Read the relevant
source before implementing backend behavior:

| Concern | Source |
| --- | --- |
| World identity and capabilities | `worlds/world.go` |
| Storage, lifecycle events, views, and fencing | `worlds/world_storage.go` |
| Delivery and queue fields | `worlds/world_queue.go` |
| Ordered streams and writer sessions | `worlds/world_streamer.go` |
| Runtime replay and admission callers | `worlds/runtime.go`, `worlds/durable_*.go` |
| Shared backend contract tests | `workflowtest/conformance.go` and the other files in `workflowtest/` |

`World` embeds `Storage`, `Queue`, and `Streamer` and exposes BaseURL,
Analytics, Capabilities, SpecVersion, and DeploymentID. Use the actual
interface; a documentation excerpt can omit newer methods. Report optional
capabilities honestly. Unsupported attribute writes are successful no-ops;
unsupported analytics returns nil.

Read [backend contracts](references/contracts.md) for the transaction,
ownership, delivery, and stream invariants. Reuse the SDK's exported `Data*`
event payload types instead of recreating them in an adapter. Avoid routing
backend operations through the higher-level workflow client.

## Validate the implementation

Run `workflowtest.Conformance` with a fresh, isolated backend per callback.
Use a unique schema, prefix, table, or directory where necessary. Register
cleanup for every created resource.

For storage shared between processes, also run
`workflowtest.ConformanceShared` with two independent World instances pointing
at the same isolated data. Two references to the same in-memory instance do
not exercise cross-instance coordination.

Add targeted tests for changed semantics: concurrent fenced appends, contested
hook claims, stale stream writers, and delivery failures after a committed
append are useful boundaries. Verify that retrying a delivery recovers from a
committed write whose acknowledgement was lost. Assert recorded outcomes and
invariants rather than exactly-once callback execution.

Exercise runtime integration with `testworld.NewManual` when precise delivery
control is needed. A consumer calls `Runtime.Execute(ctx, input)`, acknowledges
on nil, and retries on error. Persist and redeliver the original QueueInput
including its recovery counters and deployment metadata.

Run the adapter's tests and relevant conformance tests; use `-race` for local
concurrency changes. Integration tests that require services must use the
project's configured test infrastructure. State which backend checks ran and
which could not run.
