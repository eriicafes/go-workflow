// Package core executes deliveries against a World. Its Runtime replays the
// runners in a worlds.RunnerSet as flows and steps, and records their results as
// events in the run's log. The durable primitives the workflow package is
// built on, such as DurableStep, DurableWait, and DurableHook, declare their
// operations through the delivery's Execution.
//
// World packages build one Runtime and hand each delivery their queue
// receives to their worlds.Executor with it. A worlds.RunnerSet executor
// calls Runtime.Execute with its runners.
package core
