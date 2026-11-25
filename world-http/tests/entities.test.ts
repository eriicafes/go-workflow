import { decode } from "cbor-x";
import { HookSchema, SPEC_VERSION_SINGLE_ORCHESTRATOR, StepSchema, WorkflowRunSchema } from "@workflow/world";
import { expect, test } from "vitest";
import { createWorld } from "../index.js";
import { rpcResponse } from "./helpers.js";

test("passes run and step payloads through in the Workflow SDK's format", async () => {
	const json = (value: unknown) => new TextEncoder().encode("devl" + JSON.stringify([value]));
	const world = await createWorld({
		baseURL: "https://workflow.example/v1/workflow",
		fetch: async (_url, init) => {
			const request = decode(init?.body as Uint8Array) as Record<string, unknown>;
			switch (request.method) {
				case "client.spec":
					return rpcResponse({ RPCVersion: 1, Capabilities: { Analytics: false, SetAttributes: false } });
				case "runs.get":
					if ((request.input as { RunID?: string }).RunID === "wrun_09") {
						return rpcResponse({ RunID: "wrun_09", Status: "running", SpecVersion: 9 });
					}
					return rpcResponse({ RunID: "wrun_01", Status: "failed", Input: json({ topic: "go" }), Error: json({ message: "boom", chain: [] }) });
				default:
					return rpcResponse({ RunID: "wrun_01", StepID: "step_01", Status: "completed", Input: json(1), Output: json("done") });
			}
		},
	});
	const run = await world.runs.get("wrun_01");
	expect(run.input).toEqual(json({ topic: "go" }));
	expect(run.error).toEqual(json({ message: "boom", chain: [] }));
	// A run the World recorded no version for has none, rather than the SDK's.
	expect(run).not.toHaveProperty("specVersion");
	const recorded = await world.runs.get("wrun_09");
	expect(recorded.specVersion).toBe(9);
	const step = await world.steps.get("wrun_01", "step_01");
	expect(step.input).toEqual(json(1));
	expect(step.output).toEqual(json("done"));
});

test("passes a run's executionContext through with its keys as written", async () => {
	const inputs: Record<string, unknown>[] = [];
	const world = await createWorld({
		baseURL: "https://workflow.example/v1/workflow",
		fetch: async (_url, init) => {
			const envelope = decode(init?.body as Uint8Array) as Record<string, unknown>;
			if (envelope.method === "client.spec") {
				return rpcResponse({ RPCVersion: 2, Capabilities: {} });
			}
			if (envelope.method === "runs.get") {
				return rpcResponse({ RunID: "wrun_01", ExecutionContext: { replayedFromRunId: "wrun_00" }, StartedAt: null, CompletedAt: null });
			}
			inputs.push(envelope.input as Record<string, unknown>);
			return rpcResponse({ Event: { RunID: "wrun_01", EventID: "evnt_01", EventType: "run_created", Data: null }, Allocated: 1 });
		},
	});
	await world.events.create("wrun_01", {
		eventType: "run_created",
		specVersion: SPEC_VERSION_SINGLE_ORCHESTRATOR,
		eventData: { workflowName: "wf", input: new Uint8Array(), executionContext: { replayedFromRunId: "wrun_00" } },
	} as never);
	expect(inputs[0]?.ExecutionContext).toEqual({ replayedFromRunId: "wrun_00" });
	const run = await world.runs.get("wrun_01");
	expect(run.executionContext).toEqual({ replayedFromRunId: "wrun_00" });
});

test("shapes hooks, runs and steps so the SDK's schemas accept them", async () => {
	const now = new Date();
	const world = await createWorld({
		baseURL: "https://workflow.example/v1/workflow",
		fetch: async (_url, init) => {
			const envelope = decode(init?.body as Uint8Array) as Record<string, unknown>;
			switch (envelope.method) {
				case "client.spec":
					return rpcResponse({ RPCVersion: 1, Capabilities: {} });
				case "hooks.get":
					// A Go hook as the server encodes one: no owner, and null or
					// empty for what it does not have.
					return rpcResponse({ RunID: "wrun_01", ResumeContext: { DeploymentID: "dpl_go", WorkflowName: "app.Report", EncryptionPublicKey: "", RunSpecVersion: 9 }, HookID: "hook_01", Token: "tok", Metadata: null, IsWebhook: false, TokenRetentionUntil: null, ClaimedFrom: null, SpecVersion: 9, ResumeDedup: false, CreatedAt: now });
				case "runs.get":
					return rpcResponse({ RunID: "wrun_01", WorkflowName: "app.Report", Status: "running", Input: null, Output: null, Error: null, ErrorCode: "", Attributes: {}, DeploymentID: "dpl_go", SpecVersion: 9, CreatedAt: now, UpdatedAt: now, StartedAt: now, CompletedAt: null });
				default:
					return rpcResponse({ RunID: "wrun_01", StepID: "step_01", StepName: "app.Send", Status: "pending", Input: null, Output: null, Error: null, Attempt: 0, CreatedAt: now, UpdatedAt: now, StartedAt: null, CompletedAt: null, RetryAfter: null, SpecVersion: 9 });
			}
		},
	});
	const hook = await world.hooks.get("hook_01");
	expect(HookSchema.safeParse(hook).error).toBeUndefined();
	expect(hook).not.toHaveProperty("metadata");
	expect(hook).not.toHaveProperty("claimedFrom");
	const run = await world.runs.get("wrun_01");
	expect(WorkflowRunSchema.safeParse(run).error).toBeUndefined();
	expect(run).not.toHaveProperty("completedAt");
	expect(run).not.toHaveProperty("errorCode");
	const step = await world.steps.get("wrun_01", "step_01");
	expect(StepSchema.safeParse(step).error).toBeUndefined();
	expect(step).not.toHaveProperty("startedAt");
	expect(step).not.toHaveProperty("retryAfter");
});
