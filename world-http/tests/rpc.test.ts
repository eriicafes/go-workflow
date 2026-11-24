import { decode, encode } from "cbor-x";
import { expect, test } from "vitest";
import { HTTPWorldError, createWorld } from "../index.js";
import { rpcContentType, rpcResponse } from "./helpers.js";

test("raises structured RPC errors", async () => {
	const world = await createWorld({
		baseURL: "https://workflow.example/v1/workflow",
		fetch: async (_url, init) => {
			const request = decode(init?.body as Uint8Array) as Record<string, unknown>;
			if (request.method === "client.spec") {
				return rpcResponse({ RPCVersion: 1, Capabilities: { Analytics: false, SetAttributes: false } });
			}
			const encoded = encode({ error: { code: "not_found", message: "workflow: run not found", sentinel: "run_not_found" } });
			const body = new Uint8Array(encoded.byteLength);
			body.set(encoded);
			return new Response(body, {
				headers: { "Content-Type": rpcContentType },
			});
		},
	});

	await expect(world.runs.get("wrun_missing")).rejects.toMatchObject({
		code: "not_found",
		status: 404,
		sentinel: "run_not_found",
		name: "WorkflowRunNotFoundError",
	});
});

test("passes workflow and step names through as written", async () => {
	const calls: { method: unknown; input: unknown }[] = [];
	const world = await createWorld({
		baseURL: "https://workflow.example/v1/workflow",
		fetch: async (_url, init) => {
			const envelope = decode(init?.body as Uint8Array) as Record<string, unknown>;
			calls.push({ method: envelope.method, input: envelope.input });
			switch (envelope.method) {
				case "client.spec":
					return rpcResponse({ RPCVersion: 1, Capabilities: { Analytics: false, SetAttributes: false } });
				case "runs.get":
					return rpcResponse({ RunID: "wrun_01", WorkflowName: "workflow//example.com/app/workflows//PublishReport" });
				case "steps.list":
					return rpcResponse({
						Data: [
							{ StepID: "step_01", StepName: "step//example.com/app/flows//writeDraft" },
							{ StepID: "step_02", StepName: "step//example.com/app/flows//PublishReport/func1" },
							{ StepID: "step_03", StepName: "step//./src/jobs//send" },
						],
						HasMore: false,
					});
				default:
					return rpcResponse({ Event: { RunID: "wrun_01", EventID: "evnt_01", EventType: "run_created", Data: null }, Run: { RunID: "wrun_01" }, Allocated: 1 });
			}
		},
	});

	const run = await world.runs.get("wrun_01");
	expect(run.workflowName).toBe("workflow//example.com/app/workflows//PublishReport");

	const steps = await world.steps.list({ runId: "wrun_01" } as never);
	expect(steps.data.map((step) => step.stepName)).toEqual([
		"step//example.com/app/flows//writeDraft",
		"step//example.com/app/flows//PublishReport/func1",
		"step//./src/jobs//send",
	]);

	await world.events.create(null, {
		eventType: "run_created",
		eventData: { workflowName: run.workflowName, input: new Uint8Array() },
	} as never);
	expect(calls.find((call) => call.method === "events.createRun")?.input).toMatchObject({
		WorkflowName: "workflow//example.com/app/workflows//PublishReport",
	});
});

test("raises a throttled RPC failure as the SDK's ThrottleError", async () => {
	const world = await createWorld({
		baseURL: "https://workflow.example/v1/workflow",
		fetch: async (_url, init) => {
			const envelope = decode(init?.body as Uint8Array) as Record<string, unknown>;
			if (envelope.method === "client.spec") {
				return rpcResponse({ RPCVersion: 2, Capabilities: { DeleteData: true } });
			}
			const encoded = encode({ error: { code: "resource_exhausted", message: "busy", sentinel: "throttled", retryAfter: 2_500_000_000 } });
			return new Response(new Uint8Array(encoded), { headers: { "Content-Type": "application/cbor" } });
		},
	});
	await expect(world.runs.deleteDataMany!({ runIds: ["wrun_01"] })).rejects.toMatchObject({ name: "ThrottleError", retryAfter: 3, status: 429 });
});

test("reports World failures as the SDK's error classes with their fields", async () => {
	const failures: Record<string, Record<string, unknown>> = {
		wrun_claimed: { code: "conflict", message: "taken", sentinel: "hook_force_claimed", details: { Token: "tok", RunID: "wrun_new", HookID: "hook_new" } },
		wrun_fenced: { code: "conflict", message: "stale", sentinel: "in_band_superseded", details: { EventCount: 9, InBandCount: 4 } },
		wrun_conflict: { code: "conflict", message: "held", sentinel: "hook_conflict", conflictingRunId: "wrun_owner", details: { Token: "tok", RunID: "wrun_owner", Start: false } },
		wrun_step: { code: "not_found", message: "no step", sentinel: "step_not_found" },
	};
	const world = await createWorld({
		baseURL: "https://workflow.example/v1/workflow",
		fetch: async (_url, init) => {
			const envelope = decode(init?.body as Uint8Array) as Record<string, unknown>;
			if (envelope.method === "client.spec") {
				return rpcResponse({ RPCVersion: 1, Capabilities: {} });
			}
			const encoded = encode({ error: failures[(envelope.input as { RunID: string }).RunID] });
			return new Response(new Uint8Array(encoded), { headers: { "Content-Type": rpcContentType } });
		},
	});
	await expect(world.runs.get("wrun_claimed")).rejects.toMatchObject({ name: "HookForceClaimedError", token: "tok", claimedByRunId: "wrun_new", claimedByHookId: "hook_new" });
	await expect(world.runs.get("wrun_fenced")).rejects.toMatchObject({ name: "InBandSupersededError", status: 412, seq: 9, seqInBand: 4 });
	await expect(world.runs.get("wrun_conflict")).rejects.toMatchObject({ name: "HookConflictError", token: "tok", conflictingRunId: "wrun_owner" });
	// A failure without a class of its own is the SDK's WorkflowWorldError,
	// whose status callers read.
	await expect(world.runs.get("wrun_step")).rejects.toMatchObject({ name: "WorkflowWorldError", status: 404 });
	await expect(world.runs.get("wrun_step")).rejects.toBeInstanceOf(HTTPWorldError);
});
