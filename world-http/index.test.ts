import { execFileSync, spawn, type ChildProcess } from "node:child_process";
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { createHash } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";
import { decode, encode } from "cbor-x";
import {
	AnalyticsEventSchema,
	AnalyticsHookSchema,
	AnalyticsRunSchema,
	EventSchema,
	type EventResult,
	HOOK_RESUME_DEDUP_VERSION,
	HookSchema,
	SPEC_VERSION_SINGLE_ORCHESTRATOR,
	StepSchema,
	WorkflowRunSchema,
} from "@workflow/world";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { HTTPWorldError, createWorld, type HTTPWorld } from "./index.js";

const rpcContentType = "application/cbor";

function rpcResponse(output: unknown): Response {
	const encoded = encode({ output });
	const body = new Uint8Array(encoded.byteLength);
	body.set(encoded);
	return new Response(body, {
		headers: { "Content-Type": rpcContentType },
	});
}

test("creates a world from its spec and maps Go CBOR fields", async () => {
	const requests: { url: string | URL | Request; init: RequestInit; envelope: Record<string, unknown> }[] = [];
	const world = await createWorld({
		baseURL: "https://workflow.example/v1/workflow/",
		fetch: async (url, init) => {
			const envelope = decode(init?.body as Uint8Array) as Record<string, unknown>;
			requests.push({ url, init: init ?? {}, envelope });
			if (envelope.method === "client.spec") {
				return rpcResponse({ RPCVersion: 1, Capabilities: { Analytics: false, SetAttributes: true } });
			}
			expect(envelope.method).toBe("runs.get");
			expect(envelope.input).toEqual({ RunID: "wrun_01" });
			return rpcResponse({
				RunID: "wrun_01",
				WorkflowName: "report",
				Status: "completed",
				Attributes: { "external-id": "42" },
			});
		},
	});

	// Go runs are single-orchestrator runs, which start() stamps new runs as.
	expect(world.specVersion).toBe(SPEC_VERSION_SINGLE_ORCHESTRATOR);
	expect(world.specVersion).toBe(9);
	expect(world._spec).toEqual({ rpcVersion: 1, capabilities: { analytics: false, setAttributes: true } });
	expect(requests[0]?.envelope.rpcVersion).toBe(1);
	expect(world.capabilities).toEqual({
		hookRetention: { active: true },
		hookForceClaim: true,
		inBandFence: true,
		maxConcurrency: true,
	});

	const run = await world.runs.get("wrun_01");
	expect(run.runId).toBe("wrun_01");
	expect(run.workflowName).toBe("report");
	expect(run.attributes).toEqual({ "external-id": "42" });
	expect(requests[1]?.envelope.rpcVersion).toBe(1);
	expect(requests[0]?.url).toBe("https://workflow.example/v1/workflow/rpc");
	expect(new Headers(requests[0]?.init.headers).get("Content-Type")).toBe(rpcContentType);
});

test("passes deployment IDs through to the Go world", async () => {
	const calls: { method: unknown; input: unknown }[] = [];
	const world = await createWorld({
		baseURL: "https://workflow.example/v1/workflow",
		fetch: async (_url, init) => {
			const envelope = decode(init?.body as Uint8Array) as Record<string, unknown>;
			const input = envelope.input;
			calls.push({ method: envelope.method, input });
			switch (envelope.method) {
				case "client.spec":
					return rpcResponse({ RPCVersion: 1, Capabilities: { Analytics: false, SetAttributes: false, DeploymentAffinity: true } });
				case "world.deploymentId":
					return rpcResponse("dpl_go");
				case "events.createRun":
					return rpcResponse({ Event: { RunID: "wrun_01", EventID: "evnt_01", EventType: "run_created", Data: null }, Run: { RunID: "wrun_01" }, Allocated: 1 });
				default:
					return rpcResponse({ MessageID: "msg_01" });
			}
		},
	});

	expect(world.capabilities).toMatchObject({ deploymentAffinity: true });
	await expect(world.getDeploymentId()).resolves.toBe("dpl_go");
	await expect(world.getDeploymentId()).resolves.toBe("dpl_go");
	expect(calls.filter((call) => call.method === "world.deploymentId")).toHaveLength(2);

	await world.events.create("wrun_01", {
		eventType: "run_created",
		eventData: { workflowName: "report", input: new Uint8Array(), deploymentId: "dpl_go" },
	} as never);
	await world.queue("__wkf_workflow_report" as never, { runId: "wrun_01" } as never, { deploymentId: "dpl_go" });

	expect(calls.find((call) => call.method === "events.createRun")?.input).toMatchObject({ RunID: "wrun_01", DeploymentID: "dpl_go" });
	expect(calls.find((call) => call.method === "queue.queue")?.input).toMatchObject({ RunID: "wrun_01", DeploymentID: "dpl_go" });
});

test("uses WORKFLOW_HTTP_BASE_URL when no base URL is passed", async () => {
	const previous = process.env.WORKFLOW_HTTP_BASE_URL;
	process.env.WORKFLOW_HTTP_BASE_URL = "https://workflow.example/v1/workflow/";
	try {
		let url: string | URL | Request | undefined;
		await createWorld({
			fetch: async (requestURL, init) => {
				url = requestURL;
				const request = decode(init?.body as Uint8Array) as Record<string, unknown>;
				expect(request.method).toBe("client.spec");
				return rpcResponse({ RPCVersion: 1, Capabilities: { Analytics: false, SetAttributes: false } });
			},
		});
		expect(url).toBe("https://workflow.example/v1/workflow/rpc");
	} finally {
		if (previous === undefined) {
			delete process.env.WORKFLOW_HTTP_BASE_URL;
		} else {
			process.env.WORKFLOW_HTTP_BASE_URL = previous;
		}
	}
});

test("pulls stream chunks through bounded RPC polling", async () => {
	const calls: { method: unknown; input: unknown }[] = [];
	const world = await createWorld({
		baseURL: "https://workflow.example/v1/workflow",
		fetch: async (_url, init) => {
			const request = decode(init?.body as Uint8Array) as { method: unknown; input: unknown };
			calls.push({ method: request.method, input: request.input });
			if (request.method === "client.spec") {
				return rpcResponse({ RPCVersion: 1, Capabilities: { Analytics: true, SetAttributes: false } });
			}
			expect(request.method).toBe("streams.getChunks");
			expect(calls.at(-1)?.input).toMatchObject({ Wait: 100_000_000 });
			return rpcResponse({
				Data: [{ Index: 0, Data: new Uint8Array([1, 2, 3]) }],
				Cursor: "1",
				HasMore: false,
				Done: true,
				RetryAfter: 50_000_000,
			});
		},
	});

	const reader = (await world.streams.get("wrun_01", "strm_progress")).getReader();
	const first = await reader.read();
	const last = await reader.read();
	expect(first.value).toEqual(new Uint8Array([1, 2, 3]));
	expect(last.done).toBe(true);
	expect(calls.filter((call) => call.method === "streams.getChunks")).toHaveLength(1);
});

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

test("queues health checks with the SDK message for the app to answer", async () => {
	const calls: { method: unknown; input: Record<string, unknown> }[] = [];
	const world = await createWorld({
		baseURL: "https://workflow.example/v1/workflow",
		fetch: async (_url, init) => {
			const envelope = decode(init?.body as Uint8Array) as Record<string, unknown>;
			if (envelope.method === "client.spec") {
				return rpcResponse({ RPCVersion: 1, Capabilities: { Analytics: false, SetAttributes: false } });
			}
			calls.push({ method: envelope.method, input: envelope.input as Record<string, unknown> });
			return rpcResponse({ MessageID: "msg_probe" });
		},
	});

	await expect(world.queue("__wkf_workflow_report" as never, { __healthCheck: true, correlationId: "probe_01" })).resolves.toEqual({ messageId: "msg_probe" });
	const queued = calls.find((call) => call.method === "queue.queue")?.input;
	expect(queued?.RunID).toBeUndefined();
	expect(decode(queued?.SdkMessage as Uint8Array)).toEqual({ queueName: "__wkf_workflow_report", message: { __healthCheck: true, correlationId: "probe_01" } });
});

test("maps Go workflow and step names to Workflow SDK names and back", async () => {
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
					return rpcResponse({ RunID: "wrun_01", WorkflowName: "example.com/app/workflows.PublishReport" });
				case "steps.list":
					return rpcResponse({
						Data: [
							{ StepID: "step_01", StepName: "example.com/app/flows.writeDraft" },
							{ StepID: "step_02", StepName: "example.com/app/flows.PublishReport.func1" },
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
		WorkflowName: "example.com/app/workflows.PublishReport",
	});
});

test("exposes stored event data as eventData with Workflow SDK names", async () => {
	const goData = new TextEncoder().encode(JSON.stringify({ stepName: "example.com/app/flows.writeDraft", input: "ZGV2bA==" }));
	const world = await createWorld({
		baseURL: "https://workflow.example/v1/workflow",
		fetch: async (_url, init) => {
			const envelope = decode(init?.body as Uint8Array) as Record<string, unknown>;
			if (envelope.method === "client.spec") {
				return rpcResponse({ RPCVersion: 1, Capabilities: { Analytics: false, SetAttributes: false } });
			}
			expect(envelope.method).toBe("events.list");
			return rpcResponse({
				Data: [
					{ EventID: "evnt_01", EventType: "step_created", CorrelationID: "step_01", Data: goData },
					{ EventID: "evnt_02", EventType: "hook_received", CorrelationID: "hook_01", Data: new TextEncoder().encode('{"payload":"AQI="}') },
					{ EventID: "evnt_03", EventType: "run_started", CorrelationID: "", Data: null },
				],
				HasMore: false,
				Snapshot: { EventCount: 3, InBandCount: 2 },
			});
		},
	});

	const events = await world.events.list({ runId: "wrun_01" } as never);
	expect(events.data.map((event) => (event as { eventData?: unknown }).eventData)).toEqual([
		{ stepName: "step//example.com/app/flows//writeDraft", input: new TextEncoder().encode("devl") },
		{ payload: new Uint8Array([1, 2]) },
		undefined,
	]);
	expect(events.data[0]).not.toHaveProperty("data");
	expect(events.snapshot).toEqual({ seq: 3, seqInBand: 2 });
});

test("writes event data as JSON and shapes event results like the Workflow SDK", async () => {
	const calls: { method: unknown; input: Record<string, unknown> }[] = [];
	const world = await createWorld({
		baseURL: "https://workflow.example/v1/workflow",
		fetch: async (_url, init) => {
			const envelope = decode(init?.body as Uint8Array) as Record<string, unknown>;
			const input = envelope.input as Record<string, unknown>;
			calls.push({ method: envelope.method, input });
			switch (envelope.method) {
				case "client.spec":
					return rpcResponse({ RPCVersion: 1, Capabilities: { Analytics: false, SetAttributes: false } });
				case "events.create":
					return rpcResponse({
						Event: { RunID: "wrun_01", EventID: "evnt_02", EventType: "step_created", CorrelationID: "step_01", Data: (input.Event as Record<string, unknown>).Data },
						Step: { StepID: "step_01", StepName: "example.com/app/flows.write" },
						StepCreated: true,
						Allocated: 1,
						Events: [],
						HasMore: false,
					});
				default:
					return rpcResponse({
						Results: [
							{ Status: 409, Error: "conflict", Message: "workflow: run is terminal" },
							{ Status: 200, Event: { RunID: "wrun_01", EventID: "evnt_03", EventType: "wait_created", CorrelationID: "wait_01", Data: null } },
						],
					});
			}
		},
	});

	const result = await world.events.create(
		"wrun_01",
		{ eventType: "step_created", correlationId: "step_01", eventData: { stepName: "write", input: new Uint8Array([1, 2]) } } as never,
		{ inBand: true, expectedSeqInBand: 4, replayEventObserver: () => {} } as never,
	);
	const sent = calls.find((call) => call.method === "events.create")?.input;
	expect(new TextDecoder().decode((sent?.Event as Record<string, unknown>).Data as Uint8Array)).toBe('{"stepName":"write","input":"AQI="}');
	expect(sent?.Options).toEqual({ InBandCount: 4 });
	expect(result).toMatchObject({
		event: { eventId: "evnt_02", eventData: { stepName: "write", input: new Uint8Array([1, 2]) } },
		step: { stepId: "step_01", stepName: "step//example.com/app/flows//write" },
		stepCreated: true,
		allocated: 1,
		events: [],
		cursor: null,
		hasMore: false,
	});

	const batch = await world.events.createBatch!("wrun_01", [
		{ event: { eventType: "run_cancelled" } },
		{ event: { eventType: "wait_created", correlationId: "wait_01", eventData: { resumeAt: new Date(0) } } },
	] as never);
	expect(batch.results).toEqual([
		{ status: 409, error: "conflict", message: "workflow: run is terminal" },
		{ status: 200, event: { runId: "wrun_01", eventId: "evnt_03", eventType: "wait_created", correlationId: "wait_01" }, run: undefined, step: undefined },
	]);
});

test("reads a stream from a chunk index, then follows the cursor", async () => {
	const inputs: Record<string, unknown>[] = [];
	const world = await createWorld({
		baseURL: "https://workflow.example/v1/workflow",
		fetch: async (_url, init) => {
			const envelope = decode(init?.body as Uint8Array) as Record<string, unknown>;
			if (envelope.method === "client.spec") {
				return rpcResponse({ RPCVersion: 1, Capabilities: { Analytics: false, SetAttributes: false } });
			}
			const input = envelope.input as Record<string, unknown>;
			inputs.push(input);
			return inputs.length === 1
				? rpcResponse({ Data: [{ Index: 3, Data: new Uint8Array([3]) }], Cursor: "4", HasMore: false, Done: false })
				: rpcResponse({ Data: [{ Index: 4, Data: new Uint8Array([4]) }], Cursor: "5", HasMore: false, Done: true });
		},
	});

	const reader = (await world.streams.get("wrun_01", "strm_progress", -2)).getReader();
	expect((await reader.read()).value).toEqual(new Uint8Array([3]));
	expect((await reader.read()).value).toEqual(new Uint8Array([4]));
	expect((await reader.read()).done).toBe(true);
	expect(inputs[0]).toMatchObject({ StartIndex: -2, Cursor: "" });
	expect(inputs[1]).toMatchObject({ Cursor: "4" });
});

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

test("waits for a terminal run across bounded requests", async () => {
	const timeouts: unknown[] = [];
	const world = await createWorld({
		baseURL: "https://workflow.example/v1/workflow",
		fetch: async (_url, init) => {
			const request = decode(init?.body as Uint8Array) as Record<string, unknown>;
			if (request.method === "client.spec") {
				return rpcResponse({ RPCVersion: 1, Capabilities: { Analytics: false, SetAttributes: false } });
			}
			timeouts.push((request.input as Record<string, unknown>).Timeout);
			return rpcResponse({ RunID: "wrun_01", Status: timeouts.length < 3 ? "running" : "completed" });
		},
	});
	const run = await world.runs.waitForTerminalStatus!("wrun_01");
	expect(run.status).toBe("completed");
	// Nanoseconds past 2^32 travel as 64-bit integers, which Go decodes.
	expect(timeouts).toEqual([10_000_000_000n, 10_000_000_000n, 10_000_000_000n]);
});

test("sends a bearer token from the option or WORKFLOW_HTTP_TOKEN", async () => {
	const authorization = async (options: Parameters<typeof createWorld>[0]) => {
		let header: string | null = null;
		await createWorld({
			baseURL: "https://workflow.example/v1/workflow",
			...options,
			fetch: async (_url, init) => {
				header = new Headers(init?.headers).get("Authorization");
				return rpcResponse({ RPCVersion: 1, Capabilities: {} });
			},
		});
		return header;
	};

	const previous = process.env.WORKFLOW_HTTP_TOKEN;
	try {
		delete process.env.WORKFLOW_HTTP_TOKEN;
		expect(await authorization({})).toBeNull();
		expect(await authorization({ token: "option" })).toBe("Bearer option");

		process.env.WORKFLOW_HTTP_TOKEN = "env";
		expect(await authorization({})).toBe("Bearer env");
		expect(await authorization({ token: "option" })).toBe("Bearer option");
		expect(await authorization({ headers: { Authorization: "Basic custom" } })).toBe("Basic custom");
	} finally {
		if (previous === undefined) delete process.env.WORKFLOW_HTTP_TOKEN;
		else process.env.WORKFLOW_HTTP_TOKEN = previous;
	}
});

test("reads a run's encryption key from the Go world", async () => {
	const inputs: Record<string, unknown>[] = [];
	const world = await createWorld({
		baseURL: "https://workflow.example/v1/workflow",
		fetch: async (_url, init) => {
			const envelope = decode(init?.body as Uint8Array) as Record<string, unknown>;
			if (envelope.method === "client.spec") {
				return rpcResponse({ RPCVersion: 2, Capabilities: { Analytics: false, SetAttributes: false } });
			}
			expect(envelope.method).toBe("runs.encryptionKey");
			const input = envelope.input as Record<string, unknown>;
			inputs.push(input);
			return rpcResponse(input.RunID === "wrun_plain" ? null : new Uint8Array(32).fill(7));
		},
	});
	expect(await world.getEncryptionKeyForRun("wrun_01", { deploymentId: "dpl_1" })).toEqual(new Uint8Array(32).fill(7));
	expect(await world.getEncryptionKeyForRun("wrun_plain")).toBeUndefined();
	expect(inputs).toEqual([
		{ RunID: "wrun_01", DeploymentID: "dpl_1" },
		{ RunID: "wrun_plain", DeploymentID: "" },
	]);
});

test("deletes run data only when the Go world supports it, and reports deleted runs as expired", async () => {
	const inputs: unknown[] = [];
	const create = (deleteData: boolean) =>
		createWorld({
			baseURL: "https://workflow.example/v1/workflow",
			fetch: async (_url, init) => {
				const envelope = decode(init?.body as Uint8Array) as Record<string, unknown>;
				if (envelope.method === "client.spec") {
					return rpcResponse({ RPCVersion: 2, Capabilities: { Analytics: false, SetAttributes: false, DeleteData: deleteData } });
				}
				if (envelope.method === "runs.get") {
					return rpcResponse({ RunID: "wrun_01", Status: "completed", ExpiredAt: new Date("2026-01-02T03:04:05Z"), StartedAt: null, CompletedAt: null });
				}
				expect(envelope.method).toBe("runs.deleteDataMany");
				inputs.push(envelope.input);
				return rpcResponse({
					Summary: { Requested: 2, Deleted: 1, AlreadyDeleted: 0, Skipped: 1, NotCreated: 0, NotFound: 0, Failed: 0 },
					Results: [
						{ RunID: "wrun_01", Outcome: "deleted" },
						{ RunID: "wrun_02", Outcome: "skipped", Reason: "run_not_terminal", Status: "running" },
					],
				});
			},
		});

	expect((await create(false)).runs.deleteDataMany).toBeUndefined();
	const world = await create(true);
	const result = await world.runs.deleteDataMany!({ runIds: ["wrun_01", "wrun_02"] });
	expect(result).toEqual({
		summary: { requested: 2, deleted: 1, alreadyDeleted: 0, skipped: 1, notCreated: 0, notFound: 0, failed: 0 },
		results: [
			{ runId: "wrun_01", outcome: "deleted" },
			{ runId: "wrun_02", outcome: "skipped", reason: "run_not_terminal", status: "running" },
		],
	});
	expect(inputs).toHaveLength(1);
	const run = await world.runs.get("wrun_01");
	expect(run.expiredAt).toEqual(new Date("2026-01-02T03:04:05Z"));
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

test("creates a start hook with its run and reports a refused one as the SDK's HookConflictError", async () => {
	const inputs: Record<string, unknown>[] = [];
	const world = await createWorld({
		baseURL: "https://workflow.example/v1/workflow",
		fetch: async (_url, init) => {
			const envelope = decode(init?.body as Uint8Array) as Record<string, unknown>;
			if (envelope.method === "client.spec") {
				return rpcResponse({ RPCVersion: 2, Capabilities: { StartHook: true } });
			}
			const input = envelope.input as Record<string, unknown>;
			inputs.push(input);
			if (input.RunID === "wrun_02") {
				const encoded = encode({ error: { code: "conflict", message: "taken", sentinel: "hook_conflict", conflictingRunId: "wrun_01" } });
				return new Response(new Uint8Array(encoded), { headers: { "Content-Type": "application/cbor" } });
			}
			if (input.RunID === "wrun_03") {
				const encoded = encode({ error: { code: "invalid_argument", message: "too old", sentinel: "start_hook_rejected" } });
				return new Response(new Uint8Array(encoded), { headers: { "Content-Type": "application/cbor" } });
			}
			if (input.RunID === "wrun_04") {
				return rpcResponse({ Event: { RunID: "wrun_04", EventID: "evnt_01", EventType: "run_created", Data: null }, Run: { RunID: "wrun_04", StartedAt: null, CompletedAt: null, StartHook: { Token: "tok", HookID: "hook_04" } }, Allocated: 1 });
			}
			if (input.RunID === "wrun_05") {
				const encoded = encode({ error: { code: "invalid_argument", message: "off", sentinel: "start_hook_disabled" } });
				return new Response(new Uint8Array(encoded), { headers: { "Content-Type": "application/cbor" } });
			}
			return rpcResponse({ Event: { RunID: "wrun_01", EventID: "evnt_01", EventType: "run_created", Data: null }, Run: { RunID: "wrun_01", StartedAt: null, CompletedAt: null }, Allocated: 1 });
		},
	});
	expect(world.capabilities).toMatchObject({ atomicStartHook: { active: true } });
	const create = (runId: string) =>
		world.events.create(runId, {
			eventType: "run_created",
			specVersion: SPEC_VERSION_SINGLE_ORCHESTRATOR,
			eventData: { workflowName: "wf", input: new Uint8Array(), startHook: { token: "tok" } },
		} as never);
	const created = (await create("wrun_01")) as { run?: { startHook?: { token: string; hookId: string } } };
	expect(inputs[0]?.StartHook).toEqual({ Token: "tok" });
	expect(created.run?.startHook).toEqual({ token: "tok", hookId: "hook_01" });
	await expect(create("wrun_02")).rejects.toMatchObject({ name: "HookConflictError", token: "tok", conflictingRunId: "wrun_01" });
	await expect(create("wrun_03")).rejects.toMatchObject({ name: "WorkflowWorldError", code: "START_HOOK_ADMISSION_REJECTED" });
	const echoed = (await create("wrun_04")) as { run?: { startHook?: { token: string; hookId: string } } };
	expect(echoed.run?.startHook).toMatchObject({ token: "tok", hookId: "hook_04" });
	// A World whose admission is switched off decided nothing, which the
	// SDK's start() tells from a rejection by the code.
	await expect(create("wrun_05")).rejects.toMatchObject({ name: "WorkflowWorldError", code: "START_HOOK_ADMISSION_DISABLED", status: 400 });
});

test("reports each message of a batch and forwards a step's telemetry", async () => {
	const calls: { method: unknown; input: Record<string, unknown> }[] = [];
	const world = await createWorld({
		baseURL: "https://workflow.example/v1/workflow",
		fetch: async (_url, init) => {
			const envelope = decode(init?.body as Uint8Array) as Record<string, unknown>;
			if (envelope.method === "client.spec") {
				return rpcResponse({ RPCVersion: 1, Capabilities: {} });
			}
			calls.push({ method: envelope.method, input: envelope.input as Record<string, unknown> });
			if (envelope.method === "queue.queueBatch") {
				return rpcResponse([{ MessageID: "msg_01" }, { MessageID: "", Error: "busy", Retryable: true }, { MessageID: "", Error: "refused" }, { MessageID: "" }]);
			}
			return rpcResponse({ Event: { RunID: "wrun_01", EventID: "evnt_02", EventType: "wait_created", CorrelationID: "step_01", Data: null }, Allocated: 1 });
		},
	});
	const createdAt = 1_700_000_000_123;
	const step = (stepId: string, fields: Record<string, unknown> = {}) => ({ message: { runId: "wrun_01", stepId, stepName: "step//example.com/app//send", ...fields } as never, opts: { idempotencyKey: stepId } });
	const results = await world.queueBatch("__wkf_workflow_workflow//example.com/app//Report" as never, [
		step("step_01", { stepCreatedAt: createdAt }),
		step("step_02"),
		step("step_03"),
		step("step_04"),
	]);
	// A refused message is reported in its place, with whether sending the
	// batch again may store it, and a message stored without an ID has none.
	expect(results).toEqual([
		{ messageId: "msg_01" },
		{ messageId: null, error: "busy", retryable: true },
		{ messageId: null, error: "refused", retryable: false },
		{ messageId: null },
	]);
	const items = calls.find((call) => call.method === "queue.queueBatch")?.input.Items as Record<string, unknown>[];
	expect(items[0]?.StepCreatedAt).toEqual(new Date(createdAt));
	expect(items[1]).not.toHaveProperty("StepCreatedAt");

	// A step's creation time and the mark of a failure recorded after its
	// function ran reach the World with the write.
	await world.events.create("wrun_01", { eventType: "step_started", correlationId: "step_01", eventData: { stepName: "send", attempt: 1 } } as never, { stepCreatedAt: createdAt });
	await world.events.create("wrun_01", { eventType: "step_failed", correlationId: "step_01", eventData: { stepName: "send", attempt: 1, error: new Uint8Array([1]) } } as never, { afterStepBody: true });
	const writes = calls.filter((call) => call.method === "events.create").map((call) => call.input.Options);
	expect(writes).toEqual([{ StepCreatedAt: new Date(createdAt) }, { AfterStepBody: true }]);
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

test("forwards reserved attributes, the queue's workflow and the waits to complete", async () => {
	const calls: { method: unknown; input: Record<string, unknown> }[] = [];
	const world = await createWorld({
		baseURL: "https://workflow.example/v1/workflow",
		fetch: async (_url, init) => {
			const envelope = decode(init?.body as Uint8Array) as Record<string, unknown>;
			if (envelope.method === "client.spec") {
				return rpcResponse({ RPCVersion: 1, Capabilities: { SetAttributes: true } });
			}
			calls.push({ method: envelope.method, input: envelope.input as Record<string, unknown> });
			if (envelope.method === "events.createRun") {
				return rpcResponse({ Event: { RunID: "wrun_01", EventID: "evnt_01", EventType: "run_created", Data: null }, Allocated: 1 });
			}
			return rpcResponse({ RunID: "wrun_01", Attributes: {} });
		},
	});
	await world.events.create("wrun_01", {
		eventType: "run_created",
		eventData: { workflowName: "wf", input: new Uint8Array(), attributes: { $retention: "0" }, allowReservedAttributes: true },
	} as never);
	await world.runs.experimentalSetAttributes?.("wrun_01", [{ key: "$parentRunId", value: "wrun_00" }] as never, { allowReservedAttributes: true });
	await world.queue("__wkf_workflow_workflow//example.com/app//Report" as never, { runId: "wrun_01", runSpecVersion: 9, completeWaits: ["wait_01"] } as never);
	await world.queue("orders" as never, { runId: "wrun_01" } as never);

	expect(calls[0]?.input).toMatchObject({ AllowReservedAttributes: true });
	expect(calls.find((call) => call.method === "runs.setAttributes")?.input).toMatchObject({ AllowReservedAttributes: true });
	const queued = calls.filter((call) => call.method === "queue.queue").map((call) => call.input);
	// The SDK's queue name is the workflow's, in the Go World's form.
	expect(queued[0]).toMatchObject({ WorkflowName: "example.com/app.Report", CompleteWaits: ["wait_01"], RunSpecVersion: 9 });
	expect(queued[0]).not.toHaveProperty("QueueName");
	expect(queued[1]).toMatchObject({ QueueName: "orders" });
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

test("forwards a run's creation on its first message, keyed by the run", async () => {
	const calls: { method: unknown; input: Record<string, unknown> }[] = [];
	const world = await createWorld({
		baseURL: "https://workflow.example/v1/workflow",
		fetch: async (_url, init) => {
			const envelope = decode(init?.body as Uint8Array) as Record<string, unknown>;
			if (envelope.method === "client.spec") {
				return rpcResponse({ RPCVersion: 1, Capabilities: {} });
			}
			calls.push({ method: envelope.method, input: envelope.input as Record<string, unknown> });
			if (envelope.method === "queue.queueBatch") {
				return rpcResponse([{ MessageID: "msg_01" }, { MessageID: "msg_02" }]);
			}
			if (envelope.method === "queue.queue") {
				return rpcResponse({ MessageID: "msg_01" });
			}
			return rpcResponse({ Event: { RunID: "wrun_01", EventID: "evnt_01", EventType: "run_created", Data: null }, Allocated: 1 });
		},
	});
	const creation = {
		deploymentId: "dpl_go",
		workflowName: "workflow//example.com/app//Report",
		input: new Uint8Array([1]),
		executionContext: { workflowCoreVersion: "5.7.1" },
		attributes: { $rootRunId: "wrun_00" },
		allowReservedAttributes: true,
		startHook: { token: "tok" },
	};
	await world.events.create("wrun_01", { eventType: "run_created", specVersion: 9, eventData: creation } as never);
	await world.queue(
		"__wkf_workflow_workflow//example.com/app//Report" as never,
		{ runId: "wrun_01", traceCarrier: {}, runInput: { ...creation, specVersion: 8, environment: "production" } } as never,
		{ deploymentId: "dpl_go", specVersion: 8, retentionSeconds: 60 },
	);
	const created = calls.find((call) => call.method === "events.createRun")?.input;
	const queued = calls.find((call) => call.method === "queue.queue")?.input;
	// The message's delivery creates the run run_created describes.
	expect(queued?.RunInput).toEqual(created);
	expect(queued?.RunInput).toMatchObject({ RunID: "wrun_01", WorkflowName: "example.com/app.Report", AllowReservedAttributes: true, StartHook: { Token: "tok" } });
	// A Go World records the run at its own spec version, whatever was asked.
	expect(queued).toMatchObject({ WorkflowName: "example.com/app.Report", IdempotencyKey: "wrun_01", RunSpecVersion: 9, DeploymentID: "dpl_go", Retention: 60_000_000_000n });

	// A wake falls back to the options' spec version, and a step queue's
	// name is the SDK's, not a queue of the Go World.
	await world.queue("__wkf_step_step//example.com/app//send" as never, { runId: "wrun_01", stepId: "step_01" } as never, { specVersion: 9 });
	const step = calls.filter((call) => call.method === "queue.queue")[1]?.input;
	expect(step).toMatchObject({ RunID: "wrun_01", StepID: "step_01", RunSpecVersion: 9 });
	expect(step).not.toHaveProperty("QueueName");
	expect(step).not.toHaveProperty("IdempotencyKey");

	const results = await world.queueBatch("__wkf_workflow_workflow//example.com/app//Report" as never, [
		{ message: { runId: "wrun_01", runSpecVersion: 9 } as never },
		{ message: { runId: "wrun_02", runSpecVersion: 9 } as never, opts: { idempotencyKey: "key" } },
	]);
	expect(results).toEqual([{ messageId: "msg_01" }, { messageId: "msg_02" }]);
	expect(calls.find((call) => call.method === "queue.queueBatch")?.input).toEqual({
		Items: [
			{ WorkflowName: "example.com/app.Report", RunID: "wrun_01", RunSpecVersion: 9, SdkMessage: expect.any(Uint8Array) },
			{ WorkflowName: "example.com/app.Report", RunID: "wrun_02", RunSpecVersion: 9, IdempotencyKey: "key", SdkMessage: expect.any(Uint8Array) },
		],
	});
	// Each item carries the SDK's queue name and message, unchanged.
	const items = calls.find((call) => call.method === "queue.queueBatch")?.input.Items as Record<string, unknown>[];
	expect(decode(items[1]?.SdkMessage as Uint8Array)).toEqual({ queueName: "__wkf_workflow_workflow//example.com/app//Report", message: { runId: "wrun_02", runSpecVersion: 9 } });
});

test("reports a hook's resume deduplication, event times as Dates, and stores Go names", async () => {
	const now = new Date();
	const stored: Uint8Array[] = [];
	const world = await createWorld({
		baseURL: "https://workflow.example/v1/workflow",
		fetch: async (_url, init) => {
			const envelope = decode(init?.body as Uint8Array) as Record<string, unknown>;
			const input = envelope.input as Record<string, unknown>;
			switch (envelope.method) {
				case "client.spec":
					return rpcResponse({ RPCVersion: 1, Capabilities: { DeploymentAffinity: true } });
				case "client.runs.startDeployment":
					expect(input).toEqual({ Latest: true });
					return rpcResponse("dpl_latest");
				case "hooks.getByToken":
					return rpcResponse({ RunID: "wrun_01", HookID: "hook_01", Token: "tok", Metadata: null, IsWebhook: false, SpecVersion: 9, ResumeDedup: true, CreatedAt: now });
				case "hooks.get":
					return rpcResponse({ RunID: "wrun_01", HookID: "hook_01", Token: "tok", Metadata: null, IsWebhook: false, SpecVersion: 9, ResumeDedup: false, CreatedAt: now });
				case "events.create":
					stored.push((input.Event as { Data: Uint8Array }).Data);
					return rpcResponse({ Allocated: 1 });
				default:
					return rpcResponse({
						Data: [
							{ RunID: "wrun_01", EventID: "evnt_01", EventType: "run_created", CorrelationID: "", Data: new TextEncoder().encode('{"workflowName":"app.Report","input":"","deploymentId":"","startHook":{"token":"tok","tokenRetentionUntil":"2026-01-02T03:04:05Z"}}'), CreatedAt: now, SpecVersion: 9 },
							{ RunID: "wrun_01", EventID: "evnt_02", EventType: "wait_created", CorrelationID: "wait_01", Data: new TextEncoder().encode('{"resumeAt":"2026-01-02T03:04:05.5Z"}'), CreatedAt: now, SpecVersion: 9 },
							{ RunID: "wrun_01", EventID: "evnt_03", EventType: "step_retrying", CorrelationID: "step_01", Data: new TextEncoder().encode('{"error":"","retryAfter":"2026-01-02T03:04:05Z"}'), CreatedAt: now, SpecVersion: 9 },
						],
						HasMore: false,
						Snapshot: { EventCount: 3, InBandCount: 3 },
					});
			}
		},
	});
	// A World that deduplicates resumes says so on the hook read by token,
	// as the SDK's resumeHook reads it.
	const byToken = await world.hooks.getByToken("tok");
	expect(byToken.resumeCapabilities).toEqual({ hookResumeDedupVersion: HOOK_RESUME_DEDUP_VERSION });
	expect(HookSchema.safeParse(byToken).error).toBeUndefined();
	expect(await world.hooks.get("hook_01")).not.toHaveProperty("resumeCapabilities");
	expect(world.capabilities).not.toHaveProperty("resolvedHookResume");

	const events = await world.events.list({ runId: "wrun_01" } as never);
	const data = events.data.map((event) => (event as { eventData: Record<string, unknown> }).eventData);
	expect((data[0]?.startHook as Record<string, unknown>).tokenRetentionUntil).toEqual(new Date("2026-01-02T03:04:05Z"));
	expect(data[1]?.resumeAt).toEqual(new Date("2026-01-02T03:04:05.5Z"));
	expect(data[2]?.retryAfter).toEqual(new Date("2026-01-02T03:04:05Z"));
	// An event about the run has no correlation ID.
	expect(events.data[0]).not.toHaveProperty("correlationId");
	expect(events.data[1]?.correlationId).toBe("wait_01");
	for (const event of events.data.slice(0, 2)) {
		expect(EventSchema.safeParse(event).error).toBeUndefined();
	}

	// A resume names its run's workflow, which is stored in the Go form.
	await world.events.create("wrun_01", { eventType: "hook_received", correlationId: "hook_01", eventData: { token: "tok", workflowName: "workflow//example.com/app//Report", payload: new Uint8Array([1]) } } as never);
	expect(JSON.parse(new TextDecoder().decode(stored[0]))).toEqual({ token: "tok", workflowName: "example.com/app.Report", payload: "AQ==" });

	await expect(world.resolveLatestDeploymentId?.()).resolves.toBe("dpl_latest");
	expect(world.isDeploymentUnavailableError(new HTTPWorldError("internal", "gone", 500, "deployment_unavailable"))).toBe(true);
	expect(world.isDeploymentUnavailableError(new HTTPWorldError("internal", "boom", 500))).toBe(false);
});

// These tests drive a real Go RPC server, backed by an in-memory World that
// executes its deliveries, through the adapter with the calls the Workflow
// SDK's start, resumeHook, cancelRun and wakeUpRun make.
describe("against a Go RPC server", () => {
	let server: ChildProcess | undefined;
	let directory: string;
	let world: HTTPWorld;
	let names: { review: string; nap: string };

	beforeAll(async () => {
		const root = fileURLToPath(new URL("..", import.meta.url));
		directory = mkdtempSync(join(tmpdir(), "world-http-"));
		const binary = join(directory, "server");
		execFileSync("go", ["build", "-o", binary, "./world-http/testdata/server"], { cwd: root, stdio: "inherit" });
		// Payloads are stored uncompressed, so the tests read them as text.
		server = spawn(binary, { env: { ...process.env, WORKFLOW_DISABLE_COMPRESSION: "1" }, stdio: ["ignore", "pipe", "inherit"] });
		const line = await new Promise<string>((resolve, reject) => {
			createInterface({ input: server!.stdout! }).once("line", resolve);
			server!.once("exit", (code) => reject(new Error(`server exited with ${code}`)));
			server!.once("error", reject);
		});
		const started = JSON.parse(line) as { baseURL: string; review: string; nap: string };
		world = await createWorld({ baseURL: started.baseURL });
		// The SDK knows workflows by the names the World's reads report.
		names = { review: sdkName(started.review), nap: sdkName(started.nap) };
	}, 300_000);

	afterAll(() => {
		server?.kill();
		rmSync(directory, { recursive: true, force: true });
	});

	// createEvent writes an event the way the SDK's runtime passes one.
	const createEvent = (runId: string, event: unknown, params?: unknown) =>
		(world.events.create as (runId: string, event: unknown, params?: unknown) => Promise<EventResult>)(runId, event, params);

	// start creates a run and queues its first message at the same time, as
	// the SDK's start() does. A start hook's message is queued first.
	async function start(workflowName: string, input: string, options: { startHook?: { token: string }; create?: boolean } = {}) {
		const runId = `wrun_${ulid()}`;
		const deploymentId = await world.getDeploymentId();
		const creation = {
			deploymentId,
			workflowName,
			input: text(input),
			executionContext: { workflowCoreVersion: "5.7.1", features: { encryption: false } },
			...(options.startHook ? { startHook: options.startHook } : {}),
		};
		const enqueue = () =>
			world.queue(`__wkf_workflow_${workflowName}` as never, { runId, traceCarrier: {}, runInput: { ...creation, specVersion: world.specVersion } } as never, { deploymentId, specVersion: world.specVersion });
		const create = () => createEvent(runId, { eventType: "run_created", specVersion: world.specVersion, eventData: creation }, { inBand: false });
		if (options.create === false) {
			await enqueue();
			return { runId };
		}
		if (options.startHook) {
			await enqueue();
			return { runId, created: await create() };
		}
		const [created] = await Promise.all([create(), enqueue()]);
		return { runId, created };
	}

	test("starts a run, resumes its hook once, and reads everything back through the SDK's schemas", async () => {
		expect(world.specVersion).toBe(9);
		expect(world.capabilities).toMatchObject({ inBandFence: true, hookResumeDedup: true, atomicStartHook: { active: true } });

		const token = `review-${ulid()}`;
		const { runId, created } = await start(names.review, `devl[[1],{"token":2},"${token}"]`);
		expect(created?.run?.runId).toBe(runId);
		expect(WorkflowRunSchema.safeParse(created?.run).error).toBeUndefined();
		expect(created?.run?.specVersion).toBe(9);
		expect(created?.run?.workflowName).toBe(names.review);
		expect(EventSchema.safeParse(created?.event).error).toBeUndefined();

		// The workflow opens its hook once its first delivery reaches it.
		const hook = await eventually(() => world.hooks.getByToken(token));
		expect(HookSchema.safeParse(hook).error).toBeUndefined();
		expect(hook).toMatchObject({ runId, token, ownerId: "", projectId: "", environment: "", resumeContext: { workflowName: names.review, runSpecVersion: 9 } });
		expect((await world.hooks.get(hook.hookId)).hookId).toBe(hook.hookId);
		const hooks = await world.hooks.list({ runId });
		expect(hooks.data.map((item) => HookSchema.safeParse(item).error)).toEqual([undefined]);

		// resumeHook writes the payload with its resume's ID and digest, then
		// wakes the run. A repeat of the resume records nothing more.
		const payload = text('devl[{"approved":1},true]');
		const resume = () =>
			createEvent(
				hook.runId,
				{ eventType: "hook_received", specVersion: 9, correlationId: hook.hookId, eventData: { token: hook.token, workflowName: hook.resumeContext?.workflowName, payload } } as never,
				{ inBand: false, resumeId: "resume_01", resumePayloadDigest: createHash("sha256").update(payload).digest("hex") } as never,
			);
		const received = await resume();
		expect(EventSchema.safeParse(received.event).error).toBeUndefined();
		expect((await resume()).event?.eventId).toBe(received.event?.eventId);
		await world.queue(`__wkf_workflow_${names.review}` as never, { runId, runSpecVersion: 9, wakeEventId: received.event?.eventId } as never, { deploymentId: hook.resumeContext?.deploymentId, specVersion: 9 });

		const run = await world.runs.waitForTerminalStatus!(runId, { timeoutMs: 20_000 });
		expect(WorkflowRunSchema.safeParse(run).error).toBeUndefined();
		expect(run.status).toBe("completed");
		expect(new TextDecoder().decode(run.output as Uint8Array)).toBe('devl[{"approved":1},true]');
		expect((await world.runs.list({ workflowName: names.review } as never)).data.some((item) => item.runId === runId)).toBe(true);

		const events = await world.events.list({ runId, pagination: { sortOrder: "asc" }, resolveData: "all", specVersion: 9 } as never);
		for (const event of events.data) {
			expect(EventSchema.safeParse(event).error, event.eventType).toBeUndefined();
			expect(event.specVersion).toBe(9);
		}
		const types = events.data.map((event) => event.eventType);
		expect(types.slice(0, 2)).toEqual(["run_created", "run_started"]);
		expect(types.filter((type) => type === "hook_received")).toHaveLength(1);
		expect(types.at(-1)).toBe("run_completed");
		expect(events.snapshot?.seq).toBeGreaterThanOrEqual(events.data.length);
		// The resume names its workflow in the Go form in the stored log,
		// which reads report in the SDK's.
		const stored = events.data.find((event) => event.eventType === "hook_received") as { eventData: Record<string, unknown>; resumeId?: string };
		expect(stored.eventData).toMatchObject({ token, workflowName: names.review, payload });
		expect(stored.resumeId).toBe("resume_01");
		expect((await world.events.get(runId, received.event!.eventId)).eventId).toBe(received.event?.eventId);
		expect((await world.events.listByCorrelationId({ runId, correlationId: hook.hookId })).data.length).toBeGreaterThan(0);
		expect((await world.steps.list({ runId })).data.map((step) => StepSchema.safeParse(step).error)).toEqual([]);

		expect(AnalyticsRunSchema.safeParse(await world.analytics!.runs.get(runId)).error).toBeUndefined();
		const analyticsEvents = await world.analytics!.events.list({ runId } as never);
		expect(analyticsEvents.data.length).toBeGreaterThan(0);
		for (const event of analyticsEvents.data) {
			expect(AnalyticsEventSchema.safeParse(event).error).toBeUndefined();
		}
		for (const item of (await world.analytics!.hooks.list({ runId } as never)).data) {
			expect(AnalyticsHookSchema.safeParse(item).error).toBeUndefined();
		}
	}, 60_000);

	test("creates the run from its first message when run_created never lands, and cancels it", async () => {
		const token = `review-${ulid()}`;
		const { runId } = await start(names.review, `devl[[1],{"token":2},"${token}"]`, { create: false });
		const hook = await eventually(() => world.hooks.getByToken(token));
		expect(hook.runId).toBe(runId);
		const run = await world.runs.get(runId, { resolveData: "none" });
		expect(run).toMatchObject({ status: "running", specVersion: 9, workflowName: names.review });

		// cancelRun writes run_cancelled out of band, then wakes the run.
		await createEvent(runId, { eventType: "run_cancelled", specVersion: 9 }, { inBand: false });
		await world.queue(`__wkf_workflow_${run.workflowName}` as never, { runId, requestedAt: new Date(), runSpecVersion: run.specVersion } as never, { deploymentId: run.deploymentId, specVersion: run.specVersion });
		const cancelled = await world.runs.waitForTerminalStatus!(runId, { timeoutMs: 20_000 });
		expect(cancelled.status).toBe("cancelled");
		expect(WorkflowRunSchema.safeParse(cancelled).error).toBeUndefined();
		// The hook of a finished run is gone, which the SDK reads by name.
		await expect(world.hooks.getByToken(token)).rejects.toMatchObject({ name: "HookNotFoundError" });
	}, 60_000);

	test("wakes a sleeping run through the waits its message names", async () => {
		const { runId } = await start(names.nap, "devl[[1],{}]");
		const wait = await eventually(async () => {
			const events = await world.events.list({ runId, pagination: { sortOrder: "asc" }, resolveData: "none" } as never);
			const created = events.data.find((event) => event.eventType === "wait_created");
			if (!created) throw new Error("no wait yet");
			return created as { correlationId: string; eventData: { resumeAt: Date } };
		});
		expect(wait.eventData.resumeAt).toBeInstanceOf(Date);
		const run = await world.runs.get(runId, { resolveData: "none" });
		await world.queue(`__wkf_workflow_${run.workflowName}` as never, { runId, runSpecVersion: run.specVersion, completeWaits: [wait.correlationId] } as never, { deploymentId: run.deploymentId, specVersion: run.specVersion });
		const woken = await world.runs.waitForTerminalStatus!(runId, { timeoutMs: 20_000 });
		expect(woken.status).toBe("completed");
		expect(new TextDecoder().decode(woken.output as Uint8Array)).toBe('devl["rested"]');
	}, 60_000);

	test("refuses a second start that names a held start hook token", async () => {
		const token = `review-${ulid()}`;
		const input = `devl[[1],{"token":2},"${token}"]`;
		const first = await start(names.review, input, { startHook: { token } });
		expect((first.created?.run as { startHook?: { token: string } }).startHook?.token).toBe(token);
		await expect(start(names.review, input, { startHook: { token } })).rejects.toMatchObject({ name: "HookConflictError", token, conflictingRunId: first.runId });
		// The run's start hook is the hook its workflow waits on.
		expect((await world.hooks.getByToken(token)).runId).toBe(first.runId);
	}, 60_000);

	test("reports missing entities as the SDK's errors and queues a batch", async () => {
		await expect(world.runs.get(`wrun_${ulid()}`)).rejects.toMatchObject({ name: "WorkflowRunNotFoundError", status: 404 });
		await expect(world.hooks.getByToken("missing")).rejects.toMatchObject({ name: "HookNotFoundError" });
		const { runId } = await start(names.nap, "devl[[1],{}]");
		const results = await world.queueBatch(`__wkf_workflow_${names.nap}` as never, [
			{ message: { runId, runSpecVersion: 9 } as never },
			{ message: { runId, runSpecVersion: 9 } as never },
		]);
		expect(results).toHaveLength(2);
		expect(results.every((result) => typeof result.messageId === "string" && result.error === undefined)).toBe(true);
	}, 60_000);
});

describe("against a Go World that delivers with worlds.RemoteExecutor", () => {
	const token = "delivery-token";
	let directory: string;
	let server: ChildProcess | undefined;
	let app: Server | undefined;
	let world: HTTPWorld;
	let route: (request: Request) => Promise<Response> = async () => new Response(null, { status: 503 });
	const deliveries: { message: Record<string, unknown>; meta: Record<string, unknown> }[] = [];
	// answer returns the queue handler's result for a delivery of message.
	let answer: (message: Record<string, unknown>) => unknown = () => undefined;

	beforeAll(async () => {
		// The app serves the SDK's flow route with the World's queue handler.
		app = createServer(async (incoming, outgoing) => {
			const chunks: Buffer[] = [];
			for await (const chunk of incoming) chunks.push(chunk as Buffer);
			const headers = new Headers();
			for (const [name, value] of Object.entries(incoming.headers)) {
				if (typeof value === "string") headers.set(name, value);
			}
			const response = await route(new Request(`http://app${incoming.url}`, { method: incoming.method, headers, body: Buffer.concat(chunks) }));
			const responseHeaders: Record<string, string> = {};
			response.headers.forEach((value, name) => {
				responseHeaders[name] = value;
			});
			outgoing.writeHead(response.status, responseHeaders);
			outgoing.end(Buffer.from(await response.arrayBuffer()));
		});
		await new Promise<void>((resolve) => app!.listen(0, "127.0.0.1", resolve));
		const flowURL = `http://127.0.0.1:${(app.address() as AddressInfo).port}/.well-known/workflow/v1/flow`;

		const root = fileURLToPath(new URL("..", import.meta.url));
		directory = mkdtempSync(join(tmpdir(), "world-http-remote-"));
		const binary = join(directory, "server");
		execFileSync("go", ["build", "-o", binary, "./world-http/testdata/server"], { cwd: root, stdio: "inherit" });
		server = spawn(binary, { env: { ...process.env, WORKFLOW_REMOTE_URL: flowURL, WORKFLOW_HTTP_TOKEN: token }, stdio: ["ignore", "pipe", "inherit"] });
		const line = await new Promise<string>((resolve, reject) => {
			createInterface({ input: server!.stdout! }).once("line", resolve);
			server!.once("exit", (code) => reject(new Error(`server exited with ${code}`)));
			server!.once("error", reject);
		});
		world = await createWorld({ baseURL: (JSON.parse(line) as { baseURL: string }).baseURL, token });
		route = world.createQueueHandler("__wkf_workflow_", async (message, meta) => {
			deliveries.push({ message: message as Record<string, unknown>, meta: meta as Record<string, unknown> });
			return answer(message as Record<string, unknown>);
		});
	}, 300_000);

	afterAll(async () => {
		server?.kill();
		await new Promise((resolve) => app?.close(resolve));
		rmSync(directory, { recursive: true, force: true });
	});

	const deliveriesOf = (runId: string) => deliveries.filter((delivery) => delivery.message.runId === runId);

	test("declares that its queue handler authenticates deliveries", () => {
		expect(world.queueAuthMode).toBe("skip");
	});

	test("delivers the SDK message unchanged, and hands it off again for timeoutSeconds 0", async () => {
		const runId = `wrun_${ulid()}`;
		const message = { runId, traceCarrier: { traceparent: "00-abc" }, stepInput: new Uint8Array([1, 2, 3]), waitContinuation: { correlationId: "wait_1" } };
		let handedOff = false;
		answer = (delivered) => {
			if (delivered.runId !== runId || handedOff) return undefined;
			handedOff = true;
			return { timeoutSeconds: 0 };
		};
		const { messageId } = await world.queue("__wkf_workflow_workflow//./workflows/a//a" as never, message as never);
		await eventually(async () => {
			if (deliveriesOf(runId).length < 2) throw new Error("waiting for the redelivery");
		});
		const [first, second] = deliveriesOf(runId);
		expect(first?.message).toEqual(message);
		expect(second?.message).toEqual(message);
		expect(first?.meta).toMatchObject({ queueName: "__wkf_workflow_workflow//./workflows/a//a", messageId, attempt: 1, deliveryCount: 1 });
		expect(first?.meta.createdAt).toBeInstanceOf(Date);
		// A handoff is not an attempt.
		expect(second?.meta).toMatchObject({ messageId, attempt: 1, deliveryCount: 2 });
		await new Promise((resolve) => setTimeout(resolve, 200));
		expect(deliveriesOf(runId)).toHaveLength(2);
	});

	test("retries a delivery whose handler throws", async () => {
		const runId = `wrun_${ulid()}`;
		let failed = false;
		answer = (delivered) => {
			if (delivered.runId !== runId || failed) return undefined;
			failed = true;
			throw new Error("not yet");
		};
		await world.queue("__wkf_workflow_workflow//./workflows/a//a" as never, { runId } as never);
		await eventually(async () => {
			if (deliveriesOf(runId).length < 2) throw new Error("waiting for the retry");
		});
		expect(deliveriesOf(runId)[1]?.meta).toMatchObject({ attempt: 2, deliveryCount: 2 });
	});

	test("delivers health checks to the app", async () => {
		answer = () => undefined;
		const correlationId = `probe_${ulid()}`;
		await world.queue("__wkf_workflow_health" as never, { __healthCheck: true, correlationId } as never);
		await eventually(async () => {
			if (!deliveries.some((delivery) => delivery.message.correlationId === correlationId)) throw new Error("waiting for the probe");
		});
		const probe = deliveries.find((delivery) => delivery.message.correlationId === correlationId);
		expect(probe?.message).toEqual({ __healthCheck: true, correlationId });
		expect(probe?.meta.queueName).toBe("__wkf_workflow_health");
	});

	test("refuses a delivery without the World's token or for another prefix", async () => {
		const body = encode({ queueName: "__wkf_workflow_x", message: { runId: "wrun_x" }, messageId: "msg_x", attempt: 1 });
		const unauthorized = await route(new Request("http://app/.well-known/workflow/v1/flow", { method: "POST", body: new Uint8Array(body) }));
		expect(unauthorized.status).toBe(401);
		const other = await world.createQueueHandler("__other_", async () => undefined)(
			new Request("http://app/flow", { method: "POST", headers: { Authorization: `Bearer ${token}` }, body: new Uint8Array(body) }),
		);
		expect(other.status).toBe(400);
	});
});

function text(value: string): Uint8Array {
	return new TextEncoder().encode(value);
}

// sdkName returns a Go workflow name in the Workflow SDK's form.
function sdkName(name: string): string {
	const dot = name.indexOf(".", name.lastIndexOf("/") + 1);
	return `workflow//${name.slice(0, dot)}//${name.slice(dot + 1)}`;
}

// ulid returns a new ULID, as the SDK mints a run ID from.
function ulid(): string {
	const alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";
	let time = Date.now();
	let value = "";
	for (let index = 0; index < 10; index++) {
		value = alphabet[time % 32] + value;
		time = Math.floor(time / 32);
	}
	for (let index = 0; index < 16; index++) {
		value += alphabet[Math.floor(Math.random() * 32)];
	}
	return value;
}

// eventually returns read's result once it stops failing.
async function eventually<T>(read: () => Promise<T>): Promise<T> {
	for (let attempt = 0; ; attempt++) {
		try {
			return await read();
		} catch (error) {
			if (attempt >= 100) throw error;
			await new Promise((resolve) => setTimeout(resolve, 50));
		}
	}
}
