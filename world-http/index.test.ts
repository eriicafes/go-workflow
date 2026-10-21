import { decode, encode } from "cbor-x";
import { SPEC_VERSION_CURRENT } from "@workflow/world";
import { expect, test } from "vitest";
import { HTTPWorldError, createWorld } from "./index.js";

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
				return rpcResponse({ SpecVersion: 1, Capabilities: { Analytics: false, SetAttributes: true } });
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

	expect(world.specVersion).toBe(SPEC_VERSION_CURRENT);
	expect(world._spec).toEqual({ specVersion: 1, capabilities: { analytics: false, setAttributes: true } });
	expect(requests[0]?.envelope.specVersion).toBe(1);
	expect(world.capabilities).toEqual({
		hookRetention: { active: true },
		hookForceClaim: true,
	});

	const run = await world.runs.get("wrun_01");
	expect(run.runId).toBe("wrun_01");
	expect(run.workflowName).toBe("report");
	expect(run.attributes).toEqual({ "external-id": "42" });
	expect(requests[1]?.envelope.specVersion).toBe(1);
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
					return rpcResponse({ SpecVersion: 1, Capabilities: { Analytics: false, SetAttributes: false, DeploymentAffinity: true } });
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
				return rpcResponse({ SpecVersion: 1, Capabilities: { Analytics: false, SetAttributes: false } });
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
				return rpcResponse({ SpecVersion: 1, Capabilities: { Analytics: true, SetAttributes: false } });
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
				return rpcResponse({ SpecVersion: 1, Capabilities: { Analytics: false, SetAttributes: false } });
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
		name: HTTPWorldError.name,
	});
});

test("does not queue health checks", async () => {
	const methods: unknown[] = [];
	const world = await createWorld({
		baseURL: "https://workflow.example/v1/workflow",
		fetch: async (_url, init) => {
			const envelope = decode(init?.body as Uint8Array) as Record<string, unknown>;
			methods.push(envelope.method);
			return rpcResponse({ SpecVersion: 1, Capabilities: { Analytics: false, SetAttributes: false } });
		},
	});

	await expect(world.queue("__wkf_workflow_report" as never, { __healthCheck: true, correlationId: "probe_01" })).resolves.toEqual({ messageId: null });
	expect(methods).toEqual(["client.spec"]);
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
					return rpcResponse({ SpecVersion: 1, Capabilities: { Analytics: false, SetAttributes: false } });
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
	const goData = new TextEncoder().encode(JSON.stringify({ stepName: "example.com/app/flows.writeDraft", position: 2 }));
	const world = await createWorld({
		baseURL: "https://workflow.example/v1/workflow",
		fetch: async (_url, init) => {
			const envelope = decode(init?.body as Uint8Array) as Record<string, unknown>;
			if (envelope.method === "client.spec") {
				return rpcResponse({ SpecVersion: 1, Capabilities: { Analytics: false, SetAttributes: false } });
			}
			expect(envelope.method).toBe("events.list");
			return rpcResponse({
				Data: [
					{ EventID: "evnt_01", EventType: "step_created", CorrelationID: "step_01", Data: goData },
					{ EventID: "evnt_02", EventType: "hook_received", CorrelationID: "hook_01", Data: encode({ payload: 7 }) },
					{ EventID: "evnt_03", EventType: "run_started", CorrelationID: "", Data: null },
				],
				HasMore: false,
			});
		},
	});

	const events = await world.events.list({ runId: "wrun_01" } as never);
	expect(events.data.map((event) => (event as { eventData?: unknown }).eventData)).toEqual([
		{ stepName: "step//example.com/app/flows//writeDraft", position: 2 },
		{ payload: 7 },
		undefined,
	]);
	expect(events.data[0]).not.toHaveProperty("data");
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
					return rpcResponse({ SpecVersion: 1, Capabilities: { Analytics: false, SetAttributes: false } });
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
		event: { eventId: "evnt_02", eventData: { stepName: "write", input: "AQI=" } },
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
				return rpcResponse({ SpecVersion: 1, Capabilities: { Analytics: false, SetAttributes: false } });
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

test("decodes run and step data stored by the Go runtime", async () => {
	const json = (value: unknown) => new TextEncoder().encode(JSON.stringify(value));
	const world = await createWorld({
		baseURL: "https://workflow.example/v1/workflow",
		fetch: async (_url, init) => {
			const request = decode(init?.body as Uint8Array) as Record<string, unknown>;
			switch (request.method) {
				case "client.spec":
					return rpcResponse({ SpecVersion: 1, Capabilities: { Analytics: false, SetAttributes: false } });
				case "runs.get":
					return rpcResponse({ RunID: "wrun_01", Status: "failed", Input: json({ topic: "go" }), Error: json({ message: "boom", chain: [] }) });
				default:
					return rpcResponse({ RunID: "wrun_01", StepID: "step_01", Status: "completed", Input: json(1), Output: json("done") });
			}
		},
	});
	const run = await world.runs.get("wrun_01");
	expect(run.input).toEqual({ topic: "go" });
	expect(run.error).toEqual({ message: "boom" });
	const step = await world.steps.get("wrun_01", "step_01");
	expect(step.input).toBe(1);
	expect(step.output).toBe("done");
});

test("waits for a terminal run across bounded requests", async () => {
	const timeouts: unknown[] = [];
	const world = await createWorld({
		baseURL: "https://workflow.example/v1/workflow",
		fetch: async (_url, init) => {
			const request = decode(init?.body as Uint8Array) as Record<string, unknown>;
			if (request.method === "client.spec") {
				return rpcResponse({ SpecVersion: 1, Capabilities: { Analytics: false, SetAttributes: false } });
			}
			timeouts.push((request.input as Record<string, unknown>).Timeout);
			return rpcResponse({ RunID: "wrun_01", Status: timeouts.length < 3 ? "running" : "completed" });
		},
	});
	const run = await world.runs.waitForTerminalStatus!("wrun_01");
	expect(run.status).toBe("completed");
	expect(timeouts).toEqual([10_000_000_000, 10_000_000_000, 10_000_000_000]);
});
