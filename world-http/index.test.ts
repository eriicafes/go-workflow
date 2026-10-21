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
					return rpcResponse({ EventID: "evnt_01", EventType: "run_created" });
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
			const encoded = encode({ error: { code: "not_found", message: "workflow: run not found" } });
			const body = new Uint8Array(encoded.byteLength);
			body.set(encoded);
			return new Response(body, {
				headers: { "Content-Type": rpcContentType },
			});
		},
	});

	await expect(world.runs.get("wrun_missing")).rejects.toMatchObject({
		code: "not_found",
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
