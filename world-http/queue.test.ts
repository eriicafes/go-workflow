import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { decode } from "cbor-x";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { createWorld, type HTTPWorld } from "./index.js";
import { eventually, rpcResponse, startServer, ulid } from "./testdata/helpers.js";

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
	expect(JSON.parse(new TextDecoder().decode(queued?.SdkMessage as Uint8Array))).toEqual({ queueName: "__wkf_workflow_report", message: { __healthCheck: true, correlationId: "probe_01" } });
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
	// Dates travel as exact ISO 8601 text.
	expect(items[0]?.StepCreatedAt).toBe(new Date(createdAt).toISOString());
	expect(items[1]).not.toHaveProperty("StepCreatedAt");

	// A step's creation time and the mark of a failure recorded after its
	// function ran reach the World with the write.
	await world.events.create("wrun_01", { eventType: "step_started", correlationId: "step_01", eventData: { stepName: "send", attempt: 1 } } as never, { stepCreatedAt: createdAt });
	await world.events.create("wrun_01", { eventType: "step_failed", correlationId: "step_01", eventData: { stepName: "send", attempt: 1, error: new Uint8Array([1]) } } as never, { afterStepBody: true });
	const writes = calls.filter((call) => call.method === "events.create").map((call) => call.input.Options);
	expect(writes).toEqual([{ StepCreatedAt: new Date(createdAt).toISOString() }, { AfterStepBody: true }]);
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
	expect(JSON.parse(new TextDecoder().decode(items[1]?.SdkMessage as Uint8Array))).toEqual({ queueName: "__wkf_workflow_workflow//example.com/app//Report", message: { runId: "wrun_02", runSpecVersion: 9 } });
});
describe("against a Go World that delivers with worlds.RemoteExecutor", () => {
	const token = "delivery-token";
	let stop: (() => void) | undefined;
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

		const server = await startServer<{ baseURL: string }>({ WORKFLOW_REMOTE_URL: flowURL, WORKFLOW_HTTP_TOKEN: token });
		stop = server.stop;
		world = await createWorld({ baseURL: server.started.baseURL, token });
		route = world.createQueueHandler("__wkf_workflow_", async (message, meta) => {
			deliveries.push({ message: message as Record<string, unknown>, meta: meta as Record<string, unknown> });
			return answer(message as Record<string, unknown>);
		});
	}, 300_000);

	afterAll(async () => {
		stop?.();
		await new Promise((resolve) => app?.close(resolve));
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
		const delivery = (headers: Record<string, string>) =>
			new Request("http://app/.well-known/workflow/v1/flow", {
				method: "POST",
				headers: { "Content-Type": "application/json", "x-vqs-queue-name": "__wkf_workflow_x", "x-vqs-message-id": "msg_x", "x-vqs-message-attempt": "1", ...headers },
				body: JSON.stringify({ runId: "wrun_x" }),
			});
		expect((await route(delivery({}))).status).toBe(401);
		expect((await route(delivery({ Authorization: "Bearer wrong-token-xx" }))).status).toBe(401);
		const other = await world.createQueueHandler("__other_", async () => undefined)(delivery({ Authorization: `Bearer ${token}` }));
		expect(other.status).toBe(400);
	});
});
