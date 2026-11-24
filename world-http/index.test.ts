import { createHash } from "node:crypto";
import { decode } from "cbor-x";
import { AnalyticsEventSchema, AnalyticsHookSchema, AnalyticsRunSchema, EventSchema, type EventResult, HookSchema, SPEC_VERSION_SINGLE_ORCHESTRATOR, StepSchema, WorkflowRunSchema } from "@workflow/world";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { createWorld, type HTTPWorld } from "./index.js";
import { eventually, rpcContentType, rpcResponse, sdkName, startServer, text, ulid } from "./testdata/helpers.js";

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
test("reports a run's event ceiling only when the Go World has one", async () => {
	let maxEvents = 0;
	const world = await createWorld({
		baseURL: "https://workflow.example/v1/workflow",
		fetch: async (_url, init) => {
			const envelope = decode(init?.body as Uint8Array) as Record<string, unknown>;
			if (envelope.method === "client.spec") {
				return rpcResponse({ RPCVersion: 1, Capabilities: {} });
			}
			return rpcResponse({ Data: [], HasMore: false, MaxEvents: maxEvents, Snapshot: { EventCount: 0, InBandCount: 0 } });
		},
	});
	// The SDK reads a maxEvents of 0 as a ceiling of zero events.
	expect(await world.events.list({ runId: "wrun_01" } as never)).not.toHaveProperty("maxEvents");
	maxEvents = 25_000;
	expect(await world.events.list({ runId: "wrun_01" } as never)).toMatchObject({ maxEvents: 25_000 });
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
// These tests drive a real Go RPC server, backed by an in-memory World that
// executes its deliveries, through the adapter with the calls the Workflow
// SDK's start, resumeHook, cancelRun and wakeUpRun make.
describe("against a Go RPC server", () => {
	let stop: (() => void) | undefined;
	let world: HTTPWorld;
	let names: { review: string; nap: string };

	beforeAll(async () => {
		// Payloads are stored uncompressed, so the tests read them as text.
		const server = await startServer<{ baseURL: string; review: string; nap: string }>({ WORKFLOW_DISABLE_COMPRESSION: "1" });
		stop = server.stop;
		world = await createWorld({ baseURL: server.started.baseURL });
		// The SDK knows workflows by the names the World's reads report.
		names = { review: sdkName(server.started.review), nap: sdkName(server.started.nap) };
	}, 300_000);

	afterAll(() => {
		stop?.();
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
