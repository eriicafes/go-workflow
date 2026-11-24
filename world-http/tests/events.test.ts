import { decode, encode } from "cbor-x";
import { EventSchema, HOOK_RESUME_DEDUP_VERSION, HookSchema, SPEC_VERSION_SINGLE_ORCHESTRATOR } from "@workflow/world";
import { expect, test } from "vitest";
import { HTTPWorldError, createWorld } from "../index.js";
import { rpcResponse } from "./helpers.js";

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
