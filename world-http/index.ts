import type {
	CreateEventParams,
	CreateEventRequest,
	DeleteRunsDataResult,
	RunCreatedEventRequest,
	AnalyticsAttributeKey,
	AnalyticsEvent,
	AnalyticsHook,
	AnalyticsRun,
	AnalyticsStep,
	AnalyticsWait,
	Event,
	EventBatchResult,
	EventResult,
	Hook,
	MessageId,
	PaginatedResponse,
	Step,
	StepWithoutData,
	Wait,
	ListWorkflowRunStepsParams,
	ListWorkflowRunsParams,
	WaitForTerminalRunStatusParams,
	WorkflowRun,
	WorkflowRunWithoutData,
	World,
} from "@workflow/world";
import { HOOK_RESUME_DEDUP_VERSION, START_HOOK_ADMISSION_REJECTED_CODE } from "@workflow/world";
import { SPEC_VERSION_SINGLE_ORCHESTRATOR } from "@workflow/world/spec-version";
import { HTTPWorldError, RPCClient, devkitName, goName, type RPC } from "./rpc.js";
import { HTTPStreamWriteSession } from "./stream-socket.js";

const HTTP_WORLD_RPC_VERSION = 1;
// The code of the Workflow SDK's WorkflowWorldError for a start hook
// candidate answered while admission is switched off.
const START_HOOK_ADMISSION_DISABLED_CODE = "START_HOOK_ADMISSION_DISABLED";

// GO_SPEC_VERSION is the spec version a Go World records on every run it
// creates, whatever version the caller asked for: one orchestrator per run,
// whose in-band writes the World fences.
const GO_SPEC_VERSION = SPEC_VERSION_SINGLE_ORCHESTRATOR;

export type { World } from "@workflow/world";
export { HTTPWorldError } from "./rpc.js";
export type { RPC } from "./rpc.js";

export interface HTTPWorldOptions {
	/**
	 * The RPC server's base URL, under which it serves `/rpc`.
	 * Defaults to `WORKFLOW_HTTP_BASE_URL`.
	 */
	baseURL?: string;
	/**
	 * Sent as a bearer token with every RPC and stream socket request, for a server protected
	 * with `worlds.WithClientToken`. Defaults to `WORKFLOW_HTTP_TOKEN`. An
	 * `Authorization` header in `headers` takes precedence.
	 */
	token?: string;
	fetch?: typeof globalThis.fetch;
	headers?: HeadersInit;
}

/**
 * Creates a World backed by an RPC server.
 * `baseURL` is the server's base URL, for example
 * `https://workflows.example.com/v1/workflow`.
 */
export async function createWorld(options: HTTPWorldOptions = {}): Promise<HTTPWorld> {
	const { token = environment("WORKFLOW_HTTP_TOKEN"), ...rest } = options;
	const headers = new Headers(options.headers);
	if (token && !headers.has("Authorization")) {
		headers.set("Authorization", `Bearer ${token}`);
	}
	const client = new RPCClient({
		...rest,
		headers,
		baseURL: options.baseURL ?? environment("WORKFLOW_HTTP_BASE_URL"),
	});
	// The first request already carries the protocol version, so a mismatched
	// server fails it instead of answering.
	client.rpcVersion = HTTP_WORLD_RPC_VERSION;
	const spec = await client.call<RPC.Spec>("client.spec", {});
	return new HTTPWorld(client, spec, { token });
}

export class HTTPWorld implements World {
	/**
	 * The RPC server's protocol version and capabilities. They are separate
	 * from `specVersion` and `capabilities`, which describe the Workflow SDK
	 * World interface.
	 */
	readonly _spec: RPC.Spec;

	/**
	 * The spec version of the runs the World creates, which start() stamps on
	 * a new run and its first message.
	 */
	readonly specVersion = GO_SPEC_VERSION;
	readonly capabilities: World["capabilities"];
	readonly runs: World["runs"];
	readonly steps: World["steps"];
	readonly events: World["events"];
	readonly hooks: World["hooks"];
	readonly queue: World["queue"];
	readonly queueBatch: NonNullable<World["queueBatch"]>;
	/**
	 * Reports a queue failure that means the deployment a message was
	 * addressed to cannot receive it, so the SDK stops retrying the send.
	 */
	readonly isDeploymentUnavailableError: NonNullable<World["isDeploymentUnavailableError"]> = (error) =>
		error instanceof HTTPWorldError && error.sentinel === "deployment_unavailable";
	/**
	 * The World's newest deployment, which start() pins a run to for
	 * `deploymentId: "latest"`. Only a World with deployment affinity has it.
	 */
	readonly resolveLatestDeploymentId?: World["resolveLatestDeploymentId"];
	readonly streams: World["streams"];
	readonly analytics?: World["analytics"];
	/**
	 * The deployment of the World behind the RPC server, which runs started
	 * through this World are pinned to. It is read on every call, since the
	 * server behind the URL can be redeployed while this World is in use.
	 */
	readonly getDeploymentId: World["getDeploymentId"];
	/**
	 * Deliveries reach the app's flow route from the Go World's
	 * worlds.RemoteExecutor, which authenticates them with the World's bearer
	 * token. The handler checks it, so the SDK runtime does not check them
	 * itself.
	 */
	readonly queueAuthMode = "skip" as const;
	/**
	 * Returns the handler of the app's flow route for deliveries a Go World
	 * sends with worlds.RemoteExecutor, in the Workflow SDK's local queue
	 * format: the message as JSON, with its queue name, message ID, attempt,
	 * delivery count and creation time in x-vqs-* headers. It answers
	 * {timeoutSeconds} when `handler` asks for the message again, and
	 * {ok: true} when the message is done. A delivery without the World's
	 * token is refused with 401, and a handler that throws fails it with 500,
	 * which the Go World retries.
	 */
	readonly createQueueHandler: World["createQueueHandler"];
	/**
	 * The key of a run's payloads, which the Go World resolves, or undefined
	 * when they are not encrypted. Go runs' payloads are in the Workflow
	 * SDK's format, so the SDK's tools decrypt and hydrate them with it.
	 */
	readonly getEncryptionKeyForRun: NonNullable<World["getEncryptionKeyForRun"]>;

	constructor(client: RPCClient, spec: RPC.Spec, options: { token?: string } = {}) {
		client.rpcVersion = HTTP_WORLD_RPC_VERSION;
		this.createQueueHandler = (prefix, handler) => queueHandler(prefix, handler, options.token);
		this._spec = spec;
		this.capabilities = HTTPWorld.createCapabilities(spec.capabilities);
		this.getDeploymentId = () => client.call<string>("world.deploymentId", {});
		this.getEncryptionKeyForRun = (async (run: string | WorkflowRun, context?: Record<string, unknown>) => {
			const runId = typeof run === "string" ? run : run.runId;
			const deploymentId = typeof run === "string" ? context?.deploymentId : run.deploymentId;
			const key = await client.call<Uint8Array | null>("runs.encryptionKey", {
				runId,
				deploymentId: typeof deploymentId === "string" ? deploymentId : "",
			});
			return key ?? undefined;
		}) as NonNullable<World["getEncryptionKeyForRun"]>;
		this.runs = HTTPWorld.createRuns(client, spec.capabilities);
		this.steps = HTTPWorld.createSteps(client);
		this.events = HTTPWorld.createEvents(client);
		this.hooks = HTTPWorld.createHooks(client);
		this.queue = HTTPWorld.createQueue(client);
		this.queueBatch = HTTPWorld.createQueueBatch(client);
		if (spec.capabilities.deploymentAffinity) {
			this.resolveLatestDeploymentId = () => client.call<string>("client.runs.startDeployment", { latest: true });
		}
		this.streams = HTTPWorld.createStreams(client);
		this.analytics = HTTPWorld.createAnalytics(client, spec.capabilities);
	}

	// createCapabilities declares what a Go World does. resolvedHookResume is
	// left out: it promises that a resume sent for a cached hook is either
	// deduplicated or refused before anything is written, and a Go World's
	// write has no way to ask for that refusal. A World that always
	// deduplicates declares hookResumeDedup, and one that says so per hook
	// is reported on the hook (see toHook), which is all resumeHook needs.
	private static createCapabilities(capabilities: RPC.Capabilities): World["capabilities"] {
		return {
			hookRetention: { active: true },
			hookForceClaim: true,
			inBandFence: true,
			maxConcurrency: true,
			...(capabilities.deploymentAffinity ? { deploymentAffinity: true } : {}),
			...(capabilities.startHook ? { atomicStartHook: { active: true } } : {}),
			...(capabilities.hookResumeDedup ? { hookResumeDedup: true } : {}),
		};
	}

	private static createRuns(client: RPCClient, capabilities: RPC.Capabilities): World["runs"] {
		function get(runId: string, params: { resolveData: "none" }): Promise<WorkflowRunWithoutData>;
		function get(runId: string, params?: { resolveData?: "all" }): Promise<WorkflowRun>;
		function get(runId: string, params?: { resolveData?: "all" | "none" }): Promise<WorkflowRun | WorkflowRunWithoutData> {
			return client.call<RPC.Run>("runs.get", { runId, ...params }).then(toRun);
		}
		function list(input: { resolveData: "none" }): Promise<PaginatedResponse<WorkflowRunWithoutData>>;
		function list(input?: { resolveData?: "all" }): Promise<PaginatedResponse<WorkflowRun>>;
		function list(input: ListWorkflowRunsParams = {}): Promise<PaginatedResponse<WorkflowRun | WorkflowRunWithoutData>> {
			const normalized = {
				...input,
				...("status" in input && input.status !== undefined ? { status: Array.isArray(input.status) ? input.status : [input.status] } : {}),
			};
			return client.call<PaginatedResponse<RPC.Run>>("runs.list", normalized).then((page) => ({ ...page, data: page.data.map(toRun) }));
		}
		function waitForTerminalStatus(runId: string, params: WaitForTerminalRunStatusParams & { resolveData: "none" }): Promise<WorkflowRunWithoutData>;
		function waitForTerminalStatus(runId: string, params?: WaitForTerminalRunStatusParams & { resolveData?: "all" }): Promise<WorkflowRun>;
		function waitForTerminalStatus(runId: string, params?: WaitForTerminalRunStatusParams): Promise<WorkflowRun | WorkflowRunWithoutData>;
		async function waitForTerminalStatus(runId: string, params: WaitForTerminalRunStatusParams = {}): Promise<WorkflowRun | WorkflowRunWithoutData> {
			const { timeoutMs, signal, ...input } = params;
			// The server waits at most a bounded time per request, so the wait
			// repeats until the run finishes or timeoutMs elapses. Without
			// timeoutMs it waits until the run finishes or signal aborts.
			durationNanoseconds(timeoutMs);
			const deadline = timeoutMs == null ? Infinity : Date.now() + timeoutMs;
			for (;;) {
				const wait = Math.max(0, Math.min(terminalWaitMs, deadline - Date.now()));
				const run = await client.call<RPC.Run>("runs.waitForTerminalStatus", { runId, ...input, timeout: durationNanoseconds(wait) }, { signal });
				if (terminalStatuses.has(run.status) || Date.now() >= deadline) {
					return toRun(run);
				}
			}
		}
		return {
			get,
			list,
			waitForTerminalStatus,
			...(capabilities.setAttributes
				? {
					experimentalSetAttributes: async (runId, changes, options) => {
						await client.call<RPC.Empty>("runs.setAttributes", { runId, changes, ...(options?.allowReservedAttributes ? { allowReservedAttributes: true } : {}) });
						const run = await client.call<Pick<RPC.Run, "attributes">>("runs.get", { runId });
						return { attributes: run.attributes };
					},
				}
				: {}),
			...(capabilities.deleteData
				? {
					deleteDataMany: (request) => client.call<DeleteRunsDataResult>("runs.deleteDataMany", request),
				}
				: {}),
		};
	}

	private static createSteps(client: RPCClient): World["steps"] {
		function get(runId: string, stepId: string, params: { resolveData: "none" }): Promise<StepWithoutData>;
		function get(runId: string, stepId: string, params?: { resolveData?: "all" }): Promise<Step>;
		function get(runId: string, stepId: string, params?: { resolveData?: "all" | "none" }): Promise<Step | StepWithoutData> {
			return client.call<RPC.Step>("steps.get", { runId, stepId, ...params }).then(toStep);
		}
		function list(input: ListWorkflowRunStepsParams & { resolveData: "none" }): Promise<PaginatedResponse<StepWithoutData>>;
		function list(input: ListWorkflowRunStepsParams & { resolveData?: "all" }): Promise<PaginatedResponse<Step>>;
		function list(input: ListWorkflowRunStepsParams): Promise<PaginatedResponse<Step | StepWithoutData>>;
		function list(input: ListWorkflowRunStepsParams): Promise<PaginatedResponse<Step | StepWithoutData>> {
			return client.call<PaginatedResponse<RPC.Step>>("steps.list", input).then((page) => ({ ...page, data: page.data.map(toStep) }));
		}
		return {
			get,
			list,
		};
	}

	private static createEvents(client: RPCClient): World["events"] {
		return {
			create: (
				runId: string | null,
				event: RunCreatedEventRequest | CreateEventRequest,
				options?: CreateEventParams,
			) => {
				if (event.eventType === "run_created") {
					const { startHook } = event.eventData;
					// An empty run ID asks the server to generate one.
					return client
						.call<RPC.EventResult>("events.createRun", runCreation(runId ?? "", event.eventData))
						.then((result) => {
							const created = toEventResult(result);
							// The run echoes its start hook, as world-vercel's does,
							// which is how start() knows the World created it.
							if (startHook && created.run && !created.run.startHook && runId) {
								created.run = { ...created.run, startHook: { ...startHook, hookId: `hook_${runId.slice("wrun_".length)}` } } as typeof created.run;
							}
							return created;
						}, (error: unknown) => {
							throw startHook ? startHookConflict(error, startHook.token) : error;
						}) as Promise<EventResult<"run_created">>;
				}
				return client
					.call<RPC.EventResult>("events.create", {
						runId,
						event: {
							eventType: event.eventType,
							correlationId: event.correlationId ?? "",
							data: jsonEncoded(storedEventData(event.eventData)),
							...(event.specVersion !== undefined ? { specVersion: event.specVersion } : {}),
							...(options?.inBand && options.occurredAt ? { occurredAt: options.occurredAt } : {}),
						},
						options: createOptions(options),
					})
					.then(toEventResult, (error: unknown) => {
						// A first flow's run_started that creates its run is
						// refused like the run_created it stands for.
						const startHook = event.eventType === "run_started" ? (event.eventData as { startHook?: { token: string } } | undefined)?.startHook : undefined;
						throw startHook ? startHookConflict(error, startHook.token) : error;
					});
			},
			createBatch: async (runId, events, options) => {
				const result = await client.call<RPC.EventBatchResult>("events.createBatch", {
					runId,
					events: events.map(({ event, occurredAt }) => ({
						eventType: event.eventType,
						correlationId: event.correlationId ?? "",
						data: jsonEncoded(storedEventData(event.eventData)),
						...(event.specVersion !== undefined ? { specVersion: event.specVersion } : {}),
						...(occurredAt ? { occurredAt } : {}),
					})),
					...(options?.inBand && options.expectedSeqInBand !== undefined ? { inBandCount: options.expectedSeqInBand } : {}),
					...(options?.eventCount !== undefined ? { eventCount: options.eventCount } : {}),
				});
				return {
					results: result.results.map((item) =>
						item.status === 200 && item.event
							? { status: 200 as const, event: withEventData(item.event), run: item.run && toRun(item.run), step: item.step && toStep(item.step), ...(item.wait ? { wait: toWait(item.wait) } : {}) }
							: { status: item.status, error: item.error ?? "internal", message: item.message ?? "" },
					),
					allocated: result.allocated,
					...(result.events ? { events: result.events.map(withEventData), hasMore: result.hasMore ?? false } : {}),
				} as EventBatchResult;
			},
			get: async (runId, eventId, params) => withEventData(await client.call<RPC.Event>("events.get", { runId, eventId, ...params })),
			list: async (input) => {
				// The caller's signal aborts the request and is not part of it.
				const { signal, ...request } = input;
				const page = await client.call<RPC.EventsPage>("events.list", request, { signal });
				const { snapshot, maxEvents, ...rest } = page as RPC.EventsPage & { maxEvents?: number };
				// Go reports no limit as 0, which the SDK would read as a
				// ceiling of zero events.
				return { ...withEventDataPage(rest), snapshot: { seq: snapshot.eventCount, seqInBand: snapshot.inBandCount }, ...(maxEvents ? { maxEvents } : {}) };
			},
			listByCorrelationId: async (input) =>
				withEventDataPage(await client.call<PaginatedResponse<RPC.Event>>("events.listByCorrelationID", input)),
		};
	}

	private static createHooks(client: RPCClient): World["hooks"] {
		return {
			get: (hookId, params) => client.call<RPC.Hook>("hooks.get", { hookId, ...params }).then(toHook),
			getByToken: (token, params) => client.call<RPC.Hook>("hooks.getByToken", { token, ...params }).then(toHook),
			list: (input) => client.call<PaginatedResponse<RPC.Hook>>("hooks.list", input).then((page) => ({ ...page, data: page.data.map(toHook) })),
		};
	}

	private static createQueue(client: RPCClient): World["queue"] {
		return (queueName, message, options) => {
			return client.call<{ messageId: MessageId | null }>("queue.queue", queueInput(queueName, message, options));
		};
	}

	private static createQueueBatch(client: RPCClient): NonNullable<World["queueBatch"]> {
		return async (queueName, messages) => {
			// The World answers for each message: its ID, or the error it was
			// refused with and whether sending the batch again may store it.
			// A failure that leaves no message's outcome known fails the
			// request. A message stored without an ID has an empty one.
			const results = await client.call<RPC.QueueBatchResult[]>("queue.queueBatch", {
				items: messages.map(({ message, opts }) => queueInput(queueName, message, opts)),
			});
			if (results.length !== messages.length) {
				throw new HTTPWorldError("invalid_response", `world-http: queued ${messages.length} messages and got ${results.length} results`);
			}
			return results.map((result) =>
				result.error !== undefined && result.error !== ""
					? { messageId: null, error: result.error, retryable: result.retryable === true }
					: { messageId: result.messageId ? (result.messageId as MessageId) : null },
			);
		};
	}

	private static createStreams(client: RPCClient): World["streams"] {
		return {
			createWriteSession: (runId, name, options) =>
				new HTTPStreamWriteSession(client, { runId, name, writerId: options.writerId }),
			write: async (runId, name, chunk) => {
				await client.call<RPC.Empty>("streams.write", { runId, name, chunk: bytes(chunk) });
			},
			writeMulti: async (runId, name, chunks) => {
				await client.call<RPC.Empty>("streams.writeMulti", { runId, name, chunks: chunks.map(bytes) });
			},
			close: async (runId, name) => {
				await client.call<RPC.Empty>("streams.close", { runId, name });
			},
			get: async (runId, name, startIndex = 0) => createReadableStream(client, { runId, name, startIndex }),
			list: (runId) => client.call<string[]>("streams.list", { runId }),
			getChunks: async (runId, name, options = {}) => {
				const result = await getStreamChunks(client, { runId, name, ...options });
				return {
					data: result.data,
					cursor: result.cursor,
					hasMore: result.hasMore,
					done: result.done,
				};
			},
			getInfo: async (runId, name) => {
				const result = await client.call<RPC.StreamInfo>("streams.getInfo", { runId, name });
				return { tailIndex: result.tailIndex, done: result.done };
			},
		};
	}

	private static createAnalytics(client: RPCClient, capabilities: RPC.Capabilities): World["analytics"] {
		if (!capabilities.analytics) {
			return undefined;
		}
		return {
			runs: {
				get: (runId) => client.call<AnalyticsRun>("analytics.runs.get", { runId }),
				list: (input = {}) => client.call<PaginatedResponse<AnalyticsRun>>("analytics.runs.list", {
					...input,
					...(input.status === undefined ? {} : { status: Array.isArray(input.status) ? input.status : [input.status] }),
				}),
			},
			attributes: {
				list: (input = {}) => client.call<PaginatedResponse<AnalyticsAttributeKey>>("analytics.attributes.list", input),
			},
			steps: {
				get: (runId, stepId) => client.call<AnalyticsStep>("analytics.steps.get", { runId, stepId }),
				list: (input) => client.call<PaginatedResponse<AnalyticsStep>>("analytics.steps.list", input),
			},
			events: {
				get: (runId, eventId) => client.call<AnalyticsEvent>("analytics.events.get", { runId, eventId }),
				getMany: (runId, eventIds) => client.call<AnalyticsEvent[]>("analytics.events.getMany", { runId, eventIds }),
				list: (input) => client.call<PaginatedResponse<AnalyticsEvent>>("analytics.events.list", input),
				listByCorrelationId: (input) => client.call<PaginatedResponse<AnalyticsEvent>>("analytics.events.listByCorrelationId", input),
			},
			hooks: {
				get: (hookId, params) => client.call<AnalyticsHook>("analytics.hooks.get", { hookId, ...params }),
				list: (input) => client.call<PaginatedResponse<AnalyticsHook>>("analytics.hooks.list", input),
			},
			waits: {
				get: (runId, waitId) => client.call<AnalyticsWait>("analytics.waits.get", { runId, waitId }),
				list: (input) => client.call<PaginatedResponse<AnalyticsWait>>("analytics.waits.list", input),
			},
		};
	}
}

async function getStreamChunks(client: RPCClient, input: RPC.GetStreamChunksInput): Promise<RPC.StreamChunksResult> {
	const { waitMs, ...request } = input;
	const result = await client.call<RPC.WireStreamChunksResult>("streams.getChunks", {
		...request,
		wait: durationNanoseconds(waitMs),
	});
	return {
		data: result.data,
		cursor: result.cursor || null,
		hasMore: result.hasMore,
		done: result.done,
		retryAfterMs: durationMilliseconds(result.retryAfter),
	};
}

function createReadableStream(client: RPCClient, input: RPC.GetStreamInput): ReadableStream<Uint8Array> {
	let cursor = input.cursor ?? "";
	return new ReadableStream({
		async pull(controller) {
			try {
				const result = await getStreamChunks(client, {
					...input,
					cursor,
					waitMs: input.pollIntervalMs ?? 100,
				});
				// Once a page returns a cursor, it replaces startIndex.
				if (result.cursor !== null) {
					cursor = result.cursor;
				}
				for (const chunk of result.data) {
					controller.enqueue(chunk.data);
				}
				if (result.done && !result.hasMore) {
					controller.close();
				}
			} catch (error) {
				controller.error(error);
			}
		},
	});
}

// createOptions keeps the create parameters the Go world understands. Its
// in-band fence is the writer's expected in-band count.
function createOptions(options: CreateEventParams | undefined): Record<string, unknown> | undefined {
	if (!options) {
		return undefined;
	}
	return {
		...(options.eventCount !== undefined ? { eventCount: options.eventCount } : {}),
		...(options.sinceCursor ? { sinceCursor: options.sinceCursor } : {}),
		...(options.resolveData ? { resolveData: options.resolveData } : {}),
		...(options.inBand && options.expectedSeqInBand !== undefined ? { inBandCount: options.expectedSeqInBand } : {}),
		...(options.resumeId ? { resumeId: options.resumeId } : {}),
		...(options.resumePayloadDigest ? { resumePayloadDigest: options.resumePayloadDigest } : {}),
		...(options.afterStepBody ? { afterStepBody: true } : {}),
		// The SDK passes the step's creation time as epoch milliseconds.
		...(typeof options.stepCreatedAt === "number" && Number.isFinite(options.stepCreatedAt) ? { stepCreatedAt: new Date(options.stepCreatedAt) } : {}),
	};
}

// toEventResult shapes an RPC event result as the Workflow SDK's EventResult.
function toEventResult(result: RPC.EventResult): EventResult {
	return {
		// A run_started of a running run appends no event.
		...(result.event ? { event: withEventData(result.event) } : {}),
		...(result.run ? { run: toRun(result.run) } : {}),
		...(result.step ? { step: toStep(result.step) } : {}),
		...(result.hook ? { hook: toHook(result.hook) } : {}),
		...(result.wait ? { wait: toWait(result.wait) } : {}),
		...(result.stepCreated ? { stepCreated: true } : {}),
		allocated: result.allocated,
		...(result.events ? { events: result.events.map(withEventData), cursor: null, hasMore: result.hasMore ?? false } : {}),
	} as EventResult;
}

// jsonEncoded encodes event data as the JSON the Go world stores, with bytes
// as base64 strings, as Go encodes []byte.
function jsonEncoded(value: unknown): Uint8Array {
	if (value === undefined) {
		return new Uint8Array();
	}
	// The replacer reads the holder's original value, because a Buffer's
	// toJSON has already run on item.
	const text = JSON.stringify(value, function (this: Record<string, unknown>, key, item) {
		const original = this[key];
		return original instanceof Uint8Array ? Buffer.from(original).toString("base64") : item;
	});
	return new TextEncoder().encode(text);
}

// payloadFields are the eventData fields that hold payloads, in the Workflow
// SDK's serialized form. The Go World stores event data as JSON, with
// payload bytes as base64 strings.
const payloadFields = ["input", "output", "result", "error", "payload", "metadata"];

// dateFields are the eventData fields that hold times, which the Go World
// stores as RFC 3339 strings.
const dateFields = ["resumeAt", "retryAfter", "tokenRetentionUntil"];

// storedEventData returns event data as the Go World stores it: with its
// workflow and step names in Go form, like the names beside it in the log.
function storedEventData(value: unknown): unknown {
	if (!value || typeof value !== "object" || Array.isArray(value) || value instanceof Uint8Array) {
		return value;
	}
	const fields = { ...(value as Record<string, unknown>) };
	if (typeof fields.workflowName === "string") {
		fields.workflowName = goName("workflow", fields.workflowName);
	}
	if (typeof fields.stepName === "string") {
		fields.stepName = goName("step", fields.stepName);
	}
	return fields;
}

// reviveDates replaces the times among fields with Dates.
function reviveDates(fields: Record<string, unknown>): void {
	for (const field of dateFields) {
		const value = fields[field];
		if (typeof value === "string" && !Number.isNaN(Date.parse(value))) {
			fields[field] = new Date(value);
		}
	}
}

// withEventData exposes an event's stored data as the eventData object the
// Workflow SDK reads: its payloads as bytes, its times as Dates, and Go
// workflow and step names in SDK form. An event about the run itself has no
// correlation ID.
function withEventData(event: RPC.Event): Event {
	const { data, occurredAt, resumeId, correlationId, ...fields } = event;
	const rest = { ...fields, ...(correlationId ? { correlationId } : {}), ...(occurredAt ? { occurredAt } : {}), ...(resumeId ? { resumeId } : {}) };
	const eventData = decodeData(data);
	if (eventData === undefined) {
		return rest as Event;
	}
	if (eventData && typeof eventData === "object" && !Array.isArray(eventData)) {
		const fields = eventData as Record<string, unknown>;
		if (typeof fields.workflowName === "string") {
			fields.workflowName = devkitName("workflow", fields.workflowName);
		}
		if (typeof fields.stepName === "string") {
			fields.stepName = devkitName("step", fields.stepName);
		}
		for (const field of payloadFields) {
			if (typeof fields[field] === "string") {
				fields[field] = new Uint8Array(Buffer.from(fields[field] as string, "base64"));
			}
		}
		reviveDates(fields);
		// A run's start hook is recorded in its creation.
		const startHook = fields.startHook;
		if (startHook && typeof startHook === "object") {
			reviveDates(startHook as Record<string, unknown>);
		}
	}
	return { ...rest, eventData } as Event;
}

// decodeData decodes stored event data, the JSON the Go World writes.
// Empty or undecodable bytes are undefined.
function decodeData(data: Uint8Array | null | undefined): unknown {
	if (!data || data.byteLength === 0) {
		return undefined;
	}
	try {
		return JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(data));
	} catch {
		return undefined;
	}
}

// payload returns stored payload bytes as the Workflow SDK reads them, or
// undefined when there are none.
function payload(data: Uint8Array | null | undefined): Uint8Array | undefined {
	return data && data.byteLength > 0 ? data : undefined;
}

// toRun exposes a run as the Workflow SDK reads one. It reports the spec
// version the World recorded on the run, which decides how the SDK wakes and
// cancels it. A Go World records one on every run it creates; a run the World
// has none for is left without one, which the SDK reads as a legacy run.
function toRun(run: RPC.Run): WorkflowRun {
	const { input, output, error, errorCode, startedAt, completedAt, expiredAt, executionContext, startHook, specVersion, ...rest } = run;
	return {
		...rest,
		// The SDK's schemas take a field the run does not have as absent, not
		// null or empty.
		...(errorCode ? { errorCode } : {}),
		...(startedAt ? { startedAt } : {}),
		...(completedAt ? { completedAt } : {}),
		...(expiredAt ? { expiredAt } : {}),
		...(executionContext ? { executionContext } : {}),
		...(startHook ? { startHook } : {}),
		...(specVersion ? { specVersion } : {}),
		input: payload(input),
		output: payload(output),
		error: payload(error),
	} as WorkflowRun;
}

// toWait exposes a wait as the Workflow SDK reads one, without the times it
// does not have.
function toWait(wait: RPC.Wait): Wait {
	const { resumeAt, completedAt, ...rest } = wait;
	return { ...rest, ...(resumeAt ? { resumeAt } : {}), ...(completedAt ? { completedAt } : {}) };
}

// toStep exposes a step as the Workflow SDK reads one, without the times it
// does not have.
function toStep(step: RPC.Step): Step {
	const { input, output, error, startedAt, completedAt, retryAfter, ...rest } = step;
	return {
		...rest,
		...(startedAt ? { startedAt } : {}),
		...(completedAt ? { completedAt } : {}),
		...(retryAfter ? { retryAfter } : {}),
		input: payload(input),
		output: payload(output),
		error: payload(error),
	} as Step;
}

// toHook exposes a hook as the Workflow SDK reads one: without the fields it
// does not have, and with the owner, project and environment its schema
// requires, which a Go World does not have and are empty as in
// world-postgres. A hook the World says it deduplicates resumes of, on a read
// by token, carries the SDK's attestation of that, so resumeHook sends the
// resume with its ID and digest.
function toHook(hook: RPC.Hook): Hook {
	const { metadata, tokenRetentionUntil, claimedFrom, resumeContext, resumeDedup, ...rest } = hook;
	return {
		...rest,
		ownerId: "",
		projectId: "",
		environment: "",
		// A resume context always names its deployment and workflow, even a
		// World without deployments, whose deployment ID is empty.
		...(resumeContext ? { resumeContext: { ...present(resumeContext), deploymentId: resumeContext.deploymentId ?? "", workflowName: resumeContext.workflowName ?? "" } } : {}),
		...(payload(metadata) ? { metadata } : {}),
		...(tokenRetentionUntil ? { tokenRetentionUntil } : {}),
		...(claimedFrom ? { claimedFrom: present(claimedFrom) } : {}),
		...(resumeDedup ? { resumeCapabilities: { hookResumeDedupVersion: HOOK_RESUME_DEDUP_VERSION } } : {}),
	} as Hook;
}

// present returns value without its null, empty and zero fields, which Go
// sends for the ones it leaves unset.
function present<T extends object>(value: T): T {
	return Object.fromEntries(Object.entries(value).filter(([, item]) => item != null && item !== "" && item !== 0)) as T;
}

// sdkQueue reads a Workflow SDK queue name, "__wkf_workflow_<name>" or
// "__wkf_step_<name>", with an optional namespace as in
// "__<namespace>_wkf_workflow_<name>". It returns the workflow a workflow
// queue is for, and undefined for a name that is not the SDK's.
function sdkQueue(queueName: string): { workflowName?: string } | undefined {
	const match = /^__(?:[^_]+_)?wkf_(workflow|step)_(.+)$/.exec(queueName);
	return match ? (match[1] === "workflow" ? { workflowName: match[2] } : {}) : undefined;
}

// RunCreation is what creates a run: the data of a run_created event, and the
// runInput of the run's first queue message.
type RunCreation = {
	workflowName: string;
	input?: unknown;
	attributes?: Record<string, string>;
	allowReservedAttributes?: boolean;
	deploymentId?: string;
	encryptionPublicKey?: string;
	executionContext?: Record<string, unknown>;
	startHook?: { token: string; tokenRetentionUntil?: Date; metadata?: unknown };
};

// runCreation returns the Go World's input for creating the run runId from
// data. The run_created write and the run's first message both send it, so the
// message's delivery creates the same run when run_created never landed.
function runCreation(runId: string, data: RunCreation): Record<string, unknown> {
	const { startHook } = data;
	return {
		runId,
		workflowName: data.workflowName,
		input: data.input,
		...(data.attributes ? { attributes: data.attributes } : {}),
		...(data.allowReservedAttributes ? { allowReservedAttributes: true } : {}),
		...(data.deploymentId ? { deploymentId: data.deploymentId } : {}),
		...(data.encryptionPublicKey ? { encryptionPublicKey: data.encryptionPublicKey } : {}),
		...(data.executionContext ? { executionContext: data.executionContext } : {}),
		...(startHook ? { startHook: { token: startHook.token, tokenRetentionUntil: startHook.tokenRetentionUntil, metadata: startHook.metadata } } : {}),
	};
}

// queueInput returns the Go World's delivery for a message the Workflow SDK
// queues. What the Go runtime has no use for, such as the trace carrier and
// the resume's timing, is left out.
function queueInput(queueName: string, message: Parameters<World["queue"]>[1], options: Parameters<World["queue"]>[2]): Record<string, unknown> {
	const fields = message as {
		runId: string;
		stepId?: string;
		stepName?: string;
		stepAttempt?: number;
		stepResent?: boolean;
		stepCreatedAt?: number;
		recovery?: boolean;
		completeWaits?: string[];
		runSpecVersion?: number;
		deploymentMismatchRetryCount?: number;
		runInput?: RunCreation;
	};
	// The SDK names a queue after its workflow, which a Go World routes the
	// delivery by. A Go World's own queue names are not the SDK's, so an SDK
	// name is not passed on as one.
	const sdk = sdkQueue(queueName);
	const { runInput } = fields;
	// A run's first message carries its creation and is stored once, keyed
	// by the run, as the Go client's is. The run it creates is recorded at
	// the Go World's spec version.
	const runSpecVersion = runInput ? GO_SPEC_VERSION : (fields.runSpecVersion ?? options?.specVersion);
	const idempotencyKey = options?.idempotencyKey ?? (runInput ? fields.runId : undefined);
	return {
		...(sdk ? (sdk.workflowName ? { workflowName: sdk.workflowName } : {}) : { queueName }),
		runId: fields.runId,
		// The whole message and its queue name, which the Go World stores and
		// a worlds.RemoteExecutor delivers back to the app unchanged.
		sdkMessage: sdkMessageBytes(queueName, message),
		...(fields.stepId ? { stepId: fields.stepId } : {}),
		...(fields.stepName ? { stepName: fields.stepName } : {}),
		...(fields.stepAttempt ? { stepAttempt: fields.stepAttempt } : {}),
		...(fields.stepResent ? { stepResent: true } : {}),
		// The SDK carries the step's creation time as epoch milliseconds.
		...(typeof fields.stepCreatedAt === "number" && Number.isFinite(fields.stepCreatedAt) ? { stepCreatedAt: new Date(fields.stepCreatedAt) } : {}),
		...(fields.recovery ? { recovery: true } : {}),
		...(fields.deploymentMismatchRetryCount ? { deploymentMismatchRetryCount: fields.deploymentMismatchRetryCount } : {}),
		...(fields.completeWaits?.length ? { completeWaits: fields.completeWaits } : {}),
		...(runSpecVersion ? { runSpecVersion } : {}),
		...(runInput ? { runInput: runCreation(fields.runId, runInput) } : {}),
		...(idempotencyKey ? { idempotencyKey } : {}),
		...(options?.deploymentId ? { deploymentId: options.deploymentId } : {}),
		...(options?.delaySeconds ? { notBefore: new Date(Date.now() + options.delaySeconds * 1_000) } : {}),
		...(options?.retentionSeconds ? { retention: durationNanoseconds(options.retentionSeconds * 1_000) } : {}),
	};
}

// sdkMessageBytes returns a queued message and its queue name as JSON, with
// Uint8Array values as the Workflow SDK's local queue writes them.
function sdkMessageBytes(queueName: string, message: unknown): Uint8Array {
	return new TextEncoder().encode(JSON.stringify({ queueName, message }, typedJsonReplacer));
}

function typedJsonReplacer(_key: string, value: unknown): unknown {
	return value instanceof Uint8Array ? { __type: "Uint8Array", data: Buffer.from(value).toString("base64") } : value;
}

function typedJsonReviver(_key: string, value: unknown): unknown {
	if (value !== null && typeof value === "object" && (value as { __type?: unknown }).__type === "Uint8Array" && typeof (value as { data?: unknown }).data === "string") {
		return new Uint8Array(Buffer.from((value as { data: string }).data, "base64"));
	}
	return value;
}

// queueHandler returns the route handler createQueueHandler returns.
function queueHandler(prefix: string, handler: Parameters<World["createQueueHandler"]>[1], token: string | undefined): (request: Request) => Promise<Response> {
	const expected = token ? `Bearer ${token}` : undefined;
	return async (request) => {
		if (request.method !== "POST") {
			return Response.json({ error: "world-http: deliveries are POST requests" }, { status: 405 });
		}
		if (expected !== undefined && !sameSecret(request.headers.get("Authorization") ?? "", expected)) {
			return Response.json({ error: "world-http: the delivery's token is not the World's" }, { status: 401 });
		}
		const queueName = request.headers.get("x-vqs-queue-name");
		const messageId = request.headers.get("x-vqs-message-id");
		if (!queueName || !messageId) {
			return Response.json({ error: "world-http: the delivery has no x-vqs-queue-name or x-vqs-message-id" }, { status: 400 });
		}
		if (!queueName.startsWith(prefix)) {
			return Response.json({ error: `world-http: queue ${queueName} is not served by this route, which serves ${prefix}` }, { status: 400 });
		}
		let message: unknown;
		try {
			message = JSON.parse(await request.text(), typedJsonReviver);
		} catch (error) {
			return Response.json({ error: `world-http: could not decode the delivery: ${error instanceof Error ? error.message : String(error)}` }, { status: 400 });
		}
		const attempt = Number(request.headers.get("x-vqs-message-attempt")) || 1;
		const deliveryCount = Number(request.headers.get("x-vqs-message-delivery-count")) || attempt;
		const createdAt = request.headers.get("x-vqs-message-created-at");
		let result: unknown;
		try {
			result = await handler(message, {
				attempt,
				deliveryCount,
				...(createdAt ? { createdAt: new Date(createdAt) } : {}),
				queueName: queueName as Parameters<typeof handler>[1]["queueName"],
				messageId: messageId as MessageId,
			});
		} catch (error) {
			return Response.json(String(error), { status: 500 });
		}
		const timeoutSeconds = result && typeof result === "object" ? (result as { timeoutSeconds?: unknown }).timeoutSeconds : undefined;
		// JSON has no Infinity, which the Go World caps at its longest delay.
		return Response.json(typeof timeoutSeconds === "number" ? { timeoutSeconds: Number.isFinite(timeoutSeconds) ? timeoutSeconds : 1e9 } : { ok: true });
	};
}

// sameSecret compares two strings in time that depends only on their lengths.
function sameSecret(actual: string, expected: string): boolean {
	if (actual.length !== expected.length) {
		return false;
	}
	let difference = 0;
	for (let index = 0; index < actual.length; index++) {
		difference |= actual.charCodeAt(index) ^ expected.charCodeAt(index);
	}
	return difference === 0;
}

// terminalWaitMs is how long one runs.waitForTerminalStatus request waits.
const terminalWaitMs = 10_000;
const terminalStatuses = new Set<RPC.Status>(["completed", "failed", "cancelled"]);

function withEventDataPage(page: PaginatedResponse<RPC.Event>): PaginatedResponse<Event> {
	return { ...page, data: page.data.map(withEventData) };
}


function environment(name: string): string | undefined {
	return typeof process === "undefined" ? undefined : process.env?.[name];
}

function bytes(value: RPC.StreamChunk): Uint8Array {
	if (typeof value === "string") {
		return new TextEncoder().encode(value);
	}
	return value;
}

function durationNanoseconds(milliseconds: number | undefined): number {
	if (milliseconds == null) return 0;
	if (!Number.isFinite(milliseconds) || milliseconds < 0) {
		throw new RangeError("world-http: duration must be a non-negative finite millisecond value");
	}
	return Math.round(milliseconds * 1_000_000);
}

function durationMilliseconds(nanoseconds: number | undefined): number {
	if (nanoseconds == null) {
		return 0;
	}
	return Number(nanoseconds) / 1_000_000;
}

// startHookConflict returns a refused start hook as the Workflow SDK's
// HookConflictError, which start() recognizes by its name, naming the run that
// holds the token, and a rejected candidate as its WorkflowWorldError with
// code START_HOOK_ADMISSION_REJECTED, and a candidate answered while admission
// is switched off as its WorkflowWorldError with code
// START_HOOK_ADMISSION_DISABLED, which start() does not take as the
// candidate's outcome. Other errors are returned as they are.
function startHookConflict(error: unknown, token: string): unknown {
	if (error instanceof HTTPWorldError && error.sentinel === "start_hook_disabled") {
		return Object.assign(new Error(error.message), { name: "WorkflowWorldError", code: START_HOOK_ADMISSION_DISABLED_CODE, status: 400, reason: "disabled", cause: error });
	}
	if (error instanceof HTTPWorldError && error.sentinel === "start_hook_rejected") {
		// A candidate the World will never admit is the SDK's
		// WorkflowWorldError with code START_HOOK_ADMISSION_REJECTED.
		return Object.assign(new Error(error.message), { name: "WorkflowWorldError", code: START_HOOK_ADMISSION_REJECTED_CODE, cause: error });
	}
	if (!(error instanceof HTTPWorldError) || error.sentinel !== "hook_conflict" || !error.conflictingRunId) {
		return error;
	}
	return Object.assign(new Error(`No run was created: workflow run "${error.conflictingRunId}" already owns this hook token`), {
		name: "HookConflictError",
		token,
		conflictingRunId: error.conflictingRunId,
		cause: error,
	});
}
