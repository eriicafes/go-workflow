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
	EventBatchResult,
	EventResult,
	MessageId,
	PaginatedResponse,
	Step,
	StepWithoutData,
	ListWorkflowRunStepsParams,
	ListWorkflowRunsParams,
	WaitForTerminalRunStatusParams,
	WorkflowRun,
	WorkflowRunWithoutData,
	World,
} from "@workflow/world";
import { toHook, toRun, toStep, toWait } from "./entities.js";
import { createOptions, jsonEncoded, runCreation, startHookConflict, toEventResult, withEventData, withEventDataPage } from "./events.js";
import { queueHandler, queueInput } from "./queue.js";
import { HTTPWorldError, RPCClient, type RPC } from "./rpc.js";
import { HTTPStreamWriteSession } from "./stream-socket.js";
import { createReadableStream, getStreamChunks } from "./stream-reader.js";
import { GO_SPEC_VERSION, bytes, durationNanoseconds, environment, page } from "./utils.js";

const HTTP_WORLD_RPC_VERSION = 1;

// terminalWaitMs is how long one runs.waitForTerminalStatus request waits.
const terminalWaitMs = 10_000;
const terminalStatuses = new Set<RPC.Status>(["completed", "failed", "cancelled"]);

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
			return client.call<PaginatedResponse<RPC.Run>>("runs.list", normalized).then((result) => ({ ...result, data: page(result).data.map(toRun) }));
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
			while (true) {
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
			return client.call<PaginatedResponse<RPC.Step>>("steps.list", input).then((result) => ({ ...result, data: page(result).data.map(toStep) }));
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
							data: jsonEncoded(event.eventData),
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
						data: jsonEncoded(event.eventData),
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
			list: (input) => client.call<PaginatedResponse<RPC.Hook>>("hooks.list", input).then((result) => ({ ...result, data: page(result).data.map(toHook) })),
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
			list: async (runId) => (await client.call<string[] | null>("streams.list", { runId })) ?? [],
			getChunks: async (runId, name, options = {}) => {
				const result = await getStreamChunks(client, { runId, name, ...options });
				return {
					data: result.data,
					// The SDK ends pagination with a null cursor once no more
					// chunks are available.
					cursor: result.hasMore ? result.cursor : null,
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
				}).then(page),
			},
			attributes: {
				list: (input = {}) => client.call<PaginatedResponse<AnalyticsAttributeKey>>("analytics.attributes.list", input).then(page),
			},
			steps: {
				get: (runId, stepId) => client.call<AnalyticsStep>("analytics.steps.get", { runId, stepId }),
				list: (input) => client.call<PaginatedResponse<AnalyticsStep>>("analytics.steps.list", input).then(page),
			},
			events: {
				get: (runId, eventId) => client.call<AnalyticsEvent>("analytics.events.get", { runId, eventId }),
				getMany: async (runId, eventIds) => (await client.call<AnalyticsEvent[] | null>("analytics.events.getMany", { runId, eventIds })) ?? [],
				list: (input) => client.call<PaginatedResponse<AnalyticsEvent>>("analytics.events.list", input).then(page),
				listByCorrelationId: (input) => client.call<PaginatedResponse<AnalyticsEvent>>("analytics.events.listByCorrelationId", input).then(page),
			},
			hooks: {
				get: (hookId, params) => client.call<AnalyticsHook>("analytics.hooks.get", { hookId, ...params }),
				list: (input) => client.call<PaginatedResponse<AnalyticsHook>>("analytics.hooks.list", input).then(page),
			},
			waits: {
				get: (runId, waitId) => client.call<AnalyticsWait>("analytics.waits.get", { runId, waitId }),
				list: (input) => client.call<PaginatedResponse<AnalyticsWait>>("analytics.waits.list", input).then(page),
			},
		};
	}
}
