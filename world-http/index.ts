import type {
	CreateEventParams,
	CreateEventRequest,
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
	ListWorkflowRunStepsParams,
	ListWorkflowRunsParams,
	WaitForTerminalRunStatusParams,
	WorkflowRun,
	WorkflowRunWithoutData,
	World,
} from "@workflow/world";
import { SPEC_VERSION_CURRENT } from "@workflow/world/spec-version";
import { encode } from "cbor-x";
import { HTTPWorldError, RPCClient, type RPC } from "./rpc.js";

export type { World } from "@workflow/world";
export { HTTPWorldError } from "./rpc.js";
export type { RPC } from "./rpc.js";

export interface Capabilities {
	analytics: boolean;
	setAttributes: boolean;
}

export interface HTTPWorldOptions {
	/**
	 * The `worlds.HTTPHandler` prefix.
	 * Defaults to `WORKFLOW_HTTP_BASE_URL`.
	 */
	baseURL?: string;
	fetch?: typeof globalThis.fetch;
	headers?: HeadersInit;
}

/**
 * Creates a World backed by a go-workflow `worlds.HTTPHandler`.
 * `baseURL` is the handler prefix, for example
 * `https://workflows.example.com/v1/workflow`.
 */
export async function createWorld(options: HTTPWorldOptions = {}): Promise<HTTPWorld> {
	const client = new RPCClient({
		...options,
		baseURL: options.baseURL ?? environment("WORKFLOW_HTTP_BASE_URL"),
	});
	const capabilities = await client.call<Capabilities>("world.capabilities", {});
	return new HTTPWorld(client, capabilities);
}

export class HTTPWorld implements World {
	readonly specVersion = SPEC_VERSION_CURRENT;
	readonly _capabilities: Capabilities;
	// The RPC server capabilities remain available separately because they
	// describe its RPC surface, not the Workflow SDK's optional capabilities.
	readonly capabilities: World["capabilities"];
	readonly runs: World["runs"];
	readonly steps: World["steps"];
	readonly events: World["events"];
	readonly hooks: World["hooks"];
	readonly queue: World["queue"];
	readonly streams: World["streams"];
	readonly analytics?: World["analytics"];
	/**
	 * HTTP worlds are SDK clients for an RPC server, never a deployed JavaScript
	 * backend. They therefore have no deployment identity of their own.
	 */
	readonly getDeploymentId: World["getDeploymentId"] = async () => "";
	/**
	 * HTTP worlds cannot receive queue deliveries in JavaScript: queue handling
	 * belongs to the RPC server, not the client-side SDK adapter.
	 */
	readonly createQueueHandler: World["createQueueHandler"] = () => {
		throw new HTTPWorldError("unsupported", "workflow: HTTP worlds do not create queue handlers");
	};

	constructor(client: RPCClient, capabilities: Capabilities) {
		this._capabilities = capabilities;
		this.capabilities = HTTPWorld.createCapabilities();
		this.runs = HTTPWorld.createRuns(client, this._capabilities);
		this.steps = HTTPWorld.createSteps(client);
		this.events = HTTPWorld.createEvents(client);
		this.hooks = HTTPWorld.createHooks(client);
		this.queue = HTTPWorld.createQueue(client);
		this.streams = HTTPWorld.createStreams(client);
		this.analytics = HTTPWorld.createAnalytics(client, this._capabilities);
	}

	private static createCapabilities(): World["capabilities"] {
		return {
			hookRetention: { active: true },
			hookForceClaim: true,
		};
	}

	private static createRuns(client: RPCClient, capabilities: Capabilities): World["runs"] {
		function get(runId: string, params: { resolveData: "none" }): Promise<WorkflowRunWithoutData>;
		function get(runId: string, params?: { resolveData?: "all" }): Promise<WorkflowRun>;
		function get(runId: string, params?: { resolveData?: "all" | "none" }): Promise<WorkflowRun | WorkflowRunWithoutData> {
			return params?.resolveData === "none"
				? client.call<WorkflowRunWithoutData>("runs.get", { runId, ...params })
				: client.call<WorkflowRun>("runs.get", { runId, ...params });
		}
		function list(input: { resolveData: "none" }): Promise<PaginatedResponse<WorkflowRunWithoutData>>;
		function list(input?: { resolveData?: "all" }): Promise<PaginatedResponse<WorkflowRun>>;
		function list(input: ListWorkflowRunsParams = {}): Promise<PaginatedResponse<WorkflowRun | WorkflowRunWithoutData>> {
			const normalized = {
				...input,
				...("status" in input && input.status !== undefined ? { status: Array.isArray(input.status) ? input.status : [input.status] } : {}),
			};
			return input.resolveData === "none"
				? client.call<PaginatedResponse<WorkflowRunWithoutData>>("runs.list", normalized)
				: client.call<PaginatedResponse<WorkflowRun>>("runs.list", normalized);
		}
		function waitForTerminalStatus(runId: string, params: WaitForTerminalRunStatusParams & { resolveData: "none" }): Promise<WorkflowRunWithoutData>;
		function waitForTerminalStatus(runId: string, params?: WaitForTerminalRunStatusParams & { resolveData?: "all" }): Promise<WorkflowRun>;
		function waitForTerminalStatus(runId: string, params?: WaitForTerminalRunStatusParams): Promise<WorkflowRun | WorkflowRunWithoutData>;
		function waitForTerminalStatus(runId: string, params: WaitForTerminalRunStatusParams = {}): Promise<WorkflowRun | WorkflowRunWithoutData> {
			const { timeoutMs, signal, ...input } = params;
			return params.resolveData === "none"
				? client.call<WorkflowRunWithoutData>("runs.waitForTerminalStatus", { runId, ...input, timeout: durationNanoseconds(timeoutMs) }, { signal })
				: client.call<WorkflowRun>("runs.waitForTerminalStatus", { runId, ...input, timeout: durationNanoseconds(timeoutMs) }, { signal });
		}
		return {
			get,
			list,
			waitForTerminalStatus,
			...(capabilities.setAttributes
				? {
					experimentalSetAttributes: async (runId, changes) => {
						await client.call<RPC.Empty>("runs.setAttributes", { runId, changes });
						const run = await client.call<Pick<RPC.Run, "attributes">>("runs.get", { runId });
						return { attributes: run.attributes };
					},
				}
				: {}),
		};
	}

	private static createSteps(client: RPCClient): World["steps"] {
		function get(runId: string, stepId: string, params: { resolveData: "none" }): Promise<StepWithoutData>;
		function get(runId: string, stepId: string, params?: { resolveData?: "all" }): Promise<Step>;
		function get(runId: string, stepId: string, params?: { resolveData?: "all" | "none" }): Promise<Step | StepWithoutData> {
			return params?.resolveData === "none"
				? client.call<StepWithoutData>("steps.get", { runId, stepId, ...params })
				: client.call<Step>("steps.get", { runId, stepId, ...params });
		}
		function list(input: ListWorkflowRunStepsParams & { resolveData: "none" }): Promise<PaginatedResponse<StepWithoutData>>;
		function list(input: ListWorkflowRunStepsParams & { resolveData?: "all" }): Promise<PaginatedResponse<Step>>;
		function list(input: ListWorkflowRunStepsParams): Promise<PaginatedResponse<Step | StepWithoutData>>;
		function list(input: ListWorkflowRunStepsParams): Promise<PaginatedResponse<Step | StepWithoutData>> {
			return input.resolveData === "none"
				? client.call<PaginatedResponse<StepWithoutData>>("steps.list", input)
				: client.call<PaginatedResponse<Step>>("steps.list", input);
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
					return client.call<EventResult>("events.createRun", {
						runId: runId ?? "",
						workflowName: event.eventData.workflowName,
						input: encoded(event.eventData.input),
						...(event.eventData.attributes ? { attributes: event.eventData.attributes } : {}),
					});
				}
				return client.call<EventResult>("events.create", {
					runId,
					event: {
						eventType: event.eventType,
						correlationId: event.correlationId ?? "",
						data: encoded(event.eventData),
					},
					options,
				});
			},
			createBatch: (runId, events, options) =>
				client.call<EventBatchResult>("events.createBatch", {
					runId,
					events: events.map(({ event }) => ({
						eventType: event.eventType,
						correlationId: event.correlationId ?? "",
						data: encoded(event.eventData),
					})),
					options,
				}),
			get: (runId, eventId, params) => client.call<Event>("events.get", { runId, eventId, ...params }),
			list: (input) => client.call<PaginatedResponse<Event>>("events.list", input),
			listByCorrelationId: (input) => client.call<PaginatedResponse<Event>>("events.listByCorrelationID", input),
		};
	}

	private static createHooks(client: RPCClient): World["hooks"] {
		return {
			get: (hookId, params) => client.call<Hook>("hooks.get", { hookId, ...params }),
			getByToken: (token, params) => client.call<Hook>("hooks.getByToken", { token, ...params }),
			list: (input) => client.call<PaginatedResponse<Hook>>("hooks.list", input),
		};
	}

	private static createQueue(client: RPCClient): World["queue"] {
		return (queueName, message, options) => {
			if (!("runId" in message) || message.runId === undefined) {
				throw new TypeError("world-http: queue messages must include runId");
			}
			return client.call<{ messageId: MessageId | null }>("queue.queue", {
				queueName,
				runId: message.runId,
				...("stepId" in message && message.stepId ? { stepId: message.stepId } : {}),
				...(options?.idempotencyKey ? { idempotencyKey: options.idempotencyKey } : {}),
				...(options?.delaySeconds ? { notBefore: new Date(Date.now() + options.delaySeconds * 1_000) } : {}),
			});
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
			get: async (runId, name, startIndex = 0) => createReadableStream(client, { runId, name, cursor: String(startIndex) }),
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

	private static createAnalytics(client: RPCClient, capabilities: Capabilities): World["analytics"] {
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

class HTTPStreamWriteSession {
	readonly client: RPCClient;
	readonly input: RPC.StreamWriteSessionInput;
	sessionId?: string;
	closed = false;

	constructor(client: RPCClient, input: RPC.StreamWriteSessionInput) {
		this.client = client;
		this.input = input;
	}

	async write(sequence: number, chunks: RPC.StreamChunk[]): Promise<void> {
		if (this.closed) {
			throw new HTTPWorldError("stream_session_closed", "workflow: stream write session is closed");
		}
		await this.client.call<RPC.Empty>("streams.writeSession", {
			runId: this.input.runId,
			name: this.input.name,
			sessionId: await this.getSessionId(),
			sequence: sequence,
			chunks: chunks.map(bytes),
		});
	}

	async close(): Promise<void> {
		if (this.closed) {
			return;
		}
		await this.client.call<RPC.Empty>("streams.closeWriteSession", {
			runId: this.input.runId,
			name: this.input.name,
			sessionId: await this.getSessionId(),
			close: true,
		});
		this.closed = true;
	}

	async release(): Promise<void> {
		if (!this.sessionId || this.closed) {
			return;
		}
		await this.client.call<RPC.Empty>("streams.closeWriteSession", {
			runId: this.input.runId,
			name: this.input.name,
			sessionId: this.sessionId,
			close: false,
		});
		this.sessionId = undefined;
	}

	private async getSessionId(): Promise<string> {
		if (!this.sessionId) {
			this.sessionId = await this.client.call<string>("streams.createWriteSession", this.input);
		}
		return this.sessionId;
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

function encoded(value: unknown): Uint8Array {
	if (value instanceof Uint8Array) {
		return value;
	}
	const valueBytes = encode(value);
	const result = new Uint8Array(valueBytes.byteLength);
	result.set(valueBytes);
	return result;
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
