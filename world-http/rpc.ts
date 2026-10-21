import { decode, encode } from "cbor-x";

const rpcContentType = "application/cbor";

export interface ClientOptions {
	baseURL?: string;
	fetch?: typeof globalThis.fetch;
	headers?: HeadersInit;
}

export interface CallOptions {
	signal?: AbortSignal;
}

export namespace RPC {
	// These are the camel-cased values the RPC server returns, not
	// @workflow/world SDK entities.
	/** The RPC server's protocol version and capabilities, from `client.spec`. */
	export type Spec = {
		specVersion: number;
		capabilities: Capabilities;
	};
	/** The optional behavior the RPC server's World implements. */
	export type Capabilities = {
		analytics: boolean;
		setAttributes: boolean;
		deploymentAffinity?: boolean;
	};
	export type Status = "pending" | "running" | "completed" | "failed" | "cancelled";
	export type EventType = string;
	export type Page<T> = {
		data: T[];
		cursor: string;
		hasMore: boolean;
	};
	export type Run = {
		runId: string;
		workflowName: string;
		status: Status;
		input: Uint8Array;
		output: Uint8Array;
		error: Uint8Array;
		attributes: Record<string, string>;
		deploymentId: string;
		inBandCount: number;
		createdAt: Date;
		updatedAt: Date;
		startedAt: Date | null;
		completedAt: Date | null;
	};
	export type Step = {
		runId: string;
		stepId: string;
		stepName: string;
		status: Status;
		input: Uint8Array;
		output: Uint8Array;
		error: Uint8Array;
		attempt: number;
		createdAt: Date;
		updatedAt: Date;
		startedAt: Date | null;
		completedAt: Date | null;
		retryAfter: Date | null;
	};
	export type Event = {
		runId: string;
		eventId: string;
		eventType: EventType;
		correlationId: string;
		data: Uint8Array;
		createdAt: Date;
	};
	export type EventResult = {
		event: Event;
		run?: Run;
		step?: Step;
		hook?: Hook;
		stepCreated?: boolean;
		allocated: number;
		inBandCount: number;
		events?: Event[];
		hasMore?: boolean;
	};
	export type BatchEventResult = {
		status: number;
		error?: string;
		message?: string;
		event?: Event;
		run?: Run;
		step?: Step;
	};
	export type EventBatchResult = { results: BatchEventResult[]; inBandCount: number };
	export type WebhookResponse = {
		status: number;
		header?: Record<string, string[]>;
		body?: Uint8Array;
	};
	export type Webhook = {
		method?: string;
		manual?: boolean;
		response?: WebhookResponse;
	};
	export type Hook = {
		runId: string;
		hookId: string;
		token: string;
		metadata: Uint8Array;
		webhook: Webhook | null;
		webhookEventId: string;
		createdAt: Date;
		updatedAt: Date;
		receivedAt: Date | null;
		tokenRetentionUntil: Date | null;
		claimedFrom: { runId: string; hookId: string } | null;
		forceClaimedBy: { runId: string; hookId: string } | null;
		disposedAt: Date | null;
	};
	export type QueueResult = { messageId: string };
	export type StreamInfo = { tailIndex: number; done: boolean };
	export type AnalyticsRun = Omit<Run, "input" | "output" | "error" | "deploymentId" | "inBandCount">;
	export type AnalyticsAttribute = {
		key: string;
		runCount: number;
		firstSeenAt: Date;
		lastSeenAt: Date;
	};
	export type AnalyticsWait = {
		runId: string;
		waitId: string;
		dueAt: Date;
		queueMessageId: string;
		createdAt: Date;
		completedAt: Date | null;
	};
	export type Empty = Record<string, never>;
	export type Error = {
		code: string;
		message: string;
	};

	// Input and output are embedded CBOR items, matching the Go RawMessage fields.
	
	export type Request = {
		method: string;
		input: unknown;
	};

	export type Response = {
		output?: unknown;
		error?: Error;
	};

	export type StreamChunk = string | Uint8Array;

	export type StreamInput = {
		runId: string;
		name: string;
	};

	export type StreamWriteInput = StreamInput & {
		chunk: StreamChunk;
	};

	export type StreamWriteMultiInput = StreamInput & {
		chunks: StreamChunk[];
	};

	export type StreamWriteSessionInput = StreamInput & {
		writerId: string;
	};

	export type GetStreamChunksInput = StreamInput & {
		cursor?: string;
		limit?: number;
		waitMs?: number;
	};

	export type GetStreamInput = GetStreamChunksInput & {
		pollIntervalMs?: number;
	};

	export type StreamChunksResult = {
		data: { index: number; data: Uint8Array }[];
		cursor: string | null;
		hasMore: boolean;
		done: boolean;
		retryAfterMs: number;
	};

	export type WireStreamChunksResult = {
		data: { index: number; data: Uint8Array }[];
		cursor: string;
		hasMore: boolean;
		done: boolean;
		retryAfter?: number;
	};
}

export class HTTPWorldError extends Error {
	readonly code: string;
	readonly status?: number;

	constructor(code: string, message: string, status?: number) {
		super(message);
		this.name = "HTTPWorldError";
		this.code = code;
		this.status = status;
	}
}

export class RPCClient {
	readonly baseURL: string;
	readonly fetch: typeof globalThis.fetch;
	readonly headers: HeadersInit;
	specVersion?: number;

	constructor(options: ClientOptions) {
		const { baseURL, fetch: fetchImplementation = globalThis.fetch, headers = {} } = options;
		if (typeof baseURL !== "string" || baseURL.length === 0) {
			throw new TypeError("world-http: baseURL is required");
		}
		if (typeof fetchImplementation !== "function") {
			throw new TypeError("world-http: fetch is required");
		}
		this.baseURL = baseURL.replace(/\/+$/, "");
		this.fetch = fetchImplementation;
		this.headers = headers;
	}

	async call<T = never>(method: string, input: unknown, options: CallOptions = {}): Promise<T> {
		const encodedBody = encode({
			method,
			input: toWire(input),
			...(this.specVersion === undefined ? {} : { specVersion: this.specVersion }),
		});
		const body = new Uint8Array(encodedBody.byteLength);
		body.set(encodedBody);
		const headers = new Headers(this.headers);
		headers.set("Content-Type", rpcContentType);
		headers.set("Accept", rpcContentType);
		const response = await this.fetch(`${this.baseURL}/rpc`, {
			method: "POST",
			headers,
			body,
			signal: options.signal,
		});
		const responseBody = new Uint8Array(await response.arrayBuffer());
		if (!response.ok) {
			throw new HTTPWorldError(
				"http_error",
				`world-http: RPC request failed with ${response.status}`,
				response.status,
			);
		}
		let envelope: RPC.Response;
		try {
			envelope = decode(responseBody) as RPC.Response;
		} catch (error) {
			const message = error instanceof Error ? error.message : String(error);
			throw new HTTPWorldError(
				"invalid_response",
				`world-http: could not decode RPC response: ${message}`,
				response.status,
			);
		}
		if (envelope.error) {
			throw new HTTPWorldError(envelope.error.code, envelope.error.message, response.status);
		}
		if (envelope.output === undefined) {
			return undefined as T;
		}
		return fromWire(envelope.output) as T;
	}
}

const opaqueMapKeys = new Set(["attributes", "header"]);

// Go names workflows and steps by package path and identifier, such as
// "example.com/app/flows.Publish", while the Workflow SDK parses names shaped
// "workflow//<module>//<function>". These fields carry such names.
const nameKinds: Record<string, "workflow" | "step"> = { workflowName: "workflow", stepName: "step" };

// devkitName maps a Go name to the Workflow SDK form, splitting the package
// path from the identifier at the first dot after the last slash. Nested
// identifiers, such as "Publish.func1", use the SDK's "/" separator.
export function devkitName(kind: "workflow" | "step", name: string): string {
	if (name.includes("//")) {
		return name;
	}
	const dot = name.indexOf(".", name.lastIndexOf("/") + 1);
	if (dot <= 0 || dot === name.length - 1) {
		return name;
	}
	return `${kind}//${name.slice(0, dot)}//${name.slice(dot + 1).replaceAll(".", "/")}`;
}

// goName reverses devkitName. Names whose module is a relative path or carries
// a version came from JavaScript and are kept as they are.
function goName(kind: "workflow" | "step", name: string): string {
	const [prefix, module, ...identifier] = name.split("//");
	if (prefix !== kind || !module || identifier.length === 0 || module.startsWith(".") || module.includes("@")) {
		return name;
	}
	return `${module}.${identifier.join("//").replaceAll("/", ".")}`;
}

function toWire(value: unknown, preserveKeys = false): unknown {
	if (value == null || value instanceof Date || value instanceof Uint8Array || value instanceof ArrayBuffer) {
		return value;
	}
	if (Array.isArray(value)) {
		return value.map((item) => toWire(item, preserveKeys));
	}
	if (typeof value !== "object") {
		return value;
	}
	const result: Record<string, unknown> = {};
	for (const [key, item] of Object.entries(value)) {
		const nextKey = preserveKeys ? key : goFieldName(key);
		const kind = preserveKeys ? undefined : nameKinds[key];
		result[nextKey] = kind && typeof item === "string" ? goName(kind, item) : toWire(item, preserveKeys || opaqueMapKeys.has(key));
	}
	return result;
}

function fromWire(value: unknown, preserveKeys = false): unknown {
	if (value == null || value instanceof Date || value instanceof Uint8Array || value instanceof ArrayBuffer) {
		return value;
	}
	if (Array.isArray(value)) {
		return value.map((item) => fromWire(item, preserveKeys));
	}
	if (typeof value !== "object") {
		return value;
	}
	const result: Record<string, unknown> = {};
	for (const [key, item] of Object.entries(value)) {
		const nextKey = preserveKeys ? key : javascriptFieldName(key);
		const kind = preserveKeys ? undefined : nameKinds[nextKey];
		result[nextKey] = kind && typeof item === "string" ? devkitName(kind, item) : fromWire(item, preserveKeys || opaqueMapKeys.has(nextKey));
	}
	return result;
}

function goFieldName(key: string): string {
	const words = key.match(/[A-Z]+(?=[A-Z][a-z]|$)|[A-Z]?[a-z]+|\d+/g);
	if (!words) {
		return key;
	}
	return words
		.map((word) => {
			const lower = word.toLowerCase();
			if (lower === "id" || lower === "url") {
				return lower.toUpperCase();
			}
			return lower[0].toUpperCase() + lower.slice(1);
		})
		.join("");
}

function javascriptFieldName(key: string): string {
	const words = key.match(/[A-Z]+(?=[A-Z][a-z]|$)|[A-Z]?[a-z]+|\d+/g);
	if (!words) {
		return key;
	}
	return words
		.map((word, index) => {
			const lower = word.toLowerCase();
			return index === 0 ? lower : lower[0].toUpperCase() + lower.slice(1);
		})
		.join("");
}
