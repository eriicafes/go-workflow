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
		rpcVersion: number;
		capabilities: Capabilities;
	};
	/** The optional behavior the RPC server's World implements. */
	export type Capabilities = {
		analytics: boolean;
		setAttributes: boolean;
		deploymentAffinity?: boolean;
		deleteData?: boolean;
		startHook?: boolean;
		hookResumeDedup?: boolean;
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
		errorCode: string;
		attributes: Record<string, string>;
		deploymentId: string;
		encryptionPublicKey?: string;
		inBandCount: number;
		specVersion?: number;
		createdAt: Date;
		updatedAt: Date;
		startedAt: Date | null;
		completedAt: Date | null;
		expiredAt?: Date | null;
		executionContext?: Record<string, unknown> | null;
		/** The run's start hook, with its hookId and without its metadata. */
		startHook?: { token: string; hookId: string; tokenRetentionUntil?: Date } | null;
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
		specVersion: number;
	};
	export type Event = {
		runId: string;
		eventId: string;
		eventType: EventType;
		correlationId: string;
		data: Uint8Array;
		createdAt: Date;
		occurredAt: Date | null;
		specVersion: number;
		resumeId: string;
	};
	export type Wait = {
		waitId: string;
		runId: string;
		status: "waiting" | "completed";
		resumeAt: Date | null;
		completedAt: Date | null;
		createdAt: Date;
		updatedAt: Date;
		specVersion: number;
	};
	export type EventResult = {
		event?: Event;
		run?: Run;
		step?: Step;
		hook?: Hook;
		wait?: Wait;
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
		wait?: Wait;
	};
	export type EventBatchResult = {
		results: BatchEventResult[];
		inBandCount: number;
		allocated: number;
		events?: Event[];
		hasMore?: boolean;
	};
	/** A run's position counters, read before an events.list page. */
	export type EventsSnapshot = { eventCount: number; inBandCount: number };
	export type EventsPage = Page<Event> & { snapshot: EventsSnapshot };
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
		resumeContext: { deploymentId: string; workflowName: string; encryptionPublicKey?: string; runSpecVersion: number };
		specVersion: number;
		hookId: string;
		token: string;
		metadata: Uint8Array | null;
		isWebhook: boolean;
		isSystem?: boolean;
		createdAt: Date;
		tokenRetentionUntil: Date | null;
		claimedFrom: { runId: string; hookId: string; workflowName?: string; deploymentId?: string; runSpecVersion?: number } | null;
		/** Whether the World deduplicates resumes of the hook, on a read by token. */
		resumeDedup?: boolean;
	};
	export type QueueResult = { messageId: string };
	/** One item's outcome in a queue.queueBatch answer. */
	export type QueueBatchResult = { messageId: string; error?: string; retryable?: boolean };
	export type StreamInfo = { tailIndex: number; done: boolean };
	export type Empty = Record<string, never>;
	export type Error = {
		code: string;
		message: string;
		sentinel?: string;
		conflictingRunId?: string;
		/** Nanoseconds a throttled failure asks the caller to wait. */
		retryAfter?: number;
		/** The fields of the Go error type the sentinel names. */
		details?: unknown;
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
		// startIndex is the chunk to start at when cursor is empty; a negative
		// value counts back from the end of the stream.
		startIndex?: number;
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
		data: { index: number; data: Uint8Array }[] | null;
		cursor: string;
		hasMore: boolean;
		done: boolean;
		retryAfter?: number;
	};
}

// sdkErrorNames names the Go sentinels whose Workflow SDK error class the
// runtime checks for, by that class's name.
const sdkErrorNames: Record<string, string> = {
	throttled: "ThrottleError",
	too_early: "TooEarlyError",
	attribute_invalid: "AttributeValidationError",
	run_not_supported: "RunNotSupportedError",
	entity_conflict: "EntityConflictError",
	run_already_exists: "EntityConflictError",
	run_expired: "RunExpiredError",
	stream_expired: "StreamExpiredError",
	run_not_found: "WorkflowRunNotFoundError",
	hook_not_found: "HookNotFoundError",
	hook_conflict: "HookConflictError",
	hook_force_claimed: "HookForceClaimedError",
	in_band_superseded: "InBandSupersededError",
};

export class HTTPWorldError extends Error {
	readonly code: string;
	status?: number;
	/** The Go sentinel error the server reported, such as run_not_found. */
	readonly sentinel?: string;
	/** The run holding a hook token, for a hook_conflict failure. */
	readonly conflictingRunId?: string;
	/** Seconds a throttled failure asks the caller to wait, when it says. */
	readonly retryAfter?: number;
	/** The hook token of a hook_conflict, hook_force_claimed or hook_not_found failure. */
	readonly token?: string;
	/** The run and hook that took the token, for a hook_force_claimed failure. */
	readonly claimedByRunId?: string;
	readonly claimedByHookId?: string;
	/** The run's position counters, for an in_band_superseded failure. */
	readonly seq?: number;
	readonly seqInBand?: number;
	/** The run, its status and when its data expired, for a run_expired failure. */
	readonly runId?: string;
	readonly runStatus?: string;
	readonly expiredAt?: Date;
	/** The stream of a stream_expired failure, which also has runId and expiredAt. */
	readonly streamId?: string;
	/** The run's and the World's spec versions, for a run_not_supported failure. */
	readonly runSpecVersion?: number;
	readonly worldSpecVersion?: number;

	constructor(code: string, message: string, status?: number, sentinel?: string, conflictingRunId?: string, retryAfter?: number, details?: Record<string, unknown>) {
		super(message);
		// A failure the Workflow SDK's runtime acts on is named as its error
		// class, which callers recognize by name (EntityConflictError.is,
		// ThrottleError.is reading retryAfter, and so on). Any other failure
		// of the World is its WorkflowWorldError, which callers read the
		// status of.
		this.name = (sentinel && sdkErrorNames[sentinel]) || (sentinel || status !== undefined ? "WorkflowWorldError" : "HTTPWorldError");
		this.code = code;
		this.status = status;
		this.sentinel = sentinel;
		this.conflictingRunId = conflictingRunId;
		this.retryAfter = retryAfter;
		// The fields the SDK reads off the error classes above.
		const text = (key: string) => (typeof details?.[key] === "string" && details[key] ? (details[key] as string) : undefined);
		const count = (key: string) => (typeof details?.[key] === "number" ? (details[key] as number) : undefined);
		// Go sends a time it does not have as its zero time, year 1.
		const time = (key: string) => (details?.[key] instanceof Date && (details[key] as Date).getUTCFullYear() > 1 ? (details[key] as Date) : undefined);
		switch (sentinel) {
			case "hook_not_found":
				this.token = text("token");
				break;
			case "run_expired":
				this.runId = text("runId");
				this.runStatus = text("runStatus");
				this.expiredAt = time("expiredAt");
				break;
			case "stream_expired":
				this.runId = text("runId");
				this.streamId = text("streamId");
				this.expiredAt = time("expiredAt");
				break;
			case "run_not_supported":
				this.runSpecVersion = count("runSpecVersion");
				this.worldSpecVersion = count("worldSpecVersion");
				break;
			case "hook_conflict":
				this.token = text("token");
				this.conflictingRunId ??= text("runId");
				break;
			case "hook_force_claimed":
				this.token = text("token");
				this.claimedByRunId = text("runId");
				this.claimedByHookId = text("hookId");
				break;
			case "in_band_superseded":
				this.status = 412;
				this.seq = count("eventCount");
				this.seqInBand = count("inBandCount");
				break;
		}
	}
}

// rpcErrorStatus is the HTTP status an RPC error code stands for. The server
// reports every RPC error with status 200 inside the envelope.
const rpcErrorStatus: Record<string, number> = {
	not_found: 404,
	invalid_argument: 400,
	conflict: 409,
	unimplemented: 501,
	resource_exhausted: 429,
	too_early: 425,
	cancelled: 499,
	deadline_exceeded: 504,
};

export class RPCClient {
	readonly baseURL: string;
	readonly fetch: typeof globalThis.fetch;
	readonly headers: HeadersInit;
	rpcVersion?: number;

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
			...(this.rpcVersion === undefined ? {} : { rpcVersion: this.rpcVersion }),
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
			const { code, message, sentinel, conflictingRunId, retryAfter, details } = envelope.error;
			const seconds = retryAfter ? Math.ceil(Number(retryAfter) / 1e9) : undefined;
			const fields = details && typeof details === "object" && !(details instanceof Uint8Array) ? (fromWire(details) as Record<string, unknown>) : undefined;
			throw new HTTPWorldError(code, message, rpcErrorStatus[code] ?? 500, sentinel, conflictingRunId, seconds, fields);
		}
		if (envelope.output === undefined) {
			return undefined as T;
		}
		return fromWire(envelope.output) as T;
	}
}

const opaqueMapKeys = new Set(["attributes", "header", "executionContext"]);

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
export function goName(kind: "workflow" | "step", name: string): string {
	const [prefix, module, ...identifier] = name.split("//");
	if (prefix !== kind || !module || identifier.length === 0 || module.startsWith(".") || module.includes("@")) {
		return name;
	}
	return `${module}.${identifier.join("//").replaceAll("/", ".")}`;
}

function toWire(value: unknown, preserveKeys = false): unknown {
	// cbor-x writes an integer of 2^32 or more as a float, which Go does not
	// decode into an integer such as a duration's nanoseconds, so it is sent
	// as a 64-bit integer.
	if (typeof value === "number" && Number.isInteger(value) && Math.abs(value) >= 2 ** 32) {
		return BigInt(value);
	}
	// cbor-x writes a Date as float seconds, which loses milliseconds that a
	// float cannot hold, so a Date is sent as its exact ISO 8601 text.
	if (value instanceof Date) {
		return value.toISOString();
	}
	if (value == null || value instanceof Uint8Array || value instanceof ArrayBuffer) {
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
	// cbor-x reads a 64-bit integer as a bigint. One a number holds exactly,
	// such as a duration's nanoseconds, is returned as a number.
	if (typeof value === "bigint") {
		return Number.isSafeInteger(Number(value)) ? Number(value) : value;
	}
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
