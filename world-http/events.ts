import type { CreateEventParams, Event, EventResult, PaginatedResponse } from "@workflow/world";
import { START_HOOK_ADMISSION_REJECTED_CODE } from "@workflow/world";
import { toHook, toRun, toStep, toWait } from "./entities.js";
import { HTTPWorldError, devkitName, goName, type RPC } from "./rpc.js";

// RunCreation is what creates a run: the data of a run_created event, and the
// runInput of the run's first queue message.
export type RunCreation = {
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
export function runCreation(runId: string, data: RunCreation): Record<string, unknown> {
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

// createOptions keeps the create parameters the Go world understands. Its
// in-band fence is the writer's expected in-band count.
export function createOptions(options: CreateEventParams | undefined): Record<string, unknown> | undefined {
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
export function toEventResult(result: RPC.EventResult): EventResult {
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
export function jsonEncoded(value: unknown): Uint8Array {
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

// storedEventData returns event data as the Go World stores it: with its
// workflow and step names in Go form, like the names beside it in the log.
export function storedEventData(value: unknown): unknown {
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

export function withEventDataPage(page: PaginatedResponse<RPC.Event>): PaginatedResponse<Event> {
	return { ...page, data: page.data.map(withEventData) };
}

// payloadFields are the eventData fields that hold payloads, in the Workflow
// SDK's serialized form. The Go World stores event data as JSON, with
// payload bytes as base64 strings.
const payloadFields = ["input", "output", "result", "error", "payload", "metadata"];

// withEventData exposes an event's stored data as the eventData object the
// Workflow SDK reads: its payloads as bytes, its times as Dates, and Go
// workflow and step names in SDK form. An event about the run itself has no
// correlation ID.
export function withEventData(event: RPC.Event): Event {
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

// dateFields are the eventData fields that hold times, which the Go World
// stores as RFC 3339 strings.
const dateFields = ["resumeAt", "retryAfter", "tokenRetentionUntil"];

// reviveDates replaces the times among fields with Dates.
function reviveDates(fields: Record<string, unknown>): void {
	for (const field of dateFields) {
		const value = fields[field];
		if (typeof value === "string" && !Number.isNaN(Date.parse(value))) {
			fields[field] = new Date(value);
		}
	}
}

// The code of the Workflow SDK's WorkflowWorldError for a start hook
// candidate answered while admission is switched off.
const START_HOOK_ADMISSION_DISABLED_CODE = "START_HOOK_ADMISSION_DISABLED";

// startHookConflict returns a refused start hook as the Workflow SDK's
// HookConflictError, which start() recognizes by its name, naming the run that
// holds the token, and a rejected candidate as its WorkflowWorldError with
// code START_HOOK_ADMISSION_REJECTED, and a candidate answered while admission
// is switched off as its WorkflowWorldError with code
// START_HOOK_ADMISSION_DISABLED, which start() does not take as the
// candidate's outcome. Other errors are returned as they are.
export function startHookConflict(error: unknown, token: string): unknown {
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
