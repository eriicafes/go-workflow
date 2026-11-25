import type { MessageId, World } from "@workflow/world";
import { runCreation, type RunCreation } from "./events.js";
import { GO_SPEC_VERSION, durationNanoseconds } from "./utils.js";

// queueHandler returns the route handler createQueueHandler returns, which
// accepts only deliveries that carry token, when it is set.
export function queueHandler(prefix: string, handler: Parameters<World["createQueueHandler"]>[1], token: string | undefined): (request: Request) => Promise<Response> {
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

// queueInput returns the Go World's delivery for a message the Workflow SDK
// queues. What the Go runtime has no use for, such as the trace carrier and
// the resume's timing, is left out.
export function queueInput(queueName: string, message: Parameters<World["queue"]>[1], options: Parameters<World["queue"]>[2]): Record<string, unknown> {
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

// sdkQueue reads a Workflow SDK queue name, "__wkf_workflow_<name>" or
// "__wkf_step_<name>", with an optional namespace as in
// "__<namespace>_wkf_workflow_<name>". It returns the workflow a workflow
// queue is for, and undefined for a name that is not the SDK's.
function sdkQueue(queueName: string): { workflowName?: string } | undefined {
	const match = /^__(?:[^_]+_)?wkf_(workflow|step)_(.+)$/.exec(queueName);
	return match ? (match[1] === "workflow" ? { workflowName: match[2] } : {}) : undefined;
}

// sdkMessageBytes returns a queued message and its queue name as JSON, with
// Uint8Array values as the Workflow SDK's local queue writes them.
function sdkMessageBytes(queueName: string, message: unknown): Uint8Array {
	return new TextEncoder().encode(JSON.stringify({ queueName, message }, typedJsonReplacer));
}

// typedJsonReplacer writes a Uint8Array as {"__type":"Uint8Array","data":
// "<base64>"}, and typedJsonReviver reads one back.
function typedJsonReplacer(_key: string, value: unknown): unknown {
	return value instanceof Uint8Array ? { __type: "Uint8Array", data: Buffer.from(value).toString("base64") } : value;
}

function typedJsonReviver(_key: string, value: unknown): unknown {
	if (value !== null && typeof value === "object" && (value as { __type?: unknown }).__type === "Uint8Array" && typeof (value as { data?: unknown }).data === "string") {
		return new Uint8Array(Buffer.from((value as { data: string }).data, "base64"));
	}
	return value;
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
