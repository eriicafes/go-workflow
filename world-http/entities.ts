import type { Hook, Step, Wait, WorkflowRun } from "@workflow/world";
import { HOOK_RESUME_DEDUP_VERSION } from "@workflow/world";
import type { RPC } from "./rpc.js";

// toRun exposes a run as the Workflow SDK reads one. It reports the spec
// version the World recorded on the run, which decides how the SDK wakes and
// cancels it. A Go World records one on every run it creates; a run the World
// has none for is left without one, which the SDK reads as a legacy run.
export function toRun(run: RPC.Run): WorkflowRun {
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
export function toWait(wait: RPC.Wait): Wait {
	const { resumeAt, completedAt, ...rest } = wait;
	return { ...rest, ...(resumeAt ? { resumeAt } : {}), ...(completedAt ? { completedAt } : {}) };
}

// toStep exposes a step as the Workflow SDK reads one, without the times it
// does not have.
export function toStep(step: RPC.Step): Step {
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
export function toHook(hook: RPC.Hook): Hook {
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

// payload returns stored payload bytes as the Workflow SDK reads them, or
// undefined when there are none.
function payload(data: Uint8Array | null | undefined): Uint8Array | undefined {
	return data && data.byteLength > 0 ? data : undefined;
}
