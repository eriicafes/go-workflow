import { SPEC_VERSION_SINGLE_ORCHESTRATOR } from "@workflow/world/spec-version";
import type { PaginatedResponse } from "@workflow/world";
import type { RPC } from "./rpc.js";

// GO_SPEC_VERSION is the spec version a Go World records on every run it
// creates, whatever version the caller asked for: one orchestrator per run,
// whose in-band writes the World fences.
export const GO_SPEC_VERSION = SPEC_VERSION_SINGLE_ORCHESTRATOR;

// environment returns the environment variable name, or undefined outside
// Node.js.
export function environment(name: string): string | undefined {
	return typeof process === "undefined" ? undefined : process.env?.[name];
}

// bytes returns a stream chunk as bytes, with a string encoded as UTF-8.
export function bytes(value: RPC.StreamChunk): Uint8Array {
	return typeof value === "string" ? new TextEncoder().encode(value) : value;
}

// durationNanoseconds converts milliseconds to the nanoseconds of a Go
// time.Duration. Undefined is zero.
export function durationNanoseconds(milliseconds: number | undefined): number {
	if (milliseconds == null) return 0;
	if (!Number.isFinite(milliseconds) || milliseconds < 0) {
		throw new RangeError("world-http: duration must be a non-negative finite millisecond value");
	}
	return Math.round(milliseconds * 1_000_000);
}

// durationMilliseconds converts the nanoseconds of a Go time.Duration to
// milliseconds. Undefined is zero.
export function durationMilliseconds(nanoseconds: number | undefined): number {
	if (nanoseconds == null) {
		return 0;
	}
	return Number(nanoseconds) / 1_000_000;
}

// page returns a page of a list with no items as empty data, which a Go World
// sends as null, the encoding of a nil slice.
export function page<T>(value: PaginatedResponse<T>): PaginatedResponse<T> {
	return { ...value, data: value.data ?? [] };
}
