import { decode } from "cbor-x";
import { expect, test } from "vitest";
import { createWorld } from "./index.js";
import { rpcResponse } from "./testdata/helpers.js";

test("pulls stream chunks through bounded RPC polling", async () => {
	const calls: { method: unknown; input: unknown }[] = [];
	const world = await createWorld({
		baseURL: "https://workflow.example/v1/workflow",
		fetch: async (_url, init) => {
			const request = decode(init?.body as Uint8Array) as { method: unknown; input: unknown };
			calls.push({ method: request.method, input: request.input });
			if (request.method === "client.spec") {
				return rpcResponse({ RPCVersion: 1, Capabilities: { Analytics: true, SetAttributes: false } });
			}
			expect(request.method).toBe("streams.getChunks");
			expect(calls.at(-1)?.input).toMatchObject({ Wait: 100_000_000 });
			return rpcResponse({
				Data: [{ Index: 0, Data: new Uint8Array([1, 2, 3]) }],
				Cursor: "1",
				HasMore: false,
				Done: true,
				RetryAfter: 50_000_000,
			});
		},
	});

	const reader = (await world.streams.get("wrun_01", "strm_progress")).getReader();
	const first = await reader.read();
	const last = await reader.read();
	expect(first.value).toEqual(new Uint8Array([1, 2, 3]));
	expect(last.done).toBe(true);
	expect(calls.filter((call) => call.method === "streams.getChunks")).toHaveLength(1);
});
test("reads a stream from a chunk index, then follows the cursor", async () => {
	const inputs: Record<string, unknown>[] = [];
	const world = await createWorld({
		baseURL: "https://workflow.example/v1/workflow",
		fetch: async (_url, init) => {
			const envelope = decode(init?.body as Uint8Array) as Record<string, unknown>;
			if (envelope.method === "client.spec") {
				return rpcResponse({ RPCVersion: 1, Capabilities: { Analytics: false, SetAttributes: false } });
			}
			const input = envelope.input as Record<string, unknown>;
			inputs.push(input);
			return inputs.length === 1
				? rpcResponse({ Data: [{ Index: 3, Data: new Uint8Array([3]) }], Cursor: "4", HasMore: false, Done: false })
				: rpcResponse({ Data: [{ Index: 4, Data: new Uint8Array([4]) }], Cursor: "5", HasMore: false, Done: true });
		},
	});

	const reader = (await world.streams.get("wrun_01", "strm_progress", -2)).getReader();
	expect((await reader.read()).value).toEqual(new Uint8Array([3]));
	expect((await reader.read()).value).toEqual(new Uint8Array([4]));
	expect((await reader.read()).done).toBe(true);
	expect(inputs[0]).toMatchObject({ StartIndex: -2, Cursor: "" });
	expect(inputs[1]).toMatchObject({ Cursor: "4" });
});
test("waits through empty stream pages, and ends getChunks pagination with a null cursor", async () => {
	const pages = [
		{ Data: null, Cursor: "0", HasMore: false, Done: false },
		{ Data: null, Cursor: "0", HasMore: false, Done: false },
		{ Data: [{ Index: 0, Data: new Uint8Array([7]) }], Cursor: "1", HasMore: false, Done: true },
		{ Data: null, Cursor: "0", HasMore: false, Done: false },
		{ Data: [{ Index: 0, Data: new Uint8Array([7]) }], Cursor: "1", HasMore: true, Done: true },
		{ Data: [{ Index: 1, Data: new Uint8Array([8]) }], Cursor: "2", HasMore: false, Done: true },
	];
	const world = await createWorld({
		baseURL: "https://workflow.example/v1/workflow",
		fetch: async (_url, init) => {
			const envelope = decode(init?.body as Uint8Array) as Record<string, unknown>;
			if (envelope.method === "client.spec") {
				return rpcResponse({ RPCVersion: 1, Capabilities: { Analytics: false, SetAttributes: false } });
			}
			return rpcResponse(pages.shift());
		},
	});

	const reader = (await world.streams.get("wrun_01", "strm_progress")).getReader();
	expect((await reader.read()).value).toEqual(new Uint8Array([7]));
	expect((await reader.read()).done).toBe(true);

	expect(await world.streams.getChunks("wrun_01", "strm_progress")).toEqual({ data: [], cursor: null, hasMore: false, done: false });
	expect(await world.streams.getChunks("wrun_01", "strm_progress", { limit: 1 })).toMatchObject({ cursor: "1", hasMore: true });
	expect(await world.streams.getChunks("wrun_01", "strm_progress", { cursor: "1" })).toMatchObject({ cursor: null, hasMore: false, done: true });
});
