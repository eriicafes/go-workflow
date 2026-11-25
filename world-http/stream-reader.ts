import type { RPC, RPCClient } from "./rpc.js";
import { durationMilliseconds, durationNanoseconds } from "./utils.js";

// createReadableStream reads a stream's chunks as they arrive, polling the
// Go World, which holds each request until a chunk arrives or the poll
// interval passes.
export function createReadableStream(client: RPCClient, input: RPC.GetStreamInput): ReadableStream<Uint8Array> {
	let cursor = input.cursor ?? "";
	let cancelled = false;
	return new ReadableStream({
		cancel() {
			cancelled = true;
		},
		async pull(controller) {
			try {
				// A pull that enqueues nothing is not called again, so it reads
				// until it has a chunk or the stream is closed.
				while (!cancelled) {
					const result = await getStreamChunks(client, {
						...input,
						cursor,
						waitMs: input.pollIntervalMs ?? 100,
					});
					// Once a page returns a cursor, it replaces startIndex.
					if (result.cursor !== null) {
						cursor = result.cursor;
					}
					if (cancelled) {
						return;
					}
					for (const chunk of result.data) {
						controller.enqueue(chunk.data);
					}
					if (result.done && !result.hasMore) {
						controller.close();
						return;
					}
					if (result.data.length > 0) {
						return;
					}
				}
			} catch (error) {
				controller.error(error);
			}
		},
	});
}

// getStreamChunks returns a page of a stream's chunks, waiting up to waitMs
// for one when none is available yet.
export async function getStreamChunks(client: RPCClient, input: RPC.GetStreamChunksInput): Promise<RPC.StreamChunksResult> {
	const { waitMs, ...request } = input;
	const result = await client.call<RPC.WireStreamChunksResult>("streams.getChunks", {
		...request,
		wait: durationNanoseconds(waitMs),
	});
	return {
		// A Go World sends a page without chunks as null.
		data: result.data ?? [],
		cursor: result.cursor || null,
		hasMore: result.hasMore,
		done: result.done,
		retryAfterMs: durationMilliseconds(result.retryAfter),
	};
}
