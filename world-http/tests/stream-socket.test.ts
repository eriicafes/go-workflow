import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { decode, encode } from "cbor-x";
import { afterEach, expect, test } from "vitest";
import { WebSocketServer } from "ws";
import { RPCClient } from "../rpc.js";
import { HTTPStreamWriteSession, streamSocketThrottle, streamSocketURL } from "../stream-socket.js";

type Frame = { meta: Record<string, unknown>; chunks: string[] };

function decodeFrame(data: Uint8Array): Frame {
	const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
	const metaEnd = 4 + view.getUint32(0);
	const meta = decode(data.subarray(4, metaEnd)) as Record<string, unknown>;
	let offset = metaEnd + 4;
	const chunks: string[] = [];
	while (offset < data.byteLength) {
		const size = view.getUint32(offset);
		chunks.push(new TextDecoder().decode(data.subarray(offset + 4, offset + 4 + size)));
		offset += 4 + size;
	}
	return { meta, chunks };
}

function reply(meta: object): Uint8Array {
	const encoded = encode(meta);
	const frame = new Uint8Array(8 + encoded.byteLength);
	const view = new DataView(frame.buffer);
	view.setUint32(0, encoded.byteLength);
	frame.set(encoded, 4);
	return frame;
}

let servers: Server[] = [];

afterEach(async () => {
	await Promise.all(servers.map((server) => new Promise((resolve) => server.close(resolve))));
	servers = [];
});

// listen serves upgrades with onSocket, or declines them when it is unset,
// and records RPC calls.
async function listen(onSocket?: WebSocketServer): Promise<{ baseURL: string; calls: { method: string; input: Record<string, unknown> }[]; paths: string[] }> {
	const calls: { method: string; input: Record<string, unknown> }[] = [];
	const paths: string[] = [];
	const server = createServer(async (request, response) => {
		const body: Buffer[] = [];
		for await (const chunk of request) body.push(chunk as Buffer);
		const call = decode(Buffer.concat(body)) as { method: string; input: Record<string, unknown> };
		calls.push(call);
		response.setHeader("Content-Type", "application/cbor");
		response.end(encode({ output: {} }));
	});
	server.on("upgrade", (request, socket, head) => {
		paths.push(request.url ?? "");
		if (!onSocket || request.headers.authorization !== "Bearer secret") {
			socket.end("HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n");
			return;
		}
		onSocket.handleUpgrade(request, socket, head, (ws) => onSocket.emit("connection", ws, request));
	});
	servers.push(server);
	await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
	const { port } = server.address() as AddressInfo;
	return { baseURL: `http://127.0.0.1:${port}/v1/workflow`, calls, paths };
}

function rpcClient(baseURL: string): RPCClient {
	return new RPCClient({ baseURL, headers: { Authorization: "Bearer secret" } });
}

test("builds the stream socket URL", () => {
	expect(streamSocketURL("https://example.com/v1/workflow/", "wrun_1", "a b", "wrtr_1")).toBe(
		"wss://example.com/v1/workflow/websockets/v1/runs/wrun_1/streams/a%20b?writerId=wrtr_1",
	);
	expect(streamSocketURL("http://localhost:8080/v1/workflow", "wrun_1", "", "wrtr_1")).toBe(
		"ws://localhost:8080/v1/workflow/websockets/v1/runs/wrun_1/streams/?writerId=wrtr_1",
	);
});

test("writes and closes a stream over the socket", async () => {
	const frames: Frame[] = [];
	const wss = new WebSocketServer({ noServer: true });
	wss.on("connection", (ws) => {
		ws.on("message", (data: Buffer) => {
			const frame = decodeFrame(new Uint8Array(data));
			frames.push(frame);
			ws.send(reply({ type: frame.meta.type === "close" ? "close_ack" : "write_ack", reqId: frame.meta.reqId }));
		});
	});
	const { baseURL, calls, paths } = await listen(wss);
	const session = new HTTPStreamWriteSession(rpcClient(baseURL), { runId: "wrun_1", name: "out", writerId: "wrtr_1" });
	await new Promise((resolve) => setTimeout(resolve, 100));
	await session.write(0, ["a", "b"]);
	await session.write(2, [new TextEncoder().encode("c")]);
	await session.close();
	expect(paths).toEqual(["/v1/workflow/websockets/v1/runs/wrun_1/streams/out?writerId=wrtr_1"]);
	expect(calls).toEqual([]);
	expect(frames.map((frame) => [frame.meta.type, frame.meta.chunkSeq, frame.chunks])).toEqual([
		["write", 0, ["a", "b"]],
		["write", 2, ["c"]],
		["close", undefined, []],
	]);
	await expect(session.write(3, ["late"])).rejects.toThrow(/closed/);
	wss.close();
});

test("writes through RPC when the server declines the socket", async () => {
	const { baseURL, calls } = await listen();
	const session = new HTTPStreamWriteSession(rpcClient(baseURL), { runId: "wrun_1", name: "out", writerId: "wrtr_1" });
	await new Promise((resolve) => setTimeout(resolve, 100));
	await session.write(0, ["a"]);
	await session.close();
	expect(calls.map((call) => call.method)).toEqual(["streams.writeMulti", "streams.close"]);
});

test("reports a closed stream from the socket", async () => {
	const wss = new WebSocketServer({ noServer: true });
	wss.on("connection", (ws) => {
		ws.on("message", (data: Buffer) => {
			const frame = decodeFrame(new Uint8Array(data));
			ws.send(reply({ type: "error", reqId: frame.meta.reqId, status: 409, message: "workflow: stream is closed" }));
		});
	});
	const { baseURL } = await listen(wss);
	const session = new HTTPStreamWriteSession(rpcClient(baseURL), { runId: "wrun_1", name: "out", writerId: "wrtr_1" });
	await new Promise((resolve) => setTimeout(resolve, 100));
	await expect(session.write(0, ["a"])).rejects.toMatchObject({ code: "stream_closed" });
	session.dispose();
	wss.close();
});

test("waits out a rate limit on the socket and falls back to RPC past its budget", async () => {
	const throttle = { ...streamSocketThrottle };
	Object.assign(streamSocketThrottle, { minWaitMs: 1, budgetMs: 20 });
	try {
		const requests: string[] = [];
		let refuse = 2;
		const wss = new WebSocketServer({ noServer: true });
		wss.on("connection", (ws) => {
			ws.on("message", (data: Buffer) => {
				const frame = decodeFrame(new Uint8Array(data));
				requests.push(`${frame.meta.type} ${frame.meta.reqId}`);
				if (refuse > 0) {
					refuse--;
					ws.send(reply({ type: "error", reqId: frame.meta.reqId, status: 429, message: "busy", retryAfter: "0" }));
					return;
				}
				ws.send(reply({ type: "write_ack", reqId: frame.meta.reqId }));
			});
		});
		const { baseURL, calls } = await listen(wss);
		const session = new HTTPStreamWriteSession(rpcClient(baseURL), { runId: "wrun_1", name: "out", writerId: "wrtr_1" });
		await new Promise((resolve) => setTimeout(resolve, 100));
		// A refused write did not apply: it is sent again under a new
		// request ID, and never through RPC.
		await session.write(0, ["a"]);
		expect(requests).toEqual(["write 1", "write 2", "write 3"]);
		expect(calls).toEqual([]);

		// A server that keeps refusing is given up once the waits pass the
		// budget, and the write and what follows go through RPC.
		refuse = Number.POSITIVE_INFINITY;
		await session.write(1, ["b"]);
		const sent = requests.length;
		expect(sent).toBeGreaterThan(4);
		await session.write(2, ["c"]);
		await session.close();
		expect(requests.length).toBe(sent);
		expect(calls.map((call) => call.method)).toEqual(["streams.writeMulti", "streams.writeMulti", "streams.close"]);
		wss.close();
	} finally {
		Object.assign(streamSocketThrottle, throttle);
	}
});

test("closes through RPC a stream whose close the socket failed", async () => {
	const wss = new WebSocketServer({ noServer: true });
	wss.on("connection", (ws) => {
		ws.on("message", (data: Buffer) => {
			const frame = decodeFrame(new Uint8Array(data));
			ws.send(reply({ type: "error", reqId: frame.meta.reqId, status: 503, message: "reconciling" }));
		});
	});
	const { baseURL, calls } = await listen(wss);
	const session = new HTTPStreamWriteSession(rpcClient(baseURL), { runId: "wrun_1", name: "out", writerId: "wrtr_1" });
	await new Promise((resolve) => setTimeout(resolve, 100));
	await session.close();
	expect(calls.map((call) => call.method)).toEqual(["streams.close"]);
	wss.close();
});
