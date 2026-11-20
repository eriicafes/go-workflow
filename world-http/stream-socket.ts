import { decode, encode } from "cbor-x";
import WebSocket from "ws";
import { HTTPWorldError, type RPCClient, type RPC } from "./rpc.js";

// Stream writes travel over a WebSocket held for a writer's lifetime, in the
// Workflow SDK's v1 stream socket protocol, which the Go server's
// HTTPHandler serves at <prefix>/websockets/v1/runs/{runId}/streams/{name}.
// Every message is a frame: u32 meta length, CBOR meta, u32 body length, body.
// Writes go through RPC while the socket opens, when the server declines it,
// and after it fails or drains.

/** How long a session waits for its socket to open before giving up on it. */
const CONNECT_TIMEOUT_MS = 10_000;
/** The most chunks, and chunk bytes, one write message carries. */
const MAX_CHUNKS_PER_WRITE = 1_000;
const MAX_MESSAGE_BYTES = 1 << 20;

type Reply = {
	type: "write_ack" | "close_ack" | "error" | "drain";
	reqId?: number;
	status?: number;
	message?: string;
	retryAfter?: string;
};

type Pending = { resolve: (reply: Reply) => void; reject: (error: Error) => void };

/** The socket failed before the server answered a request. */
class SocketFailed extends Error {}

function encodeFrame(meta: object, body: Uint8Array): Uint8Array {
	const encoded = encode(meta);
	const frame = new Uint8Array(4 + encoded.byteLength + 4 + body.byteLength);
	const view = new DataView(frame.buffer);
	view.setUint32(0, encoded.byteLength);
	frame.set(encoded, 4);
	view.setUint32(4 + encoded.byteLength, body.byteLength);
	frame.set(body, 8 + encoded.byteLength);
	return frame;
}

function decodeReply(data: Uint8Array): Reply {
	const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
	const metaEnd = 4 + view.getUint32(0);
	return decode(data.subarray(4, metaEnd)) as Reply;
}

function encodeChunks(chunks: Uint8Array[]): Uint8Array {
	const size = chunks.reduce((total, chunk) => total + 4 + chunk.byteLength, 0);
	const body = new Uint8Array(size);
	const view = new DataView(body.buffer);
	let offset = 0;
	for (const chunk of chunks) {
		view.setUint32(offset, chunk.byteLength);
		body.set(chunk, offset + 4);
		offset += 4 + chunk.byteLength;
	}
	return body;
}

function bytes(value: RPC.StreamChunk): Uint8Array {
	return typeof value === "string" ? new TextEncoder().encode(value) : value;
}

/** Returns the socket URL of a writer of the stream name of runId. */
export function streamSocketURL(baseURL: string, runId: string, name: string, writerId: string): string {
	const url = new URL(baseURL.replace(/\/+$/, ""));
	url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
	url.pathname = `${url.pathname.replace(/\/+$/, "")}/websockets/v1/runs/${encodeURIComponent(runId)}/streams/${encodeURIComponent(name)}`;
	url.search = new URLSearchParams({ writerId }).toString();
	return url.toString();
}

/**
 * A Workflow SDK stream write session over the Go server's stream socket,
 * falling back to RPC writes.
 */
export class HTTPStreamWriteSession {
	readonly client: RPCClient;
	readonly input: RPC.StreamWriteSessionInput;
	private socket?: WebSocket;
	private ready: Promise<WebSocket | undefined>;
	private pending = new Map<number, Pending>();
	private reqId = 0;
	private draining = false;
	private done = false;

	constructor(client: RPCClient, input: RPC.StreamWriteSessionInput) {
		this.client = client;
		this.input = input;
		this.ready = this.connect();
	}

	private connect(): Promise<WebSocket | undefined> {
		return new Promise((resolve) => {
			let url: string;
			try {
				url = streamSocketURL(this.client.baseURL, this.input.runId, this.input.name, this.input.writerId);
			} catch {
				resolve(undefined);
				return;
			}
			const headers: Record<string, string> = {};
			new Headers(this.client.headers).forEach((value, key) => {
				headers[key] = value;
			});
			const socket = new WebSocket(url, { headers, handshakeTimeout: CONNECT_TIMEOUT_MS });
			socket.binaryType = "nodebuffer";
			let opened = false;
			const fail = () => {
				if (!opened) resolve(undefined);
			};
			socket.once("open", () => {
				opened = true;
				this.socket = socket;
				resolve(socket);
			});
			socket.once("unexpected-response", (_request, response) => {
				response.resume();
				socket.terminate();
				fail();
			});
			socket.once("error", fail);
			socket.on("message", (data: Buffer) => this.receive(new Uint8Array(data)));
			socket.on("close", () => {
				fail();
				this.socket = undefined;
				for (const pending of this.pending.values()) {
					pending.reject(new SocketFailed("workflow: stream socket closed"));
				}
				this.pending.clear();
			});
		});
	}

	private receive(data: Uint8Array): void {
		let reply: Reply;
		try {
			reply = decodeReply(data);
		} catch {
			return;
		}
		if (reply.type === "drain") {
			this.draining = true;
			return;
		}
		if (reply.reqId === undefined) {
			this.socket?.terminate();
			return;
		}
		const pending = this.pending.get(reply.reqId);
		if (pending) {
			this.pending.delete(reply.reqId);
			pending.resolve(reply);
		}
	}

	/** The open socket, or undefined while it opens or after it was given up. */
	private async currentSocket(): Promise<WebSocket | undefined> {
		const opened = await Promise.race([this.ready, Promise.resolve(undefined)]);
		return opened && this.socket === opened ? opened : undefined;
	}

	private request(socket: WebSocket, meta: object, body: Uint8Array): Promise<Reply> {
		const reqId = ++this.reqId;
		return new Promise<Reply>((resolve, reject) => {
			this.pending.set(reqId, { resolve, reject });
			socket.send(encodeFrame({ ...meta, reqId }, body), (error) => {
				if (error) {
					this.pending.delete(reqId);
					reject(new SocketFailed(error.message));
				}
			});
		});
	}

	private async send(socket: WebSocket, meta: object, body: Uint8Array, ack: Reply["type"]): Promise<void> {
		for (;;) {
			const reply = await this.request(socket, meta, body);
			if (reply.type === ack) return;
			if (reply.status === 429) {
				const seconds = Number(reply.retryAfter) || 0.1;
				await new Promise((resolve) => setTimeout(resolve, seconds * 1_000));
				continue;
			}
			const code = reply.status === 409 || reply.status === 410 ? "stream_closed" : "stream_socket_error";
			throw new HTTPWorldError(code, reply.message ?? `workflow: stream socket refused with ${reply.status}`, reply.status);
		}
	}

	private giveUp(): void {
		this.socket?.close(1000);
		this.socket = undefined;
	}

	async write(chunkSeq: number, chunks: RPC.StreamChunk[]): Promise<void> {
		if (this.done) {
			throw new HTTPWorldError("stream_session_closed", "workflow: stream write session is closed");
		}
		const binary = chunks.map(bytes);
		for (let start = 0; start < binary.length; ) {
			let end = start + 1;
			let size = binary[start].byteLength;
			while (end < binary.length && end - start < MAX_CHUNKS_PER_WRITE && size + binary[end].byteLength <= MAX_MESSAGE_BYTES) {
				size += binary[end].byteLength;
				end++;
			}
			await this.writeBatch(chunkSeq + start, binary.slice(start, end));
			start = end;
		}
	}

	private async writeBatch(chunkSeq: number, chunks: Uint8Array[]): Promise<void> {
		const socket = await this.currentSocket();
		if (socket) {
			try {
				await this.send(socket, { type: "write", chunkSeq, numChunks: chunks.length }, encodeChunks(chunks), "write_ack");
				if (this.draining) this.giveUp();
				return;
			} catch (error) {
				if (!(error instanceof SocketFailed)) throw error;
				// The write may have landed; storing it again keeps the chunks,
				// at the cost of a possible duplicate.
				this.giveUp();
			}
		}
		await this.client.call<RPC.Empty>("streams.writeMulti", { runId: this.input.runId, name: this.input.name, chunks });
	}

	async close(): Promise<void> {
		if (this.done) return;
		const socket = await this.currentSocket();
		if (socket) {
			try {
				await this.send(socket, { type: "close" }, new Uint8Array(), "close_ack");
				this.done = true;
				this.giveUp();
				return;
			} catch (error) {
				if (!(error instanceof SocketFailed)) throw error;
				this.giveUp();
			}
		}
		await this.client.call<RPC.Empty>("streams.close", { runId: this.input.runId, name: this.input.name });
		this.done = true;
	}

	async release(): Promise<void> {
		this.dispose();
	}

	dispose(): void {
		this.done = true;
		void this.ready.then((socket) => socket?.close(1000));
		this.socket = undefined;
	}
}
