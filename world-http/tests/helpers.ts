// Shared helpers of world-http's tests.
import { execFileSync, spawn } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";
import { encode } from "cbor-x";

export const rpcContentType = "application/cbor";

// rpcResponse returns an RPC response whose output is output.
export function rpcResponse(output: unknown): Response {
	const encoded = encode({ output });
	const body = new Uint8Array(encoded.byteLength);
	body.set(encoded);
	return new Response(body, {
		headers: { "Content-Type": rpcContentType },
	});
}

// startServer builds and starts the Go RPC server in testdata/server with
// env, and returns the line of JSON it prints once it listens and a function
// that stops it.
export async function startServer<T>(env: Record<string, string>): Promise<{ started: T; stop: () => void }> {
	const root = fileURLToPath(new URL("../..", import.meta.url));
	const directory = mkdtempSync(join(tmpdir(), "world-http-"));
	const binary = join(directory, "server");
	execFileSync("go", ["build", "-o", binary, "./world-http/testdata/server"], { cwd: root, stdio: "inherit" });
	const server = spawn(binary, { env: { ...process.env, ...env }, stdio: ["ignore", "pipe", "inherit"] });
	const line = await new Promise<string>((resolve, reject) => {
		createInterface({ input: server.stdout! }).once("line", resolve);
		server.once("exit", (code) => reject(new Error(`server exited with ${code}`)));
		server.once("error", reject);
	});
	return {
		started: JSON.parse(line) as T,
		stop: () => {
			server.kill();
			rmSync(directory, { recursive: true, force: true });
		},
	};
}

export function text(value: string): Uint8Array {
	return new TextEncoder().encode(value);
}

// ulid returns a new ULID, as the SDK mints a run ID from.
export function ulid(): string {
	const alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";
	let time = Date.now();
	let value = "";
	for (let index = 0; index < 10; index++) {
		value = alphabet[time % 32] + value;
		time = Math.floor(time / 32);
	}
	for (let index = 0; index < 16; index++) {
		value += alphabet[Math.floor(Math.random() * 32)];
	}
	return value;
}

// eventually returns read's result once it stops failing.
export async function eventually<T>(read: () => Promise<T>): Promise<T> {
	for (let attempt = 0; ; attempt++) {
		try {
			return await read();
		} catch (error) {
			if (attempt >= 100) throw error;
			await new Promise((resolve) => setTimeout(resolve, 50));
		}
	}
}
