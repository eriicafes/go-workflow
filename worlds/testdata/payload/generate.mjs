// Writes vectors.json: payloads the Workflow SDK encodes, which the worlds
// package's tests decode. Run with `npm install && npm run generate`.
import { writeFileSync } from "node:fs";
import { FatalError, RetryableError } from "@workflow/errors";
import {
	compress,
	dehydrateRunError,
	dehydrateStepArguments,
	dehydrateStepReturnValue,
	dehydrateWorkflowArguments,
	getExternalReducers,
	getSerializeStream,
} from "@workflow/core/serialization";
import { deriveRunPayloadKeys, encryptEnvelope, sealTo } from "@workflow/core/serialization-format";
import { deriveRunKey } from "@workflow/world-vercel";

const b64 = (bytes) => Buffer.from(bytes).toString("base64");
const runId = "wrun_01TEST";
const projectId = "prj_test";
const deploymentKey = Uint8Array.from({ length: 32 }, (_, i) => i);
const runKey = await deriveRunKey(deploymentKey, projectId, runId);
const keys = await deriveRunPayloadKeys(runKey);

const value = (v) => dehydrateStepReturnValue(v, runId, undefined, [], globalThis, false, false, false);

// Stacks are fixed so the vectors don't hold local paths.
const withStack = (error) => Object.assign(error, { stack: `${error.name}: ${error.message}` });
const cause = withStack(new Error("inner"));
const outer = withStack(new Error("outer", { cause }));
const values = {
	object: { s: "hi", n: 1.5, i: 42, b: true, z: null, arr: [1, "a"], nested: { x: 1 } },
	bytes: new Uint8Array([1, 2, 3]),
	emptyBytes: new Uint8Array(),
	date: new Date("2024-01-02T03:04:05.678Z"),
	bigint: 12345678901234567890n,
	map: new Map([
		[1, "a"],
		[2, "b"],
	]),
	url: new URL("https://example.com/a?b=c"),
	headers: new Headers([
		["content-type", "text/plain"],
		["x-a", "1"],
	]),
	error: outer,
	fatal: withStack(new FatalError("fatal")),
	retryable: withStack(new RetryableError("later", { retryAfter: new Date("2030-01-01T00:00:00.000Z") })),
	specials: [NaN, -0, Infinity, -Infinity, 1],
	string: "top",
	repeated: ["a", "a", { k: "a" }],
	holes: [1, undefined, 3],
};

const out = {
	deploymentKey: b64(deploymentKey),
	projectId,
	runId,
	runKey: b64(runKey),
	publicKey: b64(keys.keyPair.publicKey),
	values: {},
	shapes: {},
	encrypted: {},
	compressed: {},
	stream: {},
};
for (const [name, v] of Object.entries(values)) {
	out.values[name] = b64(await value(v));
}
out.shapes.workflowArguments = b64(await dehydrateWorkflowArguments([{ n: 1 }], runId, undefined, [], globalThis));
out.shapes.stepArguments = b64(await dehydrateStepArguments({ args: [{ n: 1 }] }, runId, undefined, globalThis));
out.shapes.runError = b64(await dehydrateRunError(withStack(new Error("failed")), runId, undefined, globalThis));

const plain = await value({ secret: "x" });
out.encrypted.plain = b64(plain);
out.encrypted.encr = b64(await encryptEnvelope(plain, keys));
out.encrypted.encp = b64(await encryptEnvelope(plain, sealTo(keys.keyPair.publicKey)));

const large = await value({ text: "workflow ".repeat(400) });
out.compressed.plain = b64(large);
out.compressed.zstd = b64(await compress(large, "zstd"));
out.compressed.gzip = b64(await compress(large, "gzip"));

async function frame(chunk, key) {
	const stream = getSerializeStream(getExternalReducers(globalThis, [], runId), key);
	const writer = stream.writable.getWriter();
	const reader = stream.readable.getReader();
	const read = reader.read();
	await writer.write(chunk);
	return b64((await read).value);
}
out.stream.plain = await frame(new Uint8Array([104, 105]));
out.stream.encr = await frame(new Uint8Array([104, 105]), keys);
out.stream.encp = await frame(new Uint8Array([104, 105]), sealTo(keys.keyPair.publicKey));

writeFileSync(new URL("./vectors.json", import.meta.url), JSON.stringify(out, null, "\t") + "\n");
