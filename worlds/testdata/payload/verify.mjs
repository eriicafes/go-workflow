// Decodes the payloads Go wrote with the Workflow SDK, as its tooling would.
// Run TestWriteGoPayloadVectors with WORKFLOW_PAYLOAD_VECTORS_OUT=go-vectors.json,
// then `npm run verify`.
import { readFileSync } from "node:fs";
import { deriveRunPayloadKeys, hydrateDataWithKey, observabilityRevivers } from "@workflow/core/serialization-format";
import { getCommonRevivers, getExternalRevivers } from "@workflow/core/serialization";
import { QueuePayloadSchema } from "@workflow/world";

const vectors = JSON.parse(readFileSync(new URL("./vectors.json", import.meta.url)));
const go = JSON.parse(readFileSync(new URL("./go-vectors.json", import.meta.url)));
const keys = await deriveRunPayloadKeys(Buffer.from(vectors.runKey, "base64"));
const revivers = { ...getCommonRevivers(globalThis), ...getExternalRevivers(globalThis, [], vectors.runId) };
let failed = false;
for (const mode of ["plain", "encr", "encp"]) {
	for (const [name, b64] of Object.entries(go[mode])) {
		try {
			const value = await hydrateDataWithKey(new Uint8Array(Buffer.from(b64, "base64")), revivers, keys);
			console.log(mode, name, value instanceof Error ? `${value.name}: ${value.message}` : typeof value === "string" ? value.slice(0, 20) : value);
		} catch (error) {
			failed = true;
			console.error("FAIL", mode, name, error);
		}
	}
}
for (const [mode, b64] of Object.entries(go.stream)) {
	const frame = new Uint8Array(Buffer.from(b64, "base64"));
	const length = new DataView(frame.buffer).getUint32(0, false);
	const value = await hydrateDataWithKey(frame.subarray(4, 4 + length), revivers, keys);
	console.log("stream", mode, value);
}
// A queue message parses, and keeps every field Go wrote, which a field the
// schema does not know would lose.
for (const [name, body] of Object.entries(go.queue)) {
	const message = JSON.parse(body);
	const parsed = QueuePayloadSchema.safeParse(message);
	const dropped = parsed.success ? Object.keys(message).filter((key) => !(key in parsed.data)) : [];
	if (!parsed.success || dropped.length > 0) {
		failed = true;
		console.error("FAIL queue", name, parsed.error ?? `drops ${dropped.join(", ")}`);
	} else {
		console.log("queue", name, parsed.data);
	}
}
process.exit(failed ? 1 : 0);
