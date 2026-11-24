import { fileURLToPath } from "node:url";
import { defineConfig } from "vitest/config";

const world = process.env.HARNESS_WORLD ?? "testworld";

// On the Vercel World, two tests check what Vercel's backend does not do. The
// fence probe writes events to a run that was never started, which the backend
// refuses whatever is sent. The stream test counts flow deliveries, and a
// stream close that crosses the network is still in flight when its step ends,
// so the runtime continues the run in a second delivery.
const vercelSkips = [
	"refuses a stale in-band write before writing anything",
	"sequential steps with stream complete in a single flow invocation",
];

export default defineConfig({
	test: {
		include: ["world-http/tests/world-testing/*.test.ts"],
		globalSetup: ["world-http/tests/world-testing/setup.ts"],
		testTimeout: 120_000,
		hookTimeout: 300_000,
		...(world === "vercel"
			? {
				testNamePattern: new RegExp(`^(?!.*(?:${vercelSkips.map((name) => name.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")).join("|")})).*$`),
				// Every request crosses the network to Vercel, so a test that
				// waits briefly for runs to finish is given another attempt.
				retry: 2,
			}
			: {}),
		env: {
			HARNESS_BINARY: fileURLToPath(new URL("./world-http/tests/world-testing/.bin/harness", import.meta.url)),
			HARNESS_WORLD: world,
			WORKFLOW_HTTP_TOKEN: process.env.WORKFLOW_HTTP_TOKEN ?? "world-testing",
			// The suite reads runs' outputs without a key, so a World that
			// encrypts them, as the Vercel World does, leaves them in plain text.
			HARNESS_UNENCRYPTED: "1",
		},
	},
});
