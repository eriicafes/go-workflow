import { fileURLToPath } from "node:url";
import { defineConfig } from "vitest/config";

export default defineConfig({
	test: {
		include: ["world-http/tests/world-testing/*.test.ts"],
		globalSetup: ["world-http/tests/world-testing/setup.ts"],
		testTimeout: 120_000,
		hookTimeout: 300_000,
		env: {
			HARNESS_BINARY: fileURLToPath(new URL("./world-http/tests/world-testing/.bin/harness", import.meta.url)),
			HARNESS_WORLD: process.env.HARNESS_WORLD ?? "testworld",
			WORKFLOW_HTTP_TOKEN: process.env.WORKFLOW_HTTP_TOKEN ?? "world-testing",
			// The suite reads runs' outputs without a key, so a World that
			// encrypts them, as the Vercel World does, leaves them in plain text.
			HARNESS_UNENCRYPTED: "1",
		},
	},
});
