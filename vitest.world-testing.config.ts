import { fileURLToPath } from "node:url";
import { defineConfig } from "vitest/config";

export default defineConfig({
	test: {
		include: ["world-http/testdata/world-testing/*.test.ts"],
		globalSetup: ["world-http/testdata/world-testing/setup.ts"],
		testTimeout: 120_000,
		hookTimeout: 300_000,
		env: {
			HARNESS_BINARY: fileURLToPath(new URL("./world-http/testdata/world-testing/.bin/harness", import.meta.url)),
			HARNESS_WORLD: process.env.HARNESS_WORLD ?? "testworld",
			WORKFLOW_HTTP_TOKEN: process.env.WORKFLOW_HTTP_TOKEN ?? "world-testing",
		},
	},
});
