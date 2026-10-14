import { defineConfig } from "vitest/config";

export default defineConfig({
	test: {
		include: ["world-http/**/*.test.ts"],
	},
});
