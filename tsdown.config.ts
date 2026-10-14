import { defineConfig } from "tsdown";

export default defineConfig({
	clean: true,
	dts: true,
	entry: ["world-http/index.ts"],
	format: ["esm"],
	outDir: "world-http/dist",
});
