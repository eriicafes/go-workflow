// Runs the Workflow SDK's World test suite against the Go World that
// HARNESS_WORLD names, through world-http and worlds.RemoteExecutor.
import { fileURLToPath } from "node:url";
import { createTestSuite } from "@workflow/world-testing";

createTestSuite(fileURLToPath(new URL("./world.mjs", import.meta.url)));
