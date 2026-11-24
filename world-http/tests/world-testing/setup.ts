// Builds world-http and the Go harness the world-testing suite runs against.
import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";

export const harnessBinary = fileURLToPath(new URL("./.bin/harness", import.meta.url));

export default function setup() {
	const root = fileURLToPath(new URL("../../..", import.meta.url));
	execFileSync("npm", ["run", "build"], { cwd: root, stdio: "inherit" });
	execFileSync("go", ["build", "-o", harnessBinary, "./world-http/testdata/harness"], { cwd: root, stdio: "inherit" });
}
