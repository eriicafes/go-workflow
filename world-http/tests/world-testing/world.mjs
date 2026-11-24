// A World module for the Workflow SDK's world-testing suite, which loads it
// through WORKFLOW_TARGET_WORLD in each app it starts. createWorld starts a Go
// World with the harness binary at HARNESS_BINARY and returns a world-http
// World on it. Once the app listens, its start() hands the harness the app's
// flow route, which the Go World's worlds.RemoteExecutor delivers to.
import { spawn } from "node:child_process";
import { createInterface } from "node:readline";
import { createWorld as createHTTPWorld } from "go-workflow/world-http";

export async function createWorld() {
	const binary = process.env.HARNESS_BINARY;
	if (!binary) {
		throw new Error("HARNESS_BINARY is not set");
	}
	// The harness exits when its standard input closes, which happens when
	// this process exits.
	const harness = spawn(binary, { env: process.env, stdio: ["pipe", "pipe", "inherit"] });
	const line = await new Promise((resolve, reject) => {
		createInterface({ input: harness.stdout }).once("line", resolve);
		harness.once("exit", (code) => reject(new Error(`the harness exited with ${code}`)));
		harness.once("error", reject);
	});
	const { baseURL } = JSON.parse(line);
	const world = await createHTTPWorld({ baseURL });
	world.start = async () => {
		const flowURL = `http://127.0.0.1:${process.env.PORT}/.well-known/workflow/v1/flow`;
		const response = await fetch(new URL("/_harness/remote", baseURL), { method: "POST", body: flowURL });
		if (!response.ok) {
			throw new Error(`the harness refused the flow route: ${response.status}`);
		}
	};
	return world;
}
