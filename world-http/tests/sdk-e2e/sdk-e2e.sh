#!/usr/bin/env bash
# Runs the Workflow SDK's e2e suite against a Go World. The SDK's nitro-v3
# workbench app runs with world-http as its World, backed by the harness in
# world-http/testdata/harness, which delivers to the app with a
# worlds.RemoteExecutor.
#
#   sdk-e2e.sh prepare   installs and builds the app in SDK_DIR
#   sdk-e2e.sh run       starts the harness and the app, then runs the suite
#
# SDK_DIR is a checkout of github.com/vercel/workflow, at the commit in
# .github/workflows/main.yml. It must be outside this repository, whose
# node_modules holds published SDK packages that the app's build would
# otherwise pick up beside the checkout's own. HARNESS_WORLD selects the Go
# World, as in the harness, and DATABASE_URL is the postgres World's database.
# Arguments after the command are passed to vitest, such as -t to select tests.
set -euo pipefail

: "${SDK_DIR:?SDK_DIR must be a checkout of github.com/vercel/workflow}"
SDK_DIR="$(cd "$SDK_DIR" && pwd)"
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
case "$SDK_DIR/" in
"$ROOT"/*)
	echo "SDK_DIR must be outside $ROOT" >&2
	exit 2
	;;
esac
APP_DIR="$SDK_DIR/workbench/nitro-v3"
APP_PORT="${APP_PORT:-3000}"
HARNESS_PORT="${HARNESS_PORT:-4100}"

export WORKFLOW_HTTP_TOKEN="${WORKFLOW_HTTP_TOKEN:-sdk-e2e}"
export WORKFLOW_HTTP_BASE_URL="http://127.0.0.1:$HARNESS_PORT/v1/workflow"
export WORKFLOW_TARGET_WORLD="$ROOT/world-http/dist/index.mjs"
# Go Worlds create spec 9 runs, which the suite checks against this.
export WORKFLOW_SPEC_VERSION=9
# The app serves its manifest, which the suite reads, when built with this.
export WORKFLOW_PUBLIC_MANIFEST=1

prepare() {
	cd "$SDK_DIR"
	pnpm install --frozen-lockfile --filter "@workflow/example-nitro..." --filter "@workflow/core..." --filter .
	pnpm turbo run build --filter "@workflow/example-nitro^..."
	# The workbench apps link their workflows from workbench/example.
	CI=true ./scripts/resolve-symlinks.sh workbench/nitro-v3
	(cd "$APP_DIR" && pnpm build)
}

run() {
	# Globals, since the trap runs after run returns.
	pids=()
	bin="$(mktemp -d)"
	trap 'kill "${pids[@]:-}" 2>/dev/null || true; wait 2>/dev/null || true; rm -rf "$bin"' EXIT

	(cd "$ROOT" && npm run build >/dev/null && go build -o "$bin/harness" ./world-http/testdata/harness)

	HARNESS_ADDR="127.0.0.1:$HARNESS_PORT" \
		HARNESS_REMOTE_URL="http://localhost:$APP_PORT/.well-known/workflow/v1/flow" \
		"$bin/harness" </dev/null &
	pids+=($!)
	(cd "$APP_DIR" && PORT="$APP_PORT" exec node .output/server/index.mjs) &
	pids+=($!)

	curl -sf -o /dev/null --retry 30 --retry-connrefused --retry-delay 1 \
		"http://localhost:$APP_PORT/.well-known/workflow/v1/manifest.json"

	cd "$SDK_DIR"
	APP_NAME=nitro-v3 WORKBENCH_APP_PATH="$APP_DIR" DEPLOYMENT_URL="http://localhost:$APP_PORT" \
		pnpm vitest run packages/core/e2e/e2e.test.ts "$@"
}

command="${1:-}"
shift || true
case "$command" in
prepare) prepare ;;
run) run "$@" ;;
*)
	echo "usage: sdk-e2e.sh prepare|run [vitest arguments]" >&2
	exit 2
	;;
esac
