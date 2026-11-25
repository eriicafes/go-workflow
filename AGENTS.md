# go-workflow

go-workflow is a Go implementation of the [Workflow SDK](https://workflow-sdk.dev)
(vercel/workflow). Runs are event-sourced: a run's progress is an event log, and
workflow code replays against it. The aim is parity with the SDK: Go runs store
the same data as JS runs (event shapes, payload envelope, stream names,
attributes such as `$retention`), so the SDK's web UI and Vercel's dashboard can
read them.

## Layout

- `*.go` at the root: the `workflow` package that workflow code calls
  (`Start`, `Step`, `InlineStep`, `Sleep`, hooks, webhooks, streams,
  cancellation).
- `worlds/`: the `World` interface, client, RPC server and runtime pieces.
  - `worlds/core`: the durable runtime that replays runs.
  - World backends: `testworld` (in-memory, for tests), `local`, `postgres`,
    `aws` (DynamoDB, SQS, Scheduler), `vercel`.
- `workflowtest/`: World conformance cases. Topic files (`steps.go`,
  `hooks.go`, …) hold the cases, and the `sdk_*.go` files hold cases ported
  from the SDK.
- `workflowai/`: a durable AI agent on go-ai
  (`github.com/digitallysavvy/go-ai`), ported from `@ai-sdk/workflow`'s
  WorkflowAgent.
- `workflowdev/`: development helpers, including the web UI.
- `check/` and `cmd/workflowcheck`: a `go vet` analyzer for workflow code.
- `internal/`: `devalue` and `envelope` (the SDK's payload formats),
  `sdkerror`, and `sdkdrift` (checks that every upstream test is ported or
  skipped).
- `world-http/`: the npm package (TypeScript) that lets a JS Workflow SDK app
  use a Go World over HTTP. Its tests are in `world-http/tests`.
- `docs/`: the docs site (Fumadocs on Next.js; see `docs/AGENTS.md`). Guides
  are in `docs/content/docs`.
- `skills/`: agent skills for users of the library.
- `examples/`: `basic` and `durable-agent`, each its own Go module.

## Commands

```sh
go vet ./...
go test ./...                    # Postgres and AWS tests need their services; see below
go test ./... -run SDK           # only tests ported from the SDKs
npm ci && npm run check          # type-check world-http
npm test                         # world-http tests (vitest)
npm run build                    # build world-http
pnpm --dir docs dev              # docs dev server
```

CI (`.github/workflows/main.yml`) runs `go test -race ./...` with Postgres and
DynamoDB Local services. Running those locally is not expected: test the
packages you changed and leave `worlds/postgres` and `worlds/aws` to CI. CI also
runs the SDK's World test suite (`npm run test:world-testing`), the SDK's e2e
suite against each World (`npm run test:sdk-e2e`), and the drift check below.

## SDK parity and the drift check

When a Go design differs from the SDK, follow the SDK's format and behavior.
Read the SDK's source (vercel/workflow, or vercel/ai `packages/workflow` for
`workflowai`) before deciding. Non-goals: resuming runs between JS and Go, and
reading data written by older go-workflow versions, so no migrations are needed.

`internal/sdkdrift` checks that every upstream test is either ported or
skipped:

```sh
SDK_DIR=/path/to/vercel/workflow AI_SDK_DIR=/path/to/vercel/ai \
  go test ./internal/sdkdrift -run TestSDKTestsAreAccountedFor -count=1
```

CI pins both commits (`SDK_COMMIT`, `AI_SDK_COMMIT`).

- A ported test goes in an `sdk_`-prefixed file beside the source it tests, in
  the same package (`worlds/sdk_attributes_test.go` for `attributes.go`). Its
  name starts with `TestSDK` (conformance cases with `SDK`), and it carries one
  comment line per upstream test it covers:
  `// SDK: <upstream file> > <describe> > <test name>` for the Workflow SDK, or
  `// AI SDK: ...` for `@ai-sdk/workflow`.
- An upstream test that has no Go counterpart goes in a skip file:
  `internal/sdkdrift/skips*.txt` for the Workflow SDK, or `ai-sdk-skips*.txt`
  for the AI SDK. Write one `<file>[ > <name>] -- <reason>` per line.
- Don't leave a port behind `t.Skip("DRIFT: ...")`. Fix the drift, or move the
  test to a skip file with the reason.

## Code conventions

- Name test files after the file they test (`world_streamer_test.go`), never
  after a topic. Shared test helpers live once in one of those files; don't add
  catch-all files like `export_test.go`. In world-http, shared helpers are in
  `world-http/tests/helpers.ts`.
- Put a file's main content first and helpers at the bottom. Split large files
  by component, and keep a class's members inside the class.
- Write plain comments that state the rule or what the code does. Don't narrate
  ("a spec 9 run says ...").
- The API is pre-1.0. Change signatures directly, without compatibility shims
  or deprecated aliases.
- Worlds keep run, step and hook payloads only in the event log. Views hold
  metadata and event slots, and reads decode them with `worlds.EventPayload`.
  Don't copy payloads into views.

## Commits, changesets and releases

- Write a single short subject line with no body.
- Before pushing, fold fix-ups into the unpushed commit they belong to. Never
  rewrite pushed commits.
- Add a changeset (`.changeset/*.md`) only for a change to the public API,
  written as one short sentence. Bug fixes and internal changes get none.
- Never edit `CHANGELOG.md`; changesets generates it. Pushing to `main`
  publishes through `changeset publish`.
