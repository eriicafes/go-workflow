# Durable agent

A durable AI chat agent: a Go `WorkflowAgent` that runs on go-workflow and is
built on the [Go AI SDK](https://goaisdk.com), with a React client that uses
the AI SDK's `useChat` and `WorkflowChatTransport`.

The [`workflowai`](../../workflowai) package matches the
`WorkflowAgent` of
[`@ai-sdk/workflow`](https://ai-sdk.dev/docs/agents/workflow-agent). Each
chat turn is one workflow run:

- Every model call is a durable step that streams its parts to the run's
  stream. Opening a call is retried with backoff, and an error in the model
  stream ends the turn with an error part. A step cut short by a crash runs
  again and starts with a `reset-step` part, so the client drops the text of
  the interrupted execution.
- Tool calls run their tool's `Execute` in workflow context, and
  `ExecuteStep` runs the call as a step, so a call executes at least once and
  its result is recorded.
- A tool with `NeedsApproval` ends the turn with a `tool-approval-request`.
  The client sends the user's answer back, and the next run executes or
  denies the call before the next model call. With `ToolApprovalSecret` set,
  approval requests are signed and an approval only runs the call it was
  issued for, with the same input.
- The stream lives in the run, not the HTTP response. A client that
  disconnects or reloads reconnects to it, and a server that crashes or
  restarts continues the run where it stopped.

## Layout

| Path | Contents |
| --- | --- |
| [`workflows`](workflows) | The `Chat` workflow type |
| [`workflows/flows`](workflows/flows) | The `Chat` runner: a flight booking agent with a `searchFlights` tool and a `bookFlight` tool that needs approval |
| [`cmd/server`](cmd/server) | The chat API, with a local world in `.workflows` |
| [`cmd/dev`](cmd/dev) | Runs the API and the client with [go-dev](https://github.com/eriicafes/go-dev) |
| [`web`](web) | The React client, built with Vite and Tailwind |

The example is its own Go module, so its dependencies stay out of
go-workflow's.

## Running

Set `AI_GATEWAY_API_KEY` to use the AI Gateway, or `ANTHROPIC_API_KEY` to use
the Anthropic API. `MODEL` picks the model, and `TOOL_APPROVAL_SECRET` signs
approvals.

```sh
export ANTHROPIC_API_KEY=...
export TOOL_APPROVAL_SECRET=$(openssl rand -hex 32)
(cd web && pnpm install)
go run ./cmd/dev
```

`cmd/dev` checks the workflows with the analyzer, starts the API on :8080, and
starts Vite on http://localhost:5173, which proxies `/api` to the API. The API
rebuilds when `cmd/server`, `workflows`, or `workflowai` change.
It runs without a go-dev proxy because the local world locks `.workflows`, so
a reload stops the old process before the new one starts.

`pnpm build` in `web` builds the client into `web/dist`, which `go run
./cmd/server` also serves at http://localhost:8080.

Ask for a flight, then reload the page or stop and restart the server while
the agent answers. The page reconnects to the run, and the restarted server
continues the model call that was interrupted. Ask to book a flight to see the
approval flow.

The [Workflow SDK web UI](../../docs/tooling/web-ui.md) shows every run's
steps and stream from `http://localhost:8080/v1/workflow`.

## The API

`POST /api/chat` starts a `Chat` run with the client's UI messages and
streams it, with the run ID in `X-Workflow-Run-Id`. `GET
/api/chat/{runId}/stream?startIndex=n` reconnects to a run's stream, skipping
the `n` UI chunks the client already received. These are the endpoints
`WorkflowChatTransport` expects.

```go
func Chat(ctx context.Context, input workflows.Chat) (workflows.ChatResult, error) {
	model, err := LanguageModel()
	if err != nil {
		return workflows.ChatResult{}, err
	}
	messages, err := workflowai.ConvertToModelMessages(ctx, input.Messages)
	if err != nil {
		return workflows.ChatResult{}, err
	}
	agent := workflowai.NewWorkflowAgent(workflowai.WorkflowAgentOptions{Model: model, Tools: tools})
	result, err := agent.Stream(ctx, workflowai.CallOptions{
		Messages: messages,
	})
	return workflows.ChatResult{Messages: result.Messages}, err
}
```

The runner builds its model with `flows.LanguageModel`, which picks the AI
Gateway or the Anthropic API from the environment. Every replay builds it
again, so the model is never stored. The server also calls it at startup, so
a missing API key fails there rather than in a run.

The run's stream holds raw `ModelCallStreamPart`s, as in `@ai-sdk/workflow`,
so a handler always reads it from the start and `UIMessageChunks` converts it
and skips the chunks the client has:

```go
reader, err := run.Reader(r.Context())
chunks := workflowai.UIMessageChunks(r.Context(), reader, startIndex)
ai.PipeUIMessageChunksToResponse(chunks, w, &ai.UIMessageStreamResponseInit{
	Headers: map[string]string{"X-Workflow-Run-Id": run.ID()},
})
```
