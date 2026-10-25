# workflowai

`workflowai` runs AI agents as durable go-workflow runs. A
`WorkflowAgent` calls a language model in a loop, executes the tools it asks
for, and streams its response, while go-workflow records every model call and
tool step. A run that crashes, restarts, or waits days for a user's approval
continues where it stopped instead of starting over.

It is a Go port of the `WorkflowAgent` from the AI SDK's
[`@ai-sdk/workflow`](https://ai-sdk.dev/docs/agents/workflow-agent): it
writes the same stream parts, takes tool approvals the same way, and serves
the same UI message stream, so the AI SDK's `useChat` and
`WorkflowChatTransport` work with it unchanged.

```sh
go get github.com/eriicafes/go-workflow
```

```go
import "github.com/eriicafes/go-workflow/workflowai"
```

The [AI Agents guide](https://go-workflow.vercel.app/guide/ai-agents) covers
tools, approvals, failures, and callbacks in more depth.

## Acknowledgements

- [Go AI SDK](https://goaisdk.com) (`github.com/digitallysavvy/go-ai`)
  provides the models, providers, messages, and UI message stream types this
  package is built on. Any go-ai `provider.LanguageModel` works as the agent's
  model.
- The [Vercel AI SDK](https://ai-sdk.dev)'s
  [`WorkflowAgent`](https://ai-sdk.dev/docs/reference/ai-sdk-workflow/workflow-agent)
  defines the agent loop, stream protocol, and approval flow this package
  follows.

## Usage

Each chat turn is one run. The client sends the conversation, the run streams
the agent's response to the run's stream, and the server relays that stream
as a UI message stream.

### Define the workflow

```go
// workflows/chat.go
package workflows

import (
	"github.com/digitallysavvy/go-ai/pkg/ai"
	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/workflowai"
)

type Chat struct {
	workflow.Struct[ChatResult]
	Messages []ai.UIMessage `json:"messages"`
}

type ChatResult struct {
	Messages []workflowai.ModelMessage `json:"messages"`
}
```

### Write the runner

Build the model and the agent in the runner, and call `Stream`, which writes
the response to the run's default stream. A tool whose `Execute` is
`ExecuteStep(fn)` runs `fn` as a durable step, like an AI SDK tool whose
`execute` is marked `'use step'`, and `NeedsApproval` pauses the turn
until the user approves the call.

```go
// workflows/flows/chat.go
package flows

import (
	"context"

	"github.com/digitallysavvy/go-ai/pkg/providers/gateway"
	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/workflowai"

	"example.com/app/workflows"
)

func Chat(ctx context.Context, input workflows.Chat) (workflows.ChatResult, error) {
	// The AI Gateway provider reads its key from AI_GATEWAY_API_KEY.
	gw, err := gateway.New(gateway.Config{})
	if err != nil {
		return workflows.ChatResult{}, err
	}
	model, err := gw.LanguageModel("anthropic/claude-sonnet-5.5")
	if err != nil {
		return workflows.ChatResult{}, err
	}
	messages, err := workflowai.ConvertToModelMessages(ctx, input.Messages)
	if err != nil {
		return workflows.ChatResult{}, err
	}

	agent := workflowai.NewWorkflowAgent(workflowai.WorkflowAgentOptions{
		Model:        model,
		Instructions: "You are a flight booking assistant.",
		Tools: workflowai.ToolSet{
			"searchFlights": {
				Description: "Search for flights.",
				Execute:     workflowai.ExecuteStep(searchFlights),
			},
			"bookFlight": {
				Description:   "Book a flight for a passenger.",
				NeedsApproval: workflowai.NeedsApprovalAlways,
				Execute:       workflowai.ExecuteStep(bookFlight),
			},
		},
		StopWhen: []workflowai.StopCondition{workflowai.IsStepCount(10)},
	})
	result, err := agent.Stream(ctx, workflowai.CallOptions{
		Messages: messages,
	})
	return workflows.ChatResult{Messages: result.Messages}, err
}

type flightSearch struct {
	Origin      string `json:"origin"`
	Destination string `json:"destination"`
	Date        string `json:"date"`
}

func searchFlights(ctx context.Context, search flightSearch) ([]Flight, error) {
	return flightAPI.Search(ctx, search)
}

type booking struct {
	FlightID      string `json:"flightId"`
	PassengerName string `json:"passengerName"`
}

func bookFlight(ctx context.Context, input booking) (Confirmation, error) {
	// The step ID stays the same when the step is retried, so the booking
	// API can use it to book only once.
	return flightAPI.Book(ctx, input, workflow.StepID(ctx))
}
```

A tool's input schema comes from its step function's input type, derived when
the agent first sends the tool to the model. Register the runner like any
other with `workflow.Use(flows.Chat)`.

Every replay and step delivery runs the runner again and builds a new model,
so the model is never serialized or stored.

### Serve the chat API

`WorkflowChatTransport` posts each turn to `/api/chat` and reconnects to
`/api/chat/{runId}/stream?startIndex=n`. Start a run for the turn, then relay
its stream with `UIMessageChunks`. The run stream holds raw model call parts,
so it is read from the start and `UIMessageChunks` skips the UI chunks the
client already has.

```go
// cmd/server/routes.go
package main

import (
	"cmp"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/digitallysavvy/go-ai/pkg/ai"
	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/workflowai"

	"example.com/app/workflows"
)

// routes registers the chat API that WorkflowChatTransport talks to.
func routes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/chat", func(w http.ResponseWriter, r *http.Request) {
		var input workflows.Chat
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		run, err := workflow.Start(r.Context(), input)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		streamRun(w, r, run.ID(), 0)
	})

	mux.HandleFunc("GET /api/chat/{runId}/stream", func(w http.ResponseWriter, r *http.Request) {
		startIndex, err := strconv.Atoi(cmp.Or(r.URL.Query().Get("startIndex"), "0"))
		if err != nil || startIndex < 0 {
			http.Error(w, "startIndex must be a non-negative integer", http.StatusBadRequest)
			return
		}
		streamRun(w, r, r.PathValue("runId"), startIndex)
	})
}

func streamRun(w http.ResponseWriter, r *http.Request, runID string, uiStartIndex int) {
	run, err := workflow.Get[workflows.ChatResult](r.Context(), runID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	reader, err := run.Reader(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	chunks := workflowai.UIMessageChunks(r.Context(), reader, uiStartIndex)
	ai.PipeUIMessageChunksToResponse(chunks, w, &ai.UIMessageStreamResponseInit{
		Headers: map[string]string{"X-Workflow-Run-Id": runID},
	})
}
```

### Use it from the client

The client is plain AI SDK: `useChat` from `@ai-sdk/react` with
`WorkflowChatTransport` from `@ai-sdk/workflow/client`.

```tsx
import { useChat } from '@ai-sdk/react';
import { WorkflowChatTransport } from '@ai-sdk/workflow/client';
import { lastAssistantMessageIsCompleteWithApprovalResponses } from 'ai';
import { useState } from 'react';

export function Chat() {
  const [transport] = useState(() => new WorkflowChatTransport({ api: '/api/chat' }));
  const { messages, sendMessage, addToolApprovalResponse } = useChat({
    transport,
    // Send the user's approvals back so the next run executes or denies the calls.
    sendAutomaticallyWhen: lastAssistantMessageIsCompleteWithApprovalResponses,
  });

  // Render messages. A tool part in the 'approval-requested' state asks the user:
  // addToolApprovalResponse({ id: part.approval.id, approved: true })
}
```

Every client feature of `@ai-sdk/workflow` works against a Go server:

| Client feature | How the Go side supports it |
| --- | --- |
| `WorkflowChatTransport` sends and reconnects | `X-Workflow-Run-Id` header, and `UIMessageChunks` with the client's `startIndex` |
| Reconnecting after a dropped connection or reload | The response lives in the run's stream, not the HTTP response |
| Text, reasoning, files, and sources | Streamed as the AI SDK's UI message chunks |
| Tool input streaming, results, and errors | `tool-input-*`, `tool-output-available`, and `tool-output-error` chunks |
| Tool approvals with `addToolApprovalResponse` | `tool-approval-request` chunks, and approval responses resumed from the next turn's messages |
| Signed approvals | `WorkflowAgentOptions.ToolApprovalSecret`, verified before an approved call executes |
| Denied approvals | `tool-output-denied` chunks and an `execution-denied` result for the model |
| Client-side tools with `addToolOutput` | A tool without `Execute` ends the turn, and its output arrives in the next turn's messages |
| Discarding output of an interrupted model call | A `reset-step` chunk before every model call execution |
| Stream errors | An `error` chunk, and `Result.Error` on the server |

[`examples/durable-agent`](../examples/durable-agent) is a complete chat app
built this way, with a React client that resumes its run after a reload.
