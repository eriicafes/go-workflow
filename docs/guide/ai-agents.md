# AI Agents

The `workflowai` package runs AI agents as durable runs. A
`WorkflowAgent` calls a language model in a loop, executes the tools it asks
for, and streams its response, while every model call and tool step is
recorded in the run. A run that crashes, restarts, or waits days for a user's
approval continues where it stopped instead of starting over.

It is a Go port of the `WorkflowAgent` from the AI SDK's
[`@ai-sdk/workflow`](https://ai-sdk.dev/docs/agents/workflow-agent), built on
the [Go AI SDK](https://goaisdk.com). It writes the same stream parts, takes
tool approvals the same way, and serves the same UI message stream, so the
AI SDK's `useChat` and `WorkflowChatTransport` work with a Go server
unchanged.

```go
import "github.com/eriicafes/go-workflow/workflowai"
```

::: info Acknowledgements
The [Go AI SDK](https://goaisdk.com) (`github.com/digitallysavvy/go-ai`)
provides the models, providers, messages, and UI message stream types this
package is built on. The [Vercel AI SDK](https://ai-sdk.dev)'s
[`WorkflowAgent`](https://ai-sdk.dev/docs/reference/ai-sdk-workflow/workflow-agent)
defines the agent loop, stream protocol, and approval flow it follows.
:::

## How a chat runs

Each chat turn is one run:

1. The client posts the conversation as UI messages, and the server starts a
   run with them.
2. The runner builds a `WorkflowAgent` and calls `Stream`. Every model call is
   a step that writes its parts to the run's [stream](/guide/streams) as they
   arrive, and every tool call runs its tool, usually as a step.
3. The server relays the run's stream to the client as a UI message stream.
   A client that disconnects reconnects to the same stream.
4. When a tool call needs the user's approval, the turn ends with an approval
   request. The user's answer arrives with the next turn's messages, and the
   next run executes or denies the call before calling the model again.

## Defining the workflow

The workflow's input is the UI messages the client sends:

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

## Writing the runner

Build the model and the agent in the runner. Every replay and step delivery
builds them again, so the model is never serialized or stored:
`WorkflowAgentOptions.Model` is any go-ai `provider.LanguageModel`.

```go
// workflows/flows/chat.go
package flows

import (
	"context"

	"github.com/digitallysavvy/go-ai/pkg/providers/gateway"
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
```

Building clients and models in the runner is fine as long as building does
no I/O. Values read while building, such as a key from the environment, are
read again on every replay and delivery, so rotated credentials are picked up.
Credentials that need I/O to fetch belong in a provider that resolves them per
request, inside the model call step. Don't read them in an inline step: its
result, and so the secret, would be stored in the run's event log.

`Stream` writes to the run's default stream, like `getWritable()` in the
Workflow SDK, and closes it when the turn ends. Streams can only be opened in
step context, so the steps that write open it with `workflow.Writer`. To write
a named stream instead, set `Writer`, and read the stream with the same name in
the server's handler:

```go
result, err := agent.Stream(ctx, workflowai.CallOptions{
	Messages: messages,
	Writer:   &workflow.WriterOption{Name: "chat"},
})
```

`Stream` returns a `Result` with the conversation including the response,
every step, the last step's tool calls and results, the total usage, and
`Error` when the model stream failed. `Generate` runs the same loop without
streaming and stops after 20 steps unless `StopWhen` is set.

## Tools

A `Tool` has a description and an `Execute` that runs its calls. `Execute`
runs in workflow context, so it replays with the run and does its side effects
through steps. `ExecuteStep(fn)` decodes a call's input and runs `fn` as a
step, like an AI SDK tool whose `execute` is marked `'use step'`. The tool's
input schema comes from `fn`'s input type: the agent derives it from the JSON
fields when it first sends the tool to the model.

```go
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
	PriceUSD      int    `json:"priceUsd"`
}

func bookFlight(ctx context.Context, input booking) (Confirmation, error) {
	// The step ID stays the same when the step is retried, so the booking
	// API can use it to book only once.
	return flightAPI.Book(ctx, input, workflow.StepID(ctx))
}

tools := workflowai.ToolSet{
	"searchFlights": {
		Description: "Search for flights.",
		Execute:     workflowai.ExecuteStep(searchFlights),
	},
	"bookFlight": {
		Description: "Book a flight for a passenger.",
		Execute:     workflowai.ExecuteStep(bookFlight),
	},
}
```

Since `Execute` runs in workflow context, a tool can also wait durably, for
example with `workflow.Sleep` or a [hook](/guide/hooks). A tool error is sent
to the model as the tool's result, so the model can recover. Calls to unknown
tools or with input that does not match the schema are reported to the model
as invalid and not executed.

`Tool.InputSchema` sets a JSON schema explicitly.

Choose an executor by where the work should run:

- `ExecuteStep(fn)` runs `fn` as a [step](/guide/steps). The flow runs the
  first few calls inline, up to `workflow.SetMaxInlineSteps`, and queues the
  rest as step deliveries that other workers can run. Use it for slow work and
  for work that should spread across workers.
- `ExecuteInlineStep(fn)` runs `fn` as an
  [inline step](/guide/inline-steps#running-inline-steps-concurrently), which
  always runs in the flow delivery and never goes through the queue. Use it
  for short work. It derives the input schema and decodes the input like
  `ExecuteStep`.
- `ExecuteFunc` adapts a plain function that takes the call's raw JSON input.
  Use it for tools that combine several operations or wait durably, such as
  with `workflow.Sleep` or a hook.

The calls of a step run concurrently, like the AI SDK's. `ExecuteStep` and
`ExecuteInlineStep` start their steps in the background, so a step's calls run
at the same time and their results reach the model in the order the model made
the calls. An `ExecuteFunc` blocks the runner until it returns, so its calls
run one at a time. `ToolCallConcurrency` in `WorkflowAgentOptions` or `CallOptions` limits
how many calls run at once, including approved calls; `1` runs them one at a
time in the model's order. A custom `Executor` returns a `workflow.Result`, such as one from
`workflow.Background`, to run concurrently.

`RepairToolCall` gets a chance to fix a call to an unknown tool or with input
that fails the tool's schema, before the call is reported to the model as
invalid. It takes go-ai's `ai.ToolCallRepairFunction`, receives the call, the
error, and the tools' schemas, and returns the repaired call or `nil`. It
runs inside the model call's step, once per invalid call, so it can call a
model itself.

A tool without `Execute` runs on the client: a call to it ends the turn, and
the client sends its output, added with `addToolOutput`, in the next turn's
messages. Give a client tool an `InputSchema`, since it has no step function
to derive one from. `WorkflowAgentOptions.ToolsContext` passes per-tool values
to `Execute` and `NeedsApproval`.

## Tool approvals

`NeedsApproval` decides whether a call waits for the user.
`workflowai.NeedsApprovalAlways` asks for every call, and a custom function can
decide from the call's input.

```go
"bookFlight": {
	Description: "Book a flight for a passenger.",
	NeedsApproval: func(ctx context.Context, input json.RawMessage, options workflowai.ToolCallOptions) (bool, error) {
		var request booking
		if err := json.Unmarshal(input, &request); err != nil {
			return false, err
		}
		return request.PriceUSD > 500, nil
	},
	Execute: workflowai.ExecuteStep(bookFlight),
},
```

A call that needs approval ends the turn with a `tool-approval-request`. The
client shows it, the user answers with `addToolApprovalResponse`, and the next
turn executes the approved calls or reports the denied ones to the model as
`execution-denied`. The user can answer much later: no run waits in between.

Set `WorkflowAgentOptions.ToolApprovalSecret` to sign approval requests with
HMAC. An approval then only executes the call it was issued for, with the same
input, so a client cannot edit a call's input and approve it. Use at least 32
random bytes, the same in every process that runs the workflow.

Tools the provider executes itself, such as Anthropic's web search, report
their results in the model's stream, sometimes in a later step than their
call. The agent keeps those results in the assistant message, streams them to
the client, and sends them back to the provider in later model calls.

A provider can also ask for approval of a tool it executes itself, such as an
MCP tool. Its `tool-approval-request` reaches the client and ends the turn the
same way, and the next turn sends the user's response back to the provider
with the request instead of executing anything locally.

## Serving the chat API

`WorkflowChatTransport` posts each turn to `/api/chat` and reconnects to
`/api/chat/{runId}/stream?startIndex=n`, where `n` is the number of UI
chunks it already received. The run's stream holds the agent's raw model call
parts, as in `@ai-sdk/workflow`, so a handler always reads it from the start,
and `UIMessageChunks` converts it and skips the first `n` UI chunks.

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

The `X-Workflow-Run-Id` header tells the transport which run to reconnect to.

## The client

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

Every client feature of `@ai-sdk/workflow` works against the Go server:

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

To resume a turn after a page reload, save the run ID from the
`x-workflow-run-id` response header with `onChatSendMessage`, pass
`resume: true` to `useChat`, and point `prepareReconnectToStreamRequest` at
`/api/chat/{runId}/stream`. The
[durable-agent example](https://github.com/eriicafes/go-workflow/tree/main/examples/durable-agent)
does this, and saves the chat between turns.

## Structured output

Set `Output` to have the agent's final answer parsed into `Result.Output`.
`OutputFrom` adapts any go-ai output, and its response format is sent with
every model call:

```go
type Itinerary struct {
	City string `json:"city"`
	Days int    `json:"days"`
}

agent := workflowai.NewWorkflowAgent(workflowai.WorkflowAgentOptions{
	Model: model,
	Output: workflowai.OutputFrom(ai.ObjectOutput[Itinerary](ai.ObjectOutputOptions{
		Schema: ai.SchemaFor[Itinerary](),
	})),
})
result, err := agent.Generate(ctx, workflowai.CallOptions{Prompt: "Plan a trip to Lagos."})
if err != nil {
	return workflows.TripResult{}, err
}
itinerary := result.Output.(Itinerary)
```

The output is parsed from the last step's text when that step did not call
tools, so a turn that stops for approval has no output. A parse failure fails
the call like any other error. In the runner `Result.Output` holds the
output's type; a run's JSON result holds it as JSON.

## Failures and retries

Model calls retry like the AI SDK's. An error the provider reports as
retryable when opening a call retries it, twice by default after 2 and 4
seconds; set `MaxRetries` in `WorkflowAgentOptions` to change that. Each
further retry waits twice as long, up to a minute. A `retry-after-ms` or
`Retry-After` header on the error's response replaces the wait when it asks
for less than a minute. An error part in the model stream is written to the
stream and ends the turn with `Result.Error`, without a retry, and the step's
tool calls are not executed.

A model call step that is cut short, such as by a crash or restart, runs again
when the World redelivers it. It first writes a `reset-step` part, so clients
discard the text of the interrupted execution. A tool step behaves like any
[step](/guide/steps): it runs at least once and its result is recorded.

When a run fails outright, `Stream` writes an `error` part and closes the
stream, so the client does not wait for parts that never come.

`Timeout` in `WorkflowAgentOptions` or `CallOptions` limits how long a call's
model calls run. It is a durable timeout, counted from the start of the call
across replays and restarts. When it passes, the running model call is
cancelled, `OnAbort` is called with the steps that finished, and `Stream`
returns the cancellation error after closing the stream. A cancelled context
passed to `Stream` aborts the call the same way.

## Telemetry

Set `Telemetry` to record OpenTelemetry spans through go-ai's telemetry
integrations, such as `telemetry.NewOpenTelemetry`:

```go
Telemetry: &telemetry.Options{
	FunctionID:   "flight-assistant",
	Integrations: []telemetry.TelemetryIntegration{telemetry.NewOpenTelemetry(telemetry.OpenTelemetryOptions{})},
},
```

A call runs across many processes and replays, so its spans cannot share one
in-memory parent. Instead each `Stream` or `Generate` call records one trace.
An inline step at the start of the call starts and ends the root span and
records its trace context, and the agent puts that context in the `ctx` it
passes to every step. Each step's [span](/guide/steps#tracing) nests under the
root: the model call steps, with go-ai's operation, step, and `chat` spans and
the model's usage inside them, and the tool steps, with anything your tools
trace. Replays reuse the recorded steps, so no span is recorded twice. The
root span is short, since a span can only end in the process that started it.

## Instructions and settings

`Instructions` is the system prompt. `InstructionMessages` adds system
messages after it, which can carry provider options such as Anthropic's cache
control. System messages in a call's `Messages` fail the call unless
`AllowSystemInMessages` is set, so a client cannot add instructions to the
conversation it sends.

`WorkflowAgentOptions` also holds the model call settings: `Temperature`,
`MaxOutputTokens`, `Reasoning`, `SendReasoning`, `ProviderOptions`, and the
rest of `GenerationSettings`.

Every model call normalizes its prompt like go-ai does. With a `Download`
function, it also fetches the file URLs in messages that the model does not
support, inside the model call's step. Without one, URLs are sent as they are.

`CallOptions` has a few more controls for one call:

- `Transform` rewrites the provider chunks of each streamed model call, in
  order, with go-ai's `ai.StreamTransformFunc`.
- `IncludeRawChunks` writes the provider's raw chunks to the run's stream as
  `raw` parts. They have no UI chunk, so `UIMessageChunks` skips them.
- `Include` keeps the request and response bodies of `Generate`'s model calls,
  and the messages sent, in each `StepResult`. They are stored with the step,
  so keep them off unless you need them.

## Steps, callbacks, and stop conditions

- `StopWhen` stops the loop after a step with tool results when any condition
  is met: `IsStepCount(n)` or `HasToolCall(names...)`. `Stream` has no step
  limit by default.
- `PrepareCall` runs once per `Stream` or `Generate` call, before the loop,
  and can change the call's model, instructions, messages, tool choice,
  active tools, tool call concurrency, stop conditions, contexts, and
  generation settings. Tools cannot change, and a `ToolCallConcurrency` set
  in `CallOptions` takes precedence over the one it returns.
- `PrepareStep` runs before each model call and can change the model,
  instructions, messages, tool choice, and active tools for that call.
- `OnStart`, `OnStepStart`, `OnToolExecutionStart`, `OnToolExecutionEnd`,
  `OnStepEnd`, `OnEnd`, `OnError`, and `OnAbort` are called as the agent
  runs. Set them on `WorkflowAgentOptions` or `CallOptions`; both are called,
  the agent's first.

::: warning Callbacks replay
`PrepareCall`, `PrepareStep`, `NeedsApproval`, `Execute`, and callbacks run
in workflow context, so they run again on every replay and must be
deterministic. They get the workflow `ctx`, so they can call `workflow.Step`
or `workflow.InlineStep` themselves to record side effects and external
reads, and replays reuse the recorded results. Plain logging in a callback
may repeat on replay, which is usually fine.
:::
