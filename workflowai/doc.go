// Package workflowai runs AI agents as durable workflow runs.
//
// # Agents
//
// A WorkflowAgent calls a language model in a loop and executes the tools the
// model calls, until the model stops calling tools, a stop condition is met,
// or a call waits for the user. Build it with NewWorkflowAgent inside a
// runner and call Stream, which writes the response to the run's stream, or
// Generate. Its model is a go-ai provider.LanguageModel. Replay builds the
// agent and its model again, so neither is ever serialized.
//
// Every model call is a step, so a run that stops, crashes, or restarts
// continues from its last recorded model call instead of starting over. A
// model call step that is cut short writes a reset-step part when it runs
// again, so readers discard the output of the interrupted execution.
//
// An Output parses the final answer into Result.Output. OutputFrom adapts a
// go-ai output, such as ai.ObjectOutput.
//
// # Tools
//
// A Tool executes calls with an Executor. Execute runs in workflow context,
// so it does its side effects through steps. ExecuteStep starts a step
// function in the background and derives the tool's input schema from the
// function's input type. ExecuteInlineStep does the same with an inline step,
// which always runs in the flow delivery, for short work. ExecuteFunc runs
// tools that combine several operations or wait durably. The calls of a step run concurrently, up to
// ToolCallConcurrency, and their results keep the order of the calls. A
// tool without Execute runs on the client. RepairToolCall can fix a call to
// an unknown tool or with invalid input before it is reported to the model.
//
// A tool with NeedsApproval ends the turn with an approval request. The
// user's response arrives with the next turn's messages, and the next Stream
// executes or denies the call before it calls the model.
//
// # Messages and streams
//
// A turn starts from a chat client's UI messages, which ConvertToModelMessages
// converts to ModelMessages. Stream writes ModelCallStreamParts to the run's
// stream, one chunk each. UIMessageChunks reads them back as the UI message
// stream that chat clients such as useChat read, skipping the chunks a
// reconnecting client already has.
package workflowai
