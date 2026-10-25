// Package workflows declares the example's workflow types, which the server
// starts runs of without importing their runners.
package workflows

import (
	"github.com/digitallysavvy/go-ai/pkg/ai"
	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/workflowai"
)

// Chat is one turn of a chat: the UI messages a useChat client sends, ending
// with the user's new message or their responses to tool approvals. The run
// streams the agent's response to its default stream.
type Chat struct {
	workflow.Struct[ChatResult]
	Messages []ai.UIMessage `json:"messages"`
}

type ChatResult struct {
	Messages []workflowai.ModelMessage `json:"messages"`
}
