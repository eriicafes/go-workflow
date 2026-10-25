package workflowai

import (
	"context"

	"github.com/digitallysavvy/go-ai/pkg/ai"
	"github.com/digitallysavvy/go-ai/pkg/provider"
)

// Output parses the final text of a Stream or Generate call into
// Result.Output, and gives the model the format to answer in. OutputFrom
// adapts a go-ai output, such as ai.ObjectOutput.
type Output interface {
	ResponseFormat(ctx context.Context) (*provider.ResponseFormat, error)
	ParseOutput(ctx context.Context, options ai.ParseCompleteOutputOptions) (any, error)
}

// goaiOutput adapts a go-ai output to Output.
type goaiOutput[O, P any] struct {
	ai.Output[O, P]
}

// OutputFrom returns an Output that parses with a go-ai output. Result.Output
// then holds an O.
//
//	Output: workflowai.OutputFrom(ai.ObjectOutput[Itinerary](ai.ObjectOutputOptions{
//		Schema: ai.SchemaFor[Itinerary](),
//	})),
func OutputFrom[O, P any](output ai.Output[O, P]) Output {
	return goaiOutput[O, P]{output}
}

// ParseOutput parses the text with the go-ai output.
func (o goaiOutput[O, P]) ParseOutput(ctx context.Context, options ai.ParseCompleteOutputOptions) (any, error) {
	return o.ParseCompleteOutput(ctx, options)
}
