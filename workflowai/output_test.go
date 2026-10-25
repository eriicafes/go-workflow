package workflowai_test

import (
	"context"
	"slices"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/digitallysavvy/go-ai/pkg/ai"
	"github.com/eriicafes/go-workflow/workflowai"
)

type itinerary struct {
	City string `json:"city"`
	Days int    `json:"days"`
}

func itineraryOutput() workflowai.Output {
	return workflowai.OutputFrom(ai.ObjectOutput[itinerary](ai.ObjectOutputOptions{Schema: ai.SchemaFor[itinerary]()}))
}

func TestStreamOutput(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		setup(text(`{"city":"Lagos","days":3}`))
		testOptions.Output = itineraryOutput()
		var mu sync.Mutex
		var output any
		testOptions.OnEnd = func(_ context.Context, result workflowai.Result) {
			mu.Lock()
			defer mu.Unlock()
			output = result.Output
		}
		result, _, err := chat(t, []workflowai.ModelMessage{user("plan a trip")})
		if err != nil {
			t.Fatal(err)
		}
		// In the runner the output has the Output's type. The run's JSON
		// result holds it as JSON.
		mu.Lock()
		defer mu.Unlock()
		if output != (itinerary{City: "Lagos", Days: 3}) {
			t.Fatalf("output = %#v", output)
		}
		if decoded, ok := result.Output.(map[string]any); !ok || decoded["city"] != "Lagos" {
			t.Fatalf("result output = %#v", result.Output)
		}
		format := testModel.calls[0].ResponseFormat
		if format == nil || format.Type != "json" || format.Schema == nil {
			t.Fatalf("response format = %+v", format)
		}
	})
}

func TestStreamOutputParseFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		setup(text("Lagos for three days"))
		testOptions.Output = itineraryOutput()
		_, chunks, err := chat(t, []workflowai.ModelMessage{user("plan a trip")})
		if err == nil {
			t.Fatal("expected a parse error")
		}
		if !slices.Contains(chunkTypes(chunks), "error") {
			t.Fatalf("chunks = %v", chunkTypes(chunks))
		}
	})
}

func TestStreamOutputSkipsToolSteps(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		setup(toolCalls(call("c1", "bookFlight", `{"flightId":"LH123"}`)))
		testOptions.Output = itineraryOutput()
		result, _, err := chat(t, []workflowai.ModelMessage{user("book LH123")})
		if err != nil {
			t.Fatal(err)
		}
		// The turn stopped for approval, so there is no final answer to parse.
		if result.Output != nil {
			t.Fatalf("output = %#v", result.Output)
		}
	})
}
