package sdke2e

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	workflow "github.com/eriicafes/go-workflow"
)

type fetchWorkflow struct {
	workflow.Struct[map[string]any]
}

func fetchRun(ctx context.Context, _ fetchWorkflow) (map[string]any, error) {
	return workflow.Step(ctx, fetchJSON, "https://jsonplaceholder.typicode.com/todos/1")
}

// unserializable is a value no payload can hold, as a class instance with no
// serialization is in TypeScript.
type unserializable struct {
	Secret chan string
}

type serializationCaught struct {
	Caught                       bool   `json:"caught"`
	Name                         string `json:"name,omitempty"`
	MessageIncludesStepArguments bool   `json:"messageIncludesStepArguments,omitempty"`
	MessageIncludesReturnValue   bool   `json:"messageIncludesReturnValue,omitempty"`
}

type serializationErrorStepArgsCaught struct {
	workflow.Struct[serializationCaught]
}

func serializationErrorStepArgsCaughtRun(ctx context.Context, _ serializationErrorStepArgsCaught) (serializationCaught, error) {
	_, err := workflow.Step(ctx, acceptAnyValue, any(unserializable{Secret: make(chan string)}))
	if err == nil {
		return serializationCaught{}, nil
	}
	return serializationCaught{Caught: true, Name: errorName(err), MessageIncludesStepArguments: strings.Contains(err.Error(), "Failed to serialize step arguments")}, nil
}

type serializationErrorStepArgsUncaught struct {
	workflow.Struct[serializationCaught]
}

func serializationErrorStepArgsUncaughtRun(ctx context.Context, _ serializationErrorStepArgsUncaught) (serializationCaught, error) {
	_, err := workflow.Step(ctx, acceptAnyValue, any(unserializable{Secret: make(chan string)}))
	return serializationCaught{}, err
}

type serializationErrorStepReturnCaught struct {
	workflow.Struct[serializationCaught]
}

func serializationErrorStepReturnCaughtRun(ctx context.Context, _ serializationErrorStepReturnCaught) (serializationCaught, error) {
	_, err := workflow.Step(ctx, returnUnserializableValue, struct{}{})
	if err == nil {
		return serializationCaught{}, nil
	}
	return serializationCaught{Caught: true, Name: errorName(err), MessageIncludesReturnValue: strings.Contains(err.Error(), "Failed to serialize step return value")}, nil
}

type serializationErrorStepReturnUncaught struct {
	workflow.Struct[serializationCaught]
}

func serializationErrorStepReturnUncaughtRun(ctx context.Context, _ serializationErrorStepReturnUncaught) (serializationCaught, error) {
	_, err := workflow.Step(ctx, returnUnserializableValue, struct{}{})
	return serializationCaught{}, err
}

type received struct {
	Received bool `json:"received"`
}

func acceptAnyValue(_ context.Context, value any) (received, error) {
	return received{Received: value != nil}, nil
}

func returnUnserializableValue(context.Context, struct{}) (any, error) {
	return unserializable{Secret: make(chan string)}, nil
}

func fetchJSON(ctx context.Context, url string) (map[string]any, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	var data map[string]any
	return data, json.NewDecoder(response.Body).Decode(&data)
}

// errorName returns the name a JavaScript reader gives a recorded error.
func errorName(err error) string {
	var recorded *workflow.Error
	switch {
	case !errors.As(err, &recorded):
		return "Error"
	case recorded.Name != "":
		return recorded.Name
	case recorded.Type != "":
		return recorded.Type
	}
	return "Error"
}
