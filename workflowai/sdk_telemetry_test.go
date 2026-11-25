package workflowai_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/digitallysavvy/go-ai/pkg/ai"
	"github.com/digitallysavvy/go-ai/pkg/provider"
	"github.com/digitallysavvy/go-ai/pkg/provider/types"
	"github.com/digitallysavvy/go-ai/pkg/telemetry"
	"github.com/eriicafes/go-workflow/workflowai"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// AI SDK: packages/workflow/src/workflow-agent-telemetry.test.ts > WorkflowAgent run telemetry > restores normalized step results, generated files, and aggregate end fields
func TestSDKTelemetryReportsFilesAndEndFields(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sdkTelemetryReported(t)
		recorder := sdkTelemetryRegister(t)
		file := types.GeneratedFileContent{MediaType: "image/png", Data: []byte{1, 2, 3}}
		setup(func() ([]provider.StreamChunk, error) {
			return []provider.StreamChunk{
				{Type: provider.ChunkTypeFile, GeneratedFileContent: &file},
				{Type: provider.ChunkTypeToolCall, ToolCall: &types.ToolCall{ID: "c1", ToolName: "getWeather", RawArguments: `{"city":"Lagos"}`}},
				{Type: provider.ChunkTypeFinish, FinishReason: types.FinishReasonToolCalls},
			}, nil
		}, text("done"))
		testOptions.Telemetry = &telemetry.Options{}
		if _, _, err := chat(t, []workflowai.ModelMessage{user("draw")}); err != nil {
			t.Fatal(err)
		}
		events := recorder.reported("onStart", "onStepEnd", "onEnd")
		if got := sdkTelemetryTypes(events); !slices.Equal(got, []string{"onStart", "onStepEnd", "onStepEnd", "onEnd"}) {
			t.Fatalf("events = %v", got)
		}
		for _, event := range events {
			if event.callID != events[0].callID {
				t.Fatalf("event %s has call ID %q, want %q", event.kind, event.callID, events[0].callID)
			}
		}
		if files := events[1].files; len(files) != 1 || files[0].MediaType != "image/png" || string(files[0].Data) != "\x01\x02\x03" {
			t.Fatalf("step files = %+v", files)
		}
		end := events[3]
		if end.text != "done" || len(end.files) != 1 || string(end.files[0].Data) != "\x01\x02\x03" {
			t.Fatalf("end event = %+v", end)
		}
	})
}

// AI SDK: packages/workflow/src/workflow-agent-telemetry.test.ts > WorkflowAgent run telemetry > closes incomplete runs and reports explicit errors
func TestSDKTelemetryReportsAbortsAndErrors(t *testing.T) {
	t.Run("aborted", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			sdkTelemetryReported(t)
			recorder := sdkTelemetryRegister(t)
			setup(hang)
			testOptions.Telemetry = &telemetry.Options{}
			testOptions.Timeout = 30 * time.Second
			_, _, _ = chat(t, []workflowai.ModelMessage{user("hi")})
			events := recorder.reported()
			if len(events) == 0 {
				t.Fatal("no events were reported")
			}
			last := events[len(events)-1]
			if last.kind != "onAbort" || last.callID != events[0].callID || last.steps != 0 {
				t.Fatalf("events = %v, last = %+v", sdkTelemetryTypes(events), last)
			}
		})
	})
	t.Run("failed", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			sdkTelemetryReported(t)
			recorder := sdkTelemetryRegister(t)
			setup(failWith(errors.New("boom")))
			testOptions.Telemetry = &telemetry.Options{}
			_, _, _ = chat(t, []workflowai.ModelMessage{user("hi")})
			events := recorder.reported()
			if len(events) == 0 {
				t.Fatal("no events were reported")
			}
			last := events[len(events)-1]
			if last.kind != "onError" || last.callID != events[0].callID || !strings.Contains(last.err, "boom") {
				t.Fatalf("events = %v, last = %+v", sdkTelemetryTypes(events), last)
			}
		})
	})
}

// AI SDK: packages/workflow/src/workflow-agent-telemetry.integration.test.ts > WorkflowAgent telemetry with global integrations > %s: ends every step span when stopping on a client-side tool
func TestSDKTelemetryClientToolStepSpans(t *testing.T) {
	for _, mode := range sdkApprovalModes {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				exporter := sdkTelemetryReported(t)
				recorder := sdkTelemetryRegister(t)
				sdkTelemetryRegisterOpenTelemetry(t, nil)
				sdkApprovalSetup(sdkApprovalResponse("", call("c1", "addNumbers", `{"a":2,"b":3}`)), sdkApprovalResponse("", call("c2", "askUser", `{"question":"Continue?"}`)))
				testOptions.Tools = sdkTelemetryTools()
				testOptions.Tools["askUser"] = workflowai.Tool{InputSchema: map[string]any{"type": "object", "properties": map[string]any{"question": map[string]any{"type": "string"}}, "required": []any{"question"}}}
				testOptions.Telemetry = &telemetry.Options{}
				stepEnds := &sdkApprovalRecorder{}
				testOptions.OnStepEnd = func(_ context.Context, step workflowai.StepResult) {
					stepEnds.record(fmt.Sprint(step.StepNumber), true)
				}
				run := sdkApprovalRun(t, mode, []workflowai.ModelMessage{user("Add 2 and 3, then ask to continue.")})
				if run.err != nil {
					t.Fatal(run.err)
				}
				steps := run.result.Steps
				if len(steps) != 2 || steps[1].ToolCalls[len(steps[1].ToolCalls)-1].ToolName != "askUser" {
					t.Fatalf("steps = %+v", steps)
				}
				// The public callback is not called for a paused stream step.
				if want := map[string]int{"generate": 2, "stream": 1}[mode]; stepEnds.len() != want {
					t.Fatalf("OnStepEnd called for %d steps, want %d", stepEnds.len(), want)
				}
				events := recorder.reported()
				types := sdkTelemetryTypes(events)
				if sdkTelemetryCount(types, "onStepEnd") != 2 || !slices.Equal(types[len(types)-2:], []string{"onStepEnd", "onEnd"}) {
					t.Fatalf("events = %v", types)
				}
				want := `{"invoke_agent fake-model":[{"step 1":["chat fake-model","execute_tool addNumbers"]},{"step 2":["chat fake-model"]}]}`
				if got := sdkTelemetryShape(t, exporter.GetSpans()); got != want {
					t.Fatalf("span tree = %s, want %s", got, want)
				}
			})
		})
	}
}

// AI SDK: packages/workflow/src/workflow-agent-telemetry.integration.test.ts > WorkflowAgent telemetry with global integrations > stream: dispatches model call telemetry from the model step
// AI SDK: packages/workflow/src/workflow-agent-telemetry.integration.test.ts > WorkflowAgent telemetry with global integrations > generate: dispatches model call telemetry from the model step
func TestSDKTelemetryModelCallEvents(t *testing.T) {
	for _, mode := range sdkApprovalModes {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				exporter := sdkTelemetryReported(t)
				recorder := sdkTelemetryRegister(t)
				sdkTelemetryRegisterOpenTelemetry(t, nil)
				setup(text("Echo: hello"))
				testOptions.Telemetry = &telemetry.Options{FunctionID: "telemetry-test"}
				run := sdkApprovalRun(t, mode, []workflowai.ModelMessage{user("hello")})
				if run.err != nil || run.result.Text != "Echo: hello" {
					t.Fatalf("result = %+v, err = %v", run.result, run.err)
				}
				// Go has no executeLanguageModelCall hook, so the events start
				// with onStart.
				events := recorder.reported()
				want := []string{"onStart", "onStepStart", "onLanguageModelCallStart", "onLanguageModelCallEnd", "onStepEnd", "onEnd"}
				if got := sdkTelemetryTypes(events); !slices.Equal(got, want) {
					t.Fatalf("events = %v, want %v", got, want)
				}
				if !strings.HasPrefix(events[0].callID, "call-") {
					t.Fatalf("call ID = %q", events[0].callID)
				}
				var names []string
				for _, span := range sdkTelemetryGenAI(exporter.GetSpans()) {
					names = append(names, span.Name)
				}
				if want := []string{"chat fake-model", "step 1", "invoke_agent fake-model"}; !slices.Equal(names, want) {
					t.Fatalf("spans = %v, want %v", names, want)
				}
			})
		})
	}
}

// AI SDK: packages/workflow/src/workflow-agent-telemetry.integration.test.ts > WorkflowAgent telemetry with global integrations > %s: includes only opted-in runtime context in model-step telemetry
func TestSDKTelemetryRuntimeContext(t *testing.T) {
	for _, mode := range []string{"stream", "generate"} {
		for _, include := range []*bool{nil, telemetry.Bool(false), telemetry.Bool(true)} {
			name := "unset"
			if include != nil {
				name = fmt.Sprint(*include)
			}
			t.Run(mode+"/"+name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					sdkTelemetryReported(t)
					recorder := sdkTelemetryRegister(t)
					var mu sync.Mutex
					var spanContexts []string
					sdkTelemetryRegisterOpenTelemetry(t, func(_ context.Context, options telemetry.EnrichSpanOptions) map[string]any {
						mu.Lock()
						defer mu.Unlock()
						spanContexts = append(spanContexts, sdkApprovalJSON(sdkTelemetryContext(options.RuntimeContext)))
						return nil
					})
					setup(text("Echo: hello"))
					options := &telemetry.Options{FunctionID: "telemetry-test"}
					if include != nil {
						options.IncludeRuntimeContext = map[string]bool{"traceIdentity": *include}
					}
					testOptions.Telemetry = options
					testOptions.RuntimeContext = map[string]any{
						"traceIdentity": map[string]any{"sessionId": "chat_1", "userId": "user_1"},
						"secret":        "must not cross the model-step boundary",
					}
					if run := sdkApprovalRun(t, mode, []workflowai.ModelMessage{user("hello")}); run.err != nil {
						t.Fatal(run.err)
					}
					included := `{}`
					if include != nil && *include {
						included = `{"traceIdentity":{"sessionId":"chat_1","userId":"user_1"}}`
					}
					// go-ai's model call start event has no runtime context,
					// so EnrichSpan gets none for the chat span.
					want := []string{included, included, `{}`}
					mu.Lock()
					defer mu.Unlock()
					if !slices.Equal(spanContexts, want) {
						t.Fatalf("span contexts = %v, want %v", spanContexts, want)
					}
					var eventContexts []string
					for _, event := range recorder.reported("onStart", "onStepStart", "onStepEnd", "onEnd") {
						eventContexts = append(eventContexts, sdkApprovalJSON(sdkTelemetryContext(event.runtimeContext)))
					}
					if len(eventContexts) != 4 || slices.ContainsFunc(eventContexts, func(got string) bool { return got != included }) {
						t.Fatalf("event contexts = %v, want 4 of %s", eventContexts, included)
					}
				})
			})
		}
	}
}

// AI SDK: packages/workflow/src/workflow-agent-telemetry.integration.test.ts > WorkflowAgent telemetry with global integrations > reports a multi-step run as one span tree like streamText
func TestSDKTelemetrySpanTreeLikeStreamText(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		exporter := sdkTelemetryReported(t)
		sdkTelemetryRegisterOpenTelemetry(t, nil)
		settings := &telemetry.Options{FunctionID: "issue-23618"}

		setup(sdkApprovalResponse("", call("c1", "addNumbers", `{"a":2,"b":3}`)), sdkApprovalResponse("The sum is 5"))
		result, err := ai.StreamText(t.Context(), ai.StreamTextOptions{
			Model:     testModel,
			Messages:  []types.Message{{Role: types.RoleUser, Content: []types.ContentPart{types.TextContent{Text: "Add 2 and 3"}}}},
			Tools:     []types.Tool{sdkTelemetryAddNumbers()},
			StopWhen:  []ai.StopCondition{ai.StepCountIs(2)},
			Telemetry: settings,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := result.ConsumeStream(); err != nil {
			t.Fatal(err)
		}
		baseline := exporter.GetSpans()
		exporter.Reset()

		setup(sdkApprovalResponse("", call("c1", "addNumbers", `{"a":2,"b":3}`)), sdkApprovalResponse("The sum is 5"))
		testOptions.Tools = sdkTelemetryTools()
		testOptions.Telemetry = settings
		run := sdkApprovalRun(t, "stream", []workflowai.ModelMessage{user("Add 2 and 3")})
		if run.err != nil || len(run.result.Steps) != 2 || run.result.Text != "The sum is 5" {
			t.Fatalf("result = %+v, err = %v", run.result, run.err)
		}
		spans := exporter.GetSpans()
		shape, baselineShape := sdkTelemetryShape(t, spans), sdkTelemetryShape(t, baseline)
		want := `{"invoke_agent fake-model":[{"step 1":["chat fake-model","execute_tool addNumbers"]},{"step 2":["chat fake-model"]}]}`
		if shape != baselineShape || shape != want {
			t.Fatalf("span tree = %s, streamText = %s, want %s", shape, baselineShape, want)
		}
		attribute := func(spans tracetest.SpanStubs, name, key string) string {
			for _, span := range sdkTelemetryGenAI(spans) {
				if span.Name != name {
					continue
				}
				for _, attr := range span.Attributes {
					if string(attr.Key) == key {
						return attr.Value.Emit()
					}
				}
			}
			return ""
		}
		for _, check := range [][2]string{
			{"invoke_agent fake-model", "gen_ai.input.messages"},
			{"invoke_agent fake-model", "gen_ai.output.messages"},
			{"chat fake-model", "gen_ai.tool.definitions"},
		} {
			if got, want := attribute(spans, check[0], check[1]), attribute(baseline, check[0], check[1]); got != want {
				t.Fatalf("%s %s = %s, streamText has %s", check[0], check[1], got, want)
			}
		}
		byID := map[string]tracetest.SpanStub{}
		for _, span := range sdkTelemetryGenAI(spans) {
			byID[span.SpanContext.SpanID().String()] = span
		}
		for _, span := range byID {
			parent, ok := byID[span.Parent.SpanID().String()]
			if ok && (span.StartTime.Before(parent.StartTime) || span.EndTime.After(parent.EndTime)) {
				t.Fatalf("span %q %v..%v is outside its parent %q %v..%v", span.Name, span.StartTime, span.EndTime, parent.Name, parent.StartTime, parent.EndTime)
			}
		}
	})
}

// sdkTelemetryReported sets up a global tracer provider with
// TelemetryIDGenerator and NewTelemetrySpanProcessor, which turn on reported
// telemetry, and returns its exporter.
func sdkTelemetryReported(t *testing.T) *tracetest.InMemoryExporter {
	t.Helper()
	exporter := tracetest.NewInMemoryExporter()
	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithIDGenerator(workflowai.TelemetryIDGenerator{}),
		sdktrace.WithSpanProcessor(workflowai.NewTelemetrySpanProcessor(sdktrace.NewSimpleSpanProcessor(exporter))),
	)
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(tracerProvider)
	t.Cleanup(func() {
		otel.SetTracerProvider(previous)
		_ = tracerProvider.Shutdown(context.Background())
	})
	return exporter
}

// sdkTelemetryRegisterOpenTelemetry registers go-ai's OpenTelemetry
// integration globally, drawing its spans with the go-ai tracer.
func sdkTelemetryRegisterOpenTelemetry(t *testing.T, enrich telemetry.EnrichSpanFunc) {
	t.Helper()
	telemetry.RegisterTelemetryIntegration(telemetry.NewOpenTelemetry(telemetry.OpenTelemetryOptions{
		Tracer:         otel.GetTracerProvider().Tracer("go-ai"),
		EnrichSpan:     enrich,
		RuntimeContext: true,
	}))
	t.Cleanup(telemetry.ClearTelemetryIntegrations)
}

// sdkTelemetryRegister registers a recording integration globally.
func sdkTelemetryRegister(t *testing.T) *sdkTelemetryRecorder {
	t.Helper()
	recorder := &sdkTelemetryRecorder{}
	telemetry.RegisterTelemetryIntegration(recorder)
	t.Cleanup(telemetry.ClearTelemetryIntegrations)
	return recorder
}

// sdkTelemetryEvent is an event a sdkTelemetryRecorder received.
type sdkTelemetryEvent struct {
	kind           string
	operation      string
	callID         string
	text           string
	err            string
	steps          int
	files          []types.GeneratedFileContent
	runtimeContext map[string]any
}

// sdkTelemetryRecorder records the events it receives.
type sdkTelemetryRecorder struct {
	telemetry.NoopTelemetryIntegration
	mu     sync.Mutex
	events []sdkTelemetryEvent
	// operations maps call IDs to their operations.
	operations map[string]string
}

func (r *sdkTelemetryRecorder) add(event sdkTelemetryEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.operations == nil {
		r.operations = map[string]string{}
	}
	if event.operation != "" {
		r.operations[event.callID] = event.operation
	}
	r.events = append(r.events, event)
}

// reported returns the events of the calls the agent reported, of the given
// kinds or of every kind.
func (r *sdkTelemetryRecorder) reported(kinds ...string) []sdkTelemetryEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []sdkTelemetryEvent
	for _, event := range r.events {
		if len(kinds) == 0 || slices.Contains(kinds, event.kind) {
			out = append(out, event)
		}
	}
	return out
}

func (r *sdkTelemetryRecorder) OnStart(ctx context.Context, e telemetry.TelemetryStartEvent) context.Context {
	r.add(sdkTelemetryEvent{kind: "onStart", operation: e.OperationType, callID: e.CallID, runtimeContext: e.RuntimeContext})
	return ctx
}

func (r *sdkTelemetryRecorder) OnStepStart(ctx context.Context, e telemetry.TelemetryStepStartEvent) context.Context {
	r.add(sdkTelemetryEvent{kind: "onStepStart", callID: e.CallID, runtimeContext: e.RuntimeContext})
	return ctx
}

func (r *sdkTelemetryRecorder) OnLanguageModelCallStart(ctx context.Context, e telemetry.LanguageModelCallStartEvent) context.Context {
	r.add(sdkTelemetryEvent{kind: "onLanguageModelCallStart", callID: e.CallID})
	return ctx
}

func (r *sdkTelemetryRecorder) OnLanguageModelCallEnd(_ context.Context, e telemetry.LanguageModelCallEndEvent) {
	r.add(sdkTelemetryEvent{kind: "onLanguageModelCallEnd", callID: e.CallID})
}

func (r *sdkTelemetryRecorder) OnStepEnd(_ context.Context, e telemetry.TelemetryStepEndEvent) {
	r.add(sdkTelemetryEvent{kind: "onStepEnd", callID: e.CallID, text: e.Text, files: e.Files, runtimeContext: e.RuntimeContext})
}

func (r *sdkTelemetryRecorder) OnEnd(_ context.Context, e telemetry.TelemetryFinishEvent) {
	r.add(sdkTelemetryEvent{kind: "onEnd", callID: e.CallID, text: e.Text, files: e.Files, runtimeContext: e.RuntimeContext})
}

func (r *sdkTelemetryRecorder) OnError(_ context.Context, e telemetry.TelemetryErrorEvent) {
	r.add(sdkTelemetryEvent{kind: "onError", callID: e.CallID, err: fmt.Sprint(e.Error)})
}

func (r *sdkTelemetryRecorder) OnAbort(_ context.Context, e telemetry.TelemetryAbortEvent) {
	r.add(sdkTelemetryEvent{kind: "onAbort", callID: e.CallID, steps: len(e.Steps)})
}

func sdkTelemetryTypes(events []sdkTelemetryEvent) []string {
	var out []string
	for _, event := range events {
		out = append(out, event.kind)
	}
	return out
}

func sdkTelemetryCount(values []string, value string) int {
	count := 0
	for _, v := range values {
		if v == value {
			count++
		}
	}
	return count
}

// sdkTelemetryContext returns runtime context, or an empty one.
func sdkTelemetryContext(runtimeContext map[string]any) map[string]any {
	if runtimeContext == nil {
		return map[string]any{}
	}
	return runtimeContext
}

// sdkTelemetryTools returns the addNumbers tool of the upstream workflows.
func sdkTelemetryTools() workflowai.ToolSet {
	return workflowai.ToolSet{"addNumbers": {Description: "Add two numbers", Execute: workflowai.ExecuteStep(sdkTelemetryAdd)}}
}

type sdkTelemetryNumbers struct {
	A float64 `json:"a"`
	B float64 `json:"b"`
}

func sdkTelemetryAdd(_ context.Context, input sdkTelemetryNumbers) (float64, error) {
	return input.A + input.B, nil
}

// sdkTelemetryAddNumbers is the addNumbers tool for go-ai's StreamText.
func sdkTelemetryAddNumbers() types.Tool {
	return types.Tool{
		Name:        "addNumbers",
		Description: "Add two numbers",
		Parameters:  ai.SchemaFor[sdkTelemetryNumbers]().Validator().JSONSchema(),
		Execute: func(_ context.Context, args map[string]any, _ types.ToolExecutionOptions) (any, error) {
			return args["a"].(float64) + args["b"].(float64), nil
		},
	}
}

// sdkTelemetryGenAI returns the spans of the go-ai tracer, in start order.
func sdkTelemetryGenAI(spans tracetest.SpanStubs) tracetest.SpanStubs {
	var out tracetest.SpanStubs
	for _, span := range spans {
		if span.InstrumentationScope.Name == "go-ai" {
			out = append(out, span)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].StartTime.Before(out[j].StartTime) })
	return out
}

// sdkTelemetryShape returns the gen_ai spans' tree as JSON: a span without
// children is its name, and a span with children an object from its name to
// its children in start order.
func sdkTelemetryShape(t *testing.T, all tracetest.SpanStubs) string {
	t.Helper()
	spans := sdkTelemetryGenAI(all)
	ids := map[string]bool{}
	for _, span := range spans {
		ids[span.SpanContext.SpanID().String()] = true
	}
	var shape func(span tracetest.SpanStub) any
	shape = func(span tracetest.SpanStub) any {
		var children []any
		for _, child := range spans {
			if child.Parent.SpanID() == span.SpanContext.SpanID() {
				children = append(children, shape(child))
			}
		}
		if len(children) == 0 {
			return span.Name
		}
		return map[string]any{span.Name: children}
	}
	var roots []any
	for _, span := range spans {
		if !ids[span.Parent.SpanID().String()] {
			roots = append(roots, shape(span))
		}
	}
	var value any = roots
	if len(roots) == 1 {
		value = roots[0]
	}
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
