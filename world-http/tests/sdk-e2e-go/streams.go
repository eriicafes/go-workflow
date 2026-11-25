package sdke2e

import (
	"context"
	"time"

	workflow "github.com/eriicafes/go-workflow"
)

// A Go workflow writes its streams only from steps, so where a TypeScript
// fixture hands its writable to a step, the Go step opens the stream by name.

type outputStreamWorkflow struct{ workflow.Struct[string] }

func outputStreamRun(ctx context.Context, _ outputStreamWorkflow) (string, error) {
	writes := []streamWrite{
		{Value: []byte("Hello, world!")},
		{Name: "test", Value: []byte("Hello, named stream!")},
		{Value: map[string]string{"foo": "test"}},
		{Name: "test", Value: map[string]string{"foo": "bar"}},
	}
	for _, write := range writes {
		if err := workflow.Sleep(ctx, time.Second); err != nil {
			return "", err
		}
		if _, err := workflow.Step(ctx, writeStream, write); err != nil {
			return "", err
		}
	}
	if err := workflow.Sleep(ctx, time.Second); err != nil {
		return "", err
	}
	for _, name := range []string{"", "test"} {
		if _, err := workflow.Step(ctx, closeStream, name); err != nil {
			return "", err
		}
	}
	return "done", nil
}

type outputStreamInsideStepWorkflow struct{ workflow.Struct[string] }

func outputStreamInsideStepRun(ctx context.Context, _ outputStreamInsideStepWorkflow) (string, error) {
	writes := []streamWrite{
		{Value: []byte("Hello from step!")},
		{Name: "step-ns", Value: map[string]string{"message": "Hello from named stream in step!"}},
		{Value: []byte("Second message")},
		{Name: "step-ns", Value: map[string]int{"counter": 42}},
	}
	for _, write := range writes {
		if err := workflow.Sleep(ctx, time.Second); err != nil {
			return "", err
		}
		if _, err := workflow.Step(ctx, writeStream, write); err != nil {
			return "", err
		}
	}
	if err := workflow.Sleep(ctx, time.Second); err != nil {
		return "", err
	}
	for _, name := range []string{"", "step-ns"} {
		if _, err := workflow.Step(ctx, closeStream, name); err != nil {
			return "", err
		}
	}
	return "done", nil
}

type utf8StreamWorkflow struct{ workflow.Struct[string] }

func utf8StreamRun(ctx context.Context, _ utf8StreamWorkflow) (string, error) {
	if err := workflow.Sleep(ctx, time.Second); err != nil {
		return "", err
	}
	texts := []string{
		"Hello, world!",
		"Café — naïve résumé",
		"你好，世界！🌍✨",
		"مرحبا بالعالم",
		`{"greeting":"안녕하세요","emoji":"🎉"}`,
	}
	for _, text := range texts {
		if _, err := workflow.Step(ctx, writeStream, streamWrite{Value: []byte(text)}); err != nil {
			return "", err
		}
	}
	if _, err := workflow.Step(ctx, closeStream, ""); err != nil {
		return "", err
	}
	return "done", nil
}

type childRunID struct {
	ChildRunID string `json:"childRunId"`
}

// writableForwardedChildWorkflow writes to its parent's stream, which it
// names by the parent's run ID, as a Go workflow cannot pass a writable.
type writableForwardedChildWorkflow struct {
	workflow.StructArgs[string]
	ParentRunID string
	Payload     string
}

func writableForwardedChildRun(ctx context.Context, input writableForwardedChildWorkflow) (string, error) {
	if _, err := workflow.Step(ctx, writeBytesToRun, runWrite{RunID: input.ParentRunID, Payload: input.Payload}); err != nil {
		return "", err
	}
	return "child-done", nil
}

type writableForwardedFromWorkflowWorkflow struct {
	workflow.StructArgs[childRunID]
	Payload string
}

func writableForwardedFromWorkflowRun(ctx context.Context, input writableForwardedFromWorkflowWorkflow) (childRunID, error) {
	child, err := workflow.Step(ctx, startChildWithWorkflowWritable, runWrite{RunID: workflow.GetWorkflowMetadata(ctx).RunID, Payload: input.Payload})
	if err != nil {
		return childRunID{}, err
	}
	if _, err := workflow.Step(ctx, closeStream, ""); err != nil {
		return childRunID{}, err
	}
	return childRunID{ChildRunID: child}, nil
}

type writableForwardedFromStepWorkflow struct {
	workflow.StructArgs[childRunID]
	Payload string
}

func writableForwardedFromStepRun(ctx context.Context, input writableForwardedFromStepWorkflow) (childRunID, error) {
	child, err := workflow.Step(ctx, startChildWithStepWritable, input.Payload)
	return childRunID{ChildRunID: child}, err
}

// abortMessage is the chunk distributedAbortControllerWorkflow writes when
// it aborts.
type abortMessage struct {
	Type    string `json:"type"`
	Reason  string `json:"reason,omitempty"`
	Expired bool   `json:"expired"`
}

type abortOutcome struct {
	Aborted bool   `json:"aborted"`
	Reason  string `json:"reason,omitempty"`
	Expired bool   `json:"expired"`
}

type abortReason struct {
	Reason string `json:"reason,omitempty"`
}

type distributedAbortControllerWorkflow struct {
	workflow.StructArgs[abortOutcome]
	ID      string
	TTLMs   int
	GraceMs int
}

func distributedAbortControllerRun(ctx context.Context, input distributedAbortControllerWorkflow) (abortOutcome, error) {
	startTime := workflow.Now(ctx)
	hook := workflow.CreateHook[abortReason](ctx, workflow.HookOption{Token: "distributed-abort:" + input.ID})
	timer := workflow.Timer(ctx, time.Duration(input.TTLMs)*time.Millisecond)
	winner, err := workflow.Select(ctx, hook, timer)
	if err != nil {
		return abortOutcome{}, err
	}
	message := abortMessage{Type: "abort", Reason: "Controller expired", Expired: true}
	if winner == hook {
		payload, err := hook.Next(ctx)
		if err != nil {
			return abortOutcome{}, err
		}
		message = abortMessage{Type: "abort", Reason: payload.Reason}
	}
	if _, err := workflow.Step(ctx, writeAbortSignal, message); err != nil {
		return abortOutcome{}, err
	}
	// After a TTL expiration, the hook stays alive through the grace period
	// for late subscribers.
	if message.Expired {
		elapsed := workflow.Now(ctx).Sub(startTime)
		remaining := time.Duration(input.GraceMs)*time.Millisecond - (elapsed - time.Duration(input.TTLMs)*time.Millisecond)
		if remaining > 0 {
			if err := workflow.Sleep(ctx, remaining); err != nil {
				return abortOutcome{}, err
			}
		}
	}
	return abortOutcome{Aborted: true, Reason: message.Reason, Expired: message.Expired}, nil
}

// streamWrite is the arguments of a step that writes Value as one chunk of
// the stream Name.
type streamWrite struct {
	Name  string
	Value any
}

// runWrite is the arguments of a step that writes Payload to the default
// stream of the run RunID.
type runWrite struct {
	RunID   string
	Payload string
}

func writeStream(ctx context.Context, in streamWrite) (struct{}, error) {
	writer, err := workflow.Writer(ctx, workflow.WriterOption{Name: in.Name})
	if err != nil {
		return struct{}{}, err
	}
	if data, ok := in.Value.([]byte); ok {
		_, err = writer.Write(data)
	} else {
		err = writer.WriteValue(in.Value)
	}
	if err != nil {
		return struct{}{}, err
	}
	return struct{}{}, writer.Close()
}

func closeStream(ctx context.Context, name string) (struct{}, error) {
	writer, err := workflow.Writer(ctx, workflow.WriterOption{Name: name})
	if err != nil {
		return struct{}{}, err
	}
	return struct{}{}, writer.CloseStream()
}

func writeBytesToRun(ctx context.Context, in runWrite) (struct{}, error) {
	run, err := workflow.Get[any](ctx, in.RunID)
	if err != nil {
		return struct{}{}, err
	}
	writer, err := run.Writer(ctx)
	if err != nil {
		return struct{}{}, err
	}
	if _, err := writer.Write([]byte(in.Payload)); err != nil {
		return struct{}{}, err
	}
	return struct{}{}, writer.Close()
}

func startChildWithWorkflowWritable(ctx context.Context, in runWrite) (string, error) {
	child, err := workflow.Start(ctx, writableForwardedChildWorkflow{ParentRunID: in.RunID, Payload: in.Payload})
	if err != nil {
		return "", err
	}
	// The child finishes writing before the parent closes its stream.
	if _, err := child.Value(ctx); err != nil {
		return "", err
	}
	return child.ID(), nil
}

func startChildWithStepWritable(ctx context.Context, payload string) (string, error) {
	writer, err := workflow.Writer(ctx)
	if err != nil {
		return "", err
	}
	child, err := workflow.Start(ctx, writableForwardedChildWorkflow{ParentRunID: workflow.GetWorkflowMetadata(ctx).RunID, Payload: payload})
	if err != nil {
		return "", err
	}
	if _, err := child.Value(ctx); err != nil {
		return "", err
	}
	return child.ID(), writer.CloseStream()
}

func writeAbortSignal(ctx context.Context, message abortMessage) (struct{}, error) {
	writer, err := workflow.Writer(ctx)
	if err != nil {
		return struct{}{}, err
	}
	if err := writer.WriteValue(message); err != nil {
		return struct{}{}, err
	}
	return struct{}{}, writer.CloseStream()
}
