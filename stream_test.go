package workflow_test

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/worlds/testworld"
)

type streamInput struct{ Value int }
type streamOutput struct{ Value int }
type streamStepInput struct{}
type streamStepOutput struct{ Data string }

func (streamInput) Workflow(streamOutput)         {}
func (streamStepInput) Workflow(streamStepOutput) {}

func TestRunStreamsSupportWritersReadersAndByteOffsets(t *testing.T) {
	world := testworld.New(workflow.Use(func(_ context.Context, input streamInput) (streamOutput, error) {
		return streamOutput{Value: input.Value}, nil
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, streamInput{Value: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := run.Value(ctx); err != nil {
		t.Fatal(err)
	}
	writer, release, err := run.Writer(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte(" world")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := release(); err != nil {
		t.Fatal(err)
	}
	reader, err := run.ReaderAt(ctx, 6)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "world" {
		t.Fatalf("stream = %q", data)
	}
}

func TestNamedStreamSupportsNegativeOffsetsAndClosedWritersRejectWrites(t *testing.T) {
	world := testworld.New(workflow.Use(func(_ context.Context, input streamInput) (streamOutput, error) {
		return streamOutput{Value: input.Value}, nil
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, streamInput{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := run.Value(ctx); err != nil {
		t.Fatal(err)
	}
	name := "audit"
	writer, release, err := run.Writer(ctx, workflow.WriterOption{Name: name})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte("abcdef")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte("later")); err == nil {
		t.Fatal("closed writer accepted another write")
	}
	if err := release(); err != nil {
		t.Fatal(err)
	}
	reader, err := run.ReaderAt(ctx, -3, workflow.ReaderOption{Name: name})
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(reader)
	if err != nil || string(data) != "def" {
		t.Fatalf("negative-offset stream = %q, %v", data, err)
	}
}

func TestStepStreamReaderAndWriterUseTheActiveStep(t *testing.T) {
	world := testworld.New(workflow.Use(func(ctx context.Context, _ streamStepInput) (streamStepOutput, error) {
		return workflow.Step(ctx, func(ctx context.Context, _ struct{}) (streamStepOutput, error) {
			writer, release, err := workflow.Writer(ctx)
			if err != nil {
				return streamStepOutput{}, err
			}
			if _, err := writer.Write([]byte("step stream")); err != nil {
				return streamStepOutput{}, err
			}
			if err := writer.Close(); err != nil {
				return streamStepOutput{}, err
			}
			if err := release(); err != nil {
				return streamStepOutput{}, err
			}
			reader, err := workflow.Reader(ctx)
			if err != nil {
				return streamStepOutput{}, err
			}
			data, err := io.ReadAll(reader)
			return streamStepOutput{Data: string(data)}, err
		}, struct{}{})
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, streamStepInput{})
	if err != nil {
		t.Fatal(err)
	}
	output, err := run.Value(ctx)
	if err != nil || output.Data != "step stream" {
		t.Fatalf("step stream output = %#v, %v", output, err)
	}
}
