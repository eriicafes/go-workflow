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

func TestRunStreamsReadFromAChunkIndex(t *testing.T) {
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
	writer, err := run.Writer(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte(" world")); err != nil {
		t.Fatal(err)
	}
	if err := writer.CloseStream(); err != nil {
		t.Fatal(err)
	}
	reader, err := run.Reader(ctx, workflow.ReaderOption{StartIndex: 1})
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(reader)
	if err != nil || string(data) != " world" {
		t.Fatalf("stream from chunk 1 = %q, %v", data, err)
	}
	if tail, err := reader.TailIndex(); err != nil || tail != 1 {
		t.Fatalf("tail index = %d, %v", tail, err)
	}

	// Next returns whole chunks with their indexes, and the rest of a chunk
	// after a partial Read.
	reader, err = run.Reader(ctx)
	if err != nil {
		t.Fatal(err)
	}
	part := make([]byte, 2)
	if n, err := reader.Read(part); err != nil || string(part[:n]) != "he" {
		t.Fatalf("partial read = %q, %v", part[:n], err)
	}
	for _, want := range []workflow.StreamChunk{{Index: 0, Data: []byte("llo")}, {Index: 1, Data: []byte(" world")}} {
		chunk, err := reader.Next()
		if err != nil || chunk.Index != want.Index || string(chunk.Data) != string(want.Data) {
			t.Fatalf("chunk = %d %q, %v, want %d %q", chunk.Index, chunk.Data, err, want.Index, want.Data)
		}
	}
	if _, err := reader.Next(); err != io.EOF {
		t.Fatalf("Next after the last chunk = %v, want io.EOF", err)
	}
}

func TestNamedStreamReadsLastChunksAndClosedWritersRejectWrites(t *testing.T) {
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
	writer, err := run.Writer(ctx, workflow.WriterOption{Name: name})
	if err != nil {
		t.Fatal(err)
	}
	for _, chunk := range []string{"abc", "def", "ghi"} {
		if _, err := writer.Write([]byte(chunk)); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.CloseStream(); err != nil {
		t.Fatal(err)
	}
	if err := writer.CloseStream(); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte("later")); err == nil {
		t.Fatal("closed writer accepted another write")
	}
	reader, err := run.Reader(ctx, workflow.ReaderOption{Name: name, StartIndex: -2})
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(reader)
	if err != nil || string(data) != "defghi" {
		t.Fatalf("last two chunks = %q, %v", data, err)
	}
}

func TestStepStreamReaderAndWriterUseTheActiveStep(t *testing.T) {
	world := testworld.New(workflow.Use(func(ctx context.Context, _ streamStepInput) (streamStepOutput, error) {
		return workflow.Step(ctx, func(ctx context.Context, _ struct{}) (streamStepOutput, error) {
			writer, err := workflow.Writer(ctx)
			if err != nil {
				return streamStepOutput{}, err
			}
			if _, err := writer.Write([]byte("step stream")); err != nil {
				return streamStepOutput{}, err
			}
			if err := writer.CloseStream(); err != nil {
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

type interleaveInput struct{}
type interleaveOutput struct{ Data string }

func (interleaveInput) Workflow(interleaveOutput) {}

// TestWritersDoNotWaitForEachOther checks that writers of one stream write at
// once, that Close leaves the stream open for the others, and that a step
// stores what it wrote when it returns without closing its writer.
func TestWritersDoNotWaitForEachOther(t *testing.T) {
	world := testworld.New(workflow.Use(func(ctx context.Context, _ interleaveInput) (interleaveOutput, error) {
		_, err := workflow.Step(ctx, func(ctx context.Context, _ struct{}) (struct{}, error) {
			first, err := workflow.Writer(ctx)
			if err != nil {
				return struct{}{}, err
			}
			second, err := workflow.Writer(ctx)
			if err != nil {
				return struct{}{}, err
			}
			if _, err := first.Write([]byte("a")); err != nil {
				return struct{}{}, err
			}
			if err := first.Flush(); err != nil {
				return struct{}{}, err
			}
			if _, err := second.Write([]byte("b")); err != nil {
				return struct{}{}, err
			}
			if err := first.Close(); err != nil {
				return struct{}{}, err
			}
			// second is still open: the step stores its chunk on return.
			return struct{}{}, nil
		}, struct{}{})
		return interleaveOutput{}, err
	}))
	defer world.Close()
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), 2*time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, interleaveInput{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := run.Value(ctx); err != nil {
		t.Fatal(err)
	}
	// The stream is still open, so another writer can continue it.
	writer, err := run.Writer(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte("c")); err != nil {
		t.Fatal(err)
	}
	if err := writer.CloseStream(); err != nil {
		t.Fatal(err)
	}
	reader, err := run.Reader(ctx)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(reader)
	if err != nil || string(data) != "abc" {
		t.Fatalf("stream = %q, %v", data, err)
	}

	// A closed stream drops a late writer's writes, as world-local does.
	late, err := run.Writer(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := late.Write([]byte("late")); err != nil {
		t.Fatal(err)
	}
	if err := late.Flush(); err != nil {
		t.Fatalf("flush to a closed stream = %v", err)
	}
	again, err := run.Reader(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if data, err := io.ReadAll(again); err != nil || string(data) != "abc" {
		t.Fatalf("stream after a late write = %q, %v", data, err)
	}
}
