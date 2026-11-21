package postgresworld

import (
	"context"
	"strings"
	"testing"

	"github.com/eriicafes/go-workflow/worlds"
)

func TestStreamWithALongNameNotifies(t *testing.T) {
	t.Parallel()
	world := newTestWorld(t)
	ctx := context.Background()
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runA, WorkflowName: "w"}); err != nil {
		t.Fatal(err)
	}
	name := strings.Repeat("n", 10_000)
	if err := world.StreamsWrite(ctx, worlds.StreamsWriteInput{RunID: runA, Name: name, Chunk: []byte("a")}); err != nil {
		t.Fatalf("write to a stream with a long name: %v", err)
	}
}
