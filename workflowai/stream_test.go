package workflowai_test

import (
	"slices"
	"testing"
	"testing/synctest"

	"github.com/digitallysavvy/go-ai/pkg/ai"
	"github.com/digitallysavvy/go-ai/pkg/provider"
	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/workflowai"
	"github.com/eriicafes/go-workflow/worlds/testworld"
)

func TestUIMessageChunksResume(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		setup(text("Hello!"))
		_, all, err := chat(t, []workflowai.ModelMessage{user("hi")})
		if err != nil {
			t.Fatal(err)
		}

		testModel = &fakeModel{responses: []func() ([]provider.StreamChunk, error){text("Hello!")}}
		world := testworld.New(workflow.Use(ChatRunner))
		defer world.Close()
		ctx := workflow.NewContext(t.Context(), workflow.NewClient(world))
		run, err := workflow.Start(ctx, Chat{Messages: []workflowai.ModelMessage{user("hi")}})
		if err != nil {
			t.Fatal(err)
		}
		reader, err := run.Reader(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var resumed []ai.UIMessageChunk
		for chunk := range workflowai.UIMessageChunks(ctx, reader, 4) {
			resumed = append(resumed, chunk)
		}
		if got, want := chunkTypes(resumed), chunkTypes(all[4:]); !slices.Equal(got, want) {
			t.Fatalf("resumed = %v, want %v", got, want)
		}
	})
}
