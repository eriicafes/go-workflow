package workflow_test

import (
	"context"
	"testing"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/testworld"
)

type routedInput struct {
	workflow.Struct[routedOutput]
}
type routedOutput struct{ Value string }

func TestWorkflowNameIsTheRecordedName(t *testing.T) {
	world := testworld.NewManual(workflow.Use(func(context.Context, structWorkflowInput) (structWorkflowOutput, error) {
		return structWorkflowOutput{}, nil
	}))
	defer world.Close()
	ctx := workflow.NewContext(context.Background(), workflow.NewClient(world))
	run, err := workflow.Start(ctx, structWorkflowInput{})
	if err != nil {
		t.Fatal(err)
	}
	recorded, err := run.WorkflowName(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if name := worlds.WorkflowName[structWorkflowInput](); name != recorded {
		t.Fatalf("WorkflowName = %q, recorded %q", name, recorded)
	}
	if pointer := worlds.WorkflowName[*structWorkflowInput](); pointer != recorded {
		t.Fatalf("WorkflowName of a pointer = %q, want %q", pointer, recorded)
	}
}

// routingWorld is a manual World that routes deliveries by workflow, so code
// that queues for a run it hasn't loaded reads the run's workflow name.
type routingWorld struct{ *testworld.World }

func (w routingWorld) Capabilities() worlds.Capabilities {
	capabilities := w.World.Capabilities()
	capabilities.QueueRouting = true
	return capabilities
}

func TestDeliveriesCarryTheWorkflowName(t *testing.T) {
	runner := workflow.Use(func(ctx context.Context, _ routedInput) (routedOutput, error) {
		ctx = workflow.SetMaxInlineSteps(ctx, 0)
		value, err := workflow.Step(ctx, func(context.Context, string) (string, error) { return "step", nil }, "")
		if err != nil {
			return routedOutput{}, err
		}
		hook := workflow.CreateHook[string](ctx, workflow.HookOption{Token: "routed"})
		defer hook.Dispose(ctx)
		resumed, err := hook.Next(ctx)
		return routedOutput{Value: value + ":" + resumed}, err
	})
	world := routingWorld{testworld.NewManual(runner)}
	defer world.Close()
	ctx := workflow.NewContext(context.Background(), workflow.NewClient(world))
	runtime := worlds.NewRuntime(world, worlds.Runners(runner))

	run, err := workflow.Start(ctx, routedInput{})
	if err != nil {
		t.Fatal(err)
	}
	executed := 0
	drain := func() {
		t.Helper()
		for ; executed < len(world.QueuedMessages()); executed++ {
			if err := runtime.Execute(ctx, world.QueuedMessages()[executed].Input); err != nil {
				t.Fatalf("delivery %d: %v", executed, err)
			}
		}
	}
	drain()
	if err := workflow.ResumeHook(ctx, "routed", "hook"); err != nil {
		t.Fatal(err)
	}
	drain()

	output, err := run.Value(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if output.Value != "step:hook" {
		t.Fatalf("output = %#v", output)
	}
	want := worlds.WorkflowName[routedInput]()
	steps := 0
	for _, message := range world.QueuedMessages() {
		if message.Input.WorkflowName != want {
			t.Errorf("delivery %+v has workflow %q, want %q", message.Input, message.Input.WorkflowName, want)
		}
		if message.Input.StepID != nil {
			steps++
		}
	}
	// the start, the step, its settled flow and the hook resume
	if len(world.QueuedMessages()) < 4 || steps == 0 {
		t.Fatalf("queued %d deliveries with %d steps, want flow and step deliveries", len(world.QueuedMessages()), steps)
	}
}
