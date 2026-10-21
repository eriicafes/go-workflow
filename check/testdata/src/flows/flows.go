package flows

import (
	"context"
	"math/rand/v2"
	"time"

	"github.com/eriicafes/go-workflow"
)

type Input struct{ Name string }
type Output struct{ Name string }

func (Input) Workflow(Output) {}

var _ = workflow.Use(Report)

func Report(ctx context.Context, in Input) (Output, error) {
	// A local helper runs in workflow context.
	now := func() int64 { return time.Now().Unix() } // want `workflow body calls time.Now, use workflow.Now instead`
	_ = now()
	_ = rand.IntN(3)             // want `workflow body calls math/rand/v2.IntN, use workflow.Step or workflow.Background instead`
	_ = time.Since(time.Time{})  // want `workflow body calls time.Since, use workflow.Now instead`
	time.Sleep(time.Millisecond) // want `workflow body calls time.Sleep, use workflow.Sleep instead`

	// A step closure may read fields of its input and name its results.
	out, err := workflow.Step(ctx, func(ctx context.Context, in Input) (out Output, err error) {
		out = Output{Name: in.Name}
		_ = time.Now()
		return out, nil
	}, in)
	if err != nil {
		return Output{}, err
	}

	// A step closure still may not capture runner variables.
	_, _ = workflow.Step(ctx, func(ctx context.Context, _ struct{}) (string, error) {
		return out.Name, nil // want `durable closure captures out, pass workflow-relevant state through its explicit input instead`
	}, struct{}{})

	// A Then callback runs in workflow context.
	source := workflow.Background(ctx, func(ctx context.Context, in Input) (Output, error) { return Output{}, nil }, in)
	workflow.Then(ctx, source, func(Output, error) *workflow.Result[int] {
		time.Now() // want `workflow body calls time.Now, use workflow.Now instead`
		return nil
	})
	return out, nil
}
