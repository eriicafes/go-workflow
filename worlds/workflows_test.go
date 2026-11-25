package worlds_test

import (
	"context"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

type shortLived struct{}

func (shortLived) Workflow(struct{}) {}

type kept struct{}

func (kept) Workflow(struct{}) {}

func TestRetentions(t *testing.T) {
	retentions := worlds.Retentions(
		worlds.Retain[shortLived](10*time.Minute),
		worlds.Retain[kept](0),
	)
	cases := map[string]time.Duration{
		worlds.WorkflowName[shortLived](): 10 * time.Minute,
		worlds.WorkflowName[kept]():       0,
		"example.com/other.Workflow":      24 * time.Hour,
	}
	for workflow, want := range cases {
		if got := worlds.RetentionFor(24*time.Hour, retentions, workflow); got != want {
			t.Errorf("RetentionFor(%s) = %v, want %v", workflow, got, want)
		}
	}
	if longest, negative := worlds.MaxRetention(time.Minute, retentions); longest != 10*time.Minute || negative {
		t.Errorf("MaxRetention = %v, %v", longest, negative)
	}
	if _, negative := worlds.MaxRetention(0, map[string]time.Duration{"w": -time.Second}); !negative {
		t.Error("MaxRetention missed a negative retention")
	}
}

type namedFlow struct{}

func (namedFlow) Workflow(struct{}) {}

func (namedFlow) report(context.Context, int) (int, error) { return 0, nil }

func (*namedFlow) publish(context.Context, int) (int, error) { return 0, nil }

// nested is not inlined, so its closure keeps the name it is compiled under.
//
//go:noinline
func (namedFlow) nested() func(context.Context, int) (int, error) {
	return func(context.Context, int) (int, error) { return 0, nil }
}

func plainStep(context.Context, int) (int, error) { return 0, nil }

func genericStep[T any](context.Context, T) (T, error) {
	var zero T
	return zero, nil
}

// Workflows and steps are named in the Workflow SDK's form, with a step's
// Go name after its package path, without a method value's -fm suffix.
func TestNamesTakeTheWorkflowSDKsForm(t *testing.T) {
	const pkg = "github.com/eriicafes/go-workflow/worlds_test"
	if got := worlds.WorkflowName[namedFlow](); got != "workflow//"+pkg+"//namedFlow" {
		t.Errorf("WorkflowName = %q", got)
	}
	closure := func(context.Context, int) (int, error) { return 0, nil }
	for _, test := range []struct {
		fn   any
		want string
	}{
		{plainStep, "step//" + pkg + "//plainStep"},
		{closure, "step//" + pkg + "//TestNamesTakeTheWorkflowSDKsForm.func1"},
		{namedFlow{}.report, "step//" + pkg + "//namedFlow.report"},
		{(&namedFlow{}).publish, "step//" + pkg + "//(*namedFlow).publish"},
		{namedFlow{}.nested(), "step//" + pkg + "//namedFlow.nested.func1"},
		{genericStep[time.Duration], "step//" + pkg + "//genericStep[...]"},
	} {
		if got := worlds.StepName(test.fn); got != test.want {
			t.Errorf("StepName = %q, want %q", got, test.want)
		}
	}
}
