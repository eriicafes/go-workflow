package worlds_test

import (
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
