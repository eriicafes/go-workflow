package awsworld

import (
	"context"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
)

func TestScheduleName(t *testing.T) {
	id := ulid.Make().String()
	name := scheduleName(id)
	if name != "go-workflow-"+id || len(name) > 64 || scheduleName(id) != name {
		t.Fatalf("schedule name = %q", name)
	}
	if long := scheduleName(string(make([]byte, 100))); len(long) != 64 {
		t.Fatalf("long name length = %d", len(long))
	}
}

func TestScheduleExpression(t *testing.T) {
	exact := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	if got := scheduleExpression(exact); got != "at(2026-03-04T05:06:07)" {
		t.Fatalf("exact = %s", got)
	}
	if got := scheduleExpression(exact.Add(time.Millisecond)); got != "at(2026-03-04T05:06:08)" {
		t.Fatalf("rounded = %s", got)
	}
	if got := scheduleExpression(exact.In(time.FixedZone("x", 3600))); got != "at(2026-03-04T05:06:07)" {
		t.Fatalf("zoned = %s", got)
	}
}

func TestDeleteScheduleNotFound(t *testing.T) {
	schedules := newFakeScheduler()
	world := testWorld(t, newFakeDynamo(), &fakeSQS{}, schedules)
	if err := world.deleteSchedule(context.Background(), "missing"); err != nil {
		t.Fatalf("delete missing schedule: %v", err)
	}
	if world.scheduleGroup() != "default" {
		t.Fatalf("group = %q", world.scheduleGroup())
	}
}
