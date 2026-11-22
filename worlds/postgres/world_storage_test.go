package postgresworld

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

func TestQueryBuilder(t *testing.T) {
	var q query
	if q.clause() != "" || q.limit(0) != "" {
		t.Fatalf("empty query = %q %q", q.clause(), q.limit(0))
	}
	q.where("a = " + q.arg("x"))
	q.where("b = ANY(" + q.arg([]string{"y"}) + ")")
	if got := q.clause(); got != " WHERE a = $1 AND b = ANY($2)" {
		t.Fatalf("clause = %q", got)
	}
	if got := q.limit(5); got != " LIMIT $3" || q.args[2] != 6 {
		t.Fatalf("limit = %q args=%v", got, q.args)
	}
}

func TestKeysetCursor(t *testing.T) {
	var q query
	q.keyset("", "id", true)
	if got := q.clause(); got != "" {
		t.Fatalf("keyset without a cursor = %q", got)
	}
	q.keyset("wrun_a", "id", true)
	if got := q.clause(); got != " WHERE id < $1" || q.args[0] != "wrun_a" {
		t.Fatalf("descending keyset = %q args=%v", got, q.args)
	}
	q = query{}
	q.keyset("wrun_a", "id", false)
	if got := q.clause(); got != " WHERE id > $1" {
		t.Fatalf("ascending keyset = %q", got)
	}
	if got := order("id", false); got != " ORDER BY id" {
		t.Fatalf("order = %q", got)
	}
	if got := order("id", true); got != " ORDER BY id DESC" {
		t.Fatalf("descending order = %q", got)
	}
}

func TestKeysetPage(t *testing.T) {
	id := func(v string) string { return v }
	page := keysetPage([]string{"a", "b", "c"}, 2, id)
	if !reflect.DeepEqual(page.Data, []string{"a", "b"}) || !page.HasMore || page.Cursor != "b" {
		t.Fatalf("page = %#v", page)
	}
	page = keysetPage([]string{"a", "b"}, 2, id)
	if page.HasMore || page.Cursor != "b" {
		t.Fatalf("last page = %#v", page)
	}
	page = keysetPage([]string{"a", "b"}, 0, id)
	if len(page.Data) != 2 || page.HasMore {
		t.Fatalf("unlimited page = %#v", page)
	}
}

// TestListsOrderByID checks runs, steps, and hooks list in the order of
// their IDs whatever order they were stored in, and that a page's cursor is
// its last ID.
func TestListsOrderByID(t *testing.T) {
	t.Parallel()
	world := newTestWorld(t)
	ctx := context.Background()
	// The later ID is stored first.
	earlier, later := worlds.NewRunID(), worlds.NewRunID()
	for _, id := range []worlds.RunID{later, earlier} {
		if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: id, WorkflowName: "ordered"}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Millisecond)
	}
	runs, err := world.RunsList(ctx, worlds.RunsListInput{WorkflowName: "ordered", Pagination: worlds.Pagination{Limit: 1}})
	if err != nil || len(runs.Data) != 1 || runs.Data[0].RunID != later || !runs.HasMore || runs.Cursor != string(later) {
		t.Fatalf("first page of runs = %#v err=%v, want %s", runs, err, later)
	}
	runs, err = world.RunsList(ctx, worlds.RunsListInput{WorkflowName: "ordered", Pagination: worlds.Pagination{Cursor: runs.Cursor}})
	if err != nil || len(runs.Data) != 1 || runs.Data[0].RunID != earlier || runs.Cursor != string(earlier) {
		t.Fatalf("second page of runs = %#v err=%v, want %s", runs, err, earlier)
	}
	for _, step := range []string{"step_02", "step_01"} {
		data, _ := json.Marshal(worlds.DataStepCreated{StepName: "s"})
		if _, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: earlier, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeStepCreated, CorrelationID: step, Data: data}}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Millisecond)
	}
	steps, err := world.StepsList(ctx, worlds.StepsListInput{RunID: earlier})
	if err != nil || len(steps.Data) != 2 || steps.Data[0].StepID != "step_02" || steps.Data[1].StepID != "step_01" || steps.Cursor != "step_01" {
		t.Fatalf("steps = %#v err=%v", steps, err)
	}
	for _, hook := range []string{"hook_02", "hook_01"} {
		data, _ := json.Marshal(worlds.DataHookCreated{Token: hook + string(earlier)})
		if _, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: earlier, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeHookCreated, CorrelationID: hook, Data: data}}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Millisecond)
	}
	hooks, err := world.HooksList(ctx, worlds.HooksListInput{RunID: earlier, Pagination: worlds.Pagination{Limit: 1}})
	if err != nil || len(hooks.Data) != 1 || hooks.Data[0].HookID != "hook_01" || hooks.Cursor != "hook_01" {
		t.Fatalf("first page of hooks = %#v err=%v", hooks, err)
	}
	hooks, err = world.HooksList(ctx, worlds.HooksListInput{RunID: earlier, Pagination: worlds.Pagination{Cursor: hooks.Cursor}})
	if err != nil || len(hooks.Data) != 1 || hooks.Data[0].HookID != "hook_02" {
		t.Fatalf("second page of hooks = %#v err=%v", hooks, err)
	}
}

func TestOffsetPage(t *testing.T) {
	values := []int{1, 2, 3}
	page, err := offsetPage(values, worlds.Pagination{Limit: 2})
	if err != nil || len(page.Data) != 2 || !page.HasMore || page.Cursor != "2" {
		t.Fatalf("first = %#v err=%v", page, err)
	}
	page, err = offsetPage(values, worlds.Pagination{Limit: 2, Cursor: page.Cursor})
	if err != nil || len(page.Data) != 1 || page.HasMore || page.Cursor != "3" {
		t.Fatalf("second = %#v err=%v", page, err)
	}
	if _, err := offsetPage(values, worlds.Pagination{Cursor: "x"}); !errors.Is(err, errInvalidCursor) {
		t.Fatalf("invalid cursor err = %v", err)
	}
}
