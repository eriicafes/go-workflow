package localworld

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

type entity struct {
	at time.Time
	id string
}

func entityKey(value entity) (time.Time, string) { return value.at, value.id }

func entityIDs(page worlds.Page[entity]) []string {
	ids := []string{}
	for _, value := range page.Data {
		ids = append(ids, value.id)
	}
	return ids
}

// TestEntityPage checks runs, steps, and hooks page by creation time in
// milliseconds, then ID, with a "<time>|<id>" cursor.
func TestEntityPage(t *testing.T) {
	t.Parallel()
	first := time.Date(2026, 1, 2, 3, 4, 5, 6_000_000, time.UTC)
	values := func() []entity {
		return []entity{
			// Created later within the millisecond, but ordered by ID.
			{first.Add(900 * time.Microsecond), "a"},
			{first, "b"},
			{first.Add(time.Millisecond), "c"},
			{first.Add(2 * time.Millisecond), "d"},
		}
	}
	page := entityPage(values(), worlds.Pagination{Limit: 2, SortOrder: worlds.SortOrderAscending}, entityKey)
	if !slices.Equal(entityIDs(page), []string{"a", "b"}) || !page.HasMore || page.Cursor != "2026-01-02T03:04:05.006Z|b" {
		t.Fatalf("first page = %v more=%v cursor=%q", entityIDs(page), page.HasMore, page.Cursor)
	}
	next := entityPage(values(), worlds.Pagination{Limit: 2, SortOrder: worlds.SortOrderAscending, Cursor: page.Cursor}, entityKey)
	if !slices.Equal(entityIDs(next), []string{"c", "d"}) || next.HasMore || next.Cursor != "2026-01-02T03:04:05.008Z|d" {
		t.Fatalf("second page = %v more=%v cursor=%q", entityIDs(next), next.HasMore, next.Cursor)
	}
	descending := entityPage(values(), worlds.Pagination{Limit: 3, SortOrder: worlds.SortOrderDescending}, entityKey)
	if !slices.Equal(entityIDs(descending), []string{"d", "c", "b"}) || !descending.HasMore {
		t.Fatalf("descending page = %v more=%v", entityIDs(descending), descending.HasMore)
	}
	rest := entityPage(values(), worlds.Pagination{SortOrder: worlds.SortOrderDescending, Cursor: descending.Cursor}, entityKey)
	if !slices.Equal(entityIDs(rest), []string{"a"}) || rest.HasMore {
		t.Fatalf("descending rest = %v more=%v", entityIDs(rest), rest.HasMore)
	}
	// A cursor without an ID passes every record of its millisecond.
	timeOnly := entityPage(values(), worlds.Pagination{SortOrder: worlds.SortOrderAscending, Cursor: "2026-01-02T03:04:05.006Z"}, entityKey)
	if !slices.Equal(entityIDs(timeOnly), []string{"c", "d"}) {
		t.Fatalf("page after a time = %v", entityIDs(timeOnly))
	}
	// A cursor that does not parse starts from the first page.
	invalid := entityPage(values(), worlds.Pagination{Limit: 1, SortOrder: worlds.SortOrderAscending, Cursor: "bm9wZQ"}, entityKey)
	if !slices.Equal(entityIDs(invalid), []string{"a"}) {
		t.Fatalf("page after an invalid cursor = %v", entityIDs(invalid))
	}
	if empty := entityPage(nil, worlds.Pagination{}, entityKey); empty.Data == nil || len(empty.Data) != 0 || empty.Cursor != "" || empty.HasMore {
		t.Fatalf("empty page = %#v", empty)
	}
}

func TestEventCursor(t *testing.T) {
	t.Parallel()
	id := string(worlds.EventIDForSlot(7))
	for cursor, want := range map[string]int{
		"":                                 0,
		"key:" + id:                        7,
		id:                                 7,
		"2026-01-02T03:04:05.006Z|" + id:   7,
		"key:nope":                         0,
		"7":                                0,
		"2026-01-02T03:04:05.006Z|missing": 0,
	} {
		if got := eventCursor(cursor); got != want {
			t.Errorf("eventCursor(%q) = %d, want %d", cursor, got, want)
		}
	}
}

func TestChunkCursor(t *testing.T) {
	t.Parallel()
	// base64 of {"i":3}.
	if cursor := chunkCursor(3); cursor != "eyJpIjozfQ==" {
		t.Fatalf("chunkCursor(3) = %q", cursor)
	}
	for cursor, want := range map[string]int{
		chunkCursor(0):  0,
		chunkCursor(12): 12,
		"5":             5,
		"-5":            0,
		"":              0,
		"not a cursor":  0,
		"eyJpIjotMX0=":  0, // {"i":-1}
		"bm9wZQ==":      0, // nope
	} {
		if got := parseChunkCursor(cursor); got != want {
			t.Errorf("parseChunkCursor(%q) = %d, want %d", cursor, got, want)
		}
	}
}

func TestCheckID(t *testing.T) {
	t.Parallel()
	for _, id := range []string{"wrun_01JZ", "step_a", "hook-1", "evnt_00000000000000000000000001"} {
		if err := checkID("ID", id); err != nil {
			t.Errorf("checkID(%q) = %v", id, err)
		}
	}
	for _, id := range []string{"", "a.b", "a/b", `a\b`, "a\x00b", ".."} {
		if err := checkID("ID", id); !errors.Is(err, worlds.ErrEventInvalid) {
			t.Errorf("checkID(%q) = %v, want ErrEventInvalid", id, err)
		}
	}
}
