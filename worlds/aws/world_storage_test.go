package awsworld

import (
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

func TestCursor(t *testing.T) {
	cursor := encodeCursor(map[string]string{"pk": "RUN#a", "sk": "META"})
	key, err := cursorKey(cursor)
	if err != nil || read(key, "pk") != "RUN#a" || read(key, "sk") != "META" {
		t.Fatalf("key = %v err=%v", key, err)
	}
	if key, err := cursorKey(""); err != nil || key != nil {
		t.Fatalf("empty cursor = %v err=%v", key, err)
	}
	if _, err := cursorKey("not a cursor!"); err == nil {
		t.Fatal("invalid cursor accepted")
	}
}

func TestSortedPage(t *testing.T) {
	values := []string{"a", "b", "c", "d", "e"}
	identity := func(value string) string { return value }
	var seen []string
	pagination := worlds.Pagination{Limit: 2}
	for {
		page, err := sortedPage(append([]string(nil), values...), identity, pagination)
		if err != nil {
			t.Fatal(err)
		}
		seen = append(seen, page.Data...)
		if !page.HasMore {
			if page.Cursor != "" {
				t.Fatal("cursor on last page")
			}
			break
		}
		pagination.Cursor = page.Cursor
	}
	if len(seen) != 5 || seen[4] != "e" {
		t.Fatalf("ascending = %v", seen)
	}
	desc, err := sortedPage(append([]string(nil), values...), identity, worlds.Pagination{Limit: 2, SortOrder: worlds.SortOrderDescending})
	if err != nil || desc.Data[0] != "e" || !desc.HasMore {
		t.Fatalf("descending = %#v err=%v", desc, err)
	}
	next, err := sortedPage(append([]string(nil), values...), identity, worlds.Pagination{Cursor: desc.Cursor, SortOrder: worlds.SortOrderDescending})
	if err != nil || len(next.Data) != 3 || next.Data[0] != "c" || next.HasMore {
		t.Fatalf("descending rest = %#v err=%v", next, err)
	}
}

func TestBackoff(t *testing.T) {
	for attempt := range 40 {
		delay := backoff(attempt, 5*time.Millisecond, 250*time.Millisecond)
		if delay < 2*time.Millisecond || delay > 250*time.Millisecond {
			t.Fatalf("attempt %d delay = %v", attempt, delay)
		}
	}
}
