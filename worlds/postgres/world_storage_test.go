package postgresworld

import (
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
	at := time.Date(2026, 1, 2, 3, 4, 5, 6000, time.UTC)
	cursor := encodeCursor(at, "wrun_a:b")
	gotAt, gotID, err := decodeCursor(cursor)
	if err != nil || !gotAt.Equal(at) || gotID != "wrun_a:b" {
		t.Fatalf("decode = %v %q %v", gotAt, gotID, err)
	}
	for _, bad := range []string{"!!", "bm9jb2xvbg", "eDp5"} {
		if _, _, err := decodeCursor(bad); !errors.Is(err, errInvalidCursor) {
			t.Fatalf("decode(%q) err = %v", bad, err)
		}
	}
	var q query
	if err := q.keyset(cursor, "created_at", "run_id", true); err != nil {
		t.Fatal(err)
	}
	if got := q.clause(); got != " WHERE (created_at, run_id) < ($1, $2)" {
		t.Fatalf("descending keyset = %q", got)
	}
	if got := order("created_at", "run_id", false); got != " ORDER BY created_at, run_id" {
		t.Fatalf("order = %q", got)
	}
}

func TestKeysetPage(t *testing.T) {
	at := time.Unix(10, 0).UTC()
	key := func(v string) (time.Time, string) { return at, v }
	page := keysetPage([]string{"a", "b", "c"}, 2, key)
	if !reflect.DeepEqual(page.Data, []string{"a", "b"}) || !page.HasMore || page.Cursor != encodeCursor(at, "b") {
		t.Fatalf("page = %#v", page)
	}
	page = keysetPage([]string{"a", "b"}, 2, key)
	if page.HasMore || page.Cursor != "" {
		t.Fatalf("last page = %#v", page)
	}
	page = keysetPage([]string{"a", "b"}, 0, key)
	if len(page.Data) != 2 || page.HasMore {
		t.Fatalf("unlimited page = %#v", page)
	}
}

func TestOffsetPage(t *testing.T) {
	values := []int{1, 2, 3}
	page, err := offsetPage(values, worlds.Pagination{Limit: 2})
	if err != nil || len(page.Data) != 2 || !page.HasMore || page.Cursor != "2" {
		t.Fatalf("first = %#v err=%v", page, err)
	}
	page, err = offsetPage(values, worlds.Pagination{Limit: 2, Cursor: page.Cursor})
	if err != nil || len(page.Data) != 1 || page.HasMore || page.Cursor != "" {
		t.Fatalf("second = %#v err=%v", page, err)
	}
	if _, err := offsetPage(values, worlds.Pagination{Cursor: "x"}); !errors.Is(err, errInvalidCursor) {
		t.Fatalf("invalid cursor err = %v", err)
	}
}
