package devalue

import (
	"bytes"
	"errors"
	"math"
	"math/big"
	"net/http"
	"net/url"
	"reflect"
	"testing"
	"time"
)

type record struct {
	Name   string            `json:"name"`
	Data   []byte            `json:"data"`
	At     time.Time         `json:"at"`
	Big    *big.Int          `json:"big"`
	Link   *url.URL          `json:"link"`
	Header http.Header       `json:"header"`
	Tags   map[string]string `json:"tags"`
	Scores map[int]float64   `json:"scores"`
	Skip   string            `json:"-"`
	Inner  `json:"inner"`
}

type Inner struct{ Value string }

func TestUnmarshalRoundTripsMappedTypes(t *testing.T) {
	link, _ := url.Parse("https://example.com/x")
	value := record{
		Name: "n", Data: []byte{0, 1}, At: time.Date(2024, 5, 6, 7, 8, 9, 0, time.UTC), Big: big.NewInt(-5), Link: link,
		Header: http.Header{"X-Test": {"a", "b"}}, Tags: map[string]string{"b": "2", "a": "1"},
		Scores: map[int]float64{2: 0.5, 1: 1}, Skip: "skipped", Inner: Inner{Value: "in"},
	}
	data, err := Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var got record
	if err := Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	value.Skip = ""
	if got.Big.Cmp(value.Big) != 0 || got.Link.String() != link.String() || !got.At.Equal(value.At) {
		t.Fatalf("got %+v", got)
	}
	got.Big, value.Big, got.Link, value.Link, got.At, value.At = nil, nil, nil, nil, time.Time{}, time.Time{}
	if !reflect.DeepEqual(got, value) {
		t.Fatalf("got %+v, want %+v", got, value)
	}
}

func TestUnmarshalReadsSpecialsHolesAndSharedValues(t *testing.T) {
	var specials []float64
	if err := Unmarshal([]byte(`[[-3,-6,-4,-5,1],1]`), &specials); err != nil {
		t.Fatal(err)
	}
	if !math.IsNaN(specials[0]) || !math.Signbit(specials[1]) || !math.IsInf(specials[2], 1) || !math.IsInf(specials[3], -1) || specials[4] != 1 {
		t.Fatalf("specials = %v", specials)
	}
	var holes []*int
	if err := Unmarshal([]byte(`[[1,-2,2],1,3]`), &holes); err != nil || len(holes) != 3 || holes[1] != nil || *holes[2] != 3 {
		t.Fatalf("holes = %v err=%v", holes, err)
	}
	var shared []any
	if err := Unmarshal([]byte(`[[1,1,2],"a",{"k":1}]`), &shared); err != nil || !reflect.DeepEqual(shared, []any{"a", "a", map[string]any{"k": "a"}}) {
		t.Fatalf("shared = %v err=%v", shared, err)
	}
}

func TestUnmarshalDecodesIntoAny(t *testing.T) {
	var value any
	if err := Unmarshal([]byte(`[["Uint8Array",1],"AQID"]`), &value); err != nil || !bytes.Equal(value.([]byte), []byte{1, 2, 3}) {
		t.Fatalf("value = %#v err=%v", value, err)
	}
	if err := Unmarshal([]byte(`[["Request",1],"x"]`), &value); err != nil || !reflect.DeepEqual(value, Reduced{Type: "Request", Value: "x"}) {
		t.Fatalf("reduced = %#v err=%v", value, err)
	}
}

func TestUnmarshalRefusesNonPointersAndMismatches(t *testing.T) {
	var n int
	if err := Unmarshal([]byte(`[1]`), n); err == nil {
		t.Fatal("decoded into a non-pointer")
	}
	if err := Unmarshal([]byte(`["x"]`), &n); !errors.Is(err, ErrInvalid) {
		t.Fatalf("string into int: %v", err)
	}
}
