package devalue

import (
	"math"
	"testing"
	"time"
)

func TestMarshalWritesSDKShapes(t *testing.T) {
	for _, test := range []struct {
		value any
		want  string
	}{
		{[]byte{1, 2, 3}, `[["Uint8Array",1],"AQID"]`},
		{time.Date(2024, 1, 2, 3, 4, 5, 678e6, time.UTC), `[["Date",1],"2024-01-02T03:04:05.678Z"]`},
		{map[int]string{1: "a"}, `[["Map",1],[2],[3,4],1,"a"]`},
		{nil, `[null]`},
		{math.Inf(1), `-4`},
		{[]any{1, nil}, `[[1,2],1,null]`},
		{struct {
			A int `json:"a,omitempty"`
			B int `json:"b"`
		}{B: 2}, `[{"b":1},2]`},
	} {
		got, err := Marshal(test.value)
		if err != nil || string(got) != test.want {
			t.Errorf("Marshal(%#v) = %s err=%v, want %s", test.value, got, err, test.want)
		}
	}
}

func TestMarshalRefusesFuncsAndCycles(t *testing.T) {
	if _, err := Marshal(func() {}); err == nil {
		t.Fatal("encoded a func")
	}
	type node struct{ Next *node }
	loop := &node{}
	loop.Next = loop
	if _, err := Marshal(loop); err == nil {
		t.Fatal("encoded a cycle")
	}
}

type marshaled struct{ n int }

func (m marshaled) MarshalPayload() (any, error) { return map[string]int{"n": m.n}, nil }

func (m *marshaled) UnmarshalPayload(decode func(any) error) error {
	var value map[string]int
	err := decode(&value)
	m.n = value["n"]
	return err
}

func TestMarshalUsesPayloadMarshalers(t *testing.T) {
	data, err := Marshal(marshaled{n: 3})
	if err != nil || string(data) != `[{"n":1},3]` {
		t.Fatalf("Marshal = %s err=%v", data, err)
	}
	var got marshaled
	if err := Unmarshal(data, &got); err != nil || got.n != 3 {
		t.Fatalf("Unmarshal = %+v err=%v", got, err)
	}
}
