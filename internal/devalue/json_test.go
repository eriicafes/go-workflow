package devalue

import (
	"encoding/json"
	"fmt"
	"math"
	"testing"
)

// bigID marshals itself as JSON holding a 64-bit integer.
type bigID struct{ N uint64 }

func (b bigID) MarshalJSON() ([]byte, error) { return []byte(fmt.Sprintf(`{"n":%d}`, b.N)), nil }

func (b *bigID) UnmarshalJSON(data []byte) error {
	var value struct{ N uint64 }
	err := json.Unmarshal(data, &value)
	b.N = value.N
	return err
}

func TestJSONKeepsNumbersAndKeyOrder(t *testing.T) {
	type record struct {
		Raw json.RawMessage `json:"raw"`
		ID  bigID           `json:"id"`
	}
	raw := `{"z":12345678901234567890,"a":[1.50,-0,1e400],"m":{"b":null,"a":"x"}}`
	data, err := Marshal(record{Raw: json.RawMessage(raw), ID: bigID{N: math.MaxUint64}})
	if err != nil {
		t.Fatal(err)
	}
	var got record
	if err := Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if string(got.Raw) != raw || got.ID.N != math.MaxUint64 {
		t.Fatalf("raw = %s, id = %d", got.Raw, got.ID.N)
	}
}

func TestNodeJSONRoundTripsJSONNode(t *testing.T) {
	text := `{"b":[1,2.50,"x",true,null],"a":{}}`
	node, err := JSONNode([]byte(text))
	if err != nil {
		t.Fatal(err)
	}
	if got, err := NodeJSON(node); err != nil || string(got) != text {
		t.Fatalf("NodeJSON = %s err=%v", got, err)
	}
}
