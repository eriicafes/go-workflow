package devalue

import (
	"errors"
	"testing"

	"github.com/eriicafes/go-workflow/internal/sdkerror"
)

// failure holds an error where its static type is error, which is where
// errors are recorded as Errors.
type failure struct{ Err error }

func TestErrorsAreRecordedWithSDKError(t *testing.T) {
	data, err := Marshal(failure{errors.New("nope")})
	if err != nil || string(data) != `[{"Err":1},["Error",2],{"message":3,"stack":-1},"nope"]` {
		t.Fatalf("Marshal = %s err=%v", data, err)
	}
	var got failure
	if err := Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	recorded, ok := got.Err.(*sdkerror.Error)
	if !ok || recorded.Name != "" || recorded.Error() != "nope" {
		t.Fatalf("Unmarshal = %#v", got.Err)
	}
}

func TestErrorsKeepTheirCauses(t *testing.T) {
	inner := errors.New("inner")
	data, err := MarshalError(errors.Join(errors.New("outer"), inner))
	if err != nil {
		t.Fatal(err)
	}
	var got error
	if err := Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(got, inner) || got.Error() != "outer\ninner" {
		t.Fatalf("Unmarshal = %v", got)
	}
}

func TestIntegersPastTheSafeRangeAreBigInts(t *testing.T) {
	type counts struct {
		Small int64
		Large int64
		Huge  uint64
	}
	in := counts{Small: 1<<53 - 1, Large: -(1<<53 + 1), Huge: 1<<64 - 1}
	data, err := Marshal(in)
	if err != nil || string(data) != `[{"Small":1,"Large":2,"Huge":4},9007199254740991,["BigInt",3],"-9007199254740993",["BigInt",5],"18446744073709551615"]` {
		t.Fatalf("Marshal = %s err=%v", data, err)
	}
	var out counts
	if err := Unmarshal(data, &out); err != nil || out != in {
		t.Fatalf("Unmarshal = %+v err=%v", out, err)
	}
}
