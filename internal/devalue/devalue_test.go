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
	if err != nil || string(data) != `[{"Err":1},["Error",2],{"name":3,"message":4,"stack":-1,"data":5},"*errors.errorString","nope",{}]` {
		t.Fatalf("Marshal = %s err=%v", data, err)
	}
	var got failure
	if err := Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	recorded, ok := got.Err.(*sdkerror.Error)
	if !ok || recorded.Name != "*errors.errorString" || recorded.Error() != "nope" {
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
