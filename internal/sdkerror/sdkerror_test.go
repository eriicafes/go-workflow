package sdkerror

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"testing"
)

func TestIsTypeMatchesErrorTypes(t *testing.T) {
	for typ, want := range map[string]bool{"Error": true, "TypeError": true, "FatalError": true, "DOMException": true, "Date": false, "": false} {
		if got := IsType(typ); got != want {
			t.Errorf("IsType(%q) = %v, want %v", typ, got, want)
		}
	}
}

func TestFieldFindsAPropertyByName(t *testing.T) {
	value := &Error{Fields: []Field{{"a", []byte("1")}, {"b", []byte("2")}}}
	if got, ok := value.Field("b"); !ok || string(got) != "2" {
		t.Fatalf("Field(b) = %s %v", got, ok)
	}
	if _, ok := value.Field("c"); ok {
		t.Fatal("found a missing field")
	}
}

var errSentinel = errors.New("sentinel")

type codeError struct{ Code int }

func (e codeError) Error() string { return fmt.Sprintf("code %d", e.Code) }

// markedError is registered as the SDK's MarkedError.
type markedError struct {
	Cause error
	Mark  string
}

func (e markedError) Error() string { return "marked: " + e.Cause.Error() }
func (e markedError) Unwrap() error { return e.Cause }

func init() {
	Register("MarkedError",
		func(e markedError) []Field {
			return []Field{{Name: "mark", Value: json.RawMessage(strconv.Quote(e.Mark))}}
		},
		func(e *markedError, recorded *Error) {
			e.Cause = recorded.Unwrap()
			if mark, ok := recorded.Field("mark"); ok {
				_ = json.Unmarshal(mark, &e.Mark)
			}
		})
}

func TestNewRecordsTheChain(t *testing.T) {
	recorded := New(fmt.Errorf("outer: %w", errors.Join(codeError{7}, errSentinel)))
	var names []string
	for node := recorded; node != nil; node = node.Cause {
		names = append(names, node.Name)
	}
	if len(names) != 4 || names[0] != "*fmt.wrapError" || names[2] != "github.com/eriicafes/go-workflow/internal/sdkerror.codeError" {
		t.Fatalf("chain = %q", names)
	}
	if recorded.Error() != "outer: code 7\nsentinel" || New(nil) != nil {
		t.Fatalf("message = %q", recorded.Error())
	}
	var code codeError
	if !errors.Is(recorded, errSentinel) || !errors.As(recorded, &code) || code.Code != 7 {
		t.Fatalf("Is/As failed: code=%v", code)
	}
	if errors.Is(recorded, errors.New("other")) {
		t.Fatal("matched an unrelated error")
	}
	// A recorded chain wrapped again keeps its errors.
	again := New(fmt.Errorf("again: %w", recorded))
	if !errors.Is(again, errSentinel) || !errors.As(again, &code) {
		t.Fatal("rewrapped chain lost its errors")
	}
}

func TestRegisteredTypesAreRecordedAsTheSDKsTypes(t *testing.T) {
	recorded := New(markedError{Cause: errSentinel, Mark: "m"})
	if recorded.Type != "MarkedError" || recorded.Name != "" {
		t.Fatalf("recorded as %q %q", recorded.Type, recorded.Name)
	}
	data, err := json.Marshal(recorded)
	if err != nil {
		t.Fatal(err)
	}
	var read Error
	if err := json.Unmarshal(data, &read); err != nil {
		t.Fatal(err)
	}
	var marked markedError
	if !errors.As(&read, &marked) || marked.Mark != "m" || !errors.Is(marked.Cause, errSentinel) {
		t.Fatalf("marked = %+v", marked)
	}
	// A pointer keeps its Go type, and still gets its cause back.
	pointer := New(&markedError{Cause: errSentinel, Mark: "p"})
	var target *markedError
	if pointer.Type != "" || !errors.As(pointer, &target) || target.Mark != "p" || !errors.Is(target.Cause, errSentinel) {
		t.Fatalf("pointer = %+v %+v", pointer, target)
	}
}

// namedError is registered as an Error named NamedError.
type namedError struct{ Key string }

func (e *namedError) Error() string { return "named" }

func init() {
	RegisterNamed("NamedError",
		func(e *namedError) []Field { return []Field{NewField("key", e.Key)} },
		func(e **namedError, recorded *Error) {
			named := &namedError{}
			recorded.Decode("key", &named.Key)
			*e = named
		})
}

func TestNamedTypesAreRecordedAsNamedErrors(t *testing.T) {
	recorded := New(&namedError{Key: "k"})
	if recorded.Type != "" || recorded.Name != "NamedError" {
		t.Fatalf("recorded as %q %q", recorded.Type, recorded.Name)
	}
	// The SDK reads it back as an Error of that name.
	recorded.Type = "Error"
	if target, ok := errors.AsType[*namedError](recorded); !ok || target.Key != "k" {
		t.Fatalf("read back = %+v", target)
	}
}
