package worlds

import (
	"encoding/json"
	"errors"
	"reflect"
)

// Error is the error a failed step or run returns after it is recorded and read
// back. Its message is the original error's message, and it keeps the type and
// JSON encoding of every error in the original error's chain, so errors.Is and
// errors.As keep working across replay.
//
// errors.Is matches a target with the same type and message as an error in the
// chain, which is how sentinel errors such as one made by errors.New are
// matched. errors.As matches a target whose type is the type of an error in the
// chain, and decodes that error's exported fields from JSON into it.
type Error struct {
	message string
	chain   []errorCause
}

type errorCause struct {
	Type    string          `json:"type"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

// NewError returns an Error that records err and its chain. An Error in the
// chain contributes the chain it recorded. It returns nil when err is nil.
func NewError(err error) *Error {
	if err == nil {
		return nil
	}
	return &Error{message: err.Error(), chain: appendErrorChain(nil, err)}
}

// DecodeError decodes an Error encoded with json.Marshal. Data that is
// not an encoded Error becomes an Error whose message is data.
func DecodeError(data []byte) *Error {
	var value Error
	if err := json.Unmarshal(data, &value); err != nil || (value.message == "" && value.chain == nil) {
		return &Error{message: string(data)}
	}
	return &value
}

func encodeError(err error) []byte {
	// Encoding cannot fail, because NewError keeps only the JSON that an
	// error in the chain encoded successfully.
	data, _ := json.Marshal(NewError(err))
	return data
}

// Error returns the original error's message.
func (e *Error) Error() string { return e.message }

// Is reports whether an error in the chain has the type and message of
// target.
func (e *Error) Is(target error) bool {
	if target == nil {
		return false
	}
	name, message := errorTypeName(reflect.TypeOf(target)), target.Error()
	for _, cause := range e.chain {
		if cause.Type == name && cause.Message == message {
			return true
		}
	}
	return false
}

// As sets target to the first error in the chain whose type is the type
// target points to, decoded from its JSON encoding, and reports whether one
// was found. Fields the JSON encoding does not hold, such as unexported
// fields, keep their zero values.
func (e *Error) As(target any) bool {
	pointer := reflect.ValueOf(target)
	if pointer.Kind() != reflect.Pointer || pointer.IsNil() {
		return false
	}
	targetType := pointer.Type().Elem()
	if targetType.Kind() == reflect.Interface {
		return false
	}
	name := errorTypeName(targetType)
	for _, cause := range e.chain {
		if cause.Type != name {
			continue
		}
		value := reflect.New(targetType)
		if targetType.Kind() == reflect.Pointer {
			value.Elem().Set(reflect.New(targetType.Elem()))
		}
		if len(cause.Data) != 0 {
			var typeError *json.UnmarshalTypeError
			if err := json.Unmarshal(cause.Data, value.Interface()); err != nil && !errors.As(err, &typeError) {
				continue
			}
		}
		pointer.Elem().Set(value.Elem())
		return true
	}
	return false
}

// MarshalJSON encodes the message and the chain.
func (e *Error) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Message string       `json:"message"`
		Chain   []errorCause `json:"chain,omitempty"`
	}{e.message, e.chain})
}

// UnmarshalJSON decodes an Error encoded by MarshalJSON.
func (e *Error) UnmarshalJSON(data []byte) error {
	var value struct {
		Message string       `json:"message"`
		Chain   []errorCause `json:"chain"`
	}
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	e.message, e.chain = value.Message, value.Chain
	return nil
}

func appendErrorChain(chain []errorCause, err error) []errorCause {
	if err == nil {
		return chain
	}
	if recorded, ok := err.(*Error); ok {
		return append(chain, recorded.chain...)
	}
	cause := errorCause{Type: errorTypeName(reflect.TypeOf(err)), Message: err.Error()}
	if data, marshalErr := json.Marshal(err); marshalErr == nil {
		cause.Data = data
	}
	chain = append(chain, cause)
	switch wrapped := err.(type) {
	case interface{ Unwrap() error }:
		chain = appendErrorChain(chain, wrapped.Unwrap())
	case interface{ Unwrap() []error }:
		for _, err := range wrapped.Unwrap() {
			chain = appendErrorChain(chain, err)
		}
	}
	return chain
}

func errorTypeName(t reflect.Type) string {
	prefix := ""
	for t.Kind() == reflect.Pointer {
		prefix += "*"
		t = t.Elem()
	}
	if t.PkgPath() == "" {
		return prefix + t.String()
	}
	return prefix + t.PkgPath() + "." + t.Name()
}
