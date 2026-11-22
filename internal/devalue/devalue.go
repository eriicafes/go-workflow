// Package devalue encodes Go values as devalue text, the format the Workflow
// SDK stores payloads in, and decodes them back. devalue is a flat array of
// values that refer to each other by index, so it holds shared references,
// undefined, NaN, -0, sparse arrays and reduced JavaScript types such as
// Date, Uint8Array, BigInt, Map and Error, which JSON cannot. Values map onto
// devalue's JavaScript types: []byte is a Uint8Array, time.Time a Date,
// *big.Int a BigInt, *url.URL a URL, http.Header Headers, a map with keys
// other than strings a Map, and an error an Error.
//
// Values are encoded to, and decoded from, a tree of nodes: nil (null),
// Undefined, bool, string, Number, []any, *Object, Typed for a reduced type,
// and devalue's special numbers.
package devalue

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
)

// ErrInvalid reports a payload that cannot be decoded.
var ErrInvalid = errors.New("workflow: invalid payload")

// Marshaler is implemented by a type that chooses its own payload
// encoding: MarshalPayload returns the value to encode in its place.
type Marshaler interface {
	MarshalPayload() (any, error)
}

// Unmarshaler is implemented by a type that decodes its own payload:
// UnmarshalPayload calls decode with a pointer to decode the payload into.
type Unmarshaler interface {
	UnmarshalPayload(decode func(any) error) error
}

// Reduced is a value of a reduced type, devalue's ["Type", value],
// such as a Request or a ReadableStream reference. Decoding into a
// Reduced whose Value holds a pointer decodes the reduced value into
// it; otherwise Value holds it as an any does.
type Reduced struct {
	Type  string
	Value any
}

// Marshal returns value as devalue text, with reducers for the
// JavaScript types Go values map onto. An error is recorded with sdkerror.
func Marshal(value any) ([]byte, error) {
	node, err := (encoder{}).encodeNode(reflect.ValueOf(value), nil)
	if err != nil {
		return nil, err
	}
	return writeNode(node), nil
}

// MarshalError returns err as devalue text, as an error held where its
// static type is error is encoded.
func MarshalError(err error) ([]byte, error) {
	node, encodeErr := encodeError(err)
	if encodeErr != nil {
		return nil, encodeErr
	}
	return writeNode(node), nil
}

// Unmarshal decodes devalue text into value, a pointer. An Error decodes as
// an *sdkerror.Error.
func Unmarshal(data []byte, value any) error {
	target := reflect.ValueOf(value)
	if target.Kind() != reflect.Pointer || target.IsNil() {
		return fmt.Errorf("workflow: decode payload into non-pointer %T", value)
	}
	node, err := parse(data)
	if err != nil {
		return err
	}
	return (decoder{}).decodeNode(node, target.Elem())
}

// NodeJSON returns a node as JSON text, for types decoded from JSON. Numbers
// keep their text and objects their key order, so JSON written by JSONNode
// reads back as it was, including integers beyond float64's precision.
func NodeJSON(node any) ([]byte, error) {
	var buffer bytes.Buffer
	if err := (decoder{}).writeNodeJSON(&buffer, node); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}
