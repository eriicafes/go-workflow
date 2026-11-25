package postgresworld

import (
	"encoding/json"
	"reflect"

	"github.com/fxamacker/cbor/v2"
)

// cborDecoding decodes the CBOR columns the Workflow SDK's Postgres World
// keeps as bytea, such as execution_context_cbor and start_hook_cbor, with
// maps keyed by string.
var cborDecoding = func() cbor.DecMode {
	mode, err := cbor.DecOptions{DefaultMapType: reflect.TypeOf(map[string]any(nil))}.DecMode()
	if err != nil {
		panic(err)
	}
	return mode
}()

// cborEncoding encodes a time as cbor-x encodes a Date, which the Workflow
// SDK's Postgres World writes: tag 1 over its epoch seconds, fractional when
// it has milliseconds.
var cborEncoding = func() cbor.EncMode {
	mode, err := cbor.EncOptions{Time: cbor.TimeUnixDynamic, TimeTag: cbor.EncTagRequired}.EncMode()
	if err != nil {
		panic(err)
	}
	return mode
}()

// encodeJSONValue returns value as CBOR after a JSON round trip, so a read
// returns the same JSON values, such as float64 numbers, whatever Go types
// the caller passed.
func encodeJSONValue(value any) ([]byte, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var plain any
	if err := json.Unmarshal(encoded, &plain); err != nil {
		return nil, err
	}
	return cbor.Marshal(plain)
}
