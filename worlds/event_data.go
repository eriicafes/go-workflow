package worlds

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"

	"github.com/fxamacker/cbor/v2"
)

// The local and Postgres Worlds store an event's Data as CBOR rather than
// JSON: payload fields become raw byte strings instead of base64, and
// serialized payload bytes are kept as they are. The encoding is this
// package's own, and no JavaScript World reads it.
//
// Data that decodes strictly into its event type's Data struct is stored as
// that struct, a CBOR map keyed by the struct's field names. Other Data, such
// as an event type without a struct or Data with fields its struct lacks, is
// stored as its JSON value under genericEventDataTag, so nothing is lost.

// genericEventDataTag wraps Data stored as a plain JSON value.
const genericEventDataTag = 27482

var (
	eventDataEncoding = func() cbor.EncMode {
		mode, err := cbor.EncOptions{Time: cbor.TimeRFC3339Nano, Sort: cbor.SortNone}.EncMode()
		if err != nil {
			panic(err)
		}
		return mode
	}()
	eventDataStrict = func() cbor.DecMode {
		mode, err := cbor.DecOptions{DefaultMapType: reflect.TypeOf(map[string]any(nil)), ExtraReturnErrors: cbor.ExtraDecErrorUnknownField}.DecMode()
		if err != nil {
			panic(err)
		}
		return mode
	}()
	eventDataGeneric = func() cbor.DecMode {
		mode, err := cbor.DecOptions{DefaultMapType: reflect.TypeOf(map[string]any(nil))}.DecMode()
		if err != nil {
			panic(err)
		}
		return mode
	}()
)

// eventDataType returns a new value of the Data struct of eventType, or nil
// for a type without one.
func eventDataType(eventType EventType) any {
	switch eventType {
	case EventTypeRunCreated, EventTypeRunStarted:
		return &DataRunCreated{}
	case EventTypeRunCompleted:
		return &DataRunCompleted{}
	case EventTypeRunFailed:
		return &DataRunFailed{}
	case EventTypeRunCancelled:
		return &DataRunCancelled{}
	case EventTypeStepCreated:
		return &DataStepCreated{}
	case EventTypeStepStarted:
		return &DataStepStarted{}
	case EventTypeStepCompleted, EventTypeStepFailed, EventTypeStepRetrying:
		return &DataStepResult{}
	case EventTypeWaitCreated:
		return &DataWaitCreated{}
	case EventTypeWaitCompleted:
		return &DataWaitCompleted{}
	case EventTypeHookCreated, EventTypeHookConflict:
		return &DataHookCreated{}
	case EventTypeHookReceived:
		return &DataHookReceived{}
	case EventTypeHookDisposed:
		return &HookDisposedEventData{}
	case EventTypeAttrSet:
		return &DataAttrSet{}
	}
	return nil
}

// EncodeEventData returns an event's JSON Data as the CBOR a World stores.
// Empty Data encodes as nil.
func EncodeEventData(eventType EventType, data []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, nil
	}
	if typed := eventDataType(eventType); typed != nil {
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.DisallowUnknownFields()
		if decoder.Decode(typed) == nil && !decoder.More() {
			return eventDataEncoding.Marshal(typed)
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, fmt.Errorf("%w: event data is not JSON: %v", ErrEventInvalid, err)
	}
	return eventDataEncoding.Marshal(cbor.Tag{Number: genericEventDataTag, Content: jsonNumbers(value)})
}

// DecodeEventData returns the JSON Data of an event stored by
// EncodeEventData. Nil decodes as nil.
func DecodeEventData(eventType EventType, encoded []byte) ([]byte, error) {
	if len(encoded) == 0 {
		return nil, nil
	}
	if encoded[0]>>5 == 6 {
		var tag cbor.Tag
		if err := eventDataGeneric.Unmarshal(encoded, &tag); err != nil {
			return nil, fmt.Errorf("decode event data: %w", err)
		}
		if tag.Number != genericEventDataTag {
			return nil, fmt.Errorf("decode event data: unknown tag %d", tag.Number)
		}
		return json.Marshal(tag.Content)
	}
	typed := eventDataType(eventType)
	if typed == nil {
		return nil, fmt.Errorf("decode event data: %s has no Data struct", eventType)
	}
	if err := eventDataStrict.Unmarshal(encoded, typed); err != nil {
		return nil, fmt.Errorf("decode event data: %w", err)
	}
	return json.Marshal(typed)
}

// jsonNumbers replaces the json.Numbers in value with integers where they
// are whole, and floats otherwise.
func jsonNumbers(value any) any {
	switch value := value.(type) {
	case json.Number:
		if integer, err := strconv.ParseInt(string(value), 10, 64); err == nil {
			return integer
		}
		float, _ := value.Float64()
		return float
	case map[string]any:
		for key, item := range value {
			value[key] = jsonNumbers(item)
		}
	case []any:
		for index, item := range value {
			value[index] = jsonNumbers(item)
		}
	}
	return value
}
