package devalue

import (
	"fmt"
	"math"
)

// devalue's indexes for values that are not stored.
const (
	indexUndefined = -1
	indexHole      = -2
	indexNaN       = -3
	indexPosInf    = -4
	indexNegInf    = -5
	indexNegZero   = -6
	indexSparse    = -7
)

// A devalue value is decoded to, and encoded from, these nodes: nil (null),
// Undefined, bool, string, Number, special, []any, *Object, and
// Typed for a reduced type such as Date or Uint8Array.
type (
	Undefined struct{}
	Number    string
	special   int
	Object    struct {
		keys   []string
		values []any
	}
	Typed struct {
		Type  string
		Value any
	}
)

func (o *Object) Set(key string, value any) {
	o.keys, o.values = append(o.keys, key), append(o.values, value)
}

func (o *Object) Get(key string) (any, bool) {
	for index, name := range o.keys {
		if name == key {
			return o.values[index], true
		}
	}
	return nil, false
}

func NodeKind(node any) string {
	switch node := node.(type) {
	case Typed:
		return node.Type
	case *Object:
		return "object"
	case []any:
		return "array"
	case Number, special:
		return "number"
	}
	return fmt.Sprintf("%T", node)
}

func NumberText(node any) (string, bool) {
	switch value := node.(type) {
	case Number:
		return string(value), true
	case Typed:
		if value.Type == "BigInt" {
			if text, ok := value.Value.(string); ok {
				return text, true
			}
		}
	}
	return "", false
}

func specialFloat(value special) float64 {
	switch value {
	case indexNaN:
		return math.NaN()
	case indexPosInf:
		return math.Inf(1)
	case indexNegInf:
		return math.Inf(-1)
	}
	return math.Copysign(0, -1)
}

func arrayItems(node any) ([]any, bool) {
	switch value := node.(type) {
	case []any:
		return value, true
	case Typed:
		if value.Type == "Set" {
			items, ok := value.Value.([]any)
			return items, ok
		}
	}
	return nil, false
}

// bytesOf returns the bytes of a Uint8Array, ArrayBuffer, or base64 string.
func bytesOf(node any) ([]byte, bool, error) {
	var text string
	switch value := node.(type) {
	case Typed:
		switch value.Type {
		case "Uint8Array", "ArrayBuffer", "Uint8ClampedArray", "Int8Array", "DataViewBytes":
			s, ok := value.Value.(string)
			if !ok {
				return nil, true, fmt.Errorf("%w: %s is not base64", ErrInvalid, value.Type)
			}
			text = s
		default:
			return nil, false, nil
		}
	case string:
		text = value
	default:
		return nil, false, nil
	}
	data, err := decodeBase64(text)
	if err != nil {
		return nil, true, fmt.Errorf("%w: bytes: %v", ErrInvalid, err)
	}
	return data, true, nil
}

func typedString(node any, typ string) (string, error) {
	switch value := node.(type) {
	case string:
		return value, nil
	case Typed:
		if text, ok := value.Value.(string); ok && value.Type == typ {
			return text, nil
		}
	}
	return "", fmt.Errorf("%w: cannot decode %s as %s", ErrInvalid, NodeKind(node), typ)
}
