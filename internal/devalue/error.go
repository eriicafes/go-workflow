package devalue

import (
	"fmt"

	"github.com/eriicafes/go-workflow/internal/sdkerror"
)

// encodeError returns the node of err, recorded with sdkerror.
func encodeError(err error) (any, error) {
	return errorNode(sdkerror.New(err))
}

// errorNode returns the node of an Error.
func errorNode(e *sdkerror.Error) (any, error) {
	object := &Object{}
	if e.Name != "" {
		object.Set("name", e.Name)
	}
	object.Set("message", e.Message)
	object.Set("stack", Undefined{})
	if e.Cause != nil {
		cause, err := errorNode(e.Cause)
		if err != nil {
			return nil, err
		}
		object.Set("cause", cause)
	}
	for _, field := range e.Fields {
		value, err := JSONNode(field.Value)
		if err != nil {
			return nil, err
		}
		object.Set(field.Name, value)
	}
	typ := e.Type
	if typ == "" {
		typ = "Error"
	}
	return Typed{typ, object}, nil
}

// decodeError returns the error node records, as an *sdkerror.Error.
func decodeError(node any) (error, error) {
	return errorOf(node)
}

// errorOf reads an Error node and the Errors of its causes. Undefined
// properties are left out.
func errorOf(node any) (*sdkerror.Error, error) {
	typed, ok := node.(Typed)
	if !ok || !sdkerror.IsType(typed.Type) {
		return nil, fmt.Errorf("%w: cannot decode %s as an error", ErrInvalid, NodeKind(node))
	}
	object, ok := typed.Value.(*Object)
	if !ok {
		return nil, fmt.Errorf("%w: %s is not an object", ErrInvalid, typed.Type)
	}
	value := &sdkerror.Error{Type: typed.Type}
	for index, key := range object.keys {
		property := object.values[index]
		if _, ok := property.(Undefined); ok {
			continue
		}
		switch key {
		case "name":
			value.Name, _ = property.(string)
		case "message":
			value.Message, _ = property.(string)
		case "stack":
		case "cause":
			// A cause that is not an Error ends the chain.
			if typed, ok := property.(Typed); ok && sdkerror.IsType(typed.Type) {
				cause, err := errorOf(property)
				if err != nil {
					return nil, err
				}
				value.Cause = cause
			}
		default:
			data, err := NodeJSON(property)
			if err != nil {
				return nil, err
			}
			value.Fields = append(value.Fields, sdkerror.Field{Name: key, Value: data})
		}
	}
	return value, nil
}
