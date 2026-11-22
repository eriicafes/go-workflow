// Package sdkerror records errors as the Workflow SDK records them: a chain
// of JavaScript errors, each with a type such as Error, TypeError or
// FatalError, a name, a message, other properties, and a cause.
//
// A Go error is recorded as an Error named after its Go type, with its JSON
// encoding as its data property, and the errors it wraps as its causes. An
// error with no type of its own, made by errors.New, fmt.Errorf or
// errors.Join, is recorded as a plain Error without a name. A Go type
// registered with Register is recorded as the SDK's own type instead, one
// registered with RegisterNamed as an Error with the name of the SDK's class,
// as the SDK records a class it has no type for, and an error value
// registered with RegisterSentinel as an Error named after the SDK's class
// for it, as is an error that wraps it.
// Read back, the chain keeps errors.Is and errors.As working: Is matches a
// target with the type and message of an error in the chain, which is how
// sentinel errors are matched, and As decodes an error of the target's type
// from what it recorded.
package sdkerror

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// Error is one error of a recorded chain, and through Cause the errors it
// wraps.
type Error struct {
	// Type is the error's JavaScript type, such as TypeError or FatalError.
	// Empty is Error.
	Type string `json:"type,omitempty"`
	// Name is the name property: a recorded Go error's Go type.
	Name string `json:"name,omitempty"`
	// Message is the message property.
	Message string `json:"message"`
	// Fields are the error's other properties, in order, as JSON: a Go
	// error's data, or the fields of the SDK's type.
	Fields []Field `json:"fields,omitempty"`
	// Cause is the next error of the chain.
	Cause *Error `json:"cause,omitempty"`
}

// Field is a property of an Error.
type Field struct {
	Name  string          `json:"name"`
	Value json.RawMessage `json:"value"`
}

// dataField is the property that holds a Go error's JSON encoding.
const dataField = "data"

// New records err and the errors it wraps as a chain. An Error in err's
// chain contributes the chain it recorded. It returns nil when err is nil.
func New(err error) *Error {
	nodes := appendChain(nil, err)
	if len(nodes) == 0 {
		return nil
	}
	for index := len(nodes) - 2; index >= 0; index-- {
		nodes[index].Cause = nodes[index+1]
	}
	// An error that wraps a registered sentinel is one of its class.
	if nodes[0].Name == "" && nodes[0].Type == "" {
		for _, sentinel := range sentinels {
			if errors.Is(err, sentinel.err) {
				nodes[0].Name = sentinel.name
				break
			}
		}
	}
	return nodes[0]
}

// appendChain appends err and the errors it wraps to nodes, unlinked.
func appendChain(nodes []*Error, err error) []*Error {
	if err == nil {
		return nodes
	}
	if recorded, ok := err.(*Error); ok {
		for ; recorded != nil; recorded = recorded.Cause {
			node := *recorded
			node.Cause = nil
			nodes = append(nodes, &node)
		}
		return nodes
	}
	goType := reflect.TypeOf(err)
	node := &Error{Message: err.Error()}
	if registered, ok := byGoType[goType]; ok {
		node.Fields = registered.fields(err)
		if registered.named {
			node.Name = registered.typ
		} else {
			node.Type = registered.typ
		}
	} else if name, ok := sentinelName(err); ok {
		node.Name = name
	} else if !plainTypes[goType] {
		node.Name = typeName(goType)
		if data, marshalErr := json.Marshal(err); marshalErr == nil {
			node.Fields = []Field{{Name: dataField, Value: data}}
		}
	}
	nodes = append(nodes, node)
	switch wrapped := err.(type) {
	case interface{ Unwrap() error }:
		nodes = appendChain(nodes, wrapped.Unwrap())
	case interface{ Unwrap() []error }:
		for _, err := range wrapped.Unwrap() {
			nodes = appendChain(nodes, err)
		}
	}
	return nodes
}

// Error returns the error's message.
func (e *Error) Error() string { return e.Message }

// Unwrap returns the next error of the chain, or nil at its end.
func (e *Error) Unwrap() error {
	if e.Cause == nil {
		return nil
	}
	return e.Cause
}

// Field returns the value of the field named name.
func (e *Error) Field(name string) (json.RawMessage, bool) {
	for _, field := range e.Fields {
		if field.Name == name {
			return field.Value, true
		}
	}
	return nil, false
}

// NewField returns the field named name holding value's JSON encoding.
func NewField(name string, value any) Field {
	data, _ := json.Marshal(value)
	return Field{Name: name, Value: data}
}

// Decode decodes the field named name into value, a pointer, and reports
// whether the error has it and it decoded.
func (e *Error) Decode(name string, value any) bool {
	data, ok := e.Field(name)
	return ok && json.Unmarshal(data, value) == nil
}

// registration returns the registered type the error was recorded as.
func (e *Error) registration() (*registration, bool) {
	if e.Type == "" || e.Type == "Error" {
		registered, ok := byName[e.Name]
		return registered, ok
	}
	registered, ok := byType[e.Type]
	return registered, ok
}

// goType returns the Go type the error was recorded from: a registered
// type's, its name, or for an error of the SDK, its type.
func (e *Error) goType() string {
	if registered, ok := e.registration(); ok {
		return registered.name
	}
	if e.Name != "" {
		return e.Name
	}
	if e.Type == "" || e.Type == "Error" {
		// A plain Error is what errors.New records.
		return plainTypeName
	}
	return e.Type
}

// Is reports whether the error has the type and message of target, or for
// an error of a registered type, whether that type's own Is matches target.
func (e *Error) Is(target error) bool {
	if target == nil {
		return false
	}
	if e.goType() == typeName(reflect.TypeOf(target)) && e.Message == target.Error() {
		return true
	}
	for _, sentinel := range sentinels {
		if sentinel.name == e.Name && sentinel.err == target && (e.Type == "" || e.Type == "Error") {
			return true
		}
	}
	if registered, ok := e.registration(); ok {
		value := reflect.New(registered.goType)
		registered.restore(value.Interface(), e)
		if matcher, ok := value.Elem().Interface().(interface{ Is(error) bool }); ok {
			return matcher.Is(target)
		}
	}
	return false
}

// As sets target to the error when its type is the type target points to,
// decoded from what it recorded, and reports whether it was. Fields the
// record does not hold, such as unexported fields, keep their zero values.
func (e *Error) As(target any) bool {
	pointer := reflect.ValueOf(target)
	if pointer.Kind() != reflect.Pointer || pointer.IsNil() {
		return false
	}
	targetType := pointer.Type().Elem()
	if targetType.Kind() == reflect.Interface || e.goType() != typeName(targetType) {
		return false
	}
	value := reflect.New(targetType)
	if registered, ok := e.registration(); ok && registered.goType == targetType {
		registered.restore(value.Interface(), e)
		pointer.Elem().Set(value.Elem())
		return true
	}
	if targetType.Kind() == reflect.Pointer {
		value.Elem().Set(reflect.New(targetType.Elem()))
	}
	if data, ok := e.Field(dataField); ok {
		var typeError *json.UnmarshalTypeError
		if err := json.Unmarshal(data, value.Interface()); err != nil && !errors.As(err, &typeError) {
			return false
		}
	}
	// A pointer to a registered type is recorded by its Go type, and still
	// restores what JSON cannot hold, such as its cause.
	if targetType.Kind() == reflect.Pointer {
		if registered, ok := byGoType[targetType.Elem()]; ok {
			registered.restore(value.Elem().Interface(), e)
		}
	}
	pointer.Elem().Set(value.Elem())
	return true
}

// IsType reports whether typ names a JavaScript error type: Error, a type
// ending in Error, or DOMException.
func IsType(typ string) bool {
	return typ == "Error" || strings.HasSuffix(typ, "Error") || typ == "DOMException"
}

// registration is a Go error type the SDK has a type, or a class name, of
// its own for.
type registration struct {
	typ string
	// named reports that the SDK records it as an Error named typ.
	named   bool
	goType  reflect.Type
	name    string
	fields  func(error) []Field
	restore func(target any, recorded *Error)
}

var (
	byType   = map[string]*registration{}
	byName   = map[string]*registration{}
	byGoType = map[reflect.Type]*registration{}
	// sentinels are the error values registered with RegisterSentinel.
	sentinels []sentinel
)

// Register records errors of type T as the SDK's typ errors, so the SDK
// revives them as its own. fields returns the properties the SDK records for
// one, and restore sets one from its record, including what JSON cannot
// hold, such as its cause. T may be a pointer type. Register is called from
// init functions; a pointer to a non-pointer T is still recorded by its Go
// type.
func Register[T error](typ string, fields func(T) []Field, restore func(target *T, recorded *Error)) {
	registered := newRegistration(typ, fields, restore)
	byType[typ], byGoType[registered.goType] = registered, registered
}

// RegisterNamed records errors of type T as an Error named name, as the SDK
// records its errors of the class name, which it has no type for. The SDK
// revives one as an Error of that name, which its class's is() matches, and
// keeps only its message and cause; fields and restore are as Register's,
// for Go to read back.
func RegisterNamed[T error](name string, fields func(T) []Field, restore func(target *T, recorded *Error)) {
	registered := newRegistration(name, fields, restore)
	registered.named = true
	byName[name], byGoType[registered.goType] = registered, registered
}

// RegisterSentinel records the error value err, and an error that wraps it,
// as an Error named name, the SDK's class for it. Read back, errors.Is
// matches err. RegisterSentinel is called from init functions.
func RegisterSentinel(name string, err error) {
	sentinels = append(sentinels, sentinel{name: name, err: err})
}

type sentinel struct {
	name string
	err  error
}

// sentinelName returns the name err is registered under.
func sentinelName(err error) (string, bool) {
	for _, sentinel := range sentinels {
		if sentinel.err == err {
			return sentinel.name, true
		}
	}
	return "", false
}

// plainTypes are the types of errors with no type of their own, which are
// recorded as a plain Error.
var plainTypes = map[reflect.Type]bool{
	reflect.TypeOf(errors.New("")):                                     true,
	reflect.TypeOf(fmt.Errorf("%w", errors.New(""))):                   true,
	reflect.TypeOf(fmt.Errorf("%w%w", errors.New(""), errors.New(""))): true,
	reflect.TypeOf(fmt.Errorf("")):                                     true,
	reflect.TypeOf(errors.Join(errors.New(""))):                        true,
}

// plainTypeName is the type name a plain Error is matched as.
var plainTypeName = typeName(reflect.TypeOf(errors.New("")))

func newRegistration[T error](typ string, fields func(T) []Field, restore func(target *T, recorded *Error)) *registration {
	goType := reflect.TypeFor[T]()
	return &registration{
		typ:     typ,
		goType:  goType,
		name:    typeName(goType),
		fields:  func(err error) []Field { return fields(err.(T)) },
		restore: func(target any, recorded *Error) { restore(target.(*T), recorded) },
	}
}

// typeName returns a Go type's name with its package path, such as
// "*github.com/org/pkg.MyError".
func typeName(t reflect.Type) string {
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
