package workflow

import (
	"reflect"

	"github.com/eriicafes/go-workflow/internal/devalue"
)

// Args, embedded in a workflow's input struct, makes the struct's exported
// fields the workflow's arguments, in the order they are declared, as a
// Workflow SDK caller passes them: start(workflow, [a, b]) sets the first
// field to a and the second to b. Missing arguments leave their fields zero,
// and extra ones are ignored, as JavaScript treats a function's arguments.
// Without Args, the whole input is the workflow's one argument.
//
//	type Add struct {
//		workflow.Struct[int]
//		workflow.Args
//		A, B int
//	}
type Args struct{}

var argsType = reflect.TypeFor[Args]()

// argumentFields returns the indexes of the fields of the struct type t that
// are its arguments, when t embeds Args, and false when it does not.
func argumentFields(t reflect.Type) ([]int, bool) {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil, false
	}
	embeds := false
	var fields []int
	for i := range t.NumField() {
		field := t.Field(i)
		switch {
		case field.Anonymous && field.Type == argsType:
			embeds = true
		case field.Anonymous || !field.IsExported():
		default:
			fields = append(fields, i)
		}
	}
	return fields, embeds
}

// arguments returns the argument list a run of input is created with: the
// fields of an input that embeds Args, and otherwise [input].
func arguments[In any](input In) any {
	fields, ok := argumentFields(reflect.TypeFor[In]())
	if !ok {
		return [1]In{input}
	}
	value := reflect.ValueOf(input)
	for value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return []any{}
		}
		value = value.Elem()
	}
	list := make([]any, len(fields))
	for i, index := range fields {
		list[i] = value.Field(index).Interface()
	}
	return list
}

// decodeArguments decodes the argument list raw into an In, with decode
// decoding a payload into a pointer.
func decodeArguments[In any](raw []byte, decode func([]byte, any) error) (In, error) {
	var input In
	fields, ok := argumentFields(reflect.TypeFor[In]())
	if !ok {
		var list [1]In
		err := decode(raw, &list)
		return list[0], err
	}
	value := reflect.ValueOf(&input).Elem()
	for value.Kind() == reflect.Pointer {
		value.Set(reflect.New(value.Type().Elem()))
		value = value.Elem()
	}
	// Each slot decodes its argument into its field. An array decodes as
	// many items as it has slots and leaves the rest of the list alone.
	slots := reflect.New(reflect.ArrayOf(len(fields), reflect.TypeFor[argumentSlot]())).Elem()
	for i, index := range fields {
		slots.Index(i).Set(reflect.ValueOf(argumentSlot{field: value.Field(index).Addr().Interface()}))
	}
	err := decode(raw, slots.Addr().Interface())
	return input, err
}

// argumentSlot decodes one argument into the field it points to.
type argumentSlot struct{ field any }

var _ devalue.Unmarshaler = (*argumentSlot)(nil)

func (s *argumentSlot) UnmarshalPayload(decode func(any) error) error {
	if s.field == nil {
		return nil
	}
	return decode(s.field)
}
