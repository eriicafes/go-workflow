package devalue

import (
	"reflect"
	"slices"
	"strings"
	"sync"
)

func fieldByIndex(v reflect.Value, index []int) (reflect.Value, bool) {
	for i, step := range index {
		if i > 0 && v.Kind() == reflect.Pointer {
			if v.IsNil() {
				return reflect.Value{}, false
			}
			v = v.Elem()
		}
		v = v.Field(step)
	}
	return v, true
}

func isEmptyValue(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64, reflect.Interface, reflect.Pointer:
		return v.IsZero()
	}
	return false
}

// structField is a field as encoding/json names it.
type structField struct {
	name                string
	index               []int
	omitEmpty, omitZero bool
	quoted              bool
	depth               int
	tagged              bool
	fieldType           reflect.Type
}

var structFieldCache sync.Map

// structFields returns the fields of t that encoding/json encodes, by the
// names it gives them.
func structFields(t reflect.Type) []structField {
	if cached, ok := structFieldCache.Load(t); ok {
		return cached.([]structField)
	}
	var all []structField
	var walk func(t reflect.Type, index []int, depth int, visited map[reflect.Type]bool)
	walk = func(t reflect.Type, index []int, depth int, visited map[reflect.Type]bool) {
		if visited[t] {
			return
		}
		visited[t] = true
		defer delete(visited, t)
		for i := range t.NumField() {
			f := t.Field(i)
			tag := f.Tag.Get("json")
			if tag == "-" {
				continue
			}
			name, options, _ := strings.Cut(tag, ",")
			fieldIndex := append(slices.Clone(index), i)
			if f.Anonymous && name == "" {
				ft := f.Type
				if ft.Kind() == reflect.Pointer {
					ft = ft.Elem()
				}
				if ft.Kind() == reflect.Struct {
					walk(ft, fieldIndex, depth+1, visited)
					continue
				}
			}
			if !f.IsExported() {
				continue
			}
			field := structField{name: name, index: fieldIndex, depth: depth, tagged: name != "", fieldType: f.Type}
			if field.name == "" {
				field.name = f.Name
			}
			for option := range strings.SplitSeq(options, ",") {
				switch option {
				case "omitempty":
					field.omitEmpty = true
				case "omitzero":
					field.omitZero = true
				case "string":
					field.quoted = true
				}
			}
			all = append(all, field)
		}
	}
	walk(t, nil, 0, map[reflect.Type]bool{})
	// As encoding/json does, the shallowest field of a name wins, a tagged
	// one among fields of the same depth, and a name with several such
	// fields is dropped.
	var fields []structField
	for _, field := range all {
		best, ambiguous := field, false
		for _, other := range all {
			if other.name != field.name || other.index[0] == field.index[0] && slices.Equal(other.index, field.index) {
				continue
			}
			if other.depth < best.depth || other.depth == best.depth && other.tagged && !best.tagged {
				best, ambiguous = other, false
			} else if other.depth == best.depth && other.tagged == best.tagged {
				ambiguous = true
			}
		}
		if !ambiguous && slices.Equal(best.index, field.index) {
			fields = append(fields, field)
		}
	}
	structFieldCache.Store(t, fields)
	return fields
}

func findField(fields []structField, key string) *structField {
	for i := range fields {
		if fields[i].name == key {
			return &fields[i]
		}
	}
	for i := range fields {
		if strings.EqualFold(fields[i].name, key) {
			return &fields[i]
		}
	}
	return nil
}
