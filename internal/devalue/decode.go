package devalue

import (
	"bytes"
	"encoding"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"time"

	"github.com/eriicafes/go-workflow/internal/sdkerror"
)

// decoder decodes nodes into Go values.
type decoder struct{}

func (d decoder) writeNodeJSON(buffer *bytes.Buffer, node any) error {
	switch value := node.(type) {
	case nil, Undefined:
		buffer.WriteString("null")
		return nil
	case Number:
		if !json.Valid([]byte(value)) {
			return fmt.Errorf("%w: number %s", ErrInvalid, value)
		}
		buffer.WriteString(string(value))
		return nil
	case []any:
		buffer.WriteByte('[')
		for i, item := range value {
			if i > 0 {
				buffer.WriteByte(',')
			}
			if err := d.writeNodeJSON(buffer, item); err != nil {
				return err
			}
		}
		buffer.WriteByte(']')
		return nil
	case *Object:
		buffer.WriteByte('{')
		first := true
		for i, key := range value.keys {
			if _, ok := value.values[i].(Undefined); ok {
				continue
			}
			if !first {
				buffer.WriteByte(',')
			}
			first = false
			name, _ := json.Marshal(key)
			buffer.Write(name)
			buffer.WriteByte(':')
			if err := d.writeNodeJSON(buffer, value.values[i]); err != nil {
				return err
			}
		}
		buffer.WriteByte('}')
		return nil
	}
	// Strings, booleans, special numbers, and the mapped types are written as
	// encoding/json writes their Go values.
	converted, err := d.nodeAny(node)
	if err != nil {
		return err
	}
	data, err := json.Marshal(converted)
	if err != nil {
		return err
	}
	buffer.Write(data)
	return nil
}

// decodeNode stores node in v.
func (d decoder) decodeNode(node any, v reflect.Value) error {
	if _, ok := node.(Undefined); ok {
		return nil
	}
	if v.Kind() != reflect.Pointer && v.CanAddr() && v.Addr().Type().Implements(payloadUnmarsh) {
		return v.Addr().Interface().(Unmarshaler).UnmarshalPayload(func(target any) error {
			pointer := reflect.ValueOf(target)
			if pointer.Kind() != reflect.Pointer || pointer.IsNil() {
				return fmt.Errorf("workflow: decode payload into non-pointer %T", target)
			}
			return d.decodeNode(node, pointer.Elem())
		})
	}
	if node == nil {
		switch v.Kind() {
		case reflect.Interface, reflect.Pointer, reflect.Map, reflect.Slice:
			v.SetZero()
		}
		return nil
	}
	t := v.Type()
	switch t {
	case reducedType:
		typed, ok := node.(Typed)
		if !ok {
			return fmt.Errorf("%w: cannot decode %s as a reduced value", ErrInvalid, NodeKind(node))
		}
		reduced := v.Addr().Interface().(*Reduced)
		reduced.Type = typed.Type
		if target := reflect.ValueOf(reduced.Value); target.Kind() == reflect.Pointer && !target.IsNil() {
			return d.decodeNode(typed.Value, target.Elem())
		}
		value, err := d.nodeAny(typed.Value)
		reduced.Value = value
		return err
	case timeType:
		return decodeTime(node, v)
	case bigIntType:
		value, err := decodeBigInt(node)
		if err != nil {
			return err
		}
		v.Set(reflect.ValueOf(*value))
		return nil
	case urlType:
		text, err := typedString(node, "URL")
		if err != nil {
			return err
		}
		parsed, err := url.Parse(text)
		if err != nil {
			return fmt.Errorf("%w: URL: %v", ErrInvalid, err)
		}
		v.Set(reflect.ValueOf(*parsed))
		return nil
	case headerType:
		header, err := decodeHeaders(node)
		if err != nil {
			return err
		}
		v.Set(reflect.ValueOf(header))
		return nil
	case rawMessageType:
		data, err := NodeJSON(node)
		if err != nil {
			return err
		}
		v.SetBytes(data)
		return nil
	case errorType:
		err, decodeErr := decodeError(node)
		if decodeErr != nil {
			return decodeErr
		}
		v.Set(reflect.ValueOf(err))
		return nil
	}
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			v.Set(reflect.New(t.Elem()))
		}
		return d.decodeNode(node, v.Elem())
	}
	if v.CanAddr() && t.Kind() != reflect.Interface {
		pointer := v.Addr()
		if pointer.Type().Implements(jsonUnmarshaler) {
			data, err := NodeJSON(node)
			if err != nil {
				return err
			}
			return pointer.Interface().(json.Unmarshaler).UnmarshalJSON(data)
		}
		if text, ok := node.(string); ok && pointer.Type().Implements(textUnmarshaler) {
			return pointer.Interface().(encoding.TextUnmarshaler).UnmarshalText([]byte(text))
		}
	}
	switch t.Kind() {
	case reflect.Interface:
		value, err := d.nodeAny(node)
		if err != nil {
			return err
		}
		if value == nil {
			v.SetZero()
			return nil
		}
		rv := reflect.ValueOf(value)
		if !rv.Type().AssignableTo(t) {
			return fmt.Errorf("%w: cannot store %T in %s", ErrInvalid, value, t)
		}
		v.Set(rv)
		return nil
	case reflect.Bool:
		switch value := node.(type) {
		case bool:
			v.SetBool(value)
			return nil
		case string:
			parsed, err := strconv.ParseBool(value)
			if err == nil {
				v.SetBool(parsed)
				return nil
			}
		}
	case reflect.String:
		switch value := node.(type) {
		case string:
			v.SetString(value)
			return nil
		case Number:
			if t == reflect.TypeFor[json.Number]() {
				v.SetString(string(value))
				return nil
			}
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		text, ok := NumberText(node)
		if ok {
			n, err := strconv.ParseInt(text, 10, t.Bits())
			if err != nil {
				f, ferr := strconv.ParseFloat(text, 64)
				if ferr != nil || f != math.Trunc(f) || v.OverflowInt(int64(f)) {
					return fmt.Errorf("%w: %s does not fit %s", ErrInvalid, text, t)
				}
				n = int64(f)
			}
			v.SetInt(n)
			return nil
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		text, ok := NumberText(node)
		if ok {
			n, err := strconv.ParseUint(text, 10, t.Bits())
			if err != nil {
				f, ferr := strconv.ParseFloat(text, 64)
				if ferr != nil || f < 0 || f != math.Trunc(f) || v.OverflowUint(uint64(f)) {
					return fmt.Errorf("%w: %s does not fit %s", ErrInvalid, text, t)
				}
				n = uint64(f)
			}
			v.SetUint(n)
			return nil
		}
	case reflect.Float32, reflect.Float64:
		switch value := node.(type) {
		case special:
			v.SetFloat(specialFloat(value))
			return nil
		}
		if text, ok := NumberText(node); ok {
			f, err := strconv.ParseFloat(text, t.Bits())
			if err != nil {
				return fmt.Errorf("%w: %s does not fit %s", ErrInvalid, text, t)
			}
			v.SetFloat(f)
			return nil
		}
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			if data, ok, err := bytesOf(node); ok {
				if err != nil {
					return err
				}
				v.SetBytes(reflect.ValueOf(data).Convert(reflect.SliceOf(t.Elem())).Bytes())
				return nil
			}
		}
		items, ok := arrayItems(node)
		if !ok {
			break
		}
		slice := reflect.MakeSlice(t, len(items), len(items))
		for i, item := range items {
			if err := d.decodeNode(item, slice.Index(i)); err != nil {
				return err
			}
		}
		v.Set(slice)
		return nil
	case reflect.Array:
		items, ok := arrayItems(node)
		if !ok {
			break
		}
		for i := range v.Len() {
			if i < len(items) {
				if err := d.decodeNode(items[i], v.Index(i)); err != nil {
					return err
				}
			} else {
				v.Index(i).SetZero()
			}
		}
		return nil
	case reflect.Map:
		return d.decodeMap(node, v)
	case reflect.Struct:
		object, ok := node.(*Object)
		if !ok {
			break
		}
		fields := structFields(t)
		for i, key := range object.keys {
			field := findField(fields, key)
			if field == nil {
				continue
			}
			target := v
			for j, step := range field.index {
				if j > 0 && target.Kind() == reflect.Pointer {
					if target.IsNil() {
						target.Set(reflect.New(target.Type().Elem()))
					}
					target = target.Elem()
				}
				target = target.Field(step)
			}
			value := object.values[i]
			if field.quoted {
				if text, ok := value.(string); ok && target.Kind() != reflect.String {
					value = Number(text)
					if target.Kind() == reflect.Bool {
						value = text == "true"
					}
				}
			}
			if err := d.decodeNode(value, target); err != nil {
				return fmt.Errorf("%s: %w", key, err)
			}
		}
		return nil
	}
	return fmt.Errorf("%w: cannot decode %s into %s", ErrInvalid, NodeKind(node), t)
}

func decodeTime(node any, v reflect.Value) error {
	text, err := typedString(node, "Date")
	if err != nil {
		return err
	}
	if text == "." {
		return fmt.Errorf("%w: invalid Date", ErrInvalid)
	}
	parsed, err := time.Parse(time.RFC3339Nano, text)
	if err != nil {
		return fmt.Errorf("%w: Date: %v", ErrInvalid, err)
	}
	v.Set(reflect.ValueOf(parsed))
	return nil
}

func decodeBigInt(node any) (*big.Int, error) {
	text, ok := NumberText(node)
	if !ok {
		if s, isString := node.(string); isString {
			text, ok = s, true
		}
	}
	value, parsed := new(big.Int).SetString(text, 10)
	if !ok || !parsed {
		return nil, fmt.Errorf("%w: cannot decode %s as BigInt", ErrInvalid, NodeKind(node))
	}
	return value, nil
}

func decodeHeaders(node any) (http.Header, error) {
	typed, ok := node.(Typed)
	var entries []any
	switch {
	case ok && typed.Type == "Headers":
		entries, ok = typed.Value.([]any)
	case !ok:
		if object, isObject := node.(*Object); isObject {
			header := http.Header{}
			for i, key := range object.keys {
				if text, isString := object.values[i].(string); isString {
					header.Add(key, text)
				}
			}
			return header, nil
		}
	}
	if !ok {
		return nil, fmt.Errorf("%w: cannot decode %s as Headers", ErrInvalid, NodeKind(node))
	}
	header := http.Header{}
	for _, entry := range entries {
		pair, isPair := entry.([]any)
		if !isPair || len(pair) != 2 {
			return nil, fmt.Errorf("%w: Headers entry", ErrInvalid)
		}
		name, nameOK := pair[0].(string)
		value, valueOK := pair[1].(string)
		if !nameOK || !valueOK {
			return nil, fmt.Errorf("%w: Headers entry", ErrInvalid)
		}
		header.Add(name, value)
	}
	return header, nil
}

func (d decoder) decodeMap(node any, v reflect.Value) error {
	t := v.Type()
	if v.IsNil() {
		v.Set(reflect.MakeMap(t))
	}
	switch value := node.(type) {
	case *Object:
		for i, key := range value.keys {
			keyValue := reflect.New(t.Key()).Elem()
			if err := d.decodeMapKey(key, keyValue); err != nil {
				return err
			}
			item := reflect.New(t.Elem()).Elem()
			if err := d.decodeNode(value.values[i], item); err != nil {
				return fmt.Errorf("%s: %w", key, err)
			}
			v.SetMapIndex(keyValue, item)
		}
		return nil
	case Typed:
		if value.Type != "Map" {
			break
		}
		pairs, _ := value.Value.([]any)
		for _, entry := range pairs {
			pair, ok := entry.([]any)
			if !ok || len(pair) != 2 {
				return fmt.Errorf("%w: Map entry", ErrInvalid)
			}
			key := reflect.New(t.Key()).Elem()
			if err := d.decodeNode(pair[0], key); err != nil {
				return err
			}
			item := reflect.New(t.Elem()).Elem()
			if err := d.decodeNode(pair[1], item); err != nil {
				return err
			}
			v.SetMapIndex(key, item)
		}
		return nil
	}
	return fmt.Errorf("%w: cannot decode %s into %s", ErrInvalid, NodeKind(node), t)
}

func (d decoder) decodeMapKey(key string, v reflect.Value) error {
	if v.Kind() == reflect.String {
		v.SetString(key)
		return nil
	}
	if v.CanAddr() && v.Addr().Type().Implements(textUnmarshaler) {
		return v.Addr().Interface().(encoding.TextUnmarshaler).UnmarshalText([]byte(key))
	}
	return d.decodeNode(Number(key), v)
}

// nodeAny returns node as the Go value an any holds: objects as
// map[string]any, arrays as []any, numbers as float64, and the mapped
// JavaScript types as their Go types.
func (d decoder) nodeAny(node any) (any, error) {
	switch value := node.(type) {
	case nil, Undefined:
		return nil, nil
	case bool, string:
		return value, nil
	case Number:
		f, err := strconv.ParseFloat(string(value), 64)
		if err != nil {
			return nil, fmt.Errorf("%w: number %s", ErrInvalid, value)
		}
		return f, nil
	case special:
		return specialFloat(value), nil
	case []any:
		items := make([]any, len(value))
		for i, item := range value {
			converted, err := d.nodeAny(item)
			if err != nil {
				return nil, err
			}
			items[i] = converted
		}
		return items, nil
	case *Object:
		object := make(map[string]any, len(value.keys))
		for i, key := range value.keys {
			if _, ok := value.values[i].(Undefined); ok {
				continue
			}
			converted, err := d.nodeAny(value.values[i])
			if err != nil {
				return nil, err
			}
			object[key] = converted
		}
		return object, nil
	case Typed:
		switch value.Type {
		case "Date":
			var t time.Time
			if err := decodeTime(value, reflect.ValueOf(&t).Elem()); err != nil {
				return nil, err
			}
			return t, nil
		case "BigInt":
			return decodeBigInt(value)
		case "URL":
			text, err := typedString(value, "URL")
			if err != nil {
				return nil, err
			}
			return url.Parse(text)
		case "Headers":
			return decodeHeaders(value)
		case "Map":
			pairs, _ := value.Value.([]any)
			result := make(map[any]any, len(pairs))
			for _, entry := range pairs {
				pair, ok := entry.([]any)
				if !ok || len(pair) != 2 {
					return nil, fmt.Errorf("%w: Map entry", ErrInvalid)
				}
				key, err := d.nodeAny(pair[0])
				if err != nil {
					return nil, err
				}
				if key != nil && !reflect.TypeOf(key).Comparable() {
					return nil, fmt.Errorf("%w: Map key %T", ErrInvalid, key)
				}
				item, err := d.nodeAny(pair[1])
				if err != nil {
					return nil, err
				}
				result[key] = item
			}
			return result, nil
		case "Set":
			items, _ := value.Value.([]any)
			return d.nodeAny(items)
		}
		if data, ok, err := bytesOf(value); ok {
			return data, err
		}
		if sdkerror.IsType(value.Type) {
			return decodeError(value)
		}
		inner, err := d.nodeAny(value.Value)
		if err != nil {
			return nil, err
		}
		return Reduced{Type: value.Type, Value: inner}, nil
	}
	return nil, fmt.Errorf("%w: unexpected node %T", ErrInvalid, node)
}
