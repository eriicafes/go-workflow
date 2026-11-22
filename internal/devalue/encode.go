package devalue

import (
	"encoding"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"net/http"
	"net/url"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"
)

// encoder encodes Go values as nodes.
type encoder struct{}

var (
	timeType        = reflect.TypeFor[time.Time]()
	bigIntType      = reflect.TypeFor[big.Int]()
	urlType         = reflect.TypeFor[url.URL]()
	headerType      = reflect.TypeFor[http.Header]()
	errorType       = reflect.TypeFor[error]()
	rawMessageType  = reflect.TypeFor[json.RawMessage]()
	reducedType     = reflect.TypeFor[Reduced]()
	payloadMarshal  = reflect.TypeFor[Marshaler]()
	payloadUnmarsh  = reflect.TypeFor[Unmarshaler]()
	jsonMarshaler   = reflect.TypeFor[json.Marshaler]()
	jsonUnmarshaler = reflect.TypeFor[json.Unmarshaler]()
	textMarshaler   = reflect.TypeFor[encoding.TextMarshaler]()
	textUnmarshaler = reflect.TypeFor[encoding.TextUnmarshaler]()
)

// encodeNode returns the node of v. seen holds the pointers being encoded,
// to report cycles.
func (e encoder) encodeNode(v reflect.Value, seen map[uintptr]bool) (any, error) {
	if !v.IsValid() {
		return nil, nil
	}
	if v.Kind() == reflect.Interface {
		if v.IsNil() {
			return nil, nil
		}
		if v.Type() == errorType {
			return encodeError(v.Interface().(error))
		}
		return e.encodeNode(v.Elem(), seen)
	}
	if v.Kind() == reflect.Pointer && v.IsNil() {
		return nil, nil
	}
	t := v.Type()
	if t.Implements(payloadMarshal) {
		replacement, err := v.Interface().(Marshaler).MarshalPayload()
		if err != nil {
			return nil, err
		}
		return e.encodeNode(reflect.ValueOf(replacement), seen)
	}
	switch t {
	case reducedType:
		reduced := v.Interface().(Reduced)
		value, err := e.encodeNode(reflect.ValueOf(reduced.Value), seen)
		if err != nil {
			return nil, err
		}
		return Typed{reduced.Type, value}, nil
	case timeType:
		return Typed{"Date", v.Interface().(time.Time).UTC().Format("2006-01-02T15:04:05.000Z")}, nil
	case bigIntType:
		value := v.Interface().(big.Int)
		return Typed{"BigInt", value.String()}, nil
	case urlType:
		value := v.Interface().(url.URL)
		return Typed{"URL", value.String()}, nil
	case headerType:
		var entries []any
		header := v.Interface().(http.Header)
		for _, name := range slices.Sorted(mapKeys(header)) {
			for _, value := range header[name] {
				entries = append(entries, []any{strings.ToLower(name), value})
			}
		}
		return Typed{"Headers", entries}, nil
	case rawMessageType:
		if v.Len() == 0 {
			return nil, nil
		}
		return JSONNode(v.Bytes())
	}
	if t.Kind() == reflect.Pointer && t.Elem().Kind() == reflect.Struct && (t.Elem() == bigIntType || t.Elem() == urlType || t.Elem() == timeType) {
		return e.encodeNode(v.Elem(), seen)
	}
	if t.Implements(jsonMarshaler) && t.Kind() != reflect.Interface {
		data, err := v.Interface().(json.Marshaler).MarshalJSON()
		if err != nil {
			return nil, err
		}
		return JSONNode(data)
	}
	if t.Implements(textMarshaler) {
		text, err := v.Interface().(encoding.TextMarshaler).MarshalText()
		if err != nil {
			return nil, err
		}
		return string(text), nil
	}
	switch t.Kind() {
	case reflect.Bool:
		return v.Bool(), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return Number(strconv.FormatInt(v.Int(), 10)), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return Number(strconv.FormatUint(v.Uint(), 10)), nil
	case reflect.Float32, reflect.Float64:
		return floatNode(v.Float(), t.Bits()), nil
	case reflect.String:
		return v.String(), nil
	case reflect.Slice:
		if v.IsNil() {
			return nil, nil
		}
		if t.Elem().Kind() == reflect.Uint8 && !t.Elem().Implements(payloadMarshal) {
			return Typed{"Uint8Array", encodeBase64(v.Bytes())}, nil
		}
		fallthrough
	case reflect.Array:
		items := make([]any, v.Len())
		for i := range items {
			item, err := e.encodeNode(v.Index(i), seen)
			if err != nil {
				return nil, err
			}
			items[i] = item
		}
		return items, nil
	case reflect.Map:
		if v.IsNil() {
			return nil, nil
		}
		return e.encodeMap(v, seen)
	case reflect.Struct:
		return e.encodeStruct(v, seen)
	case reflect.Pointer:
		if seen == nil {
			seen = map[uintptr]bool{}
		}
		if seen[v.Pointer()] {
			return nil, fmt.Errorf("workflow: encode payload: cycle through %s", t)
		}
		seen[v.Pointer()] = true
		defer delete(seen, v.Pointer())
		return e.encodeNode(v.Elem(), seen)
	}
	return nil, fmt.Errorf("workflow: encode payload: unsupported type %s", t)
}

func mapKeys[V any](m map[string]V) func(func(string) bool) {
	return func(yield func(string) bool) {
		for key := range m {
			if !yield(key) {
				return
			}
		}
	}
}

func floatNode(f float64, bits int) any {
	switch {
	case math.IsNaN(f):
		return special(indexNaN)
	case math.IsInf(f, 1):
		return special(indexPosInf)
	case math.IsInf(f, -1):
		return special(indexNegInf)
	case f == 0 && math.Signbit(f):
		return special(indexNegZero)
	}
	data, _ := json.Marshal(f)
	if bits == 32 {
		data, _ = json.Marshal(float32(f))
	}
	return Number(data)
}

func encodeBase64(data []byte) string {
	if len(data) == 0 {
		return "."
	}
	return base64.StdEncoding.EncodeToString(data)
}

func decodeBase64(value string) ([]byte, error) {
	if value == "." || value == "" {
		return []byte{}, nil
	}
	data, err := base64.StdEncoding.DecodeString(value)
	if err != nil {
		data, err = base64.RawStdEncoding.DecodeString(strings.TrimRight(value, "="))
	}
	return data, err
}

func (e encoder) encodeMap(v reflect.Value, seen map[uintptr]bool) (any, error) {
	keyType := v.Type().Key()
	type entry struct {
		key  reflect.Value
		text string
	}
	entries := make([]entry, 0, v.Len())
	stringKeys := keyType.Kind() == reflect.String
	for _, key := range v.MapKeys() {
		text := ""
		if stringKeys {
			text = key.String()
		} else {
			text = fmt.Sprint(key.Interface())
		}
		entries = append(entries, entry{key, text})
	}
	slices.SortFunc(entries, func(a, b entry) int {
		if !stringKeys && a.key.CanInt() && b.key.CanInt() {
			return cmpInt(a.key.Int(), b.key.Int())
		}
		return strings.Compare(a.text, b.text)
	})
	if stringKeys {
		object := &Object{}
		for _, item := range entries {
			value, err := e.encodeNode(v.MapIndex(item.key), seen)
			if err != nil {
				return nil, err
			}
			object.Set(item.text, value)
		}
		return object, nil
	}
	pairs := make([]any, 0, len(entries))
	for _, item := range entries {
		key, err := e.encodeNode(item.key, seen)
		if err != nil {
			return nil, err
		}
		value, err := e.encodeNode(v.MapIndex(item.key), seen)
		if err != nil {
			return nil, err
		}
		pairs = append(pairs, []any{key, value})
	}
	return Typed{"Map", pairs}, nil
}

func cmpInt(a, b int64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

func (e encoder) encodeStruct(v reflect.Value, seen map[uintptr]bool) (any, error) {
	object := &Object{}
	for _, field := range structFields(v.Type()) {
		value, ok := fieldByIndex(v, field.index)
		if !ok {
			continue
		}
		if field.omitEmpty && isEmptyValue(value) || field.omitZero && value.IsZero() {
			continue
		}
		node, err := e.encodeNode(value, seen)
		if err != nil {
			return nil, err
		}
		if field.quoted {
			if number, ok := node.(Number); ok {
				node = string(number)
			} else if b, ok := node.(bool); ok {
				node = strconv.FormatBool(b)
			}
		}
		object.Set(field.name, node)
	}
	return object, nil
}
