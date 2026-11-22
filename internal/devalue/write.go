package devalue

import (
	"bytes"
	"encoding/json"
	"strconv"
)

// writeNode returns node as devalue text.
func writeNode(node any) []byte {
	var w writer
	root := w.add(node)
	if root < 0 {
		return []byte(strconv.Itoa(root))
	}
	return append(append([]byte{'['}, bytes.Join(w.values, []byte{','})...), ']')
}

type writer struct{ values [][]byte }

func (w *writer) add(node any) int {
	switch node := node.(type) {
	case Undefined:
		return indexUndefined
	case special:
		return int(node)
	}
	index := len(w.values)
	w.values = append(w.values, nil)
	var out []byte
	switch node := node.(type) {
	case nil:
		out = []byte("null")
	case bool:
		out = strconv.AppendBool(nil, node)
	case string:
		out = quoteString(node)
	case Number:
		out = []byte(node)
	case []any:
		out = append(out, '[')
		for i, item := range node {
			if i > 0 {
				out = append(out, ',')
			}
			out = strconv.AppendInt(out, int64(w.add(item)), 10)
		}
		out = append(out, ']')
	case *Object:
		out = append(out, '{')
		for i, key := range node.keys {
			if i > 0 {
				out = append(out, ',')
			}
			out = append(append(out, quoteString(key)...), ':')
			out = strconv.AppendInt(out, int64(w.add(node.values[i])), 10)
		}
		out = append(out, '}')
	case Typed:
		out = append(append(append(out, '['), quoteString(node.Type)...), ',')
		out = append(strconv.AppendInt(out, int64(w.add(node.Value)), 10), ']')
	}
	w.values[index] = out
	return index
}

func quoteString(s string) []byte {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	_ = encoder.Encode(s)
	return bytes.TrimSuffix(buffer.Bytes(), []byte("\n"))
}
