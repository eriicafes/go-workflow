package devalue

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// JSONNode returns the node of JSON text.
func JSONNode(data []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	node, err := readJSONNode(decoder)
	if err != nil {
		return nil, fmt.Errorf("workflow: encode payload: %w", err)
	}
	return node, nil
}

// readJSONNode reads one JSON value as a node, keeping object key order.
func readJSONNode(decoder *json.Decoder) (any, error) {
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	switch token := token.(type) {
	case json.Delim:
		switch token {
		case '[':
			items := []any{}
			for decoder.More() {
				item, err := readJSONNode(decoder)
				if err != nil {
					return nil, err
				}
				items = append(items, item)
			}
			_, err := decoder.Token()
			return items, err
		case '{':
			object := &Object{}
			for decoder.More() {
				key, err := decoder.Token()
				if err != nil {
					return nil, err
				}
				value, err := readJSONNode(decoder)
				if err != nil {
					return nil, err
				}
				object.Set(key.(string), value)
			}
			_, err := decoder.Token()
			return object, err
		}
	case json.Number:
		return Number(token), nil
	case string, bool, nil:
		return token, nil
	}
	return nil, fmt.Errorf("unexpected JSON token %v", token)
}
