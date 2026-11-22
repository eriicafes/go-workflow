package devalue

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
)

// parse parses devalue text into its root node.
func parse(data []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	raw, err := readJSONNode(decoder)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if number, ok := raw.(Number); ok {
		index, err := strconv.Atoi(string(number))
		if err != nil || index >= 0 {
			return nil, fmt.Errorf("%w: devalue root %s", ErrInvalid, number)
		}
		return specialNode(index)
	}
	values, ok := raw.([]any)
	if !ok || len(values) == 0 {
		return nil, fmt.Errorf("%w: devalue text is not an array", ErrInvalid)
	}
	p := &parser{values: values, hydrated: make([]any, len(values)), done: make([]bool, len(values)), active: make([]bool, len(values))}
	return p.hydrate(0)
}

func specialNode(index int) (any, error) {
	switch index {
	case indexUndefined, indexHole:
		return Undefined{}, nil
	case indexNaN, indexPosInf, indexNegInf, indexNegZero:
		return special(index), nil
	}
	return nil, fmt.Errorf("%w: devalue index %d", ErrInvalid, index)
}

type parser struct {
	values   []any
	hydrated []any
	done     []bool
	active   []bool
}

func (p *parser) index(node any) (int, error) {
	number, ok := node.(Number)
	if !ok {
		return 0, fmt.Errorf("%w: devalue reference is not a number", ErrInvalid)
	}
	index, err := strconv.Atoi(string(number))
	if err != nil {
		return 0, fmt.Errorf("%w: devalue reference %s", ErrInvalid, number)
	}
	return index, nil
}

func (p *parser) ref(node any) (any, error) {
	index, err := p.index(node)
	if err != nil {
		return nil, err
	}
	return p.hydrate(index)
}

func (p *parser) hydrate(index int) (any, error) {
	if index < 0 {
		return specialNode(index)
	}
	if index >= len(p.values) {
		return nil, fmt.Errorf("%w: devalue index %d out of range", ErrInvalid, index)
	}
	if p.done[index] {
		return p.hydrated[index], nil
	}
	if p.active[index] {
		return nil, fmt.Errorf("%w: circular devalue reference", ErrInvalid)
	}
	p.active[index] = true
	defer func() { p.active[index] = false }()
	node, err := p.build(p.values[index])
	if err != nil {
		return nil, err
	}
	p.hydrated[index], p.done[index] = node, true
	return node, nil
}

func (p *parser) build(raw any) (any, error) {
	switch raw := raw.(type) {
	case []any:
		if len(raw) > 0 {
			if typ, ok := raw[0].(string); ok {
				return p.buildTyped(typ, raw[1:])
			}
		}
		if len(raw) > 0 {
			if index, err := p.index(raw[0]); err == nil && index == indexSparse {
				return p.buildSparse(raw[1:])
			}
		}
		items := make([]any, len(raw))
		for i, ref := range raw {
			item, err := p.ref(ref)
			if err != nil {
				return nil, err
			}
			items[i] = item
		}
		return items, nil
	case *Object:
		object := &Object{}
		for i, key := range raw.keys {
			value, err := p.ref(raw.values[i])
			if err != nil {
				return nil, err
			}
			object.Set(key, value)
		}
		return object, nil
	}
	return raw, nil
}

func (p *parser) buildSparse(rest []any) (any, error) {
	if len(rest) == 0 {
		return nil, fmt.Errorf("%w: sparse array without length", ErrInvalid)
	}
	length, err := p.index(rest[0])
	if err != nil || length < 0 || length > 1<<24 {
		return nil, fmt.Errorf("%w: sparse array length", ErrInvalid)
	}
	items := make([]any, length)
	for i := range items {
		items[i] = Undefined{}
	}
	for i := 1; i+1 < len(rest); i += 2 {
		at, err := p.index(rest[i])
		if err != nil || at < 0 || at >= length {
			return nil, fmt.Errorf("%w: sparse array index", ErrInvalid)
		}
		if items[at], err = p.ref(rest[i+1]); err != nil {
			return nil, err
		}
	}
	return items, nil
}

// buildTyped builds ["type", ...]. Except for devalue's own null-prototype
// objects and boxed primitives, the second entry is the reduced value's
// index, or the value itself when it is not a number.
func (p *parser) buildTyped(typ string, rest []any) (any, error) {
	switch typ {
	case "null":
		object := &Object{}
		for i := 0; i+1 < len(rest); i += 2 {
			key, ok := rest[i].(string)
			if !ok {
				return nil, fmt.Errorf("%w: object key is not a string", ErrInvalid)
			}
			value, err := p.ref(rest[i+1])
			if err != nil {
				return nil, err
			}
			object.Set(key, value)
		}
		return object, nil
	case "Object":
		if len(rest) == 0 {
			return nil, fmt.Errorf("%w: empty boxed value", ErrInvalid)
		}
		return p.ref(rest[0])
	case "Set":
		if len(rest) != 1 {
			// devalue's own Set: every entry is a reference.
			items := make([]any, len(rest))
			for i, ref := range rest {
				item, err := p.ref(ref)
				if err != nil {
					return nil, err
				}
				items[i] = item
			}
			return Typed{"Set", items}, nil
		}
	}
	if len(rest) == 0 {
		return Typed{typ, Undefined{}}, nil
	}
	var value any
	if _, ok := rest[0].(Number); ok {
		var err error
		if value, err = p.ref(rest[0]); err != nil {
			return nil, err
		}
	} else {
		value = rest[0]
	}
	// devalue's own typed arrays reference an ArrayBuffer.
	if inner, ok := value.(Typed); ok && inner.Type == "ArrayBuffer" && typ != "ArrayBuffer" {
		value = inner.Value
	}
	if typ == "Map" && len(rest) > 1 {
		// devalue's own Map: key and value references in turn.
		var pairs []any
		for i := 0; i+1 < len(rest); i += 2 {
			key, err := p.ref(rest[i])
			if err != nil {
				return nil, err
			}
			item, err := p.ref(rest[i+1])
			if err != nil {
				return nil, err
			}
			pairs = append(pairs, []any{key, item})
		}
		value = pairs
	}
	return Typed{typ, value}, nil
}
