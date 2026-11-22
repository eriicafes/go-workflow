package devalue

import "testing"

func TestObjectKeepsKeyOrder(t *testing.T) {
	object := &Object{}
	object.Set("b", 1)
	object.Set("a", 2)
	if value, ok := object.Get("a"); !ok || value != 2 {
		t.Fatalf("Get(a) = %v %v", value, ok)
	}
	if _, ok := object.Get("c"); ok {
		t.Fatal("Get(c) found a value")
	}
	if object.keys[0] != "b" || object.keys[1] != "a" {
		t.Fatalf("keys = %v", object.keys)
	}
}

func TestNodeHelpers(t *testing.T) {
	if text, ok := NumberText(Number("12")); !ok || text != "12" {
		t.Fatalf("NumberText = %q %v", text, ok)
	}
	if kind := NodeKind("x"); kind == "" {
		t.Fatal("NodeKind is empty")
	}
}
