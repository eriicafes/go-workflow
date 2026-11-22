package devalue

import "testing"

func TestWriteNodeWritesOneNode(t *testing.T) {
	object := &Object{}
	object.Set("a", Number("1"))
	object.Set("u", Undefined{})
	for _, test := range []struct {
		node any
		want string
	}{
		{object, `[{"a":1,"u":-1},1]`},
		{Typed{Type: "Date", Value: "2024-01-02T03:04:05.678Z"}, `[["Date",1],"2024-01-02T03:04:05.678Z"]`},
		{Undefined{}, `-1`},
	} {
		if got := string(writeNode(test.node)); got != test.want {
			t.Errorf("writeNode(%#v) = %s, want %s", test.node, got, test.want)
		}
	}
}
