package devalue

import (
	"errors"
	"testing"
)

func TestParseRefusesMalformedText(t *testing.T) {
	for _, text := range []string{``, `{`, `[`, `[[5]]`, `[["Date"]]`, `-99`} {
		var value any
		if err := Unmarshal([]byte(text), &value); !errors.Is(err, ErrInvalid) {
			t.Errorf("Unmarshal(%q) err = %v", text, err)
		}
	}
}
