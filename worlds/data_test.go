package worlds

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestCheckAttributesMatchesTheSDKLimits(t *testing.T) {
	many := map[string]string{}
	for index := range MaxAttributesPerRun + 1 {
		many[fmt.Sprint(index)] = "v"
	}
	// Keys count UTF-16 code units: an astral rune is two.
	astral := strings.Repeat("😀", MaxAttributeKeyLength/2)
	for _, test := range []struct {
		attributes    map[string]string
		allowReserved bool
		valid         bool
	}{
		{map[string]string{astral: "v"}, false, true},
		{map[string]string{astral + "x": "v"}, false, false},
		{map[string]string{"$parentRunId": "v"}, true, true},
		{map[string]string{"$parentRunId": "v"}, false, false},
		{many, false, false},
	} {
		err := CheckAttributes(test.attributes, test.allowReserved)
		if (err == nil) != test.valid || (err != nil && !errors.Is(err, ErrAttributeInvalid)) {
			t.Errorf("CheckAttributes(%d attributes, %t) = %v, want valid %t", len(test.attributes), test.allowReserved, err, test.valid)
		}
	}
}

func TestCheckRunSpecVersion(t *testing.T) {
	if err := CheckRunSpecVersion("wrun_a", SpecVersion); err != nil {
		t.Fatalf("current spec version: %v", err)
	}
	if err := CheckRunSpecVersion("wrun_a", SpecVersion+1); !errors.Is(err, ErrRunNotSupported) {
		t.Fatalf("newer spec version = %v, want ErrRunNotSupported", err)
	}
}
