package core

import "testing"

func TestStepMessageKeyHashesTheNamesUTF16Units(t *testing.T) {
	// The FNV-1a hash of the name's UTF-16 code units.
	if got := StepMessageKey("step_1", "a"); got != "step_1:e40c292c" {
		t.Fatalf("key = %q", got)
	}
	if got := StepMessageKey("step_1", ""); got != "step_1:811c9dc5" {
		t.Fatalf("key of an empty name = %q", got)
	}
}
