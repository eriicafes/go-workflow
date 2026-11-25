package worlds_test

import (
	"strings"
	"testing"

	"github.com/eriicafes/go-workflow/worlds"
	"github.com/oklog/ulid/v2"
)

// SDK: packages/world/src/slot-identity.test.ts > time is never derived from a slot id > returns null rather than the epoch
func TestSDKRunIDTimeIsNeverReadFromASlot(t *testing.T) {
	for _, slot := range []uint64{1, 999_999} {
		body := strings.TrimPrefix(string(worlds.EventIDForSlot(slot)), "evnt_")
		if at, ok := worlds.RunIDTime(worlds.RunID("wrun_" + body)); ok {
			t.Errorf("RunIDTime of slot %d's body = %v, want no time", slot, at)
		}
	}
}

// SDK: packages/world/src/slot-identity.test.ts > time is never derived from a slot id > still decodes a real ULID
func TestSDKRunIDTimeDecodesARealULID(t *testing.T) {
	if at, ok := worlds.RunIDTime(worlds.RunID("wrun_" + ulid.Make().String())); !ok || at.UnixMilli() <= 0 {
		t.Fatalf("RunIDTime = %v, %v", at, ok)
	}
}
