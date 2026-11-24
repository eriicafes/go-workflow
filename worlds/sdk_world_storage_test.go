package worlds_test

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/eriicafes/go-workflow/worlds"
	"github.com/oklog/ulid/v2"
)

// SDK: packages/world/src/slot-identity.test.ts > slotToEventId > mints a fixed-width id whose string order is slot order
func TestSDKEventIDsAreFixedWidthAndSortInSlotOrder(t *testing.T) {
	var ids []string
	for _, slot := range []uint64{1, 2, 9, 10, 99, 100, 1000} {
		id := string(worlds.EventIDForSlot(slot))
		if len(id) != len("evnt_")+26 {
			t.Fatalf("EventIDForSlot(%d) = %q, %d characters", slot, id, len(id))
		}
		ids = append(ids, id)
	}
	if sorted := slices.Sorted(slices.Values(ids)); !slices.Equal(sorted, ids) {
		t.Fatalf("sorted ids = %v, want %v", sorted, ids)
	}
}

// SDK: packages/world/src/slot-identity.test.ts > slotToEventId > round-trips through eventIdToSlot
func TestSDKEventSlotRoundTripsThroughEventIDForSlot(t *testing.T) {
	// 9007199254740991 is the largest slot the SDK represents.
	for _, slot := range []uint64{1, 7, 12345, 9007199254740991} {
		if got, err := worlds.EventSlot(worlds.EventIDForSlot(slot)); err != nil || got != slot {
			t.Errorf("EventSlot(EventIDForSlot(%d)) = %d, %v", slot, got, err)
		}
	}
}

// SDK: packages/world/src/slot-identity.test.ts > isSlotEventId > never mistakes a ULID for a slot
func TestSDKEventSlotNeverReadsAULIDAsASlot(t *testing.T) {
	for range 100 {
		id := worlds.EventID("evnt_" + ulid.Make().String())
		if slot, err := worlds.EventSlot(id); !errors.Is(err, worlds.ErrEventInvalid) {
			t.Fatalf("EventSlot(%s) = %d, %v, want ErrEventInvalid", id, slot, err)
		}
	}
}

// SDK: packages/world/src/slot-identity.test.ts > isSlotEventId > rejects bodies of the wrong shape
func TestSDKEventSlotRefusesBodiesOfTheWrongShape(t *testing.T) {
	for _, body := range []string{
		// Right length, but the timestamp region is not all zeros.
		"0000000001" + strings.Repeat("0", 16),
		// A non-digit in the counter region.
		strings.Repeat("0", 25) + "A",
		// Wrong length.
		strings.Repeat("0", 25) + "11",
		"",
	} {
		if slot, err := worlds.EventSlot(worlds.EventID("evnt_" + body)); !errors.Is(err, worlds.ErrEventInvalid) {
			t.Errorf("EventSlot(evnt_%s) = %d, %v, want ErrEventInvalid", body, slot, err)
		}
	}
}
