package core

import (
	"testing"

	"github.com/eriicafes/go-workflow/worlds"
)

func TestBatchChunksKeepsABatchWithinItsEventAndByteLimits(t *testing.T) {
	sizes := func(chunks [][]worlds.EventsCreateEvent) []int {
		var lengths []int
		for _, chunk := range chunks {
			lengths = append(lengths, len(chunk))
		}
		return lengths
	}
	equal := func(a, b []int) bool {
		if len(a) != len(b) {
			return false
		}
		for index := range a {
			if a[index] != b[index] {
				return false
			}
		}
		return true
	}
	small := make([]worlds.EventsCreateEvent, 130)
	if got := sizes(batchChunks(small)); !equal(got, []int{64, 64, 2}) {
		t.Fatalf("130 small events are written in batches of %v, want 64, 64 and 2", got)
	}
	if got := batchChunks(nil); len(got) != 0 {
		t.Fatalf("no events are written in %d batches", len(got))
	}
	// About 300 KB of payload each: two do not fit one batch of 512 KB.
	large := worlds.EventsCreateEvent{Data: make([]byte, 400_000)}
	// Over the limit alone, so it goes in a batch of its own.
	huge := worlds.EventsCreateEvent{Data: make([]byte, 1_000_000)}
	events := []worlds.EventsCreateEvent{large, {}, large, huge, {}, {}}
	if got := sizes(batchChunks(events)); !equal(got, []int{2, 1, 1, 2}) {
		t.Fatalf("large events are written in batches of %v, want 2, 1, 1 and 2", got)
	}
}
