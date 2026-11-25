package core

import (
	"testing"
	"time"
)

func TestSafetyCheckDelayDoublesUpToSixHours(t *testing.T) {
	want := []time.Duration{5 * time.Minute, 10 * time.Minute, 20 * time.Minute, 40 * time.Minute, 80 * time.Minute, 160 * time.Minute, 320 * time.Minute, 6 * time.Hour, 6 * time.Hour, 6 * time.Hour}
	for n, delay := range want {
		if got := safetyCheckDelay(n); got != delay {
			t.Errorf("safetyCheckDelay(%d) = %s, want %s", n, got, delay)
		}
	}
	if got := safetyCheckDelay(100); got != 6*time.Hour {
		t.Errorf("safetyCheckDelay(100) = %s, want 6h", got)
	}
}
