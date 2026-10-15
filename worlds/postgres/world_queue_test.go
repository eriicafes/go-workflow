package postgresworld

import (
	"testing"
	"time"
)

func TestBackoff(t *testing.T) {
	cases := map[int]time.Duration{0: time.Second, 1: time.Second, 2: 2 * time.Second, 3: 4 * time.Second, 9: 256 * time.Second, 10: 5 * time.Minute, 64: 5 * time.Minute, 1000: 5 * time.Minute}
	for attempts, want := range cases {
		if got := backoff(attempts); got != want {
			t.Errorf("backoff(%d) = %v, want %v", attempts, got, want)
		}
	}
}
