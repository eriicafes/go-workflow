package core

import (
	"testing"
	"time"
)

func TestHopDeadlineAnchorsHopsOnTheWaitsDeadline(t *testing.T) {
	now := time.Date(2030, time.January, 1, 0, 0, 0, 0, time.UTC)
	hour := time.Hour
	for _, test := range []struct {
		name     string
		resumeAt time.Time
		after    *time.Time
		maxDelay time.Duration
		want     time.Time
	}{
		{"no limit", now.Add(100 * hour), nil, 0, now.Add(100 * hour)},
		{"within the limit", now.Add(10 * hour), nil, 23 * hour, now.Add(10 * hour)},
		{"anchored on the deadline", now.Add(50 * hour), nil, 23 * hour, now.Add(50*hour - 46*hour)},
		{"the last hop", now.Add(30 * hour), nil, 23 * hour, now.Add(30*hour - 23*hour)},
		{"after the first hop", now.Add(50 * hour), ptr(now.Add(4 * hour)), 23 * hour, now.Add(27 * hour)},
		{"after the hop before the last", now.Add(50 * hour), ptr(now.Add(27 * hour)), 23 * hour, now.Add(50 * hour)},
	} {
		if got := hopDeadline(test.resumeAt, now, test.after, test.maxDelay); !got.Equal(test.want) {
			t.Errorf("%s: hop at %v, want %v", test.name, got.Sub(now), test.want.Sub(now))
		}
	}
}

func ptr[T any](value T) *T { return &value }
