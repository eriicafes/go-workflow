package core

import "testing"

func TestStreakReportsAtThreeThenAtPowersOfTwo(t *testing.T) {
	var s streak
	var reported []int
	for range 20 {
		if count, warn := s.note("wrun_a"); warn {
			reported = append(reported, count)
		}
	}
	if len(reported) != 4 || reported[0] != 3 || reported[1] != 4 || reported[2] != 8 || reported[3] != 16 {
		t.Fatalf("reported at %v, want 3, 4, 8, 16", reported)
	}
	s.clear("wrun_a")
	if count, _ := s.note("wrun_a"); count != 1 {
		t.Fatalf("count after clear = %d", count)
	}
}
