package vercelworld

import (
	"testing"

	"github.com/eriicafes/go-workflow/worlds"
)

func TestRunIDsMatchWorldVercel(t *testing.T) {
	// Expected IDs are from world-vercel's run-id encode.
	for _, test := range []struct {
		ulid   string
		region worlds.Region
		want   string
	}{
		{"01ARZ3NDEKTSV4RRFFQ69G5FAV", worlds.RegionIAD1, "41ARZ3NDEK0GV4RRFFQ69G5FAV"},
		{"01ARZ3NDEKTSV4RRFFQ69G5FAV", worlds.RegionSYD1, "41ARZ3NDEKBGV4RRFFQ69G5FAV"},
		{"01ARZ3NDEKTSV4RRFFQ69G5FAV", worlds.RegionFRA1, "41ARZ3NDEK50V4RRFFQ69G5FAV"},
		{"01KD7T6Y0000000000000000ZZ", worlds.RegionIAD1, "41KD7T6Y000GG00000000000ZZ"},
		{"01KD7T6Y0000000000000000ZZ", worlds.RegionSYD1, "41KD7T6Y00BGG00000000000ZZ"},
		{"3ZZZZZZZZZZZZZZZZZZZZZZZZZ", worlds.RegionFRA1, "7ZZZZZZZZZ50ZZZZZZZZZZZZZZ"},
	} {
		got := tagRunID(worlds.RunID("wrun_"+test.ulid), test.region)
		if got != worlds.RunID("wrun_"+test.want) {
			t.Errorf("tag %s in %s = %s, want wrun_%s", test.ulid, test.region, got, test.want)
		}
		if region, ok := runRegion(got); !ok || region != test.region {
			t.Errorf("region of %s = %s, %t", got, region, ok)
		}
	}
	if region, ok := runRegion("wrun_01ARZ3NDEKTSV4RRFFQ69G5FAV"); !ok || region != worlds.RegionIAD1 {
		t.Errorf("untagged run region = %s, %t; want iad1", region, ok)
	}
	if _, ok := runRegion("wrun_missing"); ok {
		t.Error("a non-ULID run ID has a region")
	}
	if got := tagRunID("wrun_missing", worlds.RegionSYD1); got != "wrun_missing" {
		t.Errorf("tagging a non-ULID run ID = %s", got)
	}
	// The zero Region is VERCEL_REGION's.
	t.Setenv("VERCEL_REGION", "lhr1")
	if region, _ := runRegion(tagRunID("wrun_01ARZ3NDEKTSV4RRFFQ69G5FAV", worlds.Region{})); region != worlds.RegionLHR1 {
		t.Errorf("default region = %s, want lhr1", region)
	}
}
