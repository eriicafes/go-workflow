package vercelworld

import (
	"os"
	"strings"

	"github.com/oklog/ulid/v2"

	"github.com/eriicafes/go-workflow/worlds"
)

// Run IDs carry the region a run's data lives in, as world-vercel's tagged
// run IDs do: a ULID with the most significant timestamp bit set, a 6-bit
// region ID at the top of byte 6, and a 5-bit encoding version below it,
// spanning the bottom of byte 6 and the top of byte 7. Untagged IDs, and
// tagged IDs of a region this package does not know, live in iad1, as on the
// backend.

const (
	runIDTagBit        = 0x80
	runIDRegionMask    = 0xfc
	runIDVersionHigh   = 0x03
	runIDVersionLow    = 0xe0
	runIDRegionByte    = 6
	runIDVersionByte   = 7
	runIDVersion       = 1
	defaultRegionCode  = "iad1"
	runIDPrefix        = "wrun_"
	runIDRegionUnknown = 0
)

// regionIDs maps each region to the ID world-vercel encodes in run IDs. IDs
// are never reordered or reused.
var regionIDs = map[worlds.Region]byte{
	worlds.RegionIAD1: 1, worlds.RegionSFO1: 2, worlds.RegionPDX1: 3, worlds.RegionCLE1: 4,
	worlds.RegionYUL1: 5, worlds.RegionGRU1: 6, worlds.RegionDUB1: 7, worlds.RegionLHR1: 8,
	worlds.RegionCDG1: 9, worlds.RegionFRA1: 10, worlds.RegionBRU1: 11, worlds.RegionARN1: 12,
	worlds.RegionHEL1: 13, worlds.RegionZRH1: 14, worlds.RegionCPT1: 15, worlds.RegionDXB1: 16,
	worlds.RegionBOM1: 17, worlds.RegionSIN1: 18, worlds.RegionHKG1: 19, worlds.RegionHND1: 20,
	worlds.RegionICN1: 21, worlds.RegionKIX1: 22, worlds.RegionSYD1: 23,
}

// envRegion returns the region of VERCEL_REGION, or iad1 when it names none
// of the Workflow SDK's regions, as outside a deployment.
func envRegion() worlds.Region {
	if region, ok := worlds.ParseRegion(os.Getenv("VERCEL_REGION")); ok {
		return region
	}
	return worlds.RegionIAD1
}

// RegionRunID returns id tagged with region, or with the region of
// VERCEL_REGION, else iad1, for the zero Region, as world-vercel's
// createRunId does. An id that is not wrun_ and a ULID is returned unchanged.
func (w *World) RegionRunID(id worlds.RunID, region worlds.Region) worlds.RunID {
	return tagRunID(id, region)
}

// RegionForRunID returns the region a run's data lives in, as world-vercel's
// regionForRunId does: a tagged ID's region, and iad1 for an untagged one or
// a region this package does not know.
func (w *World) RegionForRunID(id worlds.RunID) (worlds.Region, bool) {
	return runRegion(id)
}

func tagRunID(id worlds.RunID, region worlds.Region) worlds.RunID {
	rest, ok := strings.CutPrefix(string(id), runIDPrefix)
	if !ok {
		return id
	}
	value, err := ulid.ParseStrict(rest)
	if err != nil {
		return id
	}
	regionID, ok := regionIDs[region]
	if !ok {
		regionID = regionIDs[envRegion()]
	}
	value[0] |= runIDTagBit
	value[runIDRegionByte] = regionID<<2 | (runIDVersion>>3)&runIDVersionHigh
	value[runIDVersionByte] = value[runIDVersionByte]&^runIDVersionLow | (runIDVersion&0x07)<<5
	return worlds.RunID(runIDPrefix + value.String())
}

func runRegion(id worlds.RunID) (worlds.Region, bool) {
	rest, _ := strings.CutPrefix(string(id), runIDPrefix)
	value, err := ulid.ParseStrict(rest)
	if err != nil {
		return worlds.Region{}, false
	}
	if value[0]&runIDTagBit == 0 {
		return worlds.RegionIAD1, true
	}
	regionID := (value[runIDRegionByte] & runIDRegionMask) >> 2
	for region, known := range regionIDs {
		if known == regionID && regionID != runIDRegionUnknown {
			return region, true
		}
	}
	return worlds.RegionIAD1, true
}
