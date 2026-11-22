package vercelworld

import (
	"os"
	"strings"

	"github.com/oklog/ulid/v2"

	"github.com/eriicafes/go-workflow/worlds"
)

// Run IDs carry the region a run's data lives in: a ULID with the most
// significant timestamp bit set, a 6-bit region ID at the top of byte 6, and
// a 5-bit encoding version below it, spanning the bottom of byte 6 and the
// top of byte 7. Untagged IDs, and tagged IDs of a region this package does
// not know, live in iad1, as on the backend.

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

// The Vercel compute regions a run can live in, including hel1 and zrh1,
// which Vercel reserves for future rollout.
const (
	RegionIAD1 worlds.Region = "iad1"
	RegionSFO1 worlds.Region = "sfo1"
	RegionPDX1 worlds.Region = "pdx1"
	RegionCLE1 worlds.Region = "cle1"
	RegionYUL1 worlds.Region = "yul1"
	RegionGRU1 worlds.Region = "gru1"
	RegionDUB1 worlds.Region = "dub1"
	RegionLHR1 worlds.Region = "lhr1"
	RegionCDG1 worlds.Region = "cdg1"
	RegionFRA1 worlds.Region = "fra1"
	RegionBRU1 worlds.Region = "bru1"
	RegionARN1 worlds.Region = "arn1"
	RegionHEL1 worlds.Region = "hel1"
	RegionZRH1 worlds.Region = "zrh1"
	RegionCPT1 worlds.Region = "cpt1"
	RegionDXB1 worlds.Region = "dxb1"
	RegionBOM1 worlds.Region = "bom1"
	RegionSIN1 worlds.Region = "sin1"
	RegionHKG1 worlds.Region = "hkg1"
	RegionHND1 worlds.Region = "hnd1"
	RegionICN1 worlds.Region = "icn1"
	RegionKIX1 worlds.Region = "kix1"
	RegionSYD1 worlds.Region = "syd1"
)

// regionIDs maps each region to the ID encoded in its run IDs. IDs are never
// reordered or reused.
var regionIDs = map[worlds.Region]byte{
	RegionIAD1: 1, RegionSFO1: 2, RegionPDX1: 3, RegionCLE1: 4,
	RegionYUL1: 5, RegionGRU1: 6, RegionDUB1: 7, RegionLHR1: 8,
	RegionCDG1: 9, RegionFRA1: 10, RegionBRU1: 11, RegionARN1: 12,
	RegionHEL1: 13, RegionZRH1: 14, RegionCPT1: 15, RegionDXB1: 16,
	RegionBOM1: 17, RegionSIN1: 18, RegionHKG1: 19, RegionHND1: 20,
	RegionICN1: 21, RegionKIX1: 22, RegionSYD1: 23,
}

// knownRegion returns region and whether it is one of the Vercel regions.
func knownRegion(region worlds.Region) (worlds.Region, bool) {
	_, ok := regionIDs[region]
	return region, ok
}

// envRegion returns the region of VERCEL_REGION, or iad1 when it names none
// of the Vercel regions, as outside a deployment.
func envRegion() worlds.Region {
	if region, ok := knownRegion(worlds.Region(os.Getenv("VERCEL_REGION"))); ok {
		return region
	}
	return RegionIAD1
}

// RegionRunID returns id tagged with region, or with the region of
// VERCEL_REGION, else iad1, for the empty Region or one that is not a Vercel
// region. An id that is not wrun_ and a ULID is returned unchanged.
func (w *World) RegionRunID(id worlds.RunID, region worlds.Region) worlds.RunID {
	return tagRunID(id, region)
}

// RegionForRunID returns the region a run's data lives in: a tagged ID's
// region, and iad1 for an untagged one or a region this package does not
// know.
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
		return "", false
	}
	if value[0]&runIDTagBit == 0 {
		return RegionIAD1, true
	}
	regionID := (value[runIDRegionByte] & runIDRegionMask) >> 2
	for region, known := range regionIDs {
		if known == regionID && regionID != runIDRegionUnknown {
			return region, true
		}
	}
	return RegionIAD1, true
}
