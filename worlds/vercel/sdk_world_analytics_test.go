package vercelworld

import (
	"testing"
	"time"
)

func sdkAnalyticsRun(fields map[string]any) map[string]any {
	values := map[string]any{
		"runId":        "wrun_01KX2M5N3RBNC12RYWYYH4WWQJ",
		"status":       "completed",
		"deploymentId": "dpl_1",
		"workflowName": "workflow//./src/w//myWorkflow",
		"updatedAt":    "2026-07-13 17:09:11.593",
	}
	for key, value := range fields {
		values[key] = value
	}
	return values
}

// SDK: packages/world/src/analytics.test.ts > analytics date coercion > parses timezone-naive datetime strings as UTC regardless of process TZ
func TestSDKAnalyticsReadsNaiveTimesAsUTC(t *testing.T) {
	run := toAnalyticsRun(sdkAnalyticsRun(map[string]any{"createdAt": "2026-07-13 17:09:11.593"}))
	if want := time.Date(2026, time.July, 13, 17, 9, 11, 593_000_000, time.UTC); !run.CreatedAt.Equal(want) || run.CreatedAt.Format(time.RFC3339Nano) != "2026-07-13T17:09:11.593Z" {
		t.Fatalf("createdAt = %v, want %v", run.CreatedAt, want)
	}
}

// SDK: packages/world/src/analytics.test.ts > analytics date coercion > accepts naive strings without fractional seconds
func TestSDKAnalyticsReadsNaiveTimesWithoutFractionalSeconds(t *testing.T) {
	run := toAnalyticsRun(sdkAnalyticsRun(map[string]any{"createdAt": "2026-07-13 17:09:11"}))
	if want := time.Date(2026, time.July, 13, 17, 9, 11, 0, time.UTC); !run.CreatedAt.Equal(want) {
		t.Fatalf("createdAt = %v, want %v", run.CreatedAt, want)
	}
}

// SDK: packages/world/src/analytics.test.ts > analytics date coercion > parses attribute key timestamps as UTC
func TestSDKAnalyticsReadsAttributeTimesAsUTC(t *testing.T) {
	values := map[string]any{"key": "source", "runCount": "42", "firstSeenAt": "2026-07-13 17:09:11.593", "lastSeenAt": "2026-07-14 09:30:00"}
	if got, want := requiredTime(values, "firstSeenAt"), time.Date(2026, time.July, 13, 17, 9, 11, 593_000_000, time.UTC); !got.Equal(want) {
		t.Fatalf("firstSeenAt = %v, want %v", got, want)
	}
	if got, want := requiredTime(values, "lastSeenAt"), time.Date(2026, time.July, 14, 9, 30, 0, 0, time.UTC); !got.Equal(want) {
		t.Fatalf("lastSeenAt = %v, want %v", got, want)
	}
}

// SDK: packages/world/src/analytics.test.ts > analytics date coercion > leaves timezone-aware strings untouched
func TestSDKAnalyticsKeepsTheZoneOfZonedTimes(t *testing.T) {
	want := time.Date(2026, time.July, 13, 17, 9, 11, 593_000_000, time.UTC)
	for _, value := range []string{"2026-07-13T10:09:11.593-07:00", "2026-07-13T17:09:11.593Z"} {
		if run := toAnalyticsRun(sdkAnalyticsRun(map[string]any{"createdAt": value})); !run.CreatedAt.Equal(want) {
			t.Errorf("createdAt of %s = %v, want %v", value, run.CreatedAt, want)
		}
	}
}

// SDK: packages/world/src/analytics.test.ts > analytics date coercion > passes through Date objects and nullable fields
func TestSDKAnalyticsPassesTimesAndNullsThrough(t *testing.T) {
	at := time.Date(2026, time.July, 13, 17, 9, 11, 593_000_000, time.UTC)
	run := toAnalyticsRun(sdkAnalyticsRun(map[string]any{"createdAt": at, "startedAt": "2026-07-13 17:09:12.000", "completedAt": nil}))
	if !run.CreatedAt.Equal(at) {
		t.Fatalf("createdAt = %v, want %v", run.CreatedAt, at)
	}
	if want := time.Date(2026, time.July, 13, 17, 9, 12, 0, time.UTC); run.StartedAt == nil || !run.StartedAt.Equal(want) {
		t.Fatalf("startedAt = %v, want %v", run.StartedAt, want)
	}
	if run.CompletedAt != nil {
		t.Fatalf("completedAt = %v, want none", run.CompletedAt)
	}
}
