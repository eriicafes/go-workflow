package workflowtest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

// SDK: packages/world-local/src/storage/delete-data-many.test.ts > runs.deleteDataMany (world-local) > does not expose the original writer cache after another instance writes %s and deletes data
func conformSDKSharedDeleteData(t *testing.T, a, b worlds.World) {
	if !b.Capabilities().DeleteData {
		t.Skip("the World does not delete data on demand")
	}
	ctx := context.Background()
	for _, end := range []worlds.EventType{worlds.EventTypeRunCompleted, worlds.EventTypeRunCancelled} {
		t.Run(string(end), func(t *testing.T) {
			runID := sdkRetentionRun(t, a, nil, nil)
			correlated := func(t *testing.T) []worlds.Event {
				t.Helper()
				page, err := a.EventsListByCorrelationID(ctx, worlds.EventsListByCorrelationIDInput{RunID: runID, CorrelationID: "step_001"})
				if err != nil || len(page.Data) != 2 {
					t.Fatalf("step events = %+v err = %v", page.Data, err)
				}
				return page.Data
			}
			correlated(t)
			if len(sdkPayloads(t, a, runID)) == 0 {
				t.Fatal("the run holds no payloads to delete")
			}
			var data any
			if end == worlds.EventTypeRunCompleted {
				data = worlds.DataRunCompleted{Output: sdkRunOutput}
			}
			conformAppend(t, b, runID, end, "", data)
			deleted, err := b.RunsDeleteDataMany(ctx, worlds.RunsDeleteDataManyInput{RunIDs: []worlds.RunID{runID}})
			if err != nil || deleted.Results[0].Outcome != worlds.DeleteDataDeleted {
				t.Fatalf("deletion = %+v err = %v", deleted, err)
			}
			for range 2 {
				events := correlated(t)
				var created worlds.DataStepCreated
				var completed worlds.DataStepResult
				_ = json.Unmarshal(events[0].Data, &created)
				_ = json.Unmarshal(events[1].Data, &completed)
				if created.Input != nil || completed.Result != nil {
					t.Fatalf("step events after the deletion = %s and %s", events[0].Data, events[1].Data)
				}
				if payloads := sdkPayloads(t, a, runID); len(payloads) != 0 {
					t.Fatalf("payloads left = %v", payloads)
				}
			}
		})
	}
}

// SDK: packages/world-local/src/storage/hook-resume-dedup.test.ts > world-local hook_received resume dedup > when another instance takes the position the claim named > still commits the resume, at the position actually free
// SDK: packages/world-local/src/storage/hook-resume-dedup.test.ts > world-local hook_received resume dedup > when another instance takes the position the claim named > converges the redelivery on the committed event, not on the occupant
func conformSDKSharedResumes(t *testing.T, a, b worlds.World) {
	ctx := context.Background()
	setup := func(t *testing.T) worlds.RunID {
		t.Helper()
		runID := sdkRun(t, a, "test-workflow", []byte(`[]`))
		token := "order:1:" + string(runID)
		sdkClaim(t, a, runID, sdkHookID("hook_1", runID), token, 0)
		sdkRequireResumeDedup(t, a, token)
		// The other instance writes, then this one writes past it.
		attr := func(world worlds.World, key string) {
			x := "x"
			conformAppend(t, world, runID, worlds.EventTypeAttrSet, "attr_"+key, worlds.DataAttrSet{Changes: []worlds.RunsAttributeChange{{Key: key, Value: &x}}, Writer: worlds.AttributeWriter{Type: worlds.AttributeWriterWorkflow}})
		}
		attr(b, "seed")
		attr(a, "ahead_1")
		attr(a, "ahead_2")
		return runID
	}
	payload := []byte(`[1,2,3]`)
	resume := func(world worlds.World, runID worlds.RunID) (worlds.EventResult, error) {
		data, _ := worlds.EncodeHookReceived(payload, "test-workflow")
		return world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeHookReceived, CorrelationID: sdkHookID("hook_1", runID), Data: data},
			Options: worlds.EventsCreateOptions{ResumeID: "resume_1", ResumePayloadDigest: worlds.ResumeDigest(payload)}})
	}
	t.Run("the resume's own position", func(t *testing.T) {
		runID := setup(t)
		result, err := resume(b, runID)
		if err != nil {
			t.Fatal(err)
		}
		event, err := b.EventsGet(ctx, worlds.EventsGetInput{RunID: runID, EventID: result.EventID})
		if err != nil || result.EventType != worlds.EventTypeHookReceived || event.EventType != worlds.EventTypeHookReceived || event.ResumeID != "resume_1" {
			t.Fatalf("resume = %+v, stored %+v err = %v", result, event, err)
		}
		if count := sdkCount(t, a, runID, worlds.EventTypeHookReceived); count != 1 {
			t.Fatalf("hook_received events = %d, want 1", count)
		}
	})
	t.Run("redelivery", func(t *testing.T) {
		runID := setup(t)
		first, err := resume(b, runID)
		if err != nil {
			t.Fatal(err)
		}
		second, err := resume(a, runID)
		if err != nil || second.EventID != first.EventID {
			t.Fatalf("redelivery = %+v err = %v, want %s", second, err, first.EventID)
		}
		if count := sdkCount(t, a, runID, worlds.EventTypeHookReceived); count != 1 {
			t.Fatalf("hook_received events = %d, want 1", count)
		}
	})
}

// SDK: packages/world-local/src/storage/in-band-fence.test.ts > in-band fence (world-local) > keeps the count in the database: another storage instance reads it
// SDK: packages/world-local/src/storage/in-band-fence.test.ts > in-band fence (world-local) > across OS processes on one data directory > a stale writer in this process is refused after another process wrote in-band
// SDK: packages/world-local/src/storage/in-band-fence.test.ts > in-band fence (world-local) > across OS processes on one data directory > of writers in several processes holding the same count, exactly one is accepted
func conformSDKSharedInBandFence(t *testing.T, a, b worlds.World) {
	ctx := context.Background()
	attrSet := func(world worlds.World, runID worlds.RunID, value string, count int) error {
		data, _ := json.Marshal(worlds.DataAttrSet{Changes: []worlds.RunsAttributeChange{{Key: "k", Value: &value}}, Writer: worlds.AttributeWriter{Type: worlds.AttributeWriterWorkflow}})
		_, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeAttrSet, Data: data}, Options: worlds.EventsCreateOptions{InBandCount: &count}})
		return err
	}
	superseded := func(t *testing.T, err error, count int) {
		t.Helper()
		refusal, ok := errors.AsType[*worlds.InBandSupersededError](err)
		if !ok || refusal.InBandCount != count {
			t.Fatalf("stale write = %v, want a refusal reporting InBandCount %d", err, count)
		}
	}
	t.Run("another instance reads the count", func(t *testing.T) {
		runID := sdkRun(t, a, "fenceWorkflow", []byte(`[]`))
		if err := attrSet(a, runID, "one", 1); err != nil {
			t.Fatal(err)
		}
		if page, err := b.EventsList(ctx, worlds.EventsListInput{RunID: runID}); err != nil || page.Snapshot.InBandCount != 2 {
			t.Fatalf("snapshot from the other instance = %+v err = %v", page.Snapshot, err)
		}
		superseded(t, attrSet(b, runID, "stale", 1), 2)
	})
	t.Run("a stale writer", func(t *testing.T) {
		runID := sdkRun(t, a, "fenceWorkflow", []byte(`[]`))
		loaded := conformSnapshot(t, a, runID).InBandCount
		if err := attrSet(b, runID, "there", loaded); err != nil {
			t.Fatal(err)
		}
		superseded(t, attrSet(a, runID, "stale", loaded), loaded+1)
		if snapshot := conformSnapshot(t, a, runID); snapshot.InBandCount != loaded+1 {
			t.Fatalf("snapshot = %+v, want InBandCount %d", snapshot, loaded+1)
		}
		if slots := sdkSlots(t, conformAllEvents(t, a, runID)); !slices.Equal(slots, []uint64{1, 2}) {
			t.Fatalf("slots = %v", slots)
		}
	})
	t.Run("writers holding the same count", func(t *testing.T) {
		runID := sdkRun(t, a, "fenceWorkflow", []byte(`[]`))
		loaded := conformSnapshot(t, a, runID).InBandCount
		const writers = 4
		errs := make([]error, writers)
		var group sync.WaitGroup
		for index := range writers {
			world := a
			if index%2 == 1 {
				world = b
			}
			group.Go(func() { errs[index] = attrSet(world, runID, fmt.Sprintf("writer-%d", index), loaded) })
		}
		group.Wait()
		accepted, refused := 0, 0
		for _, err := range errs {
			switch {
			case err == nil:
				accepted++
			case is[*worlds.InBandSupersededError](err):
				refused++
			default:
				t.Fatalf("writer error = %v", err)
			}
		}
		if accepted != 1 || refused != writers-1 {
			t.Fatalf("accepted %d, refused %d: %v", accepted, refused, errs)
		}
		if snapshot := conformSnapshot(t, a, runID); snapshot.InBandCount != loaded+1 {
			t.Fatalf("snapshot = %+v, want InBandCount %d", snapshot, loaded+1)
		}
		if slots := sdkSlots(t, conformAllEvents(t, a, runID)); !slices.Equal(slots, []uint64{1, 2}) {
			t.Fatalf("slots = %v", slots)
		}
	})
}

// SDK: packages/world-local/src/storage.test.ts > Storage > hooks > create > admits one replacement across storage instances after retention ends
func conformSDKSharedHookRetention(t *testing.T, a, b worlds.World) {
	owner := sdkRun(t, a, "test-workflow", []byte(`[]`))
	token := "concurrent-expired-retention-token:" + string(owner)
	until := time.Now().Add(500 * time.Millisecond).UTC()
	conformAppend(t, a, owner, worlds.EventTypeHookCreated, "hook_expiring_owner", worlds.DataHookCreated{Token: token, TokenRetentionUntil: &until})
	conformAppend(t, a, owner, worlds.EventTypeRunStarted, "", nil)
	conformAppend(t, a, owner, worlds.EventTypeRunCompleted, "", worlds.DataRunCompleted{Output: []byte(`[]`)})
	const contenders = 10
	instance := func(index int) worlds.World {
		if index%2 == 1 {
			return b
		}
		return a
	}
	runs := make([]worlds.RunID, contenders)
	for index := range runs {
		runs[index] = sdkRun(t, instance(index), "retention-contender", []byte(`[]`))
	}
	time.Sleep(time.Until(until) + time.Millisecond)
	results := make([]worlds.EventResult, contenders)
	errs := make([]error, contenders)
	var group sync.WaitGroup
	for index := range contenders {
		group.Go(func() {
			results[index], errs[index] = sdkTry(instance(index), runs[index], worlds.EventTypeHookCreated, fmt.Sprintf("hook_contender_%d", index), worlds.DataHookCreated{Token: token})
		})
	}
	group.Wait()
	if err := errors.Join(errs...); err != nil {
		t.Fatal(err)
	}
	created, conflicts := 0, 0
	for _, result := range results {
		switch result.EventType {
		case worlds.EventTypeHookCreated:
			created++
		case worlds.EventTypeHookConflict:
			conflicts++
		}
	}
	if created != 1 || conflicts != contenders-1 {
		t.Fatalf("created %d, conflicts %d", created, conflicts)
	}
}
