package workflowtest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

func conformConcurrentAppends(t *testing.T, a, b worlds.World) {
	ctx := context.Background()
	runID := conformCreateRun(t, a, "concurrent")
	const writers, perWriter = 4, 10
	var group sync.WaitGroup
	errs := make(chan error, writers*perWriter)
	for writer := range writers {
		world := a
		if writer%2 == 1 {
			world = b
		}
		group.Add(1)
		go func() {
			defer group.Done()
			for i := range perWriter {
				correlation := fmt.Sprintf("wait_%d_%d", writer, i)
				if _, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeWaitCreated, CorrelationID: correlation}}); err != nil {
					errs <- err
				}
			}
		}()
	}
	group.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	for _, world := range []worlds.World{a, b} {
		events := conformAllEvents(t, world, runID)
		if len(events) != writers*perWriter+1 {
			t.Fatalf("events = %d, want %d", len(events), writers*perWriter+1)
		}
		seen := map[string]bool{}
		for index, event := range events {
			if event.EventID != worlds.EventIDForSlot(uint64(index+1)) {
				t.Fatalf("event %d has ID %s", index, event.EventID)
			}
			if seen[event.CorrelationID] {
				t.Fatalf("duplicate event %s", event.CorrelationID)
			}
			seen[event.CorrelationID] = true
		}
	}
	run, err := b.RunsGet(ctx, worlds.RunsGetInput{RunID: runID})
	if err != nil || run.RunID != runID {
		t.Fatalf("run from second instance = %#v err=%v", run, err)
	}
}

func conformConcurrentHookClaims(t *testing.T, a, b worlds.World) {
	token := fmt.Sprintf("race-%d", time.Now().UnixNano())
	const claimants = 6
	runs := make([]worlds.RunID, claimants)
	for i := range runs {
		runs[i] = conformCreateRun(t, a, "race")
	}
	var group sync.WaitGroup
	var mu sync.Mutex
	winners := 0
	for i := range claimants {
		world := a
		if i%2 == 1 {
			world = b
		}
		group.Add(1)
		go func() {
			defer group.Done()
			_, err := conformClaim(world, runs[i], worlds.HookID(fmt.Sprintf("hook_race_%d", i)), token, false)
			if err == nil {
				mu.Lock()
				winners++
				mu.Unlock()
			} else if !errors.Is(err, worlds.ErrHookConflict) {
				t.Errorf("claim error = %v", err)
			}
		}()
	}
	group.Wait()
	if winners != 1 {
		t.Fatalf("winners = %d, want 1", winners)
	}
	hook, err := b.HooksGetByToken(context.Background(), worlds.HooksGetByTokenInput{Token: token})
	if err != nil {
		t.Fatal(err)
	}
	created := 0
	for _, runID := range runs {
		for _, event := range conformAllEvents(t, a, runID) {
			if event.EventType == worlds.EventTypeHookCreated {
				created++
				if event.RunID != hook.RunID {
					t.Fatalf("hook_created in losing run %s", runID)
				}
			}
		}
	}
	if created != 1 {
		t.Fatalf("hook_created events = %d, want 1", created)
	}
}

func conformSharedStreamSessions(t *testing.T, a, b worlds.World) {
	ctx := context.Background()
	runID := conformCreateRun(t, a, "shared_stream")
	session, err := a.StreamsCreateWriteSession(ctx, worlds.StreamsCreateWriteSessionInput{RunID: runID, Name: "out"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = b.StreamsCreateWriteSession(ctx, worlds.StreamsCreateWriteSessionInput{RunID: runID, Name: "out"})
	conformExpect(t, err, worlds.ErrStreamLocked, "session from second instance")
	if err := session.Write(ctx, worlds.StreamWriteSessionWriteInput{Sequence: 0, Chunks: [][]byte{[]byte("x")}}); err != nil {
		t.Fatal(err)
	}
	go func() {
		time.Sleep(100 * time.Millisecond)
		_ = session.Write(context.Background(), worlds.StreamWriteSessionWriteInput{Sequence: 1, Chunks: [][]byte{[]byte("y")}})
	}()
	chunks, err := b.StreamsGetChunks(ctx, worlds.StreamsGetChunksInput{RunID: runID, Name: "out", Cursor: "1", Wait: 10 * time.Second})
	if err != nil || len(chunks.Data) != 1 || string(chunks.Data[0].Data) != "y" {
		t.Fatalf("chunks seen by second instance = %#v err=%v", chunks, err)
	}
	if err := session.Close(ctx); err != nil {
		t.Fatal(err)
	}
	info, err := b.StreamsGetInfo(ctx, worlds.StreamsGetInfoInput{RunID: runID, Name: "out"})
	if err != nil || !info.Done || info.TailIndex != 1 {
		t.Fatalf("stream info from second instance = %#v err=%v", info, err)
	}
}
