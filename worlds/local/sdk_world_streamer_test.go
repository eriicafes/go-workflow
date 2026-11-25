package localworld

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

// idleReaderCPU returns the CPU time 16 idle readers at the tail of a
// stream of chunks chunks use in a second.
func idleReaderCPU(t *testing.T, world *World, name string, chunks int) time.Duration {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runID := worlds.NewRunID()
	chunk := make([]byte, 16)
	for written := 0; written < chunks; written += 1000 {
		group := make([][]byte, min(1000, chunks-written))
		for i := range group {
			group[i] = chunk
		}
		if err := world.StreamsWriteMulti(ctx, worlds.StreamsWriteMultiInput{RunID: runID, Name: name, Chunks: group}); err != nil {
			t.Fatal(err)
		}
	}
	var readers sync.WaitGroup
	for range 16 {
		readers.Go(func() {
			cursor := fmt.Sprint(chunks)
			for ctx.Err() == nil {
				page, err := world.StreamsGetChunks(ctx, worlds.StreamsGetChunksInput{RunID: runID, Name: name, Cursor: cursor, Wait: time.Minute})
				if err == nil && page.Cursor != "" {
					cursor = page.Cursor
				}
			}
		})
	}
	// The readers settle at the tail before the measurement.
	time.Sleep(200 * time.Millisecond)
	before := cpuTime(t)
	time.Sleep(time.Second)
	used := cpuTime(t) - before
	cancel()
	readers.Wait()
	return used
}

func cpuTime(t *testing.T) time.Duration {
	t.Helper()
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		t.Fatal(err)
	}
	return time.Duration(usage.Utime.Nano() + usage.Stime.Nano())
}

// TestSDKIdleStreamReaderCostsTheSameOnALongStream measures the process's
// CPU time, so it does not run in parallel with other tests.
//
// SDK: packages/world-local/src/stream-idle-readers.test.ts > an idle reader costs the same at the tail of a long stream as of a short one
func TestSDKIdleStreamReaderCostsTheSameOnALongStream(t *testing.T) {
	world := queueWorld(t, Config{}, func(context.Context, worlds.QueueInput) error { return nil })
	short := idleReaderCPU(t, world, "short", 100)
	long := idleReaderCPU(t, world, "long", 20_000)
	// Against the same measurement on a short stream, so the bound does not
	// depend on the machine, with a floor for the noise of a near-idle
	// process.
	if long >= max(2*short, 100*time.Millisecond) {
		t.Fatalf("idle readers of a long stream used %s, of a short one %s", long, short)
	}
}

// The untagged case runs in the shared conformance suite.
//
// SDK: packages/world-local/src/streamer.test.ts > streamer > list > concurrent first writes > keeps every name when two streamers share the data directory ($label)
func TestSDKTaggedStreamersShareNames(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	first, second := writer(t, dir, "vitest-x"), writer(t, dir, "vitest-x")
	for i := range 10 {
		runID := worlds.NewRunID()
		var names []string
		var writers sync.WaitGroup
		errs := make(chan error, 6)
		for _, streamer := range []struct {
			world  *World
			prefix string
		}{{first, "first"}, {second, "second"}} {
			for j := range 3 {
				name := worlds.UserStreamName(runID, fmt.Sprintf("%s-%d-%d", streamer.prefix, i, j))
				names = append(names, name)
				writers.Go(func() {
					errs <- streamer.world.StreamsWrite(ctx, worlds.StreamsWriteInput{RunID: runID, Name: name, Chunk: []byte(streamer.prefix)})
				})
			}
		}
		writers.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatal(err)
			}
		}
		listed, err := first.StreamsList(ctx, worlds.StreamsListInput{RunID: runID})
		slices.Sort(names)
		if err != nil || !slices.Equal(listed, names) {
			t.Fatalf("streams = %q err=%v, want %q", listed, err, names)
		}
	}
}
