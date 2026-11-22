package vercelworld

import (
	"bytes"
	"context"
	"net/http"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/internal/envelope"
	"github.com/eriicafes/go-workflow/worlds"
)

func TestStepsListReturnsANotFound(t *testing.T) {
	world := newBackendWorld(t, func(writer http.ResponseWriter, request *http.Request) {
		writeError(writer, http.StatusNotFound, "run-not-found", "run not found")
	})
	if _, err := world.StepsList(context.Background(), worlds.StepsListInput{RunID: "wrun_1"}); statusOf(err) != http.StatusNotFound {
		t.Fatalf("steps of a missing run = %v, want the backend's 404", err)
	}
}

func TestListsKeepTheCursorOnTheLastPage(t *testing.T) {
	world := newBackendWorld(t, func(writer http.ResponseWriter, request *http.Request) {
		writeCBOR(writer, map[string]any{"data": []map[string]any{}, "cursor": "resume", "hasMore": false})
	})
	page, err := world.StepsList(context.Background(), worlds.StepsListInput{RunID: "wrun_1"})
	if err != nil {
		t.Fatal(err)
	}
	if page.Cursor != "resume" || page.HasMore {
		t.Fatalf("last page = %+v, want the backend's cursor", page)
	}

	all, _, _ := newWorlds(t, 1)
	ctx := context.Background()
	runID := worlds.NewRunID()
	if _, err := all[0].EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: runID, WorkflowName: "cursor", Input: []byte(`"x"`)}); err != nil {
		t.Fatal(err)
	}
	events, err := all[0].EventsList(ctx, worlds.EventsListInput{RunID: runID})
	if err != nil {
		t.Fatal(err)
	}
	if events.HasMore || len(events.Data) == 0 || events.Cursor == "" {
		t.Fatalf("last events page = %+v, want the backend's cursor", events.Page)
	}
}

func TestRunsWaitFallsBackToPlainReads(t *testing.T) {
	t.Setenv("WORKFLOW_REQUEST_TIMEOUT_MS", "")
	shorten(t, &readRetryBase, time.Millisecond)
	for _, test := range []struct {
		name   string
		status int
		held   int32
	}{
		// The route is missing, an intermediary gave up on the hold, as it
		// did on each retry of it, or the hold outlived the request's own
		// deadline.
		{"a missing route", http.StatusNotFound, 1},
		{"a gateway timeout", http.StatusGatewayTimeout, maxReadRetries + 1},
		{"a hold past the request's deadline", 0, 1},
	} {
		var held, plain atomic.Int32
		world := newBackendWorld(t, func(writer http.ResponseWriter, request *http.Request) {
			if !strings.HasSuffix(request.URL.Path, "/status") {
				plain.Add(1)
				writeCBOR(writer, map[string]any{"runId": "wrun_1", "status": "completed"})
				return
			}
			held.Add(1)
			if test.status == 0 {
				<-request.Context().Done()
				return
			}
			writeError(writer, test.status, "", "no")
		})
		if test.status == 0 {
			// The hold asked for is the timeout less its headroom, which the
			// backend here ignores.
			shorten(t, &runWaitHeadroom, 10*time.Millisecond)
			shorten(t, &defaultRequestTimeout, 60*time.Millisecond)
		}
		run, err := world.RunsWaitForTerminalStatus(context.Background(), worlds.RunsWaitForTerminalStatusInput{RunID: "wrun_1", Timeout: time.Minute})
		if err != nil || run.Status != worlds.StatusCompleted || held.Load() != test.held || plain.Load() != 1 {
			t.Errorf("%s: %+v, %v after %d held and %d plain reads, want the run from one plain read", test.name, run.Status, err, held.Load(), plain.Load())
		}
	}
	// A backend that is unwell is reported, not read around.
	world := newBackendWorld(t, func(writer http.ResponseWriter, request *http.Request) {
		writeError(writer, http.StatusBadRequest, "validation-error", "no")
	})
	if _, err := world.RunsWaitForTerminalStatus(context.Background(), worlds.RunsWaitForTerminalStatusInput{RunID: "wrun_1", Timeout: time.Minute}); statusOf(err) != http.StatusBadRequest {
		t.Fatalf("refused wait = %v, want the backend's 400", err)
	}
}

func TestRunsWaitAsksForTheHoldItsDeadlineAllows(t *testing.T) {
	t.Setenv("WORKFLOW_REQUEST_TIMEOUT_MS", "")
	var waits []string
	world := newBackendWorld(t, func(writer http.ResponseWriter, request *http.Request) {
		waits = append(waits, request.URL.Query().Get("waitMs"))
		writeCBOR(writer, map[string]any{"runId": "wrun_1", "status": "completed"})
	})
	ctx := context.Background()
	if _, err := world.RunsWaitForTerminalStatus(ctx, worlds.RunsWaitForTerminalStatusInput{RunID: "wrun_1", Timeout: time.Hour}); err != nil {
		t.Fatal(err)
	}
	// With a deadline no longer than the headroom, there is no hold to ask for.
	t.Setenv("WORKFLOW_REQUEST_TIMEOUT_MS", "10000")
	if _, err := world.RunsWaitForTerminalStatus(ctx, worlds.RunsWaitForTerminalStatusInput{RunID: "wrun_1", Timeout: time.Hour}); err != nil {
		t.Fatal(err)
	}
	if want := []string{"50000", ""}; !reflect.DeepEqual(waits, want) {
		t.Fatalf("waits asked for = %q, want %q", waits, want)
	}
}

func TestEntityViewsReturnPayloadsUncompressed(t *testing.T) {
	plain := envelope.Encode(envelope.PrefixDevalue, bytes.Repeat([]byte(`["a long value"]`), 200))
	encrypted := envelope.Encode(envelope.PrefixEncrypted, []byte("sealed"))
	for _, codec := range []envelope.Codec{envelope.CodecGzip, envelope.CodecZstd} {
		stored := envelope.Compress(plain, codec)
		if bytes.Equal(stored, plain) {
			t.Fatalf("%s did not compress the payload", codec)
		}
		world := newBackendWorld(t, func(writer http.ResponseWriter, request *http.Request) {
			switch {
			case strings.Contains(request.URL.Path, "/steps/"):
				writeCBOR(writer, map[string]any{"runId": "wrun_1", "stepId": "step_1", "status": "completed", "input": stored, "output": stored})
			case strings.HasSuffix(request.URL.Path, "/events"):
				writer.Header().Set("Content-Type", frameContentType)
				_, _ = writer.Write(eventFrames(t, true))
			default:
				writeCBOR(writer, map[string]any{"runId": "wrun_1", "status": "completed", "input": stored, "output": encrypted})
			}
		})
		ctx := context.Background()
		run, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: "wrun_1"})
		if err != nil || !bytes.Equal(run.Input, plain) || !bytes.Equal(run.Output, encrypted) {
			t.Fatalf("%s run = input %d bytes, output %q, %v, want the payload inside the compression and the encrypted one as stored", codec, len(run.Input), run.Output, err)
		}
		step, err := world.StepsGet(ctx, worlds.StepsGetInput{RunID: "wrun_1", StepID: "step_1"})
		if err != nil || !bytes.Equal(step.Input, plain) || !bytes.Equal(step.Output, plain) {
			t.Fatalf("%s step = %d and %d bytes, %v", codec, len(step.Input), len(step.Output), err)
		}
	}
	// A payload that only looks compressed is returned as stored.
	if broken := append([]byte("gzip"), "not gzip"...); !bytes.Equal(entityPayload(broken), broken) {
		t.Fatal("a payload that does not decompress was changed")
	}
}
