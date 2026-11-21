package vercelworld

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/eriicafes/go-workflow/worlds"
)

// The Vercel World pushes a run's events over the backend's events socket,
// as world-vercel's live feed does. On the run's socket the client sends
//
//	{reqId, type: "subscribe", afterSlot}
//
// which the backend answers with a 2xx subscribe_ack, and from then on
// pushes, in slot order,
//
//	{reqId, type: "run_event", slot}  body: one event frame, as a list frame
//
// for every event appended above afterSlot. When the backend ends a feed it
// pushes one last run_event with end set, its status, error and message. A
// backend that sends heartbeats says so on the ack (heartbeatMs) and then
// sends {reqId, type: "run_heartbeat", slot} naming the highest slot it
// pushed: silence for heartbeatSilenceFactor intervals ends the feed, a slot
// above the last one delivered is read from the log, and a heartbeat under
// another reqId means a later subscription replaced this one. Nothing depends
// on the feed for correctness: the runtime polls the log too.

const (
	// eventsSocketFlagsHeader lists the protocol features a client takes;
	// run-heartbeat asks for heartbeats.
	eventsSocketFlagsHeader = "X-Workflow-Ws-Flags"
	// heartbeatSilenceFactor is how many heartbeat intervals of silence end
	// a feed.
	heartbeatSilenceFactor = 3
	// gapFillPageSize is the most events one gap-filling read asks for.
	gapFillPageSize = 100
)

// eventsSocketURL returns the events socket URL of run runID.
func eventsSocketURL(base string, runID worlds.RunID) (string, error) {
	parsed, err := url.Parse(base)
	if err != nil {
		return "", err
	}
	scheme := map[string]string{"https": "wss", "http": "ws"}[parsed.Scheme]
	if scheme == "" || parsed.Host == "" {
		return "", fmt.Errorf("workflow/vercel: events socket base URL must be an absolute http or https URL: %q", base)
	}
	return scheme + "://" + parsed.Host + strings.TrimRight(parsed.EscapedPath(), "/") + "/websockets/v1/runs/" + url.PathEscape(string(runID)), nil
}

// EventsSubscribe pushes the run's events after afterSlot over the backend's
// events socket. The socket is reachable with a deployment's OIDC token only,
// so through the API proxy the feed reports itself down at once.
func (w *World) EventsSubscribe(ctx context.Context, runID worlds.RunID, afterSlot uint64, onEvent func(worlds.Event), onError func(error)) func() {
	ctx, cancel := context.WithCancel(ctx)
	feed := &eventsFeed{world: w, runID: runID, lastSlot: afterSlot, onEvent: onEvent, onError: onError, cancel: cancel}
	var done sync.WaitGroup
	done.Go(func() { feed.run(ctx, afterSlot) })
	return func() {
		feed.mu.Lock()
		feed.stopped = true
		feed.mu.Unlock()
		cancel()
		done.Wait()
	}
}

// eventsFeed is one subscription to a run's events socket.
type eventsFeed struct {
	world    *World
	runID    worlds.RunID
	onEvent  func(worlds.Event)
	onError  func(error)
	cancel   context.CancelFunc
	mu       sync.Mutex
	lastSlot uint64
	stopped  bool
	failed   bool
}

// fail reports why the feed ended, once, unless it was unsubscribed.
func (f *eventsFeed) fail(err error) {
	f.mu.Lock()
	report := !f.stopped && !f.failed
	f.failed = true
	f.mu.Unlock()
	f.cancel()
	if report {
		f.onError(err)
	}
}

// deliver passes on event, at slot, when it is above the last one delivered.
func (f *eventsFeed) deliver(event worlds.Event, slot uint64) {
	f.mu.Lock()
	if f.stopped || slot <= f.lastSlot {
		f.mu.Unlock()
		return
	}
	f.lastSlot = slot
	f.mu.Unlock()
	f.onEvent(event)
}

func (f *eventsFeed) run(ctx context.Context, afterSlot uint64) {
	w := f.world
	if w.api.proxy {
		f.fail(errors.New("workflow/vercel: the events socket needs a deployment's OIDC token"))
		return
	}
	target, err := eventsSocketURL(w.api.baseURL, f.runID)
	if err != nil {
		f.fail(err)
		return
	}
	header := http.Header{}
	if err := w.api.authorize(ctx, header); err != nil {
		f.fail(err)
		return
	}
	if w.api.userAgent != "" {
		header.Set("User-Agent", w.api.userAgent)
	}
	header.Set(eventsSocketFlagsHeader, "run-heartbeat")
	conn, _, err := websocket.Dial(ctx, target, &websocket.DialOptions{HTTPClient: w.api.http, HTTPHeader: header})
	if err != nil {
		f.fail(fmt.Errorf("workflow/vercel: open the events socket of %s: %w", f.runID, err))
		return
	}
	defer conn.CloseNow()
	conn.SetReadLimit(64 << 20)
	const reqID = 1
	request, err := encodeFrame(nil, map[string]any{"reqId": reqID, "type": "subscribe", "afterSlot": afterSlot}, nil)
	if err != nil {
		f.fail(err)
		return
	}
	if err := conn.Write(ctx, websocket.MessageBinary, request); err != nil {
		f.fail(fmt.Errorf("workflow/vercel: subscribe to the events of %s: %w", f.runID, err))
		return
	}
	acked := false
	var silence time.Duration
	var watchdog *time.Timer
	heard := func() {
		if silence > 0 {
			watchdog.Reset(silence)
		}
	}
	for {
		_, raw, err := conn.Read(ctx)
		if err != nil {
			f.fail(fmt.Errorf("workflow/vercel: the events socket of %s closed: %w", f.runID, err))
			return
		}
		message, err := newFrameReader(bytes.NewReader(raw)).next()
		if err != nil {
			f.fail(fmt.Errorf("workflow/vercel: decode an events socket frame: %w", err))
			return
		}
		meta := message.Meta
		switch str(meta, "type") {
		case "drain":
			// The close that follows ends the feed.
		case "run_heartbeat":
			if !acked {
				continue
			}
			if id, _ := asInt(meta["reqId"]); id != reqID {
				f.fail(errors.New("workflow/vercel: a later subscription on the connection replaced the events feed"))
				return
			}
			heard()
			if slot, ok := asInt(meta["slot"]); ok {
				f.fillGap(ctx, uint64(slot))
			}
		case "run_event":
			if end, _ := meta["end"].(bool); end {
				status, _ := asInt(meta["status"])
				f.fail(fmt.Errorf("workflow/vercel: the backend ended the events feed of %s (status %d, %s): %s", f.runID, status, str(meta, "error"), str(meta, "message")))
				return
			}
			heard()
			pushed, err := newFrameReader(bytes.NewReader(message.Body)).next()
			if err != nil {
				f.fail(fmt.Errorf("workflow/vercel: a malformed pushed event: %w", err))
				return
			}
			wire, err := decodeWireEvent(pushed.Meta, pushed.Body)
			if err != nil {
				f.fail(err)
				return
			}
			event, err := toEvent(f.runID, wire, worlds.EventsResolveDataAll)
			if err != nil {
				f.fail(err)
				return
			}
			if event.RunID != f.runID {
				continue
			}
			slot, ok := asInt(meta["slot"])
			if !ok {
				value, err := worlds.EventSlot(event.EventID)
				if err != nil {
					continue
				}
				slot = int(value)
			}
			f.deliver(event, uint64(slot))
		default:
			if id, _ := asInt(meta["reqId"]); id != reqID || acked {
				continue
			}
			if status, _ := asInt(meta["status"]); status < 200 || status >= 300 {
				f.fail(fmt.Errorf("workflow/vercel: the events subscription of %s was refused (status %d, type %s)", f.runID, status, str(meta, "type")))
				return
			}
			acked = true
			if interval, ok := asInt(meta["heartbeatMs"]); ok && interval > 0 {
				silence = time.Duration(interval) * time.Millisecond * heartbeatSilenceFactor
				watchdog = time.AfterFunc(silence, func() {
					f.fail(fmt.Errorf("workflow/vercel: the events feed of %s went silent", f.runID))
				})
				defer watchdog.Stop()
			}
		}
	}
}

// fillGap reads from the log the events up to slot through, which a
// heartbeat says the backend pushed above the last one delivered, and
// delivers what continues the sequence. A failed read ends the feed.
func (f *eventsFeed) fillGap(ctx context.Context, through uint64) {
	for {
		f.mu.Lock()
		last := f.lastSlot
		f.mu.Unlock()
		if last >= through {
			return
		}
		page, err := f.world.EventsList(ctx, worlds.EventsListInput{RunID: f.runID, Pagination: worlds.Pagination{Cursor: string(worlds.EventIDForSlot(last)), Limit: gapFillPageSize}})
		if err != nil {
			f.fail(fmt.Errorf("workflow/vercel: read the events the feed of %s missed: %w", f.runID, err))
			return
		}
		advanced := false
		for _, event := range page.Data {
			slot, err := worlds.EventSlot(event.EventID)
			if err != nil || slot <= last {
				continue
			}
			if slot != last+1 {
				break
			}
			f.deliver(event, slot)
			last, advanced = slot, true
		}
		if !advanced {
			return
		}
	}
}
