package vercelworld

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/worlds"
)

type e2eInput struct{ N int }

type e2eOutput struct {
	Sum     int
	Payload string
	Written string
}

func (e2eInput) Workflow(e2eOutput) {}

func double(_ context.Context, n int) (int, error) { return 2 * n, nil }

// publishes returns the deliveries published to Vercel Queues so far.
func (f *fakeVQS) publishes() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var bodies []string
	for index, request := range f.requests {
		if request.Method == http.MethodPost && strings.HasPrefix(request.URL.Path, "/api/v3/topic/") && !strings.Contains(request.URL.Path, "/consumer/") {
			bodies = append(bodies, string(f.bodies[index]))
		}
	}
	return bodies
}

// deliverAll plays Vercel Queues: it pushes every published delivery to the
// consumer once it is due, redelivering failed ones, until ctx is done.
func deliverAll(ctx context.Context, t *testing.T, queue *fakeVQS, consumer http.Handler) {
	delivered := 0
	for ctx.Err() == nil {
		published := queue.publishes()
		if delivered == len(published) {
			time.Sleep(5 * time.Millisecond)
			continue
		}
		body := published[delivered]
		delivered++
		go func(id int, body string) {
			var message queueMessage
			_ = json.Unmarshal([]byte(body), &message)
			if message.NotBefore != nil {
				time.Sleep(time.Until(*message.NotBefore))
			}
			for attempt := 0; ctx.Err() == nil && attempt < 20; attempt++ {
				response := httptest.NewRecorder()
				consumer.ServeHTTP(response, deliveryRequest("msg_"+string(rune('a'+id%26))+time.Now().Format("150405.000000"), body))
				if response.Code == http.StatusOK {
					return
				}
				time.Sleep(20 * time.Millisecond)
			}
			if ctx.Err() == nil {
				t.Errorf("delivery %s kept failing", body)
			}
		}(delivered, body)
	}
}

// TestWorkflowsRunEndToEnd runs workflows through the queue consumer and the
// fake backend: steps, a batch of inline steps, a timer declared under a
// cancellation context, a hook, and a stream. Waits keep only their resume
// time in the backend, so replaying past them relies on positions carried
// by operation IDs and on cancellation contexts read from the replay.
func TestWorkflowsRunEndToEnd(t *testing.T) {
	all, queue, backend := newWorlds(t, 1)
	runEndToEnd(t, all[0], queue)
	if backend.count("POST v4/runs/:id/events/batch") == 0 {
		t.Fatal("inline steps were not written as a batch")
	}
}

// TestWorkflowsRunEndToEndLive runs the same workflows against Vercel's
// workflow backend, with Vercel Queues played locally. It needs either
// WORKFLOW_VERCEL_LIVE_OIDC_TOKEN, a deployment's OIDC token, or
// WORKFLOW_VERCEL_LIVE_TOKEN, a Vercel access token, with VERCEL_PROJECT_ID
// and VERCEL_TEAM_ID for the project to use. WORKFLOW_VERCEL_LIVE_BACKEND_URL
// overrides the backend URL.
func TestWorkflowsRunEndToEndLive(t *testing.T) {
	oidc, token := os.Getenv("WORKFLOW_VERCEL_LIVE_OIDC_TOKEN"), os.Getenv("WORKFLOW_VERCEL_LIVE_TOKEN")
	if oidc == "" && token == "" {
		t.Skip("set WORKFLOW_VERCEL_LIVE_OIDC_TOKEN or WORKFLOW_VERCEL_LIVE_TOKEN to run against Vercel")
	}
	queue := &fakeVQS{}
	server := httptest.NewServer(queue)
	t.Cleanup(server.Close)
	world, err := New(Config{
		BaseURL:    "http://localhost/v1/workflow",
		QueueURL:   server.URL + "/api/v3",
		Topic:      "workflow",
		OIDCToken:  staticOIDCToken(oidc),
		Token:      token,
		ProjectID:  os.Getenv("VERCEL_PROJECT_ID"),
		TeamID:     os.Getenv("VERCEL_TEAM_ID"),
		BackendURL: os.Getenv("WORKFLOW_VERCEL_LIVE_BACKEND_URL"),
	})
	if err != nil {
		t.Fatal(err)
	}
	runEndToEnd(t, world, queue)
}

func runEndToEnd(t *testing.T, world *World, queue *fakeVQS) {
	runners := worlds.Runners(workflow.Use(func(ctx context.Context, input e2eInput) (e2eOutput, error) {
		first, err := workflow.Step(ctx, double, input.N)
		if err != nil {
			return e2eOutput{}, err
		}
		many, err := workflow.All(ctx, double, []int{1, 2, 3})
		if err != nil {
			return e2eOutput{}, err
		}
		// The timer is declared under a cancellation context, whose
		// reference the runtime reads back from the replay.
		timerCtx, _, err := workflow.WithCancel(ctx)
		if err != nil {
			return e2eOutput{}, err
		}
		if err := workflow.Sleep(timerCtx, 30*time.Millisecond); err != nil {
			return e2eOutput{}, err
		}
		hook := workflow.CreateHook[string](ctx, workflow.HookOption{Token: "e2e-" + workflow.GetWorkflowMetadata(ctx).RunID})
		payload, err := hook.Next(ctx)
		if err != nil {
			return e2eOutput{}, err
		}
		written, err := workflow.Step(ctx, func(ctx context.Context, text string) (string, error) {
			writer, err := workflow.Writer(ctx)
			if err != nil {
				return "", err
			}
			defer writer.Close()
			if _, err := writer.Write([]byte(text)); err != nil {
				return "", err
			}
			return text, writer.CloseStream()
		}, payload)
		if err != nil {
			return e2eOutput{}, err
		}
		sum := first
		for _, value := range many {
			sum += value
		}
		return e2eOutput{Sum: sum, Payload: payload, Written: written}, nil
	}))
	consumer := Queue(world, runners)
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), 20*time.Second)
	defer cancel()
	go deliverAll(ctx, t, queue, consumer)

	run, err := workflow.Start(ctx, e2eInput{N: 5})
	if err != nil {
		t.Fatal(err)
	}
	token := "e2e-" + run.ID()
	for {
		if _, err := world.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: token}); err == nil {
			break
		}
		if ctx.Err() != nil {
			events, _ := world.EventsList(context.Background(), worlds.EventsListInput{RunID: worlds.RunID(run.ID())})
			for _, event := range events.Data {
				t.Logf("%s %s %s %s", event.EventID, event.EventType, event.CorrelationID, event.Data)
			}
			t.Fatalf("hook was not created: %v", ctx.Err())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := workflow.ResumeHook(ctx, token, "hello"); err != nil {
		t.Fatal(err)
	}
	output, err := run.Value(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if want := (e2eOutput{Sum: 10 + 2 + 4 + 6, Payload: "hello", Written: "hello"}); output != want {
		t.Fatalf("output = %+v, want %+v", output, want)
	}
	reader, err := run.Reader(ctx)
	if err != nil {
		t.Fatal(err)
	}
	chunk, err := reader.Next()
	if err != nil || string(chunk.Data) != "hello" {
		t.Fatalf("stream chunk = %+v err=%v", chunk, err)
	}

	// The backend kept the wait's resume time only.
	events, err := world.EventsList(ctx, worlds.EventsListInput{RunID: worlds.RunID(run.ID())})
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events.Data {
		if event.EventType != worlds.EventTypeWaitCreated {
			continue
		}
		var data worlds.DataWaitCreated
		if err := json.Unmarshal(event.Data, &data); err != nil || data.QueueMessageID != "" || data.Cancellation != nil || data.ResumeAt.IsZero() {
			t.Fatalf("wait as the backend keeps it = %s err=%v", event.Data, err)
		}
		if position, ok := worlds.OperationPosition(event.CorrelationID); !ok || position != data.Position || position == 0 {
			t.Fatalf("wait position %d from %s", data.Position, event.CorrelationID)
		}
	}
}
