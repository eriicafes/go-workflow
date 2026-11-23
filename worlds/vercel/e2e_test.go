package vercelworld

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/internal/envelope"
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

// deliverAll plays Vercel Queues' push: it pushes each message of queue to
// the consumer once it is visible, until ctx is done.
func deliverAll(ctx context.Context, t *testing.T, queue *fakeVQS, consumer http.Handler) {
	for ctx.Err() == nil {
		message, request := queue.next()
		if message == nil {
			time.Sleep(5 * time.Millisecond)
			continue
		}
		if message.deliveries > 50 {
			t.Errorf("message %s kept being redelivered: %s", message.id, message.body)
		}
		go func() {
			defer queue.pushed(message)
			consumer.ServeHTTP(httptest.NewRecorder(), request.WithContext(ctx))
		}()
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
	if backend.count("POST v5/runs/:id/events/batch") == 0 {
		t.Fatal("inline steps were not written as a batch")
	}
}

// TestWorkflowsRunEndToEndEncrypted runs the same workflows with the
// deployment's key set, so every payload is encrypted with the run's key.
func TestWorkflowsRunEndToEndEncrypted(t *testing.T) {
	t.Setenv("VERCEL", "1")
	t.Setenv("VERCEL_DEPLOYMENT_KEY", base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{4}, 32)))
	t.Setenv("VERCEL_PROJECT_ID", "prj_e2e")
	all, queue, backend := newWorlds(t, 1)
	runEndToEnd(t, all[0], queue)
	runs, err := backend.store.RunsList(context.Background(), worlds.RunsListInput{})
	if err != nil || len(runs.Data) != 1 {
		t.Fatalf("runs = %#v err=%v", runs, err)
	}
	if envelope.Peek(runs.Data[0].Output) != envelope.PrefixEncrypted || runs.Data[0].EncryptionPublicKey == "" {
		t.Fatalf("run output %q is not encrypted", runs.Data[0].Output)
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

type mailboxInput struct{ ChatID int }

func (mailboxInput) Workflow(string) {}

// TestRunInputSurvivesReplay runs a workflow whose hook token comes from its
// input, so a replay that reads the run's input back wrong reaches a
// different token than the first execution recorded.
func TestRunInputSurvivesReplay(t *testing.T) {
	all, queue, _ := newWorlds(t, 1)
	world := all[0]
	runners := worlds.Runners(workflow.Use(func(ctx context.Context, input mailboxInput) (string, error) {
		hook := workflow.CreateHook[string](ctx, workflow.HookOption{Token: fmt.Sprintf("mailbox:%d", input.ChatID)})
		return hook.Next(ctx)
	}))
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), workflow.NewClient(world)), 20*time.Second)
	defer cancel()
	go deliverAll(ctx, t, queue, Queue(world, runners))

	run, err := workflow.Start(ctx, mailboxInput{ChatID: 948721655})
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := world.HooksGetByToken(ctx, worlds.HooksGetByTokenInput{Token: "mailbox:948721655"}); err == nil {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("hook was not created: %v", ctx.Err())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := workflow.ResumeHook(ctx, "mailbox:948721655", "hello"); err != nil {
		t.Fatal(err)
	}
	output, err := run.Value(ctx)
	if err != nil || output != "hello" {
		events, _ := world.EventsList(context.Background(), worlds.EventsListInput{RunID: worlds.RunID(run.ID())})
		for _, event := range events.Data {
			t.Logf("%s %s %s %s", event.EventID, event.EventType, event.CorrelationID, event.Data)
		}
		t.Fatalf("output = %q err=%v", output, err)
	}
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

	// Flows of the run were serialized by their run ID, and step deliveries
	// not at all.
	queue.mu.Lock()
	keyed := 0
	for _, message := range queue.messages {
		var body queueMessage
		_ = json.Unmarshal(message.body, &body)
		switch {
		case body.StepID != nil && message.concurrencyKey != "":
			t.Errorf("step delivery %s has concurrency key %q", message.body, message.concurrencyKey)
		case body.StepID == nil && message.concurrencyKey != run.ID():
			t.Errorf("flow %s has concurrency key %q", message.body, message.concurrencyKey)
		case body.StepID == nil:
			keyed++
		}
	}
	queue.mu.Unlock()
	if keyed == 0 {
		t.Error("no flow was published")
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
		if err := json.Unmarshal(event.Data, &data); err != nil || data.ResumeAt.IsZero() {
			t.Fatalf("wait as the backend keeps it = %s err=%v", event.Data, err)
		}
	}
}
