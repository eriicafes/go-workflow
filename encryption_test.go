package workflow_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/testworld"
)

type encryptedInput struct{ Secret string }
type encryptedOutput struct {
	Secret  string
	Resumed string
}

func (encryptedInput) Workflow(encryptedOutput) {}

func shout(_ context.Context, text string) (string, error) { return text + "!", nil }

func writeSecret(ctx context.Context, text string) (struct{}, error) {
	writer, err := workflow.Writer(ctx)
	if err != nil {
		return struct{}{}, err
	}
	defer writer.Close()
	_, err = writer.Write([]byte(text))
	return struct{}{}, err
}

// TestEncryptedRunsKeepPayloadsOpaque runs a workflow on a World with an
// encryption key: its own payloads are encrypted with the run's key (encr),
// and what is sent from outside the run is sealed to its public key (encp),
// as in the Workflow SDK, while every value reads back.
func TestEncryptedRunsKeepPayloadsOpaque(t *testing.T) {
	for _, mode := range []string{"in process", "over HTTP"} {
		t.Run(mode, func(t *testing.T) { testEncryptedRun(t, mode == "over HTTP") })
	}
}

func testEncryptedRun(t *testing.T, overHTTP bool) {
	world := testworld.New(workflow.Use(func(ctx context.Context, input encryptedInput) (encryptedOutput, error) {
		secret, err := workflow.Step(ctx, shout, input.Secret)
		if err != nil {
			return encryptedOutput{}, err
		}
		if _, err := workflow.Step(ctx, writeSecret, secret); err != nil {
			return encryptedOutput{}, err
		}
		hook := workflow.CreateHook[string](ctx, workflow.HookOption{Token: "encrypted-" + workflow.GetWorkflowMetadata(ctx).RunID})
		resumed, err := hook.Next(ctx)
		if err != nil {
			return encryptedOutput{}, err
		}
		return encryptedOutput{Secret: secret, Resumed: resumed}, nil
	}))
	defer world.Close()
	if err := world.SetEncryptionKey(bytes.Repeat([]byte{3}, 32)); err != nil {
		t.Fatal(err)
	}
	client := workflow.NewClient(world)
	if overHTTP {
		// The client reads the run's key over RPC, and seals what it sends.
		server := httptest.NewServer(worlds.HTTPHandler(world, ""))
		defer server.Close()
		client = workflow.NewHTTPClient(server.URL+"/v1/workflow", server.Client())
	}
	ctx, cancel := context.WithTimeout(workflow.NewContext(context.Background(), client), 3*time.Second)
	defer cancel()
	run, err := workflow.Start(ctx, encryptedInput{Secret: "plaintext-secret"})
	if err != nil {
		t.Fatal(err)
	}
	if err := workflow.ResumeHookWithRetries(ctx, "encrypted-"+run.ID(), "resumed-secret", 100, 10*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	writer, err := run.Writer(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte("outside-secret")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	output, err := run.Value(ctx)
	if err != nil || output != (encryptedOutput{Secret: "plaintext-secret!", Resumed: "resumed-secret"}) {
		t.Fatalf("output = %#v err=%v", output, err)
	}

	stored, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: worlds.RunID(run.ID())})
	if err != nil || worlds.DecodePublicKey(stored.EncryptionPublicKey) == nil {
		t.Fatalf("run = %#v err=%v", stored, err)
	}
	events, err := world.EventsList(ctx, worlds.EventsListInput{RunID: worlds.RunID(run.ID())})
	if err != nil {
		t.Fatal(err)
	}
	labels := map[worlds.EventType]string{}
	for _, event := range events.Data {
		if bytes.Contains(event.Data, []byte("secret")) {
			t.Fatalf("%s holds plaintext: %s", event.EventType, event.Data)
		}
		var data map[string]json.RawMessage
		_ = json.Unmarshal(event.Data, &data)
		for _, field := range []string{"input", "result", "output", "payload"} {
			var payload []byte
			if json.Unmarshal(data[field], &payload) == nil && len(payload) > 0 {
				labels[event.EventType] = worlds.PayloadLabel(payload)
			}
		}
	}
	want := map[worlds.EventType]string{
		worlds.EventTypeRunCreated:    worlds.PayloadEncrypted,
		worlds.EventTypeStepCreated:   worlds.PayloadEncrypted,
		worlds.EventTypeStepCompleted: worlds.PayloadEncrypted,
		worlds.EventTypeHookReceived:  worlds.PayloadSealed,
		worlds.EventTypeRunCompleted:  worlds.PayloadEncrypted,
	}
	for eventType, label := range want {
		if labels[eventType] != label {
			t.Errorf("%s payload = %q, want %q", eventType, labels[eventType], label)
		}
	}

	// The stream holds the step's frame, encrypted, and the outside writer's,
	// sealed, and reads back as written.
	chunks, err := world.StreamsGetChunks(ctx, worlds.StreamsGetChunksInput{RunID: worlds.RunID(run.ID()), Name: worlds.UserStreamName(worlds.RunID(run.ID()), "")})
	if err != nil || len(chunks.Data) != 2 {
		t.Fatalf("stream = %#v err=%v", chunks, err)
	}
	for index, label := range []string{worlds.PayloadEncrypted, worlds.PayloadSealed} {
		payloads, err := worlds.SplitStreamFrames(chunks.Data[index].Data)
		if err != nil || len(payloads) != 1 || worlds.PayloadLabel(payloads[0]) != label {
			t.Fatalf("frame %d = %q err=%v, want %s", index, chunks.Data[index].Data, err, label)
		}
	}
	reader, err := run.Reader(ctx)
	if err != nil {
		t.Fatal(err)
	}
	first, err := reader.Next()
	if err != nil || string(first.Data) != "plaintext-secret!" {
		t.Fatalf("first chunk = %q err=%v", first.Data, err)
	}
	second, err := reader.Next()
	if err != nil || string(second.Data) != "outside-secret" {
		t.Fatalf("second chunk = %q err=%v", second.Data, err)
	}
}
