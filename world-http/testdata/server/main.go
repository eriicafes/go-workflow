// Command server serves an in-memory World over HTTP for world-http's tests,
// which drive it through the adapter as the Workflow SDK would. It prints one
// line of JSON with its base URL and the names of its workflows, then serves
// until it is killed.
//
// With WORKFLOW_REMOTE_URL set, the World sends its deliveries to that URL
// with a worlds.RemoteExecutor instead of running Go workflows, and with
// WORKFLOW_HTTP_TOKEN set, both the deliveries and the client endpoints use
// that token.
package main

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/testworld"
)

// Review waits for one payload on the hook Token and returns it.
type Review struct {
	workflow.Struct[Decision]
	Token string `json:"token"`
}

// Decision is the payload a Review's hook receives.
type Decision struct {
	Approved bool `json:"approved"`
}

// Nap sleeps for an hour, so a test can wake it.
type Nap struct {
	workflow.Struct[string]
}

func main() {
	var executor worlds.Executor = worlds.Runners(
		workflow.Use(func(ctx context.Context, input Review) (Decision, error) {
			return workflow.CreateHook[Decision](ctx, workflow.HookOption{Token: input.Token}).Next(ctx)
		}),
		workflow.Use(func(ctx context.Context, _ Nap) (string, error) {
			if err := workflow.Sleep(ctx, time.Hour); err != nil {
				return "", err
			}
			return "rested", nil
		}),
	)
	token := os.Getenv("WORKFLOW_HTTP_TOKEN")
	if url := os.Getenv("WORKFLOW_REMOTE_URL"); url != "" {
		executor = worlds.RemoteExecutor{URL: url, Token: token}
	}
	world := testworld.New(executor)
	var options []worlds.HTTPOption
	if token != "" {
		options = append(options, worlds.WithClientToken(token))
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	if err := json.NewEncoder(os.Stdout).Encode(map[string]string{
		"baseURL": "http://" + listener.Addr().String() + worlds.HTTPPrefixPath(""),
		"review":  worlds.WorkflowName[Review](),
		"nap":     worlds.WorkflowName[Nap](),
	}); err != nil {
		log.Fatal(err)
	}
	log.Fatal(http.Serve(listener, worlds.HTTPHandler(world, "", options...)))
}
