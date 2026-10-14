// queue runs the Vercel Queue consumer for the basic workflow worker.
package main

import (
	"cmp"
	"log"
	"net/http"
	"os"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/examples/basic/flows"
	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/vercel"
)

func main() {
	world, err := vercelworld.New(vercelworld.Config{
		BaseURL:   os.Getenv("WORKFLOW_BASE_URL"),
		RedisURL:  os.Getenv("REDIS_URL"),
		QueueURL:  os.Getenv("VERCEL_QUEUE_URL"),
		OIDCToken: os.Getenv("VERCEL_OIDC_TOKEN"),
		Topic:     os.Getenv("VERCEL_WORKFLOW_QUEUE_TOPIC"),
	})
	if err != nil {
		log.Fatal(err)
	}

	runners := worlds.Runners(
		workflow.Use(flows.GenerateReport),
		workflow.Use(flows.PublishReport),
	)

	address := cmp.Or(os.Getenv("PORT"), ":8081")
	log.Printf("workflow queue worker listening on %s", address)
	log.Fatal(http.ListenAndServe(address, vercelworld.Queue(world, runners)))
}
