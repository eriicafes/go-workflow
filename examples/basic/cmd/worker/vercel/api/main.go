// api runs the app-facing Vercel workflow API.
package main

import (
	"cmp"
	"log"
	"net/http"
	"os"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/examples/basic/flows"
	"github.com/eriicafes/go-workflow/worlds/vercel"
)

func main() {
	world, err := vercelworld.New(vercelworld.Config{
		RedisURL:  os.Getenv("REDIS_URL"),
		QueueURL:  os.Getenv("VERCEL_QUEUE_URL"),
		OIDCToken: os.Getenv("VERCEL_OIDC_TOKEN"),
		Topic:     os.Getenv("VERCEL_WORKFLOW_QUEUE_TOPIC"),
	})
	if err != nil {
		log.Fatal(err)
	}

	address := cmp.Or(os.Getenv("PORT"), ":8081")
	log.Printf("workflow API listening on %s", address)
	runners := workflow.Runners(
		workflow.Use(flows.GenerateReport),
		workflow.Use(flows.PublishReport),
	)
	log.Fatal(http.ListenAndServe(address, vercelworld.HTTP(world, runners)))
}
