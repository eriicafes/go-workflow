// queue runs the Vercel Queue consumer for the basic workflow worker.
package main

import (
	"cmp"
	"log"
	"net/http"
	"os"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/examples/basic/workflows/flows"
	"github.com/eriicafes/go-workflow/worlds"
	vercelworld "github.com/eriicafes/go-workflow/worlds/vercel"
)

func main() {
	world, err := vercelworld.New(vercelworld.Config{
		BaseURL:        os.Getenv("WORKFLOW_BASE_URL"),
		RedisURL:       os.Getenv("REDIS_URL"),
		RedisRESTURL:   os.Getenv("UPSTASH_REDIS_REST_URL"),
		RedisRESTToken: os.Getenv("UPSTASH_REDIS_REST_TOKEN"),
		QueueURL:       os.Getenv("VERCEL_QUEUE_URL"),
		Topic:          os.Getenv("VERCEL_WORKFLOW_QUEUE_TOPIC"),
		OIDCToken:      vercelworld.OIDCTokenFromEnv(vercelworld.DefaultOIDCTokenEnv),
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
