// api runs the app-facing Vercel workflow API.
package main

import (
	"cmp"
	"log"
	"net/http"
	"os"

	vercelworld "github.com/eriicafes/go-workflow/worlds/vercel"
)

func main() {
	world, err := vercelworld.New(vercelworld.Config{
		BaseURL: os.Getenv("WORKFLOW_BASE_URL"),
		Topic:   os.Getenv("VERCEL_WORKFLOW_QUEUE_TOPIC"),
	})
	if err != nil {
		log.Fatal(err)
	}

	address := cmp.Or(os.Getenv("PORT"), ":8081")
	log.Printf("workflow API listening on %s", address)
	log.Fatal(http.ListenAndServe(address, vercelworld.HTTP(world)))
}
