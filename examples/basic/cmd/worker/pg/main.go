// pg runs the Postgres-backed example worker.
package main

import (
	"cmp"
	"context"
	"log"
	"net/http"
	"os"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/examples/basic/flows"
	"github.com/eriicafes/go-workflow/worlds/postgres"
)

func main() {
	ctx := context.Background()
	world, err := postgresworld.Open(
		ctx,
		postgresworld.Config{URL: os.Getenv("DATABASE_URL")},
	)
	if err != nil {
		log.Fatal(err)
	}
	defer world.Close()

	runners := workflow.Runners(
		workflow.Use(flows.GenerateReport),
		workflow.Use(flows.PublishReport),
	)
	go func() {
		if err := postgresworld.Run(ctx, world, runners); err != nil {
			log.Printf("workflow worker stopped: %v", err)
		}
	}()

	address := cmp.Or(os.Getenv("WORKFLOW_ADDR"), ":8081")
	log.Printf("workflow worker listening on %s", address)
	log.Fatal(http.ListenAndServe(address, postgresworld.HTTP(world)))
}
