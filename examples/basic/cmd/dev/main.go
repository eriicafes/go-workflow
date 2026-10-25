package main

import (
	"cmp"
	"fmt"
	"os"

	"github.com/eriicafes/go-dev"
	"github.com/eriicafes/go-workflow/workflowdev"
)

func main() {
	app := dev.Cmd{
		Dir:        dev.Dir("..", ".."),
		Run:        dev.Package("./cmd/app"),
		Watch:      dev.Values("cmd", "workflows"),
		ServerAddr: ":8080",
	}
	env := cmp.Or(os.Getenv("ENVIRONMENT"), "development")
	// In development, the default ENVIRONMENT, workflows run inside the app,
	// which also serves the local world to the web UI, so the Postgres worker
	// is not started. The app listens on a dev-assigned PORT, so webhook URLs
	// use the stable proxy.
	if env == "development" {
		app.Env = dev.OsEnv(
			dev.Pair("WORKFLOWS_LOCAL_BASE_URL", "http://localhost:8080/v1/workflow"),
		)
		app.Prepare = dev.Hooks(workflowdev.Check("./..."))
		app.Commands = dev.Commands(
			workflowdev.Web("http://localhost:8080/v1/workflow"),
		)
	} else {
		dbURL := cmp.Or(os.Getenv("DATABASE_URL"), "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable")
		app.Commands = dev.Commands(
			dev.Cmd{
				Run: dev.Package("./cmd/worker/pg"),
				Env: dev.OsEnv(
					dev.Pair("WORKFLOW_BASE_URL", "http://localhost:8081/v1/workflow"),
					dev.Pair("DATABASE_URL", dbURL),
				),
				Prepare: dev.Hooks(workflowdev.Check("./...")),
				Watch:   dev.Values("cmd", "workflows"),
				Phase:   dev.Before,
			},
			workflowdev.Web("http://localhost:8081/v1/workflow"),
		)
	}

	session := dev.New()
	task := session.NewTask(app)
	session.Catch(task.Run())

	fmt.Printf("> application listening on %s\n", task.URL())
	session.Catch(session.Wait())
}
