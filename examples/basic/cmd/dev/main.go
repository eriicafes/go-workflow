package main

import (
	"fmt"

	"github.com/eriicafes/go-dev"
	"github.com/eriicafes/go-workflow/workflowdev"
)

func main() {
	session := dev.New()
	task := session.NewTask(dev.Cmd{
		Dir:        dev.Dir("..", ".."),
		Run:        dev.Package("./cmd/app"),
		Watch:      dev.Values("cmd", "flows", "workflows"),
		ServerAddr: ":8080",
		Commands: dev.Commands(
			dev.Cmd{
				Run:     dev.Package("./cmd/worker/pg"),
				Prepare: dev.Hooks(workflowdev.Check("./...")),
				Watch:   dev.Values("cmd", "flows", "workflows"),
				Phase:   dev.Before,
			},
			workflowdev.Web("http://localhost:8081/v1/workflow"),
		),
	})
	session.Catch(task.Run())

	fmt.Printf("basic application listening on %s\n", task.URL())
	session.Catch(session.Wait())
}
