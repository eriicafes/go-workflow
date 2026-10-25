// dev runs the chat API and the web client's Vite server for development,
// rebuilding the API when its Go sources change. Install the client's
// dependencies with pnpm install in web first.
package main

import (
	"fmt"

	"github.com/eriicafes/go-dev"
	"github.com/eriicafes/go-workflow/workflowdev"
)

func main() {
	session := dev.New()
	// The API holds the local world's lock on .workflows, so it runs without
	// a go-dev proxy: a reload stops the old process before the new one
	// starts. Vite proxies /api to it.
	session.Catch(session.Run(dev.Cmd{
		Dir:     dev.Dir("..", ".."),
		Run:     dev.Package("./cmd/server"),
		Env:     dev.OsEnv(dev.Pair("PORT", "8080")),
		Prepare: dev.Hooks(workflowdev.Check("./...")),
		Watch:   dev.Values("cmd/server", "workflows", "../../workflowai"),
		Commands: dev.Commands(
			dev.Cmd{
				Dir:   "web",
				Run:   dev.Binary("pnpm"),
				Args:  dev.Values("run", "dev"),
				Phase: dev.Before,
			},
		),
	}))

	fmt.Println("> chat at http://localhost:5173")
	session.Catch(session.Wait())
}
