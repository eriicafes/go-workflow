# go-dev Integration

The `workflowdev` package integrates go-workflow with
[go-dev](https://github.com/eriicafes/go-dev), a Go development runner with file
watching and live reload.

```go
import "github.com/eriicafes/go-workflow/workflowdev"
```

## Web

`workflowdev.Web(baseURL)` returns a go-dev command that runs the
[Workflow SDK web UI](/tooling/web-ui) through `npx`, connected to the World
served by `HTTP(world)` at `baseURL`.

```go
workflowdev.Web("http://localhost:8080/v1/workflow")
```

The web UI needs the Workflow SDK CLI and the go-workflow World adapter,
installed where the command runs: the parent command's `Dir`, or the directory
go-dev runs from when `Dir` is empty:

```sh
npm install --save-dev workflow go-workflow
```

## Check

`workflowdev.Check` returns a go-dev hook that runs the
[analyzer](/tooling/analyzer) before every start or reload. Package patterns are
relative to `dev.Cmd.Dir`, and default to `./...`.

```go
dev.Cmd{
	Run:     dev.Package("./cmd/app"),
	Prepare: dev.Hooks(workflowdev.Check("./...")),
}
```

## Full example

This runs an application on `:8080` that opens the World, runs the worker, and
mounts `HTTP(world)` under `/v1/workflow/`, along with the web UI connected to
it. It checks runners and reloads on changes to `cmd` and
`workflows`:

```go
package main

import (
	"github.com/eriicafes/go-dev"
	"github.com/eriicafes/go-workflow/workflowdev"
)

func main() {
	session := dev.New()
	err := session.Run(dev.Cmd{
		Run:        dev.Package("./cmd/app"),
		Prepare:    dev.Hooks(workflowdev.Check("./...")),
		Watch:      dev.Values("cmd", "workflows"),
		ServerAddr: ":8080",
		Commands: dev.Commands(
			workflowdev.Web("http://localhost:8080/v1/workflow"),
		),
	})
	session.Catch(err)
	session.Catch(session.Wait())
}
```

[Development](/guide/development) shows the application side.
