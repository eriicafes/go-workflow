# Getting Started

This page builds a small workflow and runs it on the local World, which stores
runs as JSON files and needs no database or worker.

## Install

```sh
go get github.com/eriicafes/go-workflow
```

go-workflow requires Go 1.25 or later.

## 1. Define a workflow

A workflow is a struct type whose `Workflow` method declares its output. Keep
workflows in their own package so request handlers can start them without
importing their runners.

```go
// workflows/reports.go
package workflows

type GenerateReport struct {
	Topic string
}

type GenerateReportResult struct {
	Summary string
}

func (GenerateReport) Workflow(GenerateReportResult) {}
```

The workflow name is the package path and type name, for example
`example.com/app/workflows.GenerateReport`.

## 2. Write a runner and its steps

A runner executes the workflow. It is a plain function that takes the workflow
as input and calls steps, which do the real work. Keep each runner in a file
with the steps it calls.

```go
// flows/generate_report.go
package flows

import (
	"context"
	"strings"

	"example.com/app/workflows"
	"github.com/eriicafes/go-workflow"
)

func GenerateReport(ctx context.Context, input workflows.GenerateReport) (workflows.GenerateReportResult, error) {
	sources, err := workflow.Step(ctx, findSources, input.Topic)
	if err != nil {
		return workflows.GenerateReportResult{}, err
	}

	return workflows.GenerateReportResult{Summary: strings.Join(sources, ", ")}, nil
}

func findSources(ctx context.Context, topic string) ([]string, error) {
	return []string{"source for " + topic}, nil
}
```

## 3. Register runners

`workflow.Use` pairs a runner with its workflow, and `worlds.Runners` collects
them into a set the runtime executes.

```go
runners := worlds.Runners(
	workflow.Use(flows.GenerateReport),
)
```

## 4. Open a World and start a run

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/worlds"
	localworld "github.com/eriicafes/go-workflow/worlds/local"

	"example.com/app/flows"
	"example.com/app/workflows"
)

func main() {
	ctx := context.Background()

	runners := worlds.Runners(workflow.Use(flows.GenerateReport))

	world, err := localworld.Open(localworld.Config{Dir: ".workflows"}, runners)
	if err != nil {
		log.Fatal(err)
	}
	defer world.Close()

	if err := world.Prepare(ctx); err != nil {
		log.Fatal(err)
	}

	ctx = workflow.NewContext(ctx, workflow.NewClient(world))
	run, err := workflow.Start(ctx, workflows.GenerateReport{Topic: "workflows"})
	if err != nil {
		log.Fatal(err)
	}

	result, err := run.Value(ctx)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(result.Summary)
}
```

`workflow.Start` needs only the workflow type and a Client in the context. It
returns a [`Run`](/guide/runs) handle without waiting, and `Value` blocks until
the run finishes.

## 5. Check your runners

The workflow analyzer reports code in runners that would break replay, such as
goroutines, `time.Now`, or I/O outside steps.

The easiest way to run it is with [go-dev](https://github.com/eriicafes/go-dev),
which checks your runners before every start or reload of your app. Add
`workflowdev.Check` as a `Prepare` hook:

```go
// cmd/dev/main.go
package main

import (
	"github.com/eriicafes/go-dev"
	"github.com/eriicafes/go-workflow/workflowdev"
)

func main() {
	session := dev.New()
	err := session.Run(dev.Cmd{
		Run:     dev.Package("./cmd/app"),
		Prepare: dev.Hooks(workflowdev.Check("./...")),
		Watch:   dev.Values("cmd", "flows", "workflows"),
	})
	session.Catch(err)
	session.Catch(session.Wait())
}
```

```sh
go run ./cmd/dev
```

[Development](/guide/development) adds the Workflow SDK web UI to the same
setup.

Without go-dev, for example in CI, install the analyzer and run it as a vet
tool:

```sh
go install github.com/eriicafes/go-workflow/cmd/workflowcheck@latest
go vet -vettool=$(which workflowcheck) ./...
```

## Next steps

- Learn [how replay works](/guide/replay) and why runners must be deterministic.
- Add [retries and error handling](/guide/errors) to steps.
- Run steps [concurrently](/guide/concurrency), [sleep](/guide/sleep), or wait
  for [hooks](/guide/hooks).
- Set up the [development loop](/guide/development) with go-dev, the analyzer, and the web UI.
- Split your [application and worker](/guide/deployment) and pick a
  production [World](/worlds/).
- Browse [`examples/basic`](https://github.com/eriicafes/go-workflow/tree/main/examples/basic),
  a full application with a worker for each World.
