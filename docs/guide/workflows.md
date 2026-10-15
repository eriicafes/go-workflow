# Workflows and Runners

## Workflows

A workflow is a struct type with a `Workflow` method that takes the output type
as its only parameter. The method body is empty: it exists so the type system
knows the workflow's output.

```go
package workflows

type GenerateReport struct {
	Topic string
}

type GenerateReportResult struct {
	Summary string
}

func (GenerateReport) Workflow(GenerateReportResult) {}
```

- The **workflow name** is the package path and type name of the struct, for
  example `example.com/app/workflows.GenerateReport`. Runs record this name to
  select their runner.
- A **value** of the struct is the input of a run. Inputs and outputs are
  encoded as JSON, so use exported fields and JSON-friendly types.
- The workflow type is all an application needs to [start a run](/guide/runs).
  Keep workflows in their own package, separate from runners, so request
  handlers can start runs without importing runner code or its dependencies.

## Runners

A runner executes a workflow. It is a named, package-level function with this
shape:

```go
func(ctx context.Context, input In) (Out, error)
```

where `In` is a workflow whose `Workflow` method takes `Out`.

```go
package flows

func GenerateReport(ctx context.Context, input workflows.GenerateReport) (workflows.GenerateReportResult, error) {
	sources, err := workflow.Step(ctx, findSources, input.Topic)
	if err != nil {
		return workflows.GenerateReportResult{}, err
	}

	return workflows.GenerateReportResult{Summary: strings.Join(sources, ", ")}, nil
}
```

The runner's code runs in **workflow context**. It replays on every delivery,
so it must be deterministic. See [How Replay Works](/guide/replay).

Returning an error fails the run. The run's error is recorded as a
[`*workflow.Error`](/guide/errors), and any [rollbacks](/guide/rollbacks)
registered so far run first.

## Registering runners

`workflow.Use` registers a runner for its workflow and returns a
`worlds.Runner`. `worlds.Runners` collects runners into a `worlds.RunnerSet`,
which you pass to the World or worker that executes deliveries.

```go
runners := worlds.Runners(
	workflow.Use(flows.GenerateReport),
	workflow.Use(flows.PublishReport),
)
```

Only processes that execute deliveries need runners. An application that only
starts and reads runs needs the workflow types and a [Client](/guide/runs#clients).

A run that names a workflow with no registered runner fails with
`worlds.ErrWorkflowNotFound`.

## Workflow metadata

`workflow.GetWorkflowMetadata` returns information about the run that the
context belongs to, in both workflow and step context.

```go
meta := workflow.GetWorkflowMetadata(ctx)
meta.Name      // workflow name
meta.RunID     // run ID
meta.StartedAt // when the run started
```

Outside a run it returns the zero `WorkflowMetadata`.

## Suggested layout

```
workflows/         workflow types: imported by the application and flows
  reports.go
flows/             runners and their steps: imported only by workers
  generate_report.go
  publish_report.go
cmd/
  app/             starts runs with a Client
  worker/          opens a World with the runners
```
