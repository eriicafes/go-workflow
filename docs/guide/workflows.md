# Workflows and Runners

## Workflows

A workflow is a struct type that embeds `workflow.Struct[Out]`, where `Out` is
the workflow's output type. `Struct` has no fields, so it adds nothing to the
workflow's input.

```go
package workflows

type GenerateReport struct {
	workflow.Struct[GenerateReportResult]
	Topic string
}

type GenerateReportResult struct {
	Summary string
}
```

`Struct` gives the workflow a `Workflow(Out)` method, which is what marks
the type as a workflow. A struct can declare the method itself instead:

```go
type GenerateReport struct {
	Topic string
}

func (GenerateReport) Workflow(GenerateReportResult) {}
```

The struct's package path and type name form the **workflow name**, such as
`example.com/app/workflows.GenerateReport`. Runs record this name to select
their runner. Each value of the struct supplies a run's input. Inputs and
outputs use JSON, so give them exported fields and JSON-friendly types.

Keep workflow types in a `workflows` package and their runners in a `flows`
package inside it, `workflows/flows`. Request handlers import only
`workflows`, so they can [start runs](/guide/runs) without importing runners
or their dependencies.

## Runners

A runner is a named, package-level function that executes a workflow, kept in
`workflows/flows`. Its input and output types match the workflow declaration:

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

## Suggested layout

```
workflows/           workflow types: imported by the application and flows
  reports.go
  flows/             runners and their steps: imported only by workers
    generate_report.go
    publish_report.go
cmd/
  app/             starts runs with a Client
  worker/          opens a World with the runners
```

## Workflow metadata

`workflow.GetWorkflowMetadata` returns information about the run that the
context belongs to, in both workflow and step context.

```go
meta := workflow.GetWorkflowMetadata(ctx)
log.Printf("workflow %s, run %s, started %s", meta.Name, meta.RunID, meta.StartedAt)
```

Outside a run it returns the zero `WorkflowMetadata`.
