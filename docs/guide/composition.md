# Composing Workflows

A runner can use another workflow in two ways, as in the Workflow SDK.

## Calling a runner directly

A runner is an ordinary function, so another runner can call it. Its steps then
run **in the current run**, as if they were written inline.

```go
func PublishReport(ctx context.Context, input workflows.PublishReport) (workflows.PublishReportResult, error) {
	report, err := GenerateReport(ctx, workflows.GenerateReport{Topic: input.Topic})
	if err != nil {
		return workflows.PublishReportResult{}, err
	}

	location, err := workflow.Step(ctx, storeReport, report.Summary)
	if err != nil {
		return workflows.PublishReportResult{}, err
	}

	return workflows.PublishReportResult{Report: report, Location: location}, nil
}
```

This is the simplest option: one run, one event log, one result.

## Starting a child run

`workflow.Start` in workflow context starts a **child run**. The child is
started in a step, so replay returns the same run, and it carries two
attributes:

| Attribute | Value |
| --- | --- |
| `$parentRunId` (`worlds.ParentRunIDAttribute`) | the run that started it |
| `$rootRunId` (`worlds.RootRunIDAttribute`) | the run at the root of the lineage |

A child run is independent: it has its own event log, can run on other
workers, and can outlive its parent.

### Waiting for a child

In workflow context, `Run` methods read or change the run once in a step and
replay returns the recorded result, so they never wait. `Run.Await` returns
`workflow.ErrRunNotFinished` for a child that had not finished.

To wait for a child, pass it a hook token and have it resume the hook when it
finishes:

```go
func Parent(ctx context.Context, input workflows.Parent) (workflows.ParentResult, error) {
	hook, err := workflow.CreateHook[string](ctx)
	if err != nil {
		return workflows.ParentResult{}, err
	}

	if _, err := workflow.Start(ctx, workflows.Child{Token: hook.Token()}); err != nil {
		return workflows.ParentResult{}, err
	}

	value, err := hook.Await(ctx)
	return workflows.ParentResult{Value: value}, err
}

func Child(ctx context.Context, input workflows.Child) (workflows.ChildResult, error) {
	value, err := workflow.Step(ctx, work, input)
	if err != nil {
		return workflows.ChildResult{}, err
	}

	_, err = workflow.Step(ctx, resumeParent, completion{Token: input.Token, Value: value})
	return workflows.ChildResult{Value: value}, err
}

type completion struct {
	Token string
	Value string
}

func resumeParent(ctx context.Context, c completion) (struct{}, error) {
	return struct{}{}, workflow.ResumeHook(ctx, c.Token, c.Value)
}
```

`ResumeHook` works in step context, using the World that records the run.

### Fan-out to child runs

```go
hooks := make([]workflow.Promise[string], len(input.Items))
for i, item := range input.Items {
	hook, err := workflow.CreateHook[string](ctx)
	if err != nil {
		return Result{}, err
	}

	if _, err := workflow.Start(ctx, workflows.Child{Token: hook.Token(), Item: item}); err != nil {
		return Result{}, err
	}

	hooks[i] = hook
}
values, err := workflow.Join(ctx, hooks...)
```

### Cancelling a child

A `Run` implements `Promise`, and cancelling it cancels the child run:

```go
child, err := workflow.Start(ctx, workflows.Child{Token: hook.Token()})
if err != nil {
	return Result{}, err
}

// ...
if err := child.Cancel(ctx); err != nil {
	return Result{}, err
}
```
