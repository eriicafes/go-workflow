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
started in an [inline step](/guide/steps#inline-steps), so replay returns the
same run. Its ID is derived from that recorded step, so calling it again never
starts a second child. It is pinned to the deployment executing the parent, and
it carries two attributes:

| Attribute | Value |
| --- | --- |
| `$parentRunId` (`worlds.ParentRunIDAttribute`) | the run that started it |
| `$rootRunId` (`worlds.RootRunIDAttribute`) | the run at the root of the lineage |

A child run is independent: it has its own event log, can run on other
workers, and can outlive its parent.

### Waiting for a child

In workflow context, `Run` methods read or change the run once in an inline
step and replay returns the recorded result, so they never wait. `Run.Value` returns
`workflow.ErrRunNotFinished` for a child that had not finished.

To wait for a child, pass it a hook token and have it resume the hook when it
finishes:

```go
func Parent(ctx context.Context, input workflows.Parent) (workflows.ParentResult, error) {
	hook, err := workflow.CreateHook[string](ctx)
	if err != nil {
		return workflows.ParentResult{}, err
	}
	if owner, err := hook.GetConflict(ctx); err != nil {
		return workflows.ParentResult{}, err
	} else if owner != nil {
		return workflows.ParentResult{}, workflow.ErrHookConflict
	}

	if _, err := workflow.Start(ctx, workflows.Child{Token: hook.Token()}); err != nil {
		return workflows.ParentResult{}, err
	}

	defer hook.Dispose(ctx)
	value, err := hook.Next(ctx)
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
hooks := make([]*workflow.Hook[string], len(input.Items))
for i, item := range input.Items {
    hook, err := workflow.CreateHook[string](ctx)
    if err != nil {
        return Output{}, err
    }
    defer hook.Dispose(ctx)
    if owner, err := hook.GetConflict(ctx); err != nil {
        return Output{}, err
    } else if owner != nil {
        return Output{}, workflow.ErrHookConflict
    }
    if _, err := workflow.Start(ctx, workflows.Child{Token: hook.Token(), Item: item}); err != nil {
        return Output{}, err
    }
    hooks[i] = hook
}
values := make([]string, len(hooks))
for i, hook := range hooks {
    value, err := hook.Next(ctx)
    if err != nil {
        return Output{}, err
    }
    values[i] = value
}
```

### Cancelling a child

A `Run` has its own `Cancel` method. Calling it in workflow context records a durable inline step. A cancelled run stops new work and lets admitted steps finish naturally:

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

A child run has an independent lifetime, even when `Start` receives a cancelled context. Check the cancellation context and call `Cancel` explicitly when the child should stop:

```go
if cancelCtx.Err() != nil {
    if err := child.Cancel(ctx); err != nil {
        return Result{}, err
    }
}
```

This check runs when the workflow reaches it; it does not watch for future cancellation. Steps and timers observe a cancellation context passed to them. The base context and cancellation sources for `CancelAny` are explicit; no context values are merged.
