# Local world

`localworld.Open(".workflow-data", runners)` stores workflow runs, commands, and events on
disk, so local work survives a server restart. Register workflow runners before
calling `Start`; recovery and dispatch begin only at `Start`.

The local world intentionally supports one developer process for each data
directory. It uses an exclusive lock and is not a shared or production worker
backend. Use the Postgres, AWS, or Vercel worlds for shared or production
execution.

The directory is durable until it is deliberately removed with `world.Clear`
or `rm -rf .workflow-data`. Development UIs should use the normal workflow
client/server API and must not read these records directly.

```go
runners := workflow.Runners(workflow.Use(flows.GenerateReport))

world, err := localworld.Open(".workflow-data", runners)
if err != nil {
	return err
}
defer world.Close()

if err := world.Start(ctx); err != nil {
	return err
}
```

`Open` owns and closes its filesystem root. `New(root)` borrows a caller-owned
`*os.Root`; the caller closes that root.
