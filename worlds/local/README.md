# Local world

`localworld` is a port of the Workflow SDK's `@workflow/world-local`, for
development and single-machine deployments. It stores runs, events, steps,
waits, hooks, and stream chunks in a SQLite database, `world.db` in its data
directory, and delivers queued work in-process from an in-memory queue.

```go
runners := core.Runners(workflow.Use(flows.GenerateReport))

world, err := localworld.Open(localworld.Config{Dir: ".workflows"}, runners)
if err != nil {
	return err
}
defer world.Close()

if err := world.Prepare(ctx); err != nil {
	return err
}

client := workflow.NewClient(world)
```

`Prepare` opens the database, begins delivering queued work, and queues the
active runs of the World's tag again, since a restart lost their queued
messages. Several processes may share a data directory: the database
serializes their writes.

See [Local World](https://go-workflow.vercel.app/worlds/local) for the
configuration, the schema and how it differs from world-local's, tags, the
queue, and restart recovery.
