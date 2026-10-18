---
layout: home

hero:
  name: go-workflow
  text: Durable workflows for Go
  tagline: Write workflows as ordinary Go functions. Steps are recorded and replayed, so a run survives crashes, restarts, and deploys and resumes where it left off.
  image:
    src: /logo.svg
    alt: go-workflow
  actions:
    - theme: brand
      text: Get Started
      link: /guide/getting-started
    - theme: alt
      text: What is go-workflow?
      link: /guide/introduction
    - theme: alt
      text: GitHub
      link: https://github.com/eriicafes/go-workflow

features:
  - icon: 🧩
    title: Plain Go functions
    details: A workflow is a struct type and a runner is a func(context.Context, In) (Out, error). No generated code, no workflow interface, no SDK-specific context.
  - icon: 🔁
    title: Replay-based durability
    details: Every run is an append-only event log. Recorded steps return their results on replay instead of running again, so a run picks up exactly where it stopped.
  - icon: ⏱️
    title: Sleep for weeks
    details: Sleep, timers, timeouts, hooks, and webhooks suspend a run without holding a process. The run resumes when the World delivers it again.
  - icon: 🌍
    title: Pluggable Worlds
    details: Run on local JSON files, PostgreSQL, Redis with Vercel Queues, or DynamoDB with SQS. Every World passes the same conformance suite.
  - icon: 🛡️
    title: Static analysis
    details: The workflowcheck vet tool catches goroutines, select, map iteration, time.Now, and I/O outside steps before they break replay.
  - icon: 🖥️
    title: Workflow SDK compatible
    details: Runs follow the Workflow SDK's event model, so the Workflow SDK web UI can inspect your Go runs, steps, hooks, and streams.
---

## At a glance

Define the workflow as a struct. Its fields are the run's input, and the
`Workflow` method declares the output type.

```go
package workflows

type GenerateReport struct{ Topic string }
type GenerateReportResult struct{ Summary string }

func (GenerateReport) Workflow(GenerateReportResult) {}
```

Write the runner and its steps. The runner queries sources in a step, sleeps
for a day without holding a process, then returns the summary.

::: tip Keep runners in their own package
Runners and steps live in a separate `flows` package that only the worker
imports. The application imports `workflows` alone, so runner code and its
dependencies are never built into the app.
:::

```go
package flows

var db *sql.DB

func GenerateReport(ctx context.Context, input workflows.GenerateReport) (workflows.GenerateReportResult, error) {
	sources, err := workflow.Step(ctx, findSources, input.Topic)
	if err != nil {
		return workflows.GenerateReportResult{}, err
	}

	if err := workflow.Sleep(ctx, 24*time.Hour); err != nil {
		return workflows.GenerateReportResult{}, err
	}

	return workflows.GenerateReportResult{Summary: strings.Join(sources, ", ")}, nil
}

func findSources(ctx context.Context, topic string) ([]string, error) {
	rows, err := db.QueryContext(ctx, "SELECT url FROM sources WHERE topic = $1 ORDER BY rank", topic)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var sources []string
	for rows.Next() {
		var url string
		if err := rows.Scan(&url); err != nil {
			return nil, err
		}

		sources = append(sources, url)
	}

	return sources, rows.Err()
}
```

Start runs from the application. The handler needs only the workflow type and
a Client, which `workflow.Middleware` puts in every request context. It
responds with the run ID without waiting for the run.

```go
package main

func main() {
	client := workflow.NewHTTPClient("http://workflow-worker:8081/v1/workflow")

	mux := http.NewServeMux()
	mux.HandleFunc("POST /reports", func(w http.ResponseWriter, r *http.Request) {
		run, err := workflow.Start(r.Context(), workflows.GenerateReport{Topic: r.FormValue("topic")})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]string{"runId": run.ID()})
	})
	log.Fatal(http.ListenAndServe(":8080", workflow.Middleware(mux, client)))
}
```
