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
      text: Learn How It Works
      link: /guide/introduction

features:
  - icon: 🧩
    title: Plain Go functions
    details: A workflow is a struct type and a runner is an ordinary function that takes its input and returns its output. No generated code, no workflow interface, no SDK-specific context.
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
