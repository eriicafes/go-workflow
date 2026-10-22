---
layout: home

hero:
  name: go-workflow
  text: Durable workflows for Go
  tagline: Write plain Go code that suspends, resumes, and keeps its state across retries, sleeps, and deploys.
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
  - icon: 🌍
    title: Run anywhere, no lock-in
    details: Pick a World for local files, PostgreSQL, Redis with Vercel Queues, or DynamoDB with SQS. Runs sleep without holding a process, and your workflows stay the same on every World.
  - icon: 🖥️
    title: Inspect every run
    details: Runs follow the Workflow SDK's event model, so its web UI shows every Go run, step, hook, and stream.
  - icon: 🛡️
    title: Catch replay bugs early
    details: The workflowcheck vet tool flags goroutines, select, map iteration, time.Now, and common I/O calls in runners before they reach production.
---
