# Streams

Streams are run-owned sequences of bytes that steps write and readers consume
while the run is still going, for example progress messages, logs, or tokens
from a model. They are stored in the World, so readers can come and go and
read from any offset.

Each run has a default stream and any number of named streams. A stream is
identified by its run ID and name.

## Writing

Write to a stream from a **step** with `workflow.Writer`:

```go
func writeProgress(ctx context.Context, count int) (struct{}, error) {
	writer, release, err := workflow.Writer(ctx, workflow.WriterOption{Name: "progress"})
	if err != nil {
		return struct{}{}, err
	}
	defer release()

	_, err = fmt.Fprintf(writer, "found %d sources\n", count)
	return struct{}{}, err
}
```

Call `release` when the step is done writing so a retry or another step can
continue the stream. Call `writer.Close()` only when the stream is complete.
Closing it ends the stream for all writers and lets readers reach `io.EOF`.

A stream has **one writer at a time**. `Writer` waits while another writer
holds the stream name, and continues once the current writer releases or closes
it.

Omit `WriterOption` to write the run's default stream.

::: tip Streams are I/O
Streams can only be used in step context. `Writer` and `Reader` return
`ErrNotInStep` in workflow context and `ErrNoWorkflow` outside a run. The
[analyzer](/tooling/analyzer) reports stream access outside steps.
:::

## Reading inside a run

`workflow.Reader` reads a stream of the run the step belongs to:

```go
reader, err := workflow.Reader(ctx, workflow.ReaderOption{Name: "progress"})
```

## Reading outside a run

A `Run` handle reads its run's streams from anywhere a Client is available:

```go
run, err := workflow.Get[workflows.GenerateReportResult](ctx, runID)
if err != nil {
	return err
}

// From the beginning.
reader, err := run.Reader(ctx, workflow.ReaderOption{Name: "progress"})

// The last three writes onward.
reader, err = run.Reader(ctx, workflow.ReaderOption{Name: "progress", StartIndex: -3})
```

Each write is one **chunk**, numbered from zero. As in the Workflow SDK,
`StartIndex` picks the chunk to start at, and a negative index counts back from
the end of the stream.

A `StreamReader` is an `io.Reader` that joins chunks into bytes. `Next`
returns one chunk at a time with its `Index`, so a reader can relay whole writes
and remember where it stopped, and `TailIndex` reports the index of the last
chunk, or -1 for an empty stream. A client that reconnects resumes from the
index after the last chunk it received.

Reads **block** until data is written and return `io.EOF` once the stream is
closed.

Streaming to an HTTP response:

```go
mux.HandleFunc("GET /reports/{id}/progress", func(w http.ResponseWriter, r *http.Request) {
	run, err := workflow.Get[workflows.GenerateReportResult](r.Context(), r.PathValue("id"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// A reconnecting client passes the index after the last chunk it saw.
	start, _ := strconv.Atoi(r.URL.Query().Get("startIndex"))
	reader, err := run.Reader(r.Context(), workflow.ReaderOption{Name: "progress", StartIndex: start})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/plain")
	for {
		chunk, err := reader.Next()
		if err != nil {
			return
		}
		w.Write(chunk.Data)
		w.(http.Flusher).Flush()
	}
})
```

`Run.Writer` writes a run's stream from outside the run, with the same
single-writer rules.

## Under the hood

Streams are stored as ordered chunks. A writer holds an exclusive **write
session** on the stream, and each write carries a sequence number so a stale
session or out-of-order write fails with `worlds.ErrStreamLocked`. See the
[`Streamer` interface](/reference/worlds#streamer) for the World contract.
