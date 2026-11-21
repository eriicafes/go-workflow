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
	writer, err := workflow.Writer(ctx, workflow.WriterOption{Name: "progress"})
	if err != nil {
		return struct{}{}, err
	}
	defer writer.Close()

	_, err = fmt.Fprintf(writer, "found %d sources\n", count)
	return struct{}{}, err
}
```

`Writer` returns a `workflow.StreamWriter`, an `io.WriteCloser` with two more
methods:

| Method | Description |
| --- | --- |
| `Write` | Buffers one chunk and returns. The writer stores buffered chunks together in the background. |
| `Flush` | Waits until every buffered chunk is stored. |
| `Close` | Stores what is buffered and gives up the writer, leaving the stream open for other writers. |
| `CloseStream` | Stores what is buffered, then closes the stream for every writer, so readers reach `io.EOF`. |

A failed store is reported by a later `Write`, `Flush`, `Close`, or
`CloseStream`. When the step function returns, its writers are closed before
the step's result is recorded, so a completed step's chunks are stored and a
failed store fails the step.

As in the Workflow SDK, writers **do not wait for each other**. Several steps,
or retries of one, can write the same stream at once: each writer's chunks stay
in order, and different writers' chunks interleave. Call `CloseStream` once the
stream is complete, from the writer that finishes it.

Omit `WriterOption` to write the run's default stream.

::: tip Streams are I/O
Inside a run, streams can only be used in step context. `Writer` and `Reader` return
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

`Run.Writer` writes a run's stream from outside the run, alongside its other
writers.

## Under the hood

Streams are stored as ordered chunks, with no exclusive writers. A writer
stores its buffered chunks through the World's
[`StreamWriter`](/reference/worlds#streamer): over a socket held for the
writer's lifetime where the World offers one, as the HTTP client and the Vercel
World do, and otherwise one request per batch of chunks.

Streams are stored under the Workflow SDK's names: a run's default stream is
`strm_<ULID>_user`, from its `wrun_<ULID>` run ID, and a named stream adds `_`
and its name in base64url, so `progress` is `strm_<ULID>_user_cHJvZ3Jlc3M`.

Each chunk is stored as the Workflow SDK stores a value of a stream: a frame
holding the chunk as a `Uint8Array` payload, encrypted with the run's key when
its payloads are, or sealed to the run's public key when written from outside
it with `Run.Writer`. SDK tools such as the Workflow web UI read Go streams as
they read JS ones. See [Data Format and Encryption](/guide/encryption).
