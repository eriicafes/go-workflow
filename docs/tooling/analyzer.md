# Workflow Analyzer

The `check` package provides a static analyzer that reports runners that cannot
replay deterministically and calls that misuse the workflow API. The
`workflowcheck` command runs it as a `go vet` tool.

## Install and run

```sh
go install github.com/eriicafes/go-workflow/cmd/workflowcheck@latest
go vet -vettool=$(which workflowcheck) ./...
```

Add it to CI next to `go vet` and your tests. With
[go-dev](/tooling/dev), `workflowdev.Check` runs it before every start or
reload.

## What it reports

### Registration

Every function passed to `workflow.Use` must be a valid runner.

| Diagnostic | Cause |
| --- | --- |
| `workflow.Use requires one workflow function` | `Use` called with the wrong number of arguments. |
| `workflow.Use requires a named package-level workflow function` | A closure or method value passed to `Use`. |
| `workflow function must be func(context.Context, NamedInput) (Output, error)` | Wrong runner signature. |
| `workflow input must be a named package type` | Input is not a named type. |
| `<Input> must declare Workflow(<Output>)` | Input type has no matching `Workflow` method. |

### Runner bodies

| Diagnostic | Fix |
| --- | --- |
| `workflow body must not start a goroutine, use workflow.Background instead` | Start concurrent work with [`Background`](/guide/concurrency). |
| `workflow body must not select on channels` | Use the `Select` functions on promises. |
| `workflow body must not range over a map` | Iterate a sorted slice of keys. |
| `workflow body calls time.Now, use workflow.Now instead` | Use [`workflow.Now`](/guide/sleep#reading-the-time). |
| `workflow body calls <pkg.Func>, use workflow.Step or workflow.Background instead` | Move I/O and randomness into a step. Reported for calls into `os`, `io`, `net/http`, `database/sql`, `crypto/rand`, `math/rand`, and packages whose path contains `uuid`. |
| `workflow stream access and webhook responses require a durable step` | Move [stream](/guide/streams) access into a step. |

The analyzer also rejects the `context` package's cancellation and deadline
constructors in workflow bodies. Use [durable cancellation](/guide/cancellation)
instead. Standard context constructors remain valid inside steps. `Then`
callbacks follow the same workflow restrictions.

### Steps

| Diagnostic | Fix |
| --- | --- |
| `durable step must be a named function or inline closure, not a wrapper or factory` | Pass a named function (or a literal closure) to `Step`, `Background`, and friends, not the result of a call. |
| `durable closure captures <name>, pass workflow-relevant state through its explicit input instead` | Pass the value as the step's input. |

## Example

```go
func Bad(ctx context.Context, input workflows.Report) (workflows.ReportResult, error) {
	started := time.Now()       // use workflow.Now
	go notify(input)            // use workflow.Background
	for k := range input.Tags { // map iteration
		_ = k
	}

	body, _ := os.ReadFile("template.txt") // I/O outside a step
	out, err := workflow.Step(ctx, func(ctx context.Context, _ struct{}) (string, error) {
		return render(body, started), nil // captures body and started
	}, struct{}{})
	return workflows.ReportResult{Text: out}, err
}
```

## Using the analyzer as a library

`check.Analyzer` is a standard `*analysis.Analyzer`, so it plugs into
`multichecker`, `singlechecker`, or `analysistest`:

```go
import (
	"github.com/eriicafes/go-workflow/check"
	"golang.org/x/tools/go/analysis/singlechecker"
)

func main() { singlechecker.Main(check.Analyzer) }
```
