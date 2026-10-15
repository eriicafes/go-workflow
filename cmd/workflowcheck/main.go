// Command workflowcheck runs the check analyzer as a go vet tool.
// Run it with the command below.
//
//	go vet -vettool=$(which workflowcheck) ./...
package main

import (
	"github.com/eriicafes/go-workflow/check"
	"golang.org/x/tools/go/analysis/singlechecker"
)

func main() { singlechecker.Main(check.Analyzer) }
