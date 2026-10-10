package main

import (
	"github.com/eriicafes/go-workflow/check"
	"golang.org/x/tools/go/analysis/singlechecker"
)

func main() { singlechecker.Main(check.Analyzer) }
