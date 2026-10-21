package check_test

import (
	"testing"

	"github.com/eriicafes/go-workflow/check"
	"golang.org/x/tools/go/analysis/analysistest"
)

func TestAnalyzer(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), check.Analyzer, "flows")
}
