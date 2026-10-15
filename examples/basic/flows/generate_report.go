package flows

import (
	"context"
	"fmt"
	"strings"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/examples/basic/workflows"
)

func GenerateReport(ctx context.Context, input workflows.GenerateReport) (workflows.GenerateReportResult, error) {
	sourceGroups, err := workflow.All(ctx, findSources, []string{
		input.Topic,
		input.Topic + " examples",
	})
	if err != nil {
		return workflows.GenerateReportResult{}, err
	}
	var sources []string
	for _, group := range sourceGroups {
		sources = append(sources, group...)
	}

	if _, err := workflow.Step(ctx, writeReportProgress, len(sources)); err != nil {
		return workflows.GenerateReportResult{}, err
	}

	return workflows.GenerateReportResult{
		Summary: strings.Join(sources, ", "),
	}, nil
}

func findSources(ctx context.Context, topic string) ([]string, error) {
	return []string{"source for " + topic}, nil
}

func writeReportProgress(ctx context.Context, sourceCount int) (struct{}, error) {
	writer, release, err := workflow.Writer(ctx)
	if err != nil {
		return struct{}{}, err
	}
	defer release()
	_, err = fmt.Fprintf(writer, "found %d sources\n", sourceCount)
	return struct{}{}, err
}
