package flows

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/examples/basic/steps"
	"github.com/eriicafes/go-workflow/examples/basic/workflowdefs"
)

func GenerateReport(ctx context.Context, input workflowdefs.GenerateReport) (workflowdefs.GenerateReportResult, error) {
	sourceGroups, err := workflow.All(ctx, steps.FindSources, []string{
		input.Topic,
		input.Topic + " examples",
	})
	if err != nil {
		return workflowdefs.GenerateReportResult{}, err
	}
	var sources []string
	for _, group := range sourceGroups {
		sources = append(sources, group...)
	}

	fmt.Fprintf(workflow.Writer(ctx), "found %d sources\n", len(sources))

	return workflowdefs.GenerateReportResult{
		Summary: strings.Join(sources, ", "),
	}, nil
}

func PublishReport(ctx context.Context, input workflowdefs.PublishReport) (workflowdefs.PublishReportResult, error) {
	reportRun, err := workflow.Start(ctx, workflowdefs.GenerateReport{Topic: input.Topic})
	if err != nil {
		return workflowdefs.PublishReportResult{}, err
	}
	report, err := reportRun.Value(ctx)
	if err != nil {
		return workflowdefs.PublishReportResult{}, err
	}
	if err := workflow.Sleep(ctx, time.Millisecond); err != nil {
		return workflowdefs.PublishReportResult{}, err
	}
	location, err := workflow.Step(ctx, steps.PublishReport, report.Summary)
	if err != nil {
		return workflowdefs.PublishReportResult{}, err
	}
	return workflowdefs.PublishReportResult{Report: report, Location: location}, nil
}
