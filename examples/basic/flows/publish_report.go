package flows

import (
	"context"
	"time"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/examples/basic/workflows"
)

func PublishReport(ctx context.Context, input workflows.PublishReport) (workflows.PublishReportResult, error) {
	// Calling the GenerateReport runner directly runs its steps in this run.
	report, err := GenerateReport(ctx, workflows.GenerateReport{Topic: input.Topic})
	if err != nil {
		return workflows.PublishReportResult{}, err
	}
	if err := workflow.Sleep(ctx, time.Millisecond); err != nil {
		return workflows.PublishReportResult{}, err
	}
	location, err := workflow.Step(ctx, storeReport, report.Summary)
	if err != nil {
		return workflows.PublishReportResult{}, err
	}
	return workflows.PublishReportResult{Report: report, Location: location}, nil
}

func storeReport(ctx context.Context, summary string) (string, error) {
	return "reports/" + summary, nil
}
