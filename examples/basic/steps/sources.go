package steps

import (
	"context"
	"fmt"

	"github.com/eriicafes/go-workflow"
)

func FindSources(ctx context.Context, topic string) ([]string, error) {
	return []string{"source for " + topic}, nil
}

func PublishReport(ctx context.Context, summary string) (string, error) {
	return "reports/" + summary, nil
}

func WriteReportProgress(ctx context.Context, sourceCount int) (struct{}, error) {
	writer, release, err := workflow.Writer(ctx)
	if err != nil {
		return struct{}{}, err
	}
	defer release()
	_, err = fmt.Fprintf(writer, "found %d sources\n", sourceCount)
	return struct{}{}, err
}
