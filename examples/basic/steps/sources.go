package steps

import "context"

func FindSources(ctx context.Context, topic string) ([]string, error) {
	return []string{"source for " + topic}, nil
}

func PublishReport(ctx context.Context, summary string) (string, error) {
	return "reports/" + summary, nil
}
