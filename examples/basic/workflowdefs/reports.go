package workflowdefs

type GenerateReportResult struct {
	Summary string
}

type GenerateReport struct {
	Topic string
}

func (GenerateReport) Workflow(GenerateReportResult) {}

type PublishReport struct {
	Topic string
}

type PublishReportResult struct {
	Report   GenerateReportResult
	Location string
}

func (PublishReport) Workflow(PublishReportResult) {}
