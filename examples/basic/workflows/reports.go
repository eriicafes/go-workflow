package workflows

import "github.com/eriicafes/go-workflow"

// ProgressStream names the stream that report runs write progress lines to.
const ProgressStream = "progress"

type GenerateReport struct {
	workflow.Struct[GenerateReportResult]
	Topic string
}

type GenerateReportResult struct {
	Summary  string
	Sources  []Source
	Sections []Section
}

type Source struct {
	Provider string
	Title    string
	URL      string
}

type Section struct {
	Title string
	Body  string
}

type PublishReport struct {
	workflow.Struct[PublishReportResult]
	Topic string
}

type PublishReportResult struct {
	Draft    string
	Location string
	Reviews  []Review
}

// Review is the payload reviewers send to a publish run. A review that is not
// approved requests changes, and its comment is applied as a revision.
type Review struct {
	Reviewer string
	Approved bool
	Comment  string
}

// ReviewToken returns the hook token a publish run waits on for reviews.
func ReviewToken(runID string) string { return "review:" + runID }
