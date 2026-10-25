package flows

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/examples/basic/workflows"
)

// providers are searched concurrently for sources on the topic.
var providers = []string{"web", "papers", "news"}

// sectionTitles are drafted concurrently once the sources are summarized.
var sectionTitles = []string{"Background", "Findings", "Outlook"}

// GenerateReport researches a topic in stages: it searches every provider at
// once, waits out the providers' rate limit, summarizes each source
// concurrently, drafts the sections concurrently, and compiles the report.
func GenerateReport(ctx context.Context, input workflows.GenerateReport) (workflows.GenerateReportResult, error) {
	// Close the progress stream if the run fails, so readers reach its end.
	defer workflow.Rollback(ctx, failProgress, "report failed")

	// Stage 1: search every provider at once. A failed search fails the stage.
	queries := make([]sourceQuery, len(providers))
	for i, provider := range providers {
		queries[i] = sourceQuery{Topic: input.Topic, Provider: provider}
	}
	found, err := workflow.All(ctx, searchSources, queries)
	if err != nil {
		return workflows.GenerateReportResult{}, err
	}
	var sources []workflows.Source
	for _, providerSources := range found {
		sources = append(sources, providerSources...)
	}
	if _, err := workflow.Step(ctx, writeProgress, fmt.Sprintf("found %d sources", len(sources))); err != nil {
		return workflows.GenerateReportResult{}, err
	}

	// The providers rate-limit fetches right after a search.
	if err := workflow.Sleep(ctx, 2*time.Second); err != nil {
		return workflows.GenerateReportResult{}, err
	}

	// Stage 2: summarize every source concurrently. A source that cannot be
	// summarized is left out instead of failing the report.
	summaries := workflow.Map(ctx, summarizeSource, sources)
	var summarized workflow.WaitGroup
	summarized.Add(workflow.MapResults(summaries)...)
	if err := summarized.Wait(ctx); err != nil {
		return workflows.GenerateReportResult{}, err
	}
	var notes []string
	for _, summary := range summaries {
		if note, err := summary.Value(ctx); err == nil {
			notes = append(notes, note)
		}
	}
	if len(notes) == 0 {
		return workflows.GenerateReportResult{}, workflow.FatalError(fmt.Errorf("no sources could be summarized for %q", input.Topic))
	}
	if _, err := workflow.Step(ctx, writeProgress, fmt.Sprintf("summarized %d of %d sources", len(notes), len(sources))); err != nil {
		return workflows.GenerateReportResult{}, err
	}

	// Stage 3: draft every section concurrently. The first failure cancels the
	// sections still being drafted.
	drafting, draftingCtx, err := workflow.ErrGroupWithContext(ctx)
	if err != nil {
		return workflows.GenerateReportResult{}, err
	}
	drafts := make([]*workflow.Result[workflows.Section], len(sectionTitles))
	for i, title := range sectionTitles {
		drafts[i] = workflow.Go(draftingCtx, drafting, draftSection, sectionInput{
			Topic: input.Topic,
			Title: title,
			Notes: notes,
		})
	}
	if err := drafting.Wait(ctx); err != nil {
		return workflows.GenerateReportResult{}, err
	}
	sections := make([]workflows.Section, len(drafts))
	for i, draft := range drafts {
		sections[i] = draft.Unwrap(ctx)
	}

	// Stage 4: compile the sections into the report summary.
	summary, err := workflow.Step(ctx, compileReport, compileInput{Topic: input.Topic, Sections: sections})
	if err != nil {
		return workflows.GenerateReportResult{}, err
	}
	if _, err := workflow.Step(ctx, endProgress, "report ready"); err != nil {
		return workflows.GenerateReportResult{}, err
	}

	return workflows.GenerateReportResult{
		Summary:  summary,
		Sources:  sources,
		Sections: sections,
	}, nil
}

type sourceQuery struct {
	Topic    string
	Provider string
}

func searchSources(ctx context.Context, query sourceQuery) ([]workflows.Source, error) {
	if err := simulateLatency(ctx, 300*time.Millisecond); err != nil {
		return nil, err
	}
	slug := strings.ReplaceAll(strings.ToLower(query.Topic), " ", "-")
	return []workflows.Source{
		{
			Provider: query.Provider,
			Title:    fmt.Sprintf("%s overview (%s)", query.Topic, query.Provider),
			URL:      fmt.Sprintf("https://%s.example/%s", query.Provider, slug),
		},
		{
			Provider: query.Provider,
			Title:    fmt.Sprintf("%s in practice (%s)", query.Topic, query.Provider),
			URL:      fmt.Sprintf("https://%s.example/%s/practice", query.Provider, slug),
		},
	}, nil
}

func summarizeSource(ctx context.Context, source workflows.Source) (string, error) {
	if err := simulateLatency(ctx, 200*time.Millisecond); err != nil {
		return "", err
	}
	return fmt.Sprintf("%s: key points from %s", source.Title, source.URL), nil
}

type sectionInput struct {
	Topic string
	Title string
	Notes []string
}

func draftSection(ctx context.Context, input sectionInput) (workflows.Section, error) {
	if err := simulateLatency(ctx, 400*time.Millisecond); err != nil {
		return workflows.Section{}, err
	}
	return workflows.Section{
		Title: input.Title,
		Body:  fmt.Sprintf("%s of %s, drawn from %d notes.", input.Title, input.Topic, len(input.Notes)),
	}, nil
}

type compileInput struct {
	Topic    string
	Sections []workflows.Section
}

func compileReport(ctx context.Context, input compileInput) (string, error) {
	titles := make([]string, len(input.Sections))
	for i, section := range input.Sections {
		titles[i] = section.Title
	}
	return fmt.Sprintf("%s report covering %s", input.Topic, strings.Join(titles, ", ")), nil
}

// writeProgress appends a line to the run's progress stream.
func writeProgress(ctx context.Context, line string) (struct{}, error) {
	writer, release, err := workflow.Writer(ctx, workflow.WriterOption{Name: workflows.ProgressStream})
	if err != nil {
		return struct{}{}, err
	}
	defer release()
	_, err = fmt.Fprintln(writer, line)
	return struct{}{}, err
}

// endProgress writes a final line and closes the progress stream.
func endProgress(ctx context.Context, line string) (struct{}, error) {
	writer, release, err := workflow.Writer(ctx, workflow.WriterOption{Name: workflows.ProgressStream})
	if err != nil {
		return struct{}{}, err
	}
	defer release()
	if _, err := fmt.Fprintln(writer, line); err != nil {
		return struct{}{}, err
	}
	return struct{}{}, writer.Close()
}

// failProgress is the compensation that ends the progress stream of a run
// that failed.
func failProgress(ctx context.Context, line string) error {
	_, err := endProgress(ctx, line)
	return err
}

// simulateLatency stands in for a call to an external service.
func simulateLatency(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
