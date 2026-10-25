package flows

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/examples/basic/workflows"
)

const (
	// reviewWindow is how long each review round waits for a reviewer.
	reviewWindow = 5 * time.Minute
	// maxRevisions bounds the change requests before publishing is abandoned.
	maxRevisions = 3
)

// PublishReport drafts a report while streaming it to reviewers, then waits on
// its review hook. Each change request revises and streams the draft again and
// starts a new review round; an approval publishes it.
func PublishReport(ctx context.Context, input workflows.PublishReport) (workflows.PublishReportResult, error) {
	// Close the draft stream if the run fails, so readers reach its end.
	defer workflow.Rollback(ctx, abandonDraft, "publishing abandoned")

	draft, err := workflow.Step(ctx, writeDraft, input.Topic)
	if err != nil {
		return workflows.PublishReportResult{}, err
	}

	reviews := workflow.CreateHook[workflows.Review](ctx, workflow.HookOption{
		Token: workflows.ReviewToken(workflow.GetWorkflowMetadata(ctx).RunID),
	})
	defer reviews.Dispose(ctx)
	// Register the hook before the stream invites reviews.
	owner, err := reviews.GetConflict(ctx)
	if err != nil {
		return workflows.PublishReportResult{}, err
	}
	if owner != nil {
		return workflows.PublishReportResult{}, workflow.FatalError(workflow.ErrHookConflict)
	}

	var history []workflows.Review
	for {
		if _, err := workflow.Step(ctx, announce, "awaiting review"); err != nil {
			return workflows.PublishReportResult{}, err
		}
		deadline := workflow.Timer(ctx, reviewWindow)
		winner, err := workflow.Select(ctx, reviews, deadline)
		if err != nil {
			return workflows.PublishReportResult{}, err
		}
		if winner == deadline {
			return workflows.PublishReportResult{}, workflow.FatalError(fmt.Errorf("no review within %s", reviewWindow))
		}
		review, err := reviews.Next(ctx)
		if err != nil {
			return workflows.PublishReportResult{}, err
		}
		history = append(history, review)
		if _, err := workflow.Step(ctx, announce, describeReview(review)); err != nil {
			return workflows.PublishReportResult{}, err
		}
		if review.Approved {
			break
		}
		if len(history) > maxRevisions {
			return workflows.PublishReportResult{}, workflow.FatalError(errors.New("too many change requests"))
		}

		draft, err = workflow.Step(ctx, reviseDraft, revision{Draft: draft, Review: review})
		if err != nil {
			return workflows.PublishReportResult{}, err
		}
	}

	location, err := workflow.Step(ctx, publishDraft, draft)
	if err != nil {
		return workflows.PublishReportResult{}, err
	}
	return workflows.PublishReportResult{Draft: draft, Location: location, Reviews: history}, nil
}

// writeDraft writes the draft a paragraph at a time, streaming each one as it
// is written so reviewers can start reading early.
func writeDraft(ctx context.Context, topic string) (string, error) {
	paragraphs := []string{
		fmt.Sprintf("# %s", topic),
		fmt.Sprintf("Why %s matters, and who it is for.", topic),
		fmt.Sprintf("How teams adopt %s today, with two short case studies.", topic),
		fmt.Sprintf("Open questions about %s and what to watch next.", topic),
	}
	return streamParagraphs(ctx, paragraphs)
}

type revision struct {
	Draft  string
	Review workflows.Review
}

// reviseDraft applies a change request and streams the revised draft.
func reviseDraft(ctx context.Context, input revision) (string, error) {
	paragraphs := append(strings.Split(input.Draft, "\n\n"), "Revision: "+input.Review.Comment)
	return streamParagraphs(ctx, paragraphs)
}

func describeReview(review workflows.Review) string {
	reviewer := cmp.Or(review.Reviewer, "reviewer")
	if review.Approved {
		return reviewer + " approved"
	}
	return fmt.Sprintf("%s requested changes: %s", reviewer, review.Comment)
}

// streamParagraphs writes paragraphs to the progress stream one at a time and
// returns them joined as a draft.
func streamParagraphs(ctx context.Context, paragraphs []string) (string, error) {
	writer, release, err := workflow.Writer(ctx, workflow.WriterOption{Name: workflows.ProgressStream})
	if err != nil {
		return "", err
	}
	defer release()
	for _, paragraph := range paragraphs {
		if err := typeOut(ctx, 150*time.Millisecond); err != nil {
			return "", err
		}
		if _, err := fmt.Fprintln(writer, paragraph); err != nil {
			return "", err
		}
	}
	return strings.Join(paragraphs, "\n\n"), nil
}

// announce writes a status line to the progress stream.
func announce(ctx context.Context, line string) (struct{}, error) {
	writer, release, err := workflow.Writer(ctx, workflow.WriterOption{Name: workflows.ProgressStream})
	if err != nil {
		return struct{}{}, err
	}
	defer release()
	_, err = fmt.Fprintln(writer, line)
	return struct{}{}, err
}

// publishDraft stores the approved draft and closes the progress stream.
func publishDraft(ctx context.Context, draft string) (string, error) {
	if err := typeOut(ctx, 200*time.Millisecond); err != nil {
		return "", err
	}
	location := "reports/" + workflow.StepID(ctx) + ".md"

	writer, release, err := workflow.Writer(ctx, workflow.WriterOption{Name: workflows.ProgressStream})
	if err != nil {
		return "", err
	}
	defer release()
	if _, err := fmt.Fprintln(writer, "published to "+location); err != nil {
		return "", err
	}
	return location, writer.Close()
}

// abandonDraft is the compensation that ends the progress stream of a run
// that failed.
func abandonDraft(ctx context.Context, line string) error {
	writer, release, err := workflow.Writer(ctx, workflow.WriterOption{Name: workflows.ProgressStream})
	if err != nil {
		return err
	}
	defer release()
	if _, err := fmt.Fprintln(writer, line); err != nil {
		return err
	}
	return writer.Close()
}

// typeOut stands in for the time it takes to write or store a draft.
func typeOut(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
