package localworld

import (
	"context"
	"errors"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

// StepsGet and StepsList read a run's steps whatever its tag: only runs are
// scoped by tag.
func (w *World) StepsGet(ctx context.Context, input worlds.StepsGetInput) (worlds.Step, error) {
	if err := errors.Join(checkID("run ID", string(input.RunID)), checkID("step ID", string(input.StepID))); err != nil {
		return worlds.Step{}, err
	}
	var step worlds.Step
	err := w.read(ctx, func(s *store) error {
		record, err := s.getStep(input.RunID, input.StepID)
		if err != nil {
			return err
		}
		if record == nil {
			return worlds.ErrStepNotFound
		}
		step, err = stepView(*record, s.committed(input.RunID), input.ResolveData)
		return err
	})
	return step, err
}

func (w *World) StepsList(ctx context.Context, input worlds.StepsListInput) (worlds.Page[worlds.Step], error) {
	if err := checkID("run ID", string(input.RunID)); err != nil {
		return worlds.Page[worlds.Step]{}, err
	}
	input.Pagination = input.Pagination.WithDefaults(worlds.SortOrderDescending, 20)
	var page worlds.Page[worlds.Step]
	err := w.read(ctx, func(s *store) error {
		records, err := s.listSteps(input.RunID)
		if err != nil {
			return err
		}
		steps := make([]worlds.Step, 0, len(records))
		for _, record := range records {
			step, err := stepView(record, s.committed(input.RunID), input.ResolveData)
			if err != nil {
				return err
			}
			steps = append(steps, step)
		}
		page = entityPage(steps, input.Pagination, func(step worlds.Step) (time.Time, string) { return step.CreatedAt, string(step.StepID) })
		return nil
	})
	return page, err
}
