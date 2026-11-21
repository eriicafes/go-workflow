package localworld

import (
	"context"

	"github.com/eriicafes/go-workflow/worlds"
)

func (w *World) StepsGet(ctx context.Context, input worlds.StepsGetInput) (worlds.Step, error) {
	var step worlds.Step
	err := w.read(ctx, func(s *store) error {
		if _, err := s.getRun(input.RunID); err != nil {
			return worlds.ErrStepNotFound
		}
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
	// The list's defaults are world-local's.
	input.Pagination = input.Pagination.WithDefaults(worlds.SortOrderDescending, 20)
	var page worlds.Page[worlds.Step]
	err := w.read(ctx, func(s *store) error {
		if _, err := s.getRun(input.RunID); err != nil {
			page = worlds.Page[worlds.Step]{Data: []worlds.Step{}}
			return nil
		}
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
		page, err = keyPage(steps, input.Pagination, func(step worlds.Step) string { return sortKey(step.CreatedAt, string(step.StepID)) })
		return err
	})
	return page, err
}
