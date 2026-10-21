package localworld

import (
	"context"
	"path"

	"github.com/eriicafes/go-workflow/worlds"
)

func (w *World) StepsGet(ctx context.Context, input worlds.StepsGetInput) (worlds.Step, error) {
	if err := ctx.Err(); err != nil {
		return worlds.Step{}, err
	}
	unlock, err := w.readLock()
	if err != nil {
		return worlds.Step{}, err
	}
	defer unlock()
	step, err := w.readStep(input.RunID, input.StepID)
	if err != nil {
		return worlds.Step{}, err
	}
	if step == nil {
		return worlds.Step{}, worlds.ErrStepNotFound
	}
	return stepView(*step, w.committed(input.RunID), input.ResolveData)
}

func (w *World) StepsList(ctx context.Context, input worlds.StepsListInput) (worlds.Page[worlds.Step], error) {
	if err := ctx.Err(); err != nil {
		return worlds.Page[worlds.Step]{}, err
	}
	unlock, err := w.readLock()
	if err != nil {
		return worlds.Page[worlds.Step]{}, err
	}
	defer unlock()
	dir := stepsDir(input.RunID)
	names, err := listJSON(w.root, dir)
	if err != nil {
		return worlds.Page[worlds.Step]{}, err
	}
	steps := make([]worlds.Step, 0, len(names))
	for _, name := range names {
		var step stepRecord
		if err := readJSON(w.root, path.Join(dir, name), &step); err != nil {
			return worlds.Page[worlds.Step]{}, err
		}
		value, err := stepView(step, w.committed(input.RunID), input.ResolveData)
		if err != nil {
			return worlds.Page[worlds.Step]{}, err
		}
		steps = append(steps, value)
	}
	pagination := input.Pagination
	pagination.SortOrder = worlds.SortOrderAscending
	return keyPage(steps, pagination, func(step worlds.Step) string { return sortKey(step.CreatedAt, string(step.StepID)) })
}
