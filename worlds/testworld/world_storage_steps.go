package testworld

import (
	"context"
	"sort"

	"github.com/eriicafes/go-workflow/worlds"
)

func (w *World) StepsGet(ctx context.Context, input worlds.StepsGetInput) (worlds.Step, error) {
	if err := ctx.Err(); err != nil {
		return worlds.Step{}, err
	}
	w.mu.RLock()
	defer w.mu.RUnlock()
	value, ok := w.steps[input.RunID][input.StepID]
	if !ok {
		return worlds.Step{}, worlds.ErrStepNotFound
	}
	return w.stepPayloadsLocked(value, input.ResolveData), nil
}

func (w *World) StepsList(ctx context.Context, input worlds.StepsListInput) (worlds.Page[worlds.Step], error) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	values := make([]worlds.Step, 0, len(w.steps[input.RunID]))
	for _, step := range w.steps[input.RunID] {
		values = append(values, w.stepPayloadsLocked(step, input.ResolveData))
	}
	sort.Slice(values, func(i, j int) bool {
		if !values[i].CreatedAt.Equal(values[j].CreatedAt) {
			return values[i].CreatedAt.Before(values[j].CreatedAt)
		}
		return values[i].StepID < values[j].StepID
	})
	return page(values, input.Pagination), nil
}
