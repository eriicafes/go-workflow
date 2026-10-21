package vercelworld

import (
	"context"

	"github.com/eriicafes/go-workflow/worlds"
)

func (w *World) StepsGet(ctx context.Context, input worlds.StepsGetInput) (worlds.Step, error) {
	if err := ctx.Err(); err != nil {
		return worlds.Step{}, err
	}
	raw, ok, err := w.redis.HGet(ctx, w.key("steps", string(input.RunID)), string(input.StepID))
	if err != nil {
		return worlds.Step{}, err
	}
	if !ok {
		return worlds.Step{}, worlds.ErrStepNotFound
	}
	step, view, err := decodeStep(input.RunID, input.StepID, raw)
	if err != nil {
		return worlds.Step{}, err
	}
	steps := []worlds.Step{step}
	if err := w.resolveSteps(ctx, steps, []stepView{view}, input.ResolveData); err != nil {
		return worlds.Step{}, err
	}
	return steps[0], nil
}

func (w *World) StepsList(ctx context.Context, input worlds.StepsListInput) (worlds.Page[worlds.Step], error) {
	// Pages are in creation order, and the cursor is the creation slot of the
	// last step returned.
	if err := ctx.Err(); err != nil {
		return worlds.Page[worlds.Step]{}, err
	}
	desc := input.Pagination.SortOrder == worlds.SortOrderDescending
	min, max, err := scoreRange(input.Pagination.Cursor, desc)
	if err != nil {
		return worlds.Page[worlds.Step]{}, err
	}
	members, err := w.redis.ZRangeByScore(ctx, w.key("steporder", string(input.RunID)), min, max, desc, fetchCount(input.Pagination.Limit))
	if err != nil {
		return worlds.Page[worlds.Step]{}, err
	}
	members, cursor, hasMore := trimPage(members, input.Pagination.Limit)
	ids := make([]string, len(members))
	for index, member := range members {
		ids[index] = member.Member
	}
	raws, err := w.redis.HMGet(ctx, w.key("steps", string(input.RunID)), ids...)
	if err != nil {
		return worlds.Page[worlds.Step]{}, err
	}
	page := worlds.Page[worlds.Step]{Data: make([]worlds.Step, 0, len(ids)), Cursor: cursor, HasMore: hasMore}
	views := make([]stepView, 0, len(ids))
	for index, raw := range raws {
		if raw == "" {
			continue
		}
		step, view, err := decodeStep(input.RunID, worlds.StepID(ids[index]), raw)
		if err != nil {
			return worlds.Page[worlds.Step]{}, err
		}
		page.Data, views = append(page.Data, step), append(views, view)
	}
	if err := w.resolveSteps(ctx, page.Data, views, input.ResolveData); err != nil {
		return worlds.Page[worlds.Step]{}, err
	}
	return page, nil
}
