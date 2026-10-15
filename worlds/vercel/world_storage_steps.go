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
	return decodeStep(input.RunID, input.StepID, raw, input.ResolveData)
}

// StepsList pages steps in creation order; the cursor is the creation slot of
// the last step returned.
func (w *World) StepsList(ctx context.Context, input worlds.StepsListInput) (worlds.Page[worlds.Step], error) {
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
	for index, raw := range raws {
		if raw == "" {
			continue
		}
		step, err := decodeStep(input.RunID, worlds.StepID(ids[index]), raw, input.ResolveData)
		if err != nil {
			return worlds.Page[worlds.Step]{}, err
		}
		page.Data = append(page.Data, step)
	}
	return page, nil
}
