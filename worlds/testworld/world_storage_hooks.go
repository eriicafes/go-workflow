package testworld

import (
	"context"
	"slices"
	"sort"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

func (w *World) HooksGet(ctx context.Context, input worlds.HooksGetInput) (worlds.Hook, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	value, ok := w.hooks[input.HookID]
	if !ok {
		return worlds.Hook{}, worlds.ErrHookNotFound
	}
	return w.hookPayloadLocked(cloneHook(value), input.ResolveData), nil
}

func (w *World) HooksGetByToken(ctx context.Context, input worlds.HooksGetByTokenInput) (worlds.Hook, error) {
	if err := ctx.Err(); err != nil {
		return worlds.Hook{}, err
	}
	defer w.notify()
	w.mu.Lock()
	w.expireTokenLocked(input.Token, time.Now().UTC())
	id, ok := w.tokens[input.Token]
	w.mu.Unlock()
	if !ok {
		return worlds.Hook{}, worlds.ErrHookNotFound
	}
	return w.HooksGet(ctx, worlds.HooksGetInput{HookID: id, ResolveData: input.ResolveData})
}

func (w *World) HooksList(ctx context.Context, input worlds.HooksListInput) (worlds.Page[worlds.Hook], error) {
	// The list's defaults are world-local's.
	input.Pagination = input.Pagination.WithDefaults(worlds.SortOrderAscending, 20)
	w.mu.RLock()
	defer w.mu.RUnlock()
	values := make([]worlds.Hook, 0, len(w.hooks))
	for _, hook := range w.hooks {
		if input.RunID != "" && hook.RunID != input.RunID {
			continue
		}
		values = append(values, w.hookPayloadLocked(cloneHook(hook), input.ResolveData))
	}
	sort.Slice(values, func(i, j int) bool {
		if !values[i].CreatedAt.Equal(values[j].CreatedAt) {
			return values[i].CreatedAt.Before(values[j].CreatedAt)
		}
		return values[i].HookID < values[j].HookID
	})
	if input.Pagination.SortOrder == worlds.SortOrderDescending {
		slices.Reverse(values)
	}
	return page(values, input.Pagination), nil
}
