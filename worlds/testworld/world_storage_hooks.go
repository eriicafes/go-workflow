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
	if !ok || w.lapsedLocked(value, time.Now()) {
		return worlds.Hook{}, &worlds.HookNotFoundError{Token: string(input.HookID)}
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
		return worlds.Hook{}, &worlds.HookNotFoundError{Token: input.Token}
	}
	return w.HooksGet(ctx, worlds.HooksGetInput{HookID: id, ResolveData: input.ResolveData})
}

func (w *World) HooksList(ctx context.Context, input worlds.HooksListInput) (worlds.Page[worlds.Hook], error) {
	input.Pagination = input.Pagination.WithDefaults(worlds.SortOrderAscending, 20)
	w.mu.RLock()
	defer w.mu.RUnlock()
	values := make([]worlds.Hook, 0, len(w.hooks))
	now := time.Now()
	for _, hook := range w.hooks {
		if input.RunID != "" && hook.RunID != input.RunID || w.lapsedLocked(hook, now) {
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
	return keysetPage(values, input.Pagination, func(hook worlds.Hook) string { return entityKey(hook.CreatedAt, string(hook.HookID)) }), nil
}
