package testworld

import (
	"context"
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
	value = cloneHook(value)
	if input.ResolveData == worlds.ResolveDataNone {
		value.Metadata = nil
	}
	return value, nil
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
	w.mu.RLock()
	defer w.mu.RUnlock()
	values := make([]worlds.Hook, 0, len(w.hooks))
	for _, hook := range w.hooks {
		if input.RunID != "" && hook.RunID != input.RunID {
			continue
		}
		hook = cloneHook(hook)
		if input.ResolveData == worlds.ResolveDataNone {
			hook.Metadata = nil
		}
		values = append(values, hook)
	}
	sort.Slice(values, func(i, j int) bool { return values[i].CreatedAt.Before(values[j].CreatedAt) })
	return page(values, input.Pagination), nil
}
