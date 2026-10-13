package testworld

import (
	"bytes"
	"context"
	"sort"

	"github.com/eriicafes/go-workflow/worlds"
)

func (w *World) HooksGet(ctx context.Context, input worlds.HooksGetInput) (worlds.Hook, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	value, ok := w.hooks[input.HookID]
	if !ok {
		return worlds.Hook{}, worlds.ErrHookNotFound
	}
	value.Metadata = bytes.Clone(value.Metadata)
	value.Webhook = cloneWebhook(value.Webhook)
	return value, nil
}

func (w *World) HooksGetByToken(ctx context.Context, input worlds.HooksGetByTokenInput) (worlds.Hook, error) {
	w.mu.RLock()
	id, ok := w.tokens[input.Token]
	w.mu.RUnlock()
	if !ok {
		return worlds.Hook{}, worlds.ErrHookNotFound
	}
	return w.HooksGet(ctx, worlds.HooksGetInput{HookID: id})
}

func (w *World) HooksList(ctx context.Context, input worlds.HooksListInput) (worlds.Page[worlds.Hook], error) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	values := make([]worlds.Hook, 0, len(w.hooks))
	for _, hook := range w.hooks {
		hook.Metadata = bytes.Clone(hook.Metadata)
		hook.Webhook = cloneWebhook(hook.Webhook)
		values = append(values, hook)
	}
	sort.Slice(values, func(i, j int) bool { return values[i].CreatedAt.Before(values[j].CreatedAt) })
	return page(values, input.Pagination), nil
}
