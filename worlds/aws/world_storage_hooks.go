package awsworld

import (
	"context"
	"time"

	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/eriicafes/go-workflow/worlds"
)

func (w *World) HooksGet(ctx context.Context, input worlds.HooksGetInput) (worlds.Hook, error) {
	if err := ctx.Err(); err != nil {
		return worlds.Hook{}, err
	}
	item, found, err := w.item(ctx, hookKey(string(input.HookID)))
	if err != nil {
		return worlds.Hook{}, err
	}
	if !found {
		return worlds.Hook{}, worlds.ErrHookNotFound
	}
	live, err := w.liveHooks(ctx, []map[string]ddb.AttributeValue{item})
	if err != nil {
		return worlds.Hook{}, err
	}
	if len(live) == 0 {
		return worlds.Hook{}, worlds.ErrHookNotFound
	}
	hooks, err := w.resolveHooks(ctx, live, input.ResolveData)
	if err != nil {
		return worlds.Hook{}, err
	}
	return hooks[0], nil
}

// liveHooks returns the items of the hooks that still exist. A disposed
// hook's item records only its disposal, and a hook of a finished run whose
// token retention has lapsed is gone, as the Workflow SDK's Worlds delete
// it when its run ends: its item stays until a claim of its token replaces
// it, since DynamoDB cannot delete a run's hooks with its last event.
func (w *World) liveHooks(ctx context.Context, items []map[string]ddb.AttributeValue) ([]map[string]ddb.AttributeValue, error) {
	keys := make([]map[string]ddb.AttributeValue, 0, len(items))
	for _, item := range items {
		if !isDisposal(item) {
			keys = append(keys, runKey(read(item, "runId")))
		}
	}
	runs, err := w.items(ctx, keys...)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	live := make([]map[string]ddb.AttributeValue, 0, len(items))
	for _, item := range items {
		if isDisposal(item) {
			continue
		}
		runItem, found := runs[itemID(runKey(read(item, "runId")))]
		if !found {
			continue
		}
		if runFromItem(runItem).Status.Terminal() {
			if until := readTimePtr(item, "tokenRetentionUntil"); until == nil || !until.After(now) {
				continue
			}
		}
		live = append(live, item)
	}
	return live, nil
}

func (w *World) HooksGetByToken(ctx context.Context, input worlds.HooksGetByTokenInput) (worlds.Hook, error) {
	// A token whose retention has lapsed is unclaimed. The item itself is
	// replaced by the next claim.
	if err := ctx.Err(); err != nil {
		return worlds.Hook{}, err
	}
	item, found, err := w.item(ctx, tokenKey(input.Token))
	if err != nil {
		return worlds.Hook{}, err
	}
	if !found {
		return worlds.Hook{}, worlds.ErrHookNotFound
	}
	token := tokenFromItem(item)
	if token.expired(time.Now()) {
		return worlds.Hook{}, worlds.ErrHookNotFound
	}
	item, found, err = w.item(ctx, hookKey(string(token.hookID)))
	if err != nil {
		return worlds.Hook{}, err
	}
	if !found {
		return worlds.Hook{}, worlds.ErrHookNotFound
	}
	live, err := w.liveHooks(ctx, []map[string]ddb.AttributeValue{item})
	if err != nil {
		return worlds.Hook{}, err
	}
	if len(live) == 0 {
		return worlds.Hook{}, worlds.ErrHookNotFound
	}
	hooks, err := w.resolveHooks(ctx, live, input.ResolveData)
	if err != nil {
		return worlds.Hook{}, err
	}
	return hooks[0], nil
}

// HooksList reads a global secondary index, which is eventually consistent. A
// hook created a moment ago may be missing from the result. The hooks it
// returns are read consistently.
func (w *World) HooksList(ctx context.Context, input worlds.HooksListInput) (worlds.Page[worlds.Hook], error) {
	// The list's defaults are world-local's.
	input.Pagination = input.Pagination.WithDefaults(worlds.SortOrderAscending, 20)
	if err := ctx.Err(); err != nil {
		return worlds.Page[worlds.Hook]{}, err
	}
	query := indexQuery{index: listIndex, partition: "HOOKS"}
	if input.RunID != "" {
		query = indexQuery{index: listGroupIndex, partition: "HOOKS#" + string(input.RunID)}
	}
	items, cursor, more, err := w.indexPage(ctx, query, input.Pagination, nil)
	if err != nil {
		return worlds.Page[worlds.Hook]{}, err
	}
	if items, err = w.liveHooks(ctx, items); err != nil {
		return worlds.Page[worlds.Hook]{}, err
	}
	hooks, err := w.resolveHooks(ctx, items, input.ResolveData)
	if err != nil {
		return worlds.Page[worlds.Hook]{}, err
	}
	return worlds.Page[worlds.Hook]{Data: hooks, Cursor: cursor, HasMore: more}, nil
}
