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
	hook, err := hookFromItem(item)
	if err != nil {
		return worlds.Hook{}, err
	}
	return stripHook(hook, input.ResolveData), nil
}

// HooksGetByToken treats a token whose retention has lapsed as unclaimed;
// the item itself is replaced by the next claim.
func (w *World) HooksGetByToken(ctx context.Context, input worlds.HooksGetByTokenInput) (worlds.Hook, error) {
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
	return w.HooksGet(ctx, worlds.HooksGetInput{HookID: token.hookID, ResolveData: input.ResolveData})
}

// HooksList reads the HOOKS (list) or HOOKS#<runID> (list_group) index
// partition in creation order.
func (w *World) HooksList(ctx context.Context, input worlds.HooksListInput) (worlds.Page[worlds.Hook], error) {
	if err := ctx.Err(); err != nil {
		return worlds.Page[worlds.Hook]{}, err
	}
	query := indexQuery{index: listIndex, partition: "HOOKS"}
	if input.RunID != "" {
		query = indexQuery{index: listGroupIndex, partition: "HOOKS#" + string(input.RunID)}
	}
	pagination := worlds.Pagination{Cursor: input.Pagination.Cursor, Limit: input.Pagination.Limit}
	items, cursor, more, err := w.indexPage(ctx, query, pagination, nil)
	if err != nil {
		return worlds.Page[worlds.Hook]{}, err
	}
	return hooksPage(items, cursor, more, input.ResolveData)
}

func hooksPage(items []map[string]ddb.AttributeValue, cursor string, more bool, resolve worlds.ResolveData) (worlds.Page[worlds.Hook], error) {
	hooks := make([]worlds.Hook, len(items))
	for index, item := range items {
		hook, err := hookFromItem(item)
		if err != nil {
			return worlds.Page[worlds.Hook]{}, err
		}
		hooks[index] = stripHook(hook, resolve)
	}
	return worlds.Page[worlds.Hook]{Data: hooks, Cursor: cursor, HasMore: more}, nil
}
