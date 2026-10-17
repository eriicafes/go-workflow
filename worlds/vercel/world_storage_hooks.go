package vercelworld

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

func (w *World) HooksGet(ctx context.Context, input worlds.HooksGetInput) (worlds.Hook, error) {
	if err := ctx.Err(); err != nil {
		return worlds.Hook{}, err
	}
	raw, ok, err := w.redis.Get(ctx, w.key("hook", string(input.HookID)))
	if err != nil {
		return worlds.Hook{}, err
	}
	if !ok {
		return worlds.Hook{}, worlds.ErrHookNotFound
	}
	return decodeHook(input.HookID, raw, input.ResolveData)
}

func (w *World) HooksGetByToken(ctx context.Context, input worlds.HooksGetByTokenInput) (worlds.Hook, error) {
	// A token whose retention has elapsed is released. The stored token is
	// removed lazily by the next claim of that token.
	if err := ctx.Err(); err != nil {
		return worlds.Hook{}, err
	}
	if input.Token == "" {
		return worlds.Hook{}, worlds.ErrHookNotFound
	}
	hookID, ok, err := w.redis.Get(ctx, w.key("token", input.Token))
	if err != nil {
		return worlds.Hook{}, err
	}
	if !ok {
		return worlds.Hook{}, worlds.ErrHookNotFound
	}
	hook, err := w.HooksGet(ctx, worlds.HooksGetInput{HookID: worlds.HookID(hookID), ResolveData: input.ResolveData})
	if err != nil {
		return worlds.Hook{}, err
	}
	run, err := w.RunsGet(ctx, worlds.RunsGetInput{RunID: hook.RunID, ResolveData: worlds.ResolveDataNone})
	if err != nil {
		return worlds.Hook{}, err
	}
	if hook.Token != input.Token || hook.DisposedAt != nil || run.Status.Terminal() && (hook.TokenRetentionUntil == nil || !hook.TokenRetentionUntil.After(time.Now())) {
		return worlds.Hook{}, worlds.ErrHookNotFound
	}
	return hook, nil
}

func (w *World) HooksList(ctx context.Context, input worlds.HooksListInput) (worlds.Page[worlds.Hook], error) {
	// Pages are in creation order, optionally for one run.
	if err := ctx.Err(); err != nil {
		return worlds.Page[worlds.Hook]{}, err
	}
	key := w.prefix + ":hooks"
	if input.RunID != "" {
		key = w.key("runhooks", string(input.RunID))
	}
	desc := input.Pagination.SortOrder == worlds.SortOrderDescending
	min, max, err := scoreRange(input.Pagination.Cursor, desc)
	if err != nil {
		return worlds.Page[worlds.Hook]{}, err
	}
	members, err := w.redis.ZRangeByScore(ctx, key, min, max, desc, fetchCount(input.Pagination.Limit))
	if err != nil {
		return worlds.Page[worlds.Hook]{}, err
	}
	members, cursor, hasMore := trimPage(members, input.Pagination.Limit)
	keys := make([]string, len(members))
	for index, member := range members {
		keys[index] = w.key("hook", member.Member)
	}
	raws, err := w.redis.MGet(ctx, keys...)
	if err != nil {
		return worlds.Page[worlds.Hook]{}, err
	}
	page := worlds.Page[worlds.Hook]{Data: make([]worlds.Hook, 0, len(raws)), Cursor: cursor, HasMore: hasMore}
	for index, raw := range raws {
		if raw == "" {
			continue
		}
		hook, err := decodeHook(worlds.HookID(members[index].Member), raw, input.ResolveData)
		if err != nil {
			return worlds.Page[worlds.Hook]{}, err
		}
		page.Data = append(page.Data, hook)
	}
	return page, nil
}

func (w *World) createHook(ctx context.Context, input worlds.EventsCreateInput) (worlds.EventResult, error) {
	// Token expiry, the conflict check, the optional hook_disposed append to
	// the owner's run, and the claimant's hook_created append run in one
	// script, so exactly one concurrent claimant wins.
	if err := ctx.Err(); err != nil {
		return worlds.EventResult{}, err
	}
	if input.Event.EventType != worlds.EventTypeHookCreated {
		return worlds.EventResult{}, worlds.ErrEventInvalid
	}
	var created worlds.DataHookCreated
	if err := json.Unmarshal(input.Event.Data, &created); err != nil || created.Token == "" {
		return worlds.EventResult{}, worlds.ErrEventInvalid
	}
	now := time.Now().UTC()
	desc, err := describeEvent(input.Event, now)
	if err != nil {
		return worlds.EventResult{}, err
	}
	force := "0"
	if created.Force {
		force = "1"
	}
	// The script cannot encode event data, so the hook_disposed data that names
	// this claimant is encoded here.
	disposed, err := json.Marshal(worlds.HookDisposedEventData{ForceClaimedBy: &worlds.HookForceClaim{RunID: input.RunID, HookID: worlds.HookID(input.Event.CorrelationID)}})
	if err != nil {
		return worlds.EventResult{}, err
	}
	values, err := scriptReply(w.redis.Run(ctx, scriptAppend, w.prefix, string(input.RunID), micros(now), "claim", deltaStart(input.Options), force, inBandArg(input.Options.InBandCount), desc, base64.StdEncoding.EncodeToString(disposed)))
	if err != nil {
		return worlds.EventResult{}, err
	}
	result, _, err := decodeAppendReply(input.RunID, input.Event.CorrelationID, values, input.Options.ResolveData)
	if err != nil {
		return worlds.EventResult{}, err
	}
	event, err := w.EventsGet(ctx, worlds.EventsGetInput{RunID: input.RunID, EventID: result.EventID})
	if err != nil {
		return worlds.EventResult{}, err
	}
	result.Data = event.Data
	if result.EventType == worlds.EventTypeHookCreated {
		hook, err := w.HooksGet(ctx, worlds.HooksGetInput{HookID: worlds.HookID(input.Event.CorrelationID)})
		if err != nil {
			return worlds.EventResult{}, err
		}
		result.Hook = &hook
	}
	return result, nil
}
