package vercelworld

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

func (w *World) RunsGet(ctx context.Context, input worlds.RunsGetInput) (worlds.Run, error) {
	runs, err := w.loadRuns(ctx, []string{string(input.RunID)}, input.ResolveData)
	if err != nil {
		return worlds.Run{}, err
	}
	if runs[0] == nil {
		return worlds.Run{}, worlds.ErrRunNotFound
	}
	return *runs[0], nil
}

// loadRuns reads runs, their attributes, and the records holding their
// payloads in one script call. Missing runs are nil.
func (w *World) loadRuns(ctx context.Context, ids []string, resolve worlds.ResolveData) ([]*worlds.Run, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}
	all := "1"
	if resolve == worlds.ResolveDataNone {
		all = "0"
	}
	result, err := w.redis.Run(ctx, scriptLoadRuns, append([]string{w.prefix, all}, ids...)...)
	if err != nil {
		return nil, err
	}
	items, _ := result.([]any)
	if len(items) != len(ids) {
		return nil, errCorrupt
	}
	runs := make([]*worlds.Run, len(ids))
	for index, item := range items {
		pair, _ := item.([]any)
		if len(pair) != 4 {
			return nil, errCorrupt
		}
		fields := flatHash(pair[0])
		if len(fields) == 0 {
			continue
		}
		run, err := decodeRun(worlds.RunID(ids[index]), fields, flatHash(pair[1]), replyString(pair[2]), replyString(pair[3]), resolve)
		if err != nil {
			return nil, err
		}
		runs[index] = &run
	}
	return runs, nil
}

func (w *World) RunsList(ctx context.Context, input worlds.RunsListInput) (worlds.Page[worlds.Run], error) {
	members, cursor, hasMore, err := w.runIndexPage(ctx, input.WorkflowName, input.Status, input.Pagination)
	if err != nil {
		return worlds.Page[worlds.Run]{}, err
	}
	ids := make([]string, len(members))
	for index, member := range members {
		ids[index] = member.Member
	}
	runs, err := w.loadRuns(ctx, ids, input.ResolveData)
	if err != nil {
		return worlds.Page[worlds.Run]{}, err
	}
	page := worlds.Page[worlds.Run]{Data: make([]worlds.Run, 0, len(runs)), Cursor: cursor, HasMore: hasMore}
	for _, run := range runs {
		if run != nil {
			page.Data = append(page.Data, *run)
		}
	}
	return page, nil
}

// runIndexPage pages the run index for the filters. Several statuses are
// merged by score, which every run index shares.
func (w *World) runIndexPage(ctx context.Context, workflowName string, statuses []worlds.Status, pagination worlds.Pagination) ([]redisZ, string, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", false, err
	}
	desc := pagination.SortOrder == worlds.SortOrderDescending
	min, max, err := scoreRange(pagination.Cursor, desc)
	if err != nil {
		return nil, "", false, err
	}
	keys := []string{w.prefix + ":runs"}
	if workflowName != "" {
		keys = []string{w.key("runs:wf", workflowName)}
	}
	if len(statuses) > 0 {
		keys = keys[:0]
		seen := map[worlds.Status]bool{}
		for _, status := range statuses {
			if seen[status] {
				continue
			}
			seen[status] = true
			if workflowName != "" {
				keys = append(keys, w.key("runs:wfstatus", string(status)+":"+workflowName))
			} else {
				keys = append(keys, w.key("runs:status", string(status)))
			}
		}
	}
	count := fetchCount(pagination.Limit)
	var members []redisZ
	for _, key := range keys {
		values, err := w.redis.ZRangeByScore(ctx, key, min, max, desc, count)
		if err != nil {
			return nil, "", false, err
		}
		members = append(members, values...)
	}
	if len(keys) > 1 {
		sort.Slice(members, func(i, j int) bool {
			if desc {
				return members[i].Score > members[j].Score
			}
			return members[i].Score < members[j].Score
		})
		if count >= 0 && int64(len(members)) > count {
			members = members[:count]
		}
	}
	members, cursor, hasMore := trimPage(members, pagination.Limit)
	return members, cursor, hasMore, nil
}

func (w *World) RunsWaitForTerminalStatus(ctx context.Context, input worlds.RunsWaitForTerminalStatusInput) (worlds.Run, error) {
	// Poll with a backoff from 50ms to 500ms because neither Redis transport
	// offers a blocking read that fits every deployment.
	deadline := time.Now().Add(input.Timeout)
	delay := 50 * time.Millisecond
	for {
		run, err := w.RunsGet(ctx, worlds.RunsGetInput{RunID: input.RunID, ResolveData: input.ResolveData})
		if err != nil || run.Status.Terminal() || input.Timeout <= 0 {
			return run, err
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return run, nil
		}
		if err := sleep(ctx, min(delay, remaining)); err != nil {
			return worlds.Run{}, err
		}
		delay = min(delay*2, 500*time.Millisecond)
	}
}

func (w *World) RunsSetAttributes(ctx context.Context, input worlds.RunsSetAttributesInput) error {
	data, err := worlds.EncodeAttrSet(input)
	if err != nil {
		return err
	}
	_, err = w.EventsCreate(ctx, worlds.EventsCreateInput{RunID: input.RunID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeAttrSet, Data: data}})
	return err
}

// decodeRun decodes a run view with its run_created record input and its
// run_completed or run_failed record result, either empty when not read.
// ResolveDataNone leaves out the input and output.
func decodeRun(runID worlds.RunID, fields, attrs map[string]string, input, result string, resolve worlds.ResolveData) (worlds.Run, error) {
	run := worlds.Run{RunID: runID, WorkflowName: fields["wf"], Status: worlds.Status(fields["status"]), Attributes: attrs, DeploymentID: fields["deployment"], InBandCount: parseCount(fields["inband"]), CreatedAt: parseMicros(fields["createdAt"]), UpdatedAt: parseMicros(fields["updatedAt"]), StartedAt: optionalMicros(fields["startedAt"]), CompletedAt: optionalMicros(fields["completedAt"])}
	if resolve != worlds.ResolveDataNone {
		event, err := decodeRecord(runID, 1, input)
		if err != nil {
			return worlds.Run{}, err
		}
		run.Input = worlds.EventPayload(event)
	}
	if slot := fields["resultSlot"]; slot != "" && (resolve != worlds.ResolveDataNone || run.Status == worlds.StatusFailed) {
		n, err := strconv.ParseUint(slot, 10, 64)
		if err != nil {
			return worlds.Run{}, fmt.Errorf("%w: run %s result slot %q", errCorrupt, runID, slot)
		}
		event, err := decodeRecord(runID, n, result)
		if err != nil {
			return worlds.Run{}, err
		}
		switch event.EventType {
		case worlds.EventTypeRunCompleted:
			if resolve != worlds.ResolveDataNone {
				run.Output = worlds.EventPayload(event)
			}
		case worlds.EventTypeRunFailed:
			run.Error = worlds.EventPayload(event)
		}
	}
	return run, nil
}

func sleep(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
