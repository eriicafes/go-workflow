package awsworld

import (
	"context"
	"encoding/json"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

const (
	terminalPollMin = 50 * time.Millisecond
	terminalPollMax = 500 * time.Millisecond
)

func (w *World) RunsGet(ctx context.Context, input worlds.RunsGetInput) (worlds.Run, error) {
	if err := ctx.Err(); err != nil {
		return worlds.Run{}, err
	}
	item, found, err := w.item(ctx, runKey(string(input.RunID)))
	if err != nil {
		return worlds.Run{}, err
	}
	if !found {
		return worlds.Run{}, worlds.ErrRunNotFound
	}
	return stripRun(runFromItem(item).Run, input.ResolveData), nil
}

// RunsList reads the RUNS (list) or RUNS#<workflow> (list_group) index
// partition in creation order and filters status on the index.
func (w *World) RunsList(ctx context.Context, input worlds.RunsListInput) (worlds.Page[worlds.Run], error) {
	if err := ctx.Err(); err != nil {
		return worlds.Page[worlds.Run]{}, err
	}
	items, cursor, more, err := w.indexPage(ctx, runsIndex(input.WorkflowName, input.Status), input.Pagination, nil)
	if err != nil {
		return worlds.Page[worlds.Run]{}, err
	}
	runs := make([]worlds.Run, len(items))
	for index, item := range items {
		runs[index] = stripRun(runFromItem(item).Run, input.ResolveData)
	}
	return worlds.Page[worlds.Run]{Data: runs, Cursor: cursor, HasMore: more}, nil
}

// RunsWaitForTerminalStatus polls the run with backoff from 50ms to 500ms.
func (w *World) RunsWaitForTerminalStatus(ctx context.Context, input worlds.RunsWaitForTerminalStatusInput) (worlds.Run, error) {
	deadline := time.Now().Add(input.Timeout)
	delay := terminalPollMin
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
		delay = min(delay*2, terminalPollMax)
	}
}

func (w *World) RunsSetAttributes(ctx context.Context, input worlds.RunsSetAttributesInput) error {
	data, err := json.Marshal(attrSetData{Changes: input.Changes})
	if err != nil {
		return err
	}
	_, err = w.EventsCreate(ctx, worlds.EventsCreateInput{RunID: input.RunID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeAttrSet, Data: data}})
	return err
}

func runsIndex(workflowName string, status []worlds.Status) indexQuery {
	if workflowName != "" {
		return indexQuery{index: listGroupIndex, partition: "RUNS#" + workflowName, status: status}
	}
	return indexQuery{index: listIndex, partition: "RUNS", status: status}
}
