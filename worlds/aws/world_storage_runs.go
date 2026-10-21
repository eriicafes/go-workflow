package awsworld

import (
	"context"
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
	runs, err := w.resolveRuns(ctx, []runView{runFromItem(item)}, input.ResolveData)
	if err != nil {
		return worlds.Run{}, err
	}
	return runs[0], nil
}

// RunsList reads a global secondary index, which is eventually consistent. A
// run created a moment ago may be missing from the result. The runs it returns
// are read consistently.
func (w *World) RunsList(ctx context.Context, input worlds.RunsListInput) (worlds.Page[worlds.Run], error) {
	if err := ctx.Err(); err != nil {
		return worlds.Page[worlds.Run]{}, err
	}
	items, cursor, more, err := w.indexPage(ctx, runsIndex(input.WorkflowName, input.Status), input.Pagination, nil)
	if err != nil {
		return worlds.Page[worlds.Run]{}, err
	}
	views := make([]runView, len(items))
	for index, item := range items {
		views[index] = runFromItem(item)
	}
	runs, err := w.resolveRuns(ctx, views, input.ResolveData)
	if err != nil {
		return worlds.Page[worlds.Run]{}, err
	}
	return worlds.Page[worlds.Run]{Data: runs, Cursor: cursor, HasMore: more}, nil
}

func (w *World) RunsWaitForTerminalStatus(ctx context.Context, input worlds.RunsWaitForTerminalStatusInput) (worlds.Run, error) {
	// Poll the run with backoff from 50ms to 500ms.
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
	data, err := worlds.EncodeAttrSet(input)
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
