package localworld

import (
	"context"
	"slices"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

func (w *World) RunsGet(ctx context.Context, input worlds.RunsGetInput) (worlds.Run, error) {
	if err := ctx.Err(); err != nil {
		return worlds.Run{}, err
	}
	unlock, err := w.readLock()
	if err != nil {
		return worlds.Run{}, err
	}
	defer unlock()
	run, err := w.readRun(input.RunID)
	if err != nil {
		return worlds.Run{}, err
	}
	return runView(run, w.committed(run.RunID), input.ResolveData)
}

func (w *World) RunsList(ctx context.Context, input worlds.RunsListInput) (worlds.Page[worlds.Run], error) {
	if err := ctx.Err(); err != nil {
		return worlds.Page[worlds.Run]{}, err
	}
	unlock, err := w.readLock()
	if err != nil {
		return worlds.Page[worlds.Run]{}, err
	}
	defer unlock()
	records, err := w.readRuns()
	if err != nil {
		return worlds.Page[worlds.Run]{}, err
	}
	runs := make([]worlds.Run, 0, len(records))
	for _, record := range records {
		if !matchesRun(record.Run, input.WorkflowName, input.Status) {
			continue
		}
		run, err := runView(record, w.committed(record.RunID), input.ResolveData)
		if err != nil {
			return worlds.Page[worlds.Run]{}, err
		}
		runs = append(runs, run)
	}
	return keyPage(runs, input.Pagination, func(run worlds.Run) string { return sortKey(run.CreatedAt, string(run.RunID)) })
}

func (w *World) RunsWaitForTerminalStatus(ctx context.Context, input worlds.RunsWaitForTerminalStatusInput) (worlds.Run, error) {
	// Sleep on the change channel between reads.
	deadline := time.Now().Add(input.Timeout)
	timer := time.NewTimer(input.Timeout)
	for {
		changed := w.changed()
		run, err := w.RunsGet(ctx, worlds.RunsGetInput{RunID: input.RunID, ResolveData: input.ResolveData})
		if err != nil || run.Status.Terminal() || !time.Now().Before(deadline) {
			return run, err
		}
		select {
		case <-ctx.Done():
			return worlds.Run{}, ctx.Err()
		case <-timer.C:
		case <-changed:
		}
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

func matchesRun(run worlds.Run, workflowName string, statuses []worlds.Status) bool {
	return (workflowName == "" || run.WorkflowName == workflowName) && (len(statuses) == 0 || slices.Contains(statuses, run.Status))
}
