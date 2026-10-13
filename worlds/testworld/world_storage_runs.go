package testworld

import (
	"context"
	"encoding/json"
	"sort"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

func (m *World) RunsGet(ctx context.Context, input worlds.RunsGetInput) (worlds.Run, error) {
	if err := ctx.Err(); err != nil {
		return worlds.Run{}, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	value, ok := m.runs[input.RunID]
	if !ok {
		return worlds.Run{}, worlds.ErrWorkflowNotFound
	}
	return cloneRun(value), nil
}

func (m *World) RunsList(ctx context.Context, input worlds.RunsListInput) (worlds.Page[worlds.Run], error) {
	if err := ctx.Err(); err != nil {
		return worlds.Page[worlds.Run]{}, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	values := make([]worlds.Run, 0, len(m.runs))
	for _, run := range m.runs {
		if input.WorkflowName != "" && run.WorkflowName != input.WorkflowName {
			continue
		}
		if len(input.Status) > 0 {
			found := false
			for _, status := range input.Status {
				found = found || status == run.Status
			}
			if !found {
				continue
			}
		}
		values = append(values, cloneRun(run))
	}
	sort.Slice(values, func(i, j int) bool {
		if input.Pagination.Desc {
			return values[i].CreatedAt.After(values[j].CreatedAt)
		}
		return values[i].CreatedAt.Before(values[j].CreatedAt)
	})
	return page(values, input.Pagination), nil
}

func (m *World) RunsWaitForTerminalStatus(ctx context.Context, input worlds.RunsWaitForTerminalStatusInput) (worlds.Run, error) {
	deadline := time.Now().Add(input.Timeout)
	for {
		run, err := m.RunsGet(ctx, worlds.RunsGetInput{RunID: input.RunID})
		if err != nil || run.Status.Terminal() || input.Timeout <= 0 || !time.Now().Before(deadline) {
			return run, err
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return worlds.Run{}, ctx.Err()
		case <-timer.C:
		}
	}
}

func (m *World) RunsSetAttributes(ctx context.Context, input worlds.RunsSetAttributesInput) error {
	data, err := json.Marshal(attrSetData{Changes: input.Changes})
	if err != nil {
		return err
	}
	_, err = m.EventsCreate(ctx, worlds.EventsCreateInput{RunID: input.RunID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeAttrSet, Data: data}})
	return err
}
