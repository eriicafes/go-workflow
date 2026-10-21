package testworld

import (
	"context"
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
		return worlds.Run{}, worlds.ErrRunNotFound
	}
	return m.runPayloadsLocked(cloneRun(value), input.ResolveData), nil
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
		values = append(values, m.runPayloadsLocked(cloneRun(run), input.ResolveData))
	}
	less := func(a, b worlds.Run) bool {
		if !a.CreatedAt.Equal(b.CreatedAt) {
			return a.CreatedAt.Before(b.CreatedAt)
		}
		return a.RunID < b.RunID
	}
	sort.Slice(values, func(i, j int) bool {
		if input.Pagination.SortOrder == worlds.SortOrderDescending {
			return less(values[j], values[i])
		}
		return less(values[i], values[j])
	})
	return page(values, input.Pagination), nil
}

func (m *World) RunsWaitForTerminalStatus(ctx context.Context, input worlds.RunsWaitForTerminalStatusInput) (worlds.Run, error) {
	deadline := time.Now().Add(input.Timeout)
	timer := time.NewTimer(input.Timeout)
	for {
		changed := m.changed()
		run, err := m.RunsGet(ctx, worlds.RunsGetInput{RunID: input.RunID})
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

func (m *World) RunsSetAttributes(ctx context.Context, input worlds.RunsSetAttributesInput) error {
	data, err := worlds.EncodeAttrSet(input)
	if err != nil {
		return err
	}
	_, err = m.EventsCreate(ctx, worlds.EventsCreateInput{RunID: input.RunID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeAttrSet, Data: data}})
	return err
}
