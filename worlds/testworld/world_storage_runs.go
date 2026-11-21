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

func (m *World) RunsDeleteDataMany(ctx context.Context, input worlds.RunsDeleteDataManyInput) (worlds.RunsDeleteDataManyResult, error) {
	if err := worlds.CheckDeleteDataInput(input); err != nil {
		return worlds.RunsDeleteDataManyResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return worlds.RunsDeleteDataManyResult{}, err
	}
	defer m.notify()
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now().UTC()
	results := make([]worlds.DeleteDataResult, len(input.RunIDs))
	for index, id := range input.RunIDs {
		results[index] = worlds.DeleteDataResult{RunID: id}
		run, ok := m.runs[id]
		switch {
		case !ok:
			results[index].Outcome = worlds.DeleteDataNotFound
		case !run.Status.Terminal():
			results[index].Outcome = worlds.DeleteDataSkipped
			results[index].Reason, results[index].Status = worlds.DeleteDataRunNotTerminal, run.Status
		case worlds.RunDataExpired(run, now):
			results[index].Outcome = worlds.DeleteDataAlreadyDeleted
			m.purgeLocked(id, now)
		default:
			results[index].Outcome = worlds.DeleteDataDeleted
			m.purgeLocked(id, now)
		}
	}
	return worlds.NewDeleteDataResult(results), nil
}

// purgeLocked deletes a finished run's data: the payloads in its events,
// which are also its run, step, and hook payloads, and its streams' chunks,
// leaving each stream empty and closed. It sets the run's ExpiredAt unless it
// has already passed.
func (m *World) purgeLocked(runID worlds.RunID, now time.Time) {
	run := m.runs[runID]
	if !worlds.RunDataExpired(run, now) {
		run.ExpiredAt = &now
		m.runs[runID] = run
	}
	for index, event := range m.events[runID] {
		m.events[runID][index].Data = worlds.ScrubEventData(event.EventType, event.Data)
	}
	for _, stream := range m.streams[runID] {
		stream.chunks, stream.closed = nil, true
	}
}

// scrubLastEventLocked deletes the payload of a run's latest event.
func (m *World) scrubLastEventLocked(runID worlds.RunID) {
	events := m.events[runID]
	last := &events[len(events)-1]
	last.Data = worlds.ScrubEventData(last.EventType, last.Data)
}
