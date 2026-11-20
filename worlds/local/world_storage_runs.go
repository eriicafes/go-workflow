package localworld

import (
	"bytes"
	"context"
	"errors"
	"log"
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

func (w *World) RunsDeleteDataMany(ctx context.Context, input worlds.RunsDeleteDataManyInput) (worlds.RunsDeleteDataManyResult, error) {
	if err := worlds.CheckDeleteDataInput(input); err != nil {
		return worlds.RunsDeleteDataManyResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return worlds.RunsDeleteDataManyResult{}, err
	}
	unlock, err := w.writeLock()
	if err != nil {
		return worlds.RunsDeleteDataManyResult{}, err
	}
	defer unlock()
	defer w.signal()
	now := time.Now().UTC()
	results := make([]worlds.DeleteDataResult, len(input.RunIDs))
	for index, id := range input.RunIDs {
		results[index] = worlds.DeleteDataResult{RunID: id}
		run, err := w.readRun(id)
		switch {
		case errors.Is(err, worlds.ErrRunNotFound):
			results[index].Outcome = worlds.DeleteDataNotFound
			continue
		case err != nil:
			results[index].Outcome, results[index].Code, results[index].Retryable = worlds.DeleteDataFailed, "storage_error", true
			continue
		case !run.Status.Terminal():
			results[index].Outcome = worlds.DeleteDataSkipped
			results[index].Reason, results[index].Status = worlds.DeleteDataRunNotTerminal, run.Status
			continue
		}
		results[index].Outcome = worlds.DeleteDataAlreadyDeleted
		if !worlds.RunDataExpired(run.Run, now) {
			// The run's ExpiredAt commits first, so its data is unreadable
			// before its files are scrubbed.
			results[index].Outcome = worlds.DeleteDataDeleted
			run.ExpiredAt, run.DataPurged = &now, false
			if err := writeJSON(w.root, runPath(id), run); err != nil {
				results[index].Outcome, results[index].Code, results[index].Retryable = worlds.DeleteDataFailed, "storage_error", true
				continue
			}
		}
		if err := w.purgeData(run); err != nil {
			w.markUnpurged(id)
			results[index].Outcome, results[index].Code, results[index].Retryable = worlds.DeleteDataFailed, "storage_error", true
		}
	}
	return worlds.NewDeleteDataResult(results), nil
}

// purge deletes a run's data after its ExpiredAt has committed. A failure
// leaves its reads scrubbed and the purge to recovery.
func (w *World) purge(run runRecord) {
	if err := w.purgeData(run); err != nil {
		log.Printf("workflow/local: delete data of run %s: %v", run.RunID, err)
		w.markUnpurged(run.RunID)
	}
}

func (w *World) markUnpurged(runID worlds.RunID) {
	if w.unpurged == nil {
		w.unpurged = map[worlds.RunID]bool{}
	}
	w.unpurged[runID] = true
}

// purgeData deletes the data of a run whose ExpiredAt has committed, as the
// Workflow SDK does: the payloads in its event files, which are also its run,
// step, and hook payloads, and its stream chunks, leaving each stream empty
// and closed. It then records that the run's files are purged.
func (w *World) purgeData(run runRecord) error {
	for slot := 1; slot <= run.EventCount; slot++ {
		var event worlds.Event
		if err := readJSON(w.root, eventPath(run.RunID, slot), &event); err != nil {
			return err
		}
		scrubbed := worlds.ScrubEventData(event.EventType, event.Data)
		if bytes.Equal(scrubbed, event.Data) {
			continue
		}
		event.Data = scrubbed
		if err := writeJSON(w.root, eventPath(run.RunID, slot), event); err != nil {
			return err
		}
	}
	names, err := w.streamNames(run.RunID)
	if err != nil {
		return err
	}
	for _, name := range names {
		meta, _, err := w.readStream(run.RunID, name)
		if err != nil {
			return err
		}
		count := meta.Count
		meta.Count, meta.Closed = 0, true
		// The stream is empty before its chunk files go.
		if err := w.writeStream(run.RunID, meta); err != nil {
			return err
		}
		for index := range count {
			if err := remove(w.root, chunkPath(run.RunID, name, index)); err != nil {
				return err
			}
		}
	}
	run.DataPurged = true
	if err := writeJSON(w.root, runPath(run.RunID), run); err != nil {
		return err
	}
	delete(w.unpurged, run.RunID)
	return nil
}
