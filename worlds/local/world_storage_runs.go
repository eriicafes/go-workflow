package localworld

import (
	"context"
	"errors"
	"log"
	"slices"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

func (w *World) RunsGet(ctx context.Context, input worlds.RunsGetInput) (worlds.Run, error) {
	if err := checkID("run ID", string(input.RunID)); err != nil {
		return worlds.Run{}, err
	}
	var run worlds.Run
	err := w.read(ctx, func(s *store) error {
		record, err := s.getRun(input.RunID)
		if err != nil {
			return err
		}
		run, err = runView(record, s.committed(record.RunID), input.ResolveData)
		return err
	})
	return run, err
}

func (w *World) RunsList(ctx context.Context, input worlds.RunsListInput) (worlds.Page[worlds.Run], error) {
	input.Pagination = input.Pagination.WithDefaults(worlds.SortOrderDescending, 200)
	// The list holds the runs of every tag: only a read of one run is scoped
	// by tag. An empty status filter matches no runs, and a nil one matches
	// every status.
	if input.Status != nil && len(input.Status) == 0 {
		return worlds.Page[worlds.Run]{Data: []worlds.Run{}}, nil
	}
	var page worlds.Page[worlds.Run]
	err := w.read(ctx, func(s *store) error {
		records, err := s.listRuns(runFilter{workflowName: input.WorkflowName, statuses: input.Status})
		if err != nil {
			return err
		}
		runs := make([]worlds.Run, 0, len(records))
		for _, record := range records {
			run, err := runView(record, s.committed(record.RunID), input.ResolveData)
			if err != nil {
				return err
			}
			runs = append(runs, run)
		}
		page = entityPage(runs, input.Pagination, func(run worlds.Run) (time.Time, string) { return run.CreatedAt, string(run.RunID) })
		return nil
	})
	return page, err
}

func (w *World) RunsWaitForTerminalStatus(ctx context.Context, input worlds.RunsWaitForTerminalStatusInput) (worlds.Run, error) {
	// Sleep on this process's commits, polling for other processes'.
	deadline := time.Now().Add(input.Timeout)
	timer := time.NewTimer(input.Timeout)
	defer timer.Stop()
	for {
		changed := w.changed()
		run, err := w.RunsGet(ctx, worlds.RunsGetInput{RunID: input.RunID, ResolveData: input.ResolveData})
		if err != nil || run.Status.Terminal() || !time.Now().Before(deadline) {
			return run, err
		}
		if err := waitChange(ctx, changed, timer.C); err != nil {
			return worlds.Run{}, err
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
	if _, err := w.usable(); err != nil {
		return worlds.RunsDeleteDataManyResult{}, err
	}
	results := make([]worlds.DeleteDataResult, len(input.RunIDs))
	deleted := false
	for index, id := range input.RunIDs {
		result := worlds.DeleteDataResult{RunID: id}
		if checkID("run ID", string(id)) != nil {
			// No run is stored under an unsafe ID.
			result.Outcome = worlds.DeleteDataNotFound
			results[index] = result
			continue
		}
		err := w.write(ctx, func(s *store) error {
			run, err := s.getRun(id)
			switch {
			case errors.Is(err, worlds.ErrRunNotFound):
				result.Outcome = worlds.DeleteDataNotFound
				return nil
			case err != nil:
				return err
			case !run.Status.Terminal():
				result.Outcome = worlds.DeleteDataSkipped
				result.Reason, result.Status = worlds.DeleteDataRunNotTerminal, run.Status
				return nil
			}
			// A run whose data was deleted before keeps its first ExpiredAt,
			// and anything an earlier deletion left behind is deleted again.
			now := clock()
			result.Outcome = worlds.DeleteDataAlreadyDeleted
			if !worlds.RunDataExpired(run.Run, now) {
				result.Outcome = worlds.DeleteDataDeleted
				run.ExpiredAt = &now
				if err := s.putRun(run); err != nil {
					return err
				}
			}
			return s.purgeRun(run)
		})
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return worlds.RunsDeleteDataManyResult{}, ctxErr
			}
			log.Printf("workflow/local: delete the data of run %s: %v", id, err)
			result = worlds.DeleteDataResult{RunID: id, Outcome: worlds.DeleteDataFailed, Code: "internal_error", Retryable: true}
		}
		deleted = deleted || result.Outcome == worlds.DeleteDataDeleted || result.Outcome == worlds.DeleteDataAlreadyDeleted
		results[index] = result
	}
	// The deleted bytes stay in the WAL until it is checkpointed.
	if deleted {
		w.maintain(ctx)
	}
	return worlds.NewDeleteDataResult(results), nil
}
