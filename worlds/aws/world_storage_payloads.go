package awsworld

import (
	"context"

	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/eriicafes/go-workflow/worlds"
)

// The run, step, and hook items hold no payloads. The event log is their only
// copy: each item names the events that recorded its payloads, and reads take
// them from those events with worlds.EventPayload.
//
// A run's input is its run_created event, slot 1, and resultEventId names its
// run_completed or run_failed event. A step's createdEventId names its
// step_created event, and resultEventId its latest step_completed,
// step_failed, or step_retrying event. A hook's createdEventId names its
// hook_created event in the hook's run.

// runInputEventID is the run_created event, which holds a run's input.
var runInputEventID = worlds.EventIDForSlot(1)

// eventRef names one event of a run.
type eventRef struct {
	runID   worlds.RunID
	eventID worlds.EventID
}

// payloadEvents reads the events refs name with batched consistent reads.
// Refs without a valid event ID are skipped.
func (w *World) payloadEvents(ctx context.Context, refs []eventRef) (map[eventRef]worlds.Event, error) {
	keys := make([]map[string]ddb.AttributeValue, 0, len(refs))
	for _, ref := range refs {
		if slot, err := worlds.EventSlot(ref.eventID); err == nil {
			keys = append(keys, eventKey(string(ref.runID), slot))
		}
	}
	if len(keys) == 0 {
		return nil, nil
	}
	items, err := w.items(ctx, keys...)
	if err != nil {
		return nil, err
	}
	events := make(map[eventRef]worlds.Event, len(items))
	for _, item := range items {
		event := eventFromItem(item)
		events[eventRef{runID: event.RunID, eventID: event.EventID}] = event
	}
	return events, nil
}

// resultPayloads decodes a run or step result event into the output of a
// completion or the error of a failure or retry. ResolveDataNone leaves out
// the output.
func resultPayloads(event worlds.Event, resolve worlds.ResolveData) (output, failure []byte) {
	switch event.EventType {
	case worlds.EventTypeRunCompleted, worlds.EventTypeStepCompleted:
		if resolve != worlds.ResolveDataNone {
			return worlds.EventPayload(event), nil
		}
	case worlds.EventTypeRunFailed, worlds.EventTypeStepFailed, worlds.EventTypeStepRetrying:
		return nil, worlds.EventPayload(event)
	}
	return nil, nil
}

// wantsResult reports whether a read under resolve returns any payload of a
// result event. A completed run or step's result is only its output.
func wantsResult(resultEventID worlds.EventID, status worlds.Status, resolve worlds.ResolveData) bool {
	return resultEventID != "" && (resolve != worlds.ResolveDataNone || status != worlds.StatusCompleted)
}

// resolveRuns returns the runs of views with their input, output, and error
// read from their events. ResolveDataNone leaves out the input and output.
func (w *World) resolveRuns(ctx context.Context, views []runView, resolve worlds.ResolveData) ([]worlds.Run, error) {
	var refs []eventRef
	for _, view := range views {
		if resolve != worlds.ResolveDataNone {
			refs = append(refs, eventRef{view.RunID, runInputEventID})
		}
		if wantsResult(view.resultEventID, view.Status, resolve) {
			refs = append(refs, eventRef{view.RunID, view.resultEventID})
		}
	}
	events, err := w.payloadEvents(ctx, refs)
	if err != nil {
		return nil, err
	}
	runs := make([]worlds.Run, len(views))
	for index, view := range views {
		run := view.Run
		run.Input = nil
		if resolve != worlds.ResolveDataNone {
			run.Input = worlds.EventPayload(events[eventRef{run.RunID, runInputEventID}])
		}
		run.Output, run.Error = resultPayloads(events[eventRef{run.RunID, view.resultEventID}], resolve)
		runs[index] = run
	}
	return runs, nil
}

// resolveSteps returns the steps of views with their input, output, and error
// read from their events. ResolveDataNone leaves out the input and output.
func (w *World) resolveSteps(ctx context.Context, views []stepView, resolve worlds.ResolveData) ([]worlds.Step, error) {
	var refs []eventRef
	for _, view := range views {
		if resolve != worlds.ResolveDataNone {
			refs = append(refs, eventRef{view.RunID, view.createdEventID})
		}
		if wantsResult(view.resultEventID, view.Status, resolve) {
			refs = append(refs, eventRef{view.RunID, view.resultEventID})
		}
	}
	events, err := w.payloadEvents(ctx, refs)
	if err != nil {
		return nil, err
	}
	steps := make([]worlds.Step, len(views))
	for index, view := range views {
		step := view.Step
		step.Input = nil
		if resolve != worlds.ResolveDataNone {
			step.Input = worlds.EventPayload(events[eventRef{step.RunID, view.createdEventID}])
		}
		step.Output, step.Error = resultPayloads(events[eventRef{step.RunID, view.resultEventID}], resolve)
		steps[index] = step
	}
	return steps, nil
}

// resolveHooks decodes hook items and reads each hook's metadata from its
// hook_created event. ResolveDataNone leaves out the metadata.
func (w *World) resolveHooks(ctx context.Context, items []map[string]ddb.AttributeValue, resolve worlds.ResolveData) ([]worlds.Hook, error) {
	hooks := make([]worlds.Hook, len(items))
	created := make([]eventRef, len(items))
	for index, item := range items {
		hook, err := hookFromItem(item)
		if err != nil {
			return nil, err
		}
		hooks[index] = hook
		created[index] = eventRef{hook.RunID, worlds.EventID(read(item, "createdEventId"))}
	}
	if resolve == worlds.ResolveDataNone {
		return hooks, nil
	}
	events, err := w.payloadEvents(ctx, created)
	if err != nil {
		return nil, err
	}
	for index := range hooks {
		hooks[index].Metadata = worlds.EventPayload(events[created[index]])
	}
	return hooks, nil
}
