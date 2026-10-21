package testworld

import (
	"bytes"
	"maps"
	"slices"
	"strconv"

	"github.com/eriicafes/go-workflow/worlds"
)

// The run, step, and hook views hold no payloads. The event log is their only
// copy, and reads take them from it.

// runPayloadsLocked returns run with its input, output, and error from its
// events. ResolveDataNone leaves out the input and output.
func (m *World) runPayloadsLocked(run worlds.Run, resolve worlds.ResolveData) worlds.Run {
	for _, event := range m.events[run.RunID] {
		switch event.EventType {
		case worlds.EventTypeRunCreated:
			if resolve != worlds.ResolveDataNone {
				run.Input = worlds.EventPayload(event)
			}
		case worlds.EventTypeRunCompleted:
			if resolve != worlds.ResolveDataNone {
				run.Output = worlds.EventPayload(event)
			}
		case worlds.EventTypeRunFailed:
			run.Error = worlds.EventPayload(event)
		}
	}
	return run
}

// stepPayloadsLocked returns step with its input from step_created and its
// output or error from its latest result. ResolveDataNone leaves out the input
// and output.
func (m *World) stepPayloadsLocked(step worlds.Step, resolve worlds.ResolveData) worlds.Step {
	for _, event := range m.events[step.RunID] {
		if event.CorrelationID != string(step.StepID) {
			continue
		}
		switch event.EventType {
		case worlds.EventTypeStepCreated:
			if resolve != worlds.ResolveDataNone {
				step.Input = worlds.EventPayload(event)
			}
		case worlds.EventTypeStepCompleted:
			step.Output, step.Error = nil, nil
			if resolve != worlds.ResolveDataNone {
				step.Output = worlds.EventPayload(event)
			}
		case worlds.EventTypeStepFailed, worlds.EventTypeStepRetrying:
			step.Output, step.Error = nil, worlds.EventPayload(event)
		}
	}
	return step
}

// hookPayloadLocked returns hook with its metadata from hook_created, unless
// resolve is ResolveDataNone.
func (m *World) hookPayloadLocked(hook worlds.Hook, resolve worlds.ResolveData) worlds.Hook {
	if resolve == worlds.ResolveDataNone {
		return hook
	}
	for _, event := range m.events[hook.RunID] {
		if event.EventType == worlds.EventTypeHookCreated && event.CorrelationID == string(hook.HookID) {
			hook.Metadata = worlds.EventPayload(event)
		}
	}
	return hook
}

func cloneRun(value worlds.Run) worlds.Run {
	value.Input, value.Output, value.Error = bytes.Clone(value.Input), bytes.Clone(value.Output), bytes.Clone(value.Error)
	value.Attributes = maps.Clone(value.Attributes)
	return value
}

func cloneEvent(value worlds.Event) worlds.Event {
	value.Data = bytes.Clone(value.Data)
	return value
}

func cloneWebhook(value *worlds.Webhook) *worlds.Webhook {
	if value == nil {
		return nil
	}
	result := *value
	if value.Response != nil {
		response := *value.Response
		response.Body = bytes.Clone(value.Response.Body)
		response.Header = maps.Clone(value.Response.Header)
		for key, values := range response.Header {
			response.Header[key] = slices.Clone(values)
		}
		result.Response = &response
	}
	return &result
}

func cloneHook(value worlds.Hook) worlds.Hook {
	value.Metadata = bytes.Clone(value.Metadata)
	value.Webhook = cloneWebhook(value.Webhook)
	if value.TokenRetentionUntil != nil {
		until := *value.TokenRetentionUntil
		value.TokenRetentionUntil = &until
	}
	return value
}

func page[T any](values []T, pagination worlds.Pagination) worlds.Page[T] {
	limit := pagination.Limit
	if limit <= 0 {
		limit = len(values)
	}
	start := 0
	if pagination.Cursor != "" {
		if value, err := strconv.Atoi(pagination.Cursor); err == nil && value >= 0 {
			start = value
		}
	}
	if start > len(values) {
		start = len(values)
	}
	end := start + limit
	if end > len(values) {
		end = len(values)
	}
	result := worlds.Page[T]{Data: values[start:end], HasMore: end < len(values)}
	if result.HasMore {
		result.Cursor = strconv.Itoa(end)
	}
	return result
}
