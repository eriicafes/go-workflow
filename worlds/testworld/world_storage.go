package testworld

import (
	"bytes"
	"maps"
	"slices"
	"strconv"

	"github.com/eriicafes/go-workflow/worlds"
)

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
	if value.RetainUntil != nil {
		until := *value.RetainUntil
		value.RetainUntil = &until
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
