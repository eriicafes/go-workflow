package worlds

import (
	"context"
	"encoding/json"
	"reflect"
	"runtime"
	"sync"
	"time"
)

type cancelContextKey struct{}

type cancellationContext struct {
	context.Context
	refs   []cancellationReference
	cancel context.CancelCauseFunc
}

// cancellationReference is the durable identity of a cancellation context.
// It is runtime metadata rather than part of a user's step input.
type cancellationReference struct {
	HookID    HookID `json:"hookId"`
	CanCancel bool   `json:"canCancel,omitempty"`
}

type cancellationData struct {
	Cause json.RawMessage `json:"cause,omitempty"`
}

type cancelContextInput struct {
	Reference cancellationReference
	Cause     json.RawMessage
}

// NewCancelContext creates a durable workflow cancellation context with hook and stream backing.
func NewCancelContext(ctx context.Context, execution *Execution) (context.Context, func(error) error, error) {
	hook := DurableHook[cancellationData](DurableHookOptions{Execution: execution, System: true})
	if hook.Err != nil {
		return nil, nil, hook.Err
	}
	local, cancel := context.WithCancelCause(ctx)
	refs := CancellationReferences(ctx)
	for i := range refs {
		refs[i].CanCancel = false
	}
	ref := cancellationReference{HookID: hook.HookID, CanCancel: true}
	refs = append(refs, ref)
	execution.OnAdvance(func() {
		for _, event := range execution.events[:execution.horizon] {
			if event.EventType != EventTypeHookReceived || event.CorrelationID != string(ref.HookID) {
				continue
			}
			var data cancellationData
			if json.Unmarshal(DecodeHookReceived(event.Data), &data) == nil && len(data.Cause) != 0 {
				cancel(DecodeError(data.Cause))
				return
			}
		}
	})

	state := &cancellationContext{Context: local, refs: refs, cancel: cancel}
	cancelCtx := context.WithValue(state, cancelContextKey{}, state)
	return cancelCtx, func(cause error) error {
		if cause == nil {
			cause = context.Canceled
		}
		input := cancelContextInput{Reference: ref, Cause: encodeError(cause)}
		outcome := DurableInlineStep(execution.Context(), DurableInlineStepOptions{
			Execution: execution,
			Name:      runtime.FuncForPC(reflect.ValueOf(CancelContext).Pointer()).Name(),
		}, func(ctx context.Context, input cancelContextInput) (json.RawMessage, error) {
			return CancelContext(ctx, input.Reference, DecodeError(input.Cause))
		}, input)
		if outcome.Err != nil {
			return outcome.Err
		}
		for !execution.Settled(string(outcome.StepID)) {
			execution.Wait()
		}
		data, err := StepOutcome[json.RawMessage](execution, outcome.StepID)
		if err != nil {
			return err
		}
		cancel(DecodeError(data))
		for _, fn := range execution.continuations {
			fn()
		}
		return nil
	}, nil
}

// CancelContext persists an idempotent cancellation and notifies running steps.
func CancelContext(ctx context.Context, ref cancellationReference, cause error) (json.RawMessage, error) {
	execution, _ := ExecutionFromContext(ctx)
	world := execution.World()
	runID := execution.Run().RunID
	stream := "cancel:" + string(ref.HookID)

	data, err := readCancellation(ctx, execution, ref)
	if err != nil {
		return nil, err
	}
	if len(data.Cause) == 0 {
		payload, _ := json.Marshal(cancellationData{Cause: encodeError(cause)})
		encoded, err := EncodeHookReceived(payload)
		if err != nil {
			return nil, err
		}
		if _, err := world.EventsCreate(ctx, EventsCreateInput{RunID: runID, Event: EventsCreateEvent{EventType: EventTypeHookReceived, CorrelationID: string(ref.HookID), Data: encoded}}); err != nil {
			return nil, err
		}
		// Concurrent requests may both append, but the earliest committed cause
		// wins. Read after the append before publishing anything to the stream.
		data, err = readCancellation(ctx, execution, ref)
		if err != nil {
			return nil, err
		}
	}
	if state, ok := ctx.Value(cancelContextKey{}).(*cancellationContext); ok && state != nil && state.cancel != nil {
		state.cancel(DecodeError(data.Cause))
	}
	encoded, _ := json.Marshal(data)
	if err := world.StreamsWrite(ctx, StreamsWriteInput{RunID: runID, Name: stream, Chunk: encoded}); err != nil {
		info, infoErr := world.StreamsGetInfo(ctx, StreamsGetInfoInput{RunID: runID, Name: stream})
		if infoErr != nil || !info.Done {
			return nil, err
		}
	}
	if err := world.StreamsClose(ctx, StreamsCloseInput{RunID: runID, Name: stream}); err != nil {
		return nil, err
	}
	queue, err := RunDelivery(ctx, world, runID)
	if err != nil {
		return nil, err
	}
	queue.IdempotencyKey = stream
	_, err = world.Queue(ctx, queue)
	return data.Cause, err
}

// CancellationReferences copies the cancellation context identities carried by ctx.
func CancellationReferences(ctx context.Context) []cancellationReference {
	state, _ := ctx.Value(cancelContextKey{}).(*cancellationContext)
	if state == nil {
		return nil
	}
	return append([]cancellationReference(nil), state.refs...)
}

// CombineCancelContexts keeps ctx's values and combines its cancellation with
// sources. Workflow cancellation is delivered through the replay event walk.
func CombineCancelContexts(ctx context.Context, execution *Execution, sources ...context.Context) context.Context {
	if len(sources) == 0 {
		return ctx
	}
	local, cancel := context.WithCancelCause(ctx)
	refs := CancellationReferences(ctx)
	for _, source := range sources {
		refs = append(refs, CancellationReferences(source)...)
	}
	for i := range refs {
		refs[i].CanCancel = false
	}
	execution.OnAdvance(func() {
		for _, source := range sources {
			if source.Err() != nil {
				cancel(context.Cause(source))
				return
			}
		}
	})

	state := &cancellationContext{Context: local, refs: refs, cancel: cancel}
	return context.WithValue(state, cancelContextKey{}, state)
}

// readCancellation returns the first committed cancellation cause.
func readCancellation(ctx context.Context, execution *Execution, ref cancellationReference) (cancellationData, error) {
	cursor := ""
	for {
		page, err := execution.World().EventsListByCorrelationID(ctx, EventsListByCorrelationIDInput{RunID: execution.Run().RunID, CorrelationID: string(ref.HookID), Pagination: Pagination{Cursor: cursor}})
		if err != nil {
			return cancellationData{}, err
		}
		for _, event := range page.Data {
			if event.EventType != EventTypeHookReceived {
				continue
			}
			var data cancellationData
			if json.Unmarshal(DecodeHookReceived(event.Data), &data) == nil && len(data.Cause) != 0 {
				return data, nil
			}
		}
		if !page.HasMore {
			return cancellationData{}, nil
		}
		cursor = page.Cursor
	}
}

// StepCancelContext subscribes a running attempt to its cancellation streams.
// stop releases the readers without cancelling the workflow's cancellation contexts.
func StepCancelContext(ctx context.Context, execution *Execution, refs []cancellationReference) (context.Context, func()) {
	cancelCtx, stop, err := stepCancelContext(ctx, execution, refs)
	if err != nil {
		panic(execution.exit(deliveryError{cause: err}))
	}
	return cancelCtx, stop
}

// stepCancelContext is StepCancelContext returning the World error that
// stops it subscribing, for an attempt that runs outside the replay.
func stepCancelContext(ctx context.Context, execution *Execution, refs []cancellationReference) (context.Context, func(), error) {
	if len(refs) == 0 {
		return ctx, func() {}, nil
	}
	local, cancel := context.WithCancelCause(ctx)
	state := &cancellationContext{Context: local, refs: refs, cancel: cancel}
	cancelCtx := context.WithValue(state, cancelContextKey{}, state)
	readerCtx, stop := context.WithCancel(execution.Context())
	runID := execution.Run().RunID

	var wg sync.WaitGroup
	for _, ref := range refs {
		stream := "cancel:" + string(ref.HookID)
		page, err := execution.World().StreamsGetChunks(readerCtx, StreamsGetChunksInput{RunID: runID, Name: stream})
		if err != nil {
			stop()
			wg.Wait()
			return nil, nil, err
		}
		for _, chunk := range page.Data {
			var data cancellationData
			if json.Unmarshal(chunk.Data, &data) == nil && len(data.Cause) != 0 {
				cancel(DecodeError(data.Cause))
			}
		}
		if page.Done {
			cancel(context.Canceled)
			continue
		}
		cursor := page.Cursor
		wg.Go(func() {
			for readerCtx.Err() == nil {
				page, err := execution.World().StreamsGetChunks(readerCtx, StreamsGetChunksInput{RunID: runID, Name: stream, Cursor: cursor, Wait: time.Second})
				if err != nil {
					if readerCtx.Err() != nil {
						return
					}
					timer := time.NewTimer(time.Second)
					select {
					case <-readerCtx.Done():
						return
					case <-timer.C:
					}
					continue
				}
				for _, chunk := range page.Data {
					var data cancellationData
					if json.Unmarshal(chunk.Data, &data) == nil && len(data.Cause) != 0 {
						cancel(DecodeError(data.Cause))
						return
					}
				}
				if page.Done {
					cancel(context.Canceled)
					return
				}
				cursor = page.Cursor
				if page.RetryAfter > 0 {
					timer := time.NewTimer(page.RetryAfter)
					select {
					case <-readerCtx.Done():
						return
					case <-timer.C:
					}
				}
			}
		})
	}
	return cancelCtx, func() { stop(); wg.Wait() }, nil
}
