package core

import (
	"context"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

type cancelContextKey struct{}

type cancellationContext struct {
	context.Context
	refs   []cancellationReference
	cancel context.CancelCauseFunc
}

// cancellationReference is the durable identity of a cancellation context:
// its system hook, and the hook's token, abrt_ and an ID, which names the
// stream that notifies running steps. It is runtime metadata rather than
// part of a user's step input.
type cancellationReference struct {
	HookID    worlds.HookID `json:"hookId"`
	Token     string        `json:"token"`
	CanCancel bool          `json:"canCancel,omitempty"`
}

// stream returns the name of the stream that notifies running steps of the
// cancellation: strm_, the token's ID, and _system_abort.
func (r cancellationReference) stream() string {
	return "strm_" + strings.TrimPrefix(r.Token, "abrt_") + "_system_abort"
}

// cancellationData is a cancellation's hook payload and stream chunk:
// its cause, as an Error.
type cancellationData struct {
	Cause error `json:"cause,omitempty"`
}

type cancelContextInput struct {
	Reference cancellationReference `json:"reference"`
	Cause     error                 `json:"cause"`
}

// NewCancelContext creates a durable workflow cancellation context with hook and stream backing.
// It draws an ID from the run's seed for the hook's token, abrt_ and the ID,
// and then the hook's ID.
func NewCancelContext(ctx context.Context, execution *Execution) (context.Context, func(error) error, error) {
	token := "abrt_" + execution.seed.ulid()
	hook := DurableHook[cancellationData](DurableHookOptions{Execution: execution, Token: token, System: true})
	if hook.Err != nil {
		return nil, nil, hook.Err
	}
	local, cancel := context.WithCancelCause(ctx)
	refs := CancellationReferences(ctx)
	for i := range refs {
		refs[i].CanCancel = false
	}
	ref := cancellationReference{HookID: hook.HookID, Token: token, CanCancel: true}
	refs = append(refs, ref)
	execution.OnAdvance(func() {
		for _, event := range execution.events[:execution.horizon] {
			if event.EventType != worlds.EventTypeHookReceived || event.CorrelationID != string(ref.HookID) {
				continue
			}
			var data cancellationData
			if execution.DecodePayload(worlds.DecodeHookReceived(event.Data), &data) == nil && data.Cause != nil {
				execution.AdvanceClock(event.CreatedAt)
				cancel(data.Cause)
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
		input := cancelContextInput{Reference: ref, Cause: worlds.NewError(cause)}
		outcome := DurableInlineStep(execution.Context(), DurableInlineStepOptions{
			Execution: execution,
			Name:      runtime.FuncForPC(reflect.ValueOf(CancelContext).Pointer()).Name(),
		}, func(ctx context.Context, input cancelContextInput) (cancellationData, error) {
			return CancelContext(ctx, input.Reference, input.Cause)
		}, input)
		if outcome.Err != nil {
			return outcome.Err
		}
		for !execution.Settled(string(outcome.StepID)) {
			execution.Wait()
		}
		data, err := StepOutcome[cancellationData](execution, outcome.StepID)
		if err != nil {
			return err
		}
		cancel(data.Cause)
		for _, fn := range execution.continuations {
			fn()
		}
		return nil
	}, nil
}

// CancelContext persists an idempotent cancellation and notifies running steps.
func CancelContext(ctx context.Context, ref cancellationReference, cause error) (cancellationData, error) {
	execution, _ := ExecutionFromContext(ctx)
	world := execution.World()
	runID := execution.Run().RunID
	stream := ref.stream()

	data, err := readCancellation(ctx, execution, ref)
	if err != nil {
		return cancellationData{}, err
	}
	if data.Cause == nil {
		payload, err := execution.EncodePayload(cancellationData{Cause: worlds.NewError(cause)})
		if err != nil {
			return cancellationData{}, err
		}
		encoded, err := worlds.EncodeHookReceived(payload, execution.Run().WorkflowName)
		if err != nil {
			return cancellationData{}, err
		}
		if _, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: runID, Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeHookReceived, CorrelationID: string(ref.HookID), Data: encoded}}); err != nil {
			return cancellationData{}, err
		}
		// Concurrent requests may both append, but the earliest committed cause
		// wins. Read after the append before publishing anything to the stream.
		data, err = readCancellation(ctx, execution, ref)
		if err != nil {
			return cancellationData{}, err
		}
	}
	if state, ok := ctx.Value(cancelContextKey{}).(*cancellationContext); ok && state != nil && state.cancel != nil {
		state.cancel(data.Cause)
	}
	frame, err := worlds.EncodeStreamFrame(data, execution.keys, nil)
	if err != nil {
		return cancellationData{}, err
	}
	if err := world.StreamsWrite(ctx, worlds.StreamsWriteInput{RunID: runID, Name: stream, Chunk: frame}); err != nil {
		info, infoErr := world.StreamsGetInfo(ctx, worlds.StreamsGetInfoInput{RunID: runID, Name: stream})
		if infoErr != nil || !info.Done {
			return cancellationData{}, err
		}
	}
	if err := world.StreamsClose(ctx, worlds.StreamsCloseInput{RunID: runID, Name: stream}); err != nil {
		return cancellationData{}, err
	}
	queue, err := worlds.RunDelivery(ctx, world, runID)
	if err != nil {
		return cancellationData{}, err
	}
	queue.IdempotencyKey = stream
	_, err = world.Queue(ctx, queue)
	return data, err
}

// streamCancellation returns the cause a cancellation stream chunk holds, or
// nil.
func streamCancellation(execution *Execution, chunk []byte) error {
	payloads, err := worlds.SplitStreamFrames(chunk)
	if err != nil {
		return nil
	}
	for _, payload := range payloads {
		var data cancellationData
		if execution.DecodePayload(payload, &data) == nil && data.Cause != nil {
			return data.Cause
		}
	}
	return nil
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
		page, err := execution.World().EventsListByCorrelationID(ctx, worlds.EventsListByCorrelationIDInput{RunID: execution.Run().RunID, CorrelationID: string(ref.HookID), Pagination: worlds.Pagination{Cursor: cursor}})
		if err != nil {
			return cancellationData{}, err
		}
		for _, event := range page.Data {
			if event.EventType != worlds.EventTypeHookReceived {
				continue
			}
			var data cancellationData
			if execution.DecodePayload(worlds.DecodeHookReceived(event.Data), &data) == nil && data.Cause != nil {
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
		stream := ref.stream()
		page, err := execution.World().StreamsGetChunks(readerCtx, worlds.StreamsGetChunksInput{RunID: runID, Name: stream})
		if err != nil {
			stop()
			wg.Wait()
			return nil, nil, err
		}
		for _, chunk := range page.Data {
			if cause := streamCancellation(execution, chunk.Data); cause != nil {
				cancel(cause)
			}
		}
		if page.Done {
			cancel(context.Canceled)
			continue
		}
		cursor := page.Cursor
		wg.Go(func() {
			for readerCtx.Err() == nil {
				page, err := execution.World().StreamsGetChunks(readerCtx, worlds.StreamsGetChunksInput{RunID: runID, Name: stream, Cursor: cursor, Wait: time.Second})
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
					if cause := streamCancellation(execution, chunk.Data); cause != nil {
						cancel(cause)
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
