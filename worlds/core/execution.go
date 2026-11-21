package core

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

type executionContextKey struct{}
type stepContextKey struct{}

// StepMetadata describes the step whose function is executing. Attempt
// starts at 1 and StartedAt is the start of the current attempt.
type StepMetadata struct {
	ID        worlds.StepID
	Name      string
	StartedAt time.Time
	Attempt   int
}

// StepMetadataFromContext returns the metadata of the step whose function is
// executing. It reports false outside step context.
func StepMetadataFromContext(ctx context.Context) (StepMetadata, bool) {
	metadata, ok := ctx.Value(stepContextKey{}).(StepMetadata)
	return metadata, ok
}

// ExecutionFromContext returns the Execution of the delivery ctx belongs to. It
// reports false outside a flow or step executed by the Runtime. Application
// code does not normally need it.
func ExecutionFromContext(ctx context.Context) (*Execution, bool) {
	execution, ok := ctx.Value(executionContextKey{}).(*Execution)
	return execution, ok && execution != nil
}

// Execution holds a delivery's run, event log, and target step. Durable operations
// draw their IDs from the run's seed, match declarations by ID, and read
// recorded outcomes. Results become
// visible as Wait walks the event log.
type Execution struct {
	world  worlds.World
	ctx    context.Context
	run    worlds.Run
	events []worlds.Event
	// target identifies the step this delivery executes; nil marks a flow delivery.
	target *worlds.StepID
	// delivery is the queue message being executed.
	delivery worlds.QueueInput
	// result holds the target step's outcome for the runtime to persist,
	// and targetAttempt the attempt it is the outcome of.
	result        *worlds.DataStepResult
	targetAttempt int
	// keys encrypts and opens the run's payloads. It is nil when the run's
	// payloads are not encrypted.
	keys *worlds.RunKeys
	// seed draws the run's correlation IDs, hook tokens, and random values.
	seed *runSeed
	// index maps the ULID of each recorded declaration to its index in
	// events, and reached holds the ULIDs this replay has declared.
	index   map[string]int
	reached map[string]bool
	// clock is the replay clock workflow code reads: the run's creation time,
	// moved forward to the creation time of each outcome delivered to it.
	clock time.Time
	// rollbacks holds compensations in registration order.
	rollbacks []func(context.Context, *Execution) error
	// horizon is the number of events the walk has passed. A result at a
	// smaller index is visible to the runner.
	horizon int
	// settles maps operation IDs to their settlement event's index, and exact
	// event IDs to their own index for ordering and replay visibility.
	settles map[string]int
	// inBand is the run's InBandCount as of the last write this replay saw,
	// which fences a flow's writes.
	inBand int
	// superseded reports that the World refused one of this flow's in-band
	// writes, because another flow wrote first, so the flow writes nothing
	// more and starts no step.
	superseded bool
	// startHook is the run's start hook, decoded once by StartHook, and
	// startHookBound reports that a hook of this replay has opened it (see
	// OpenStartHook).
	startHook      *worlds.StartHook
	startHookRead  bool
	startHookBound bool
	// continuations holds callbacks that react as replay delivers events.
	continuations []func()
	// suspended marks a runtime exit before the workflow body finishes.
	suspended bool
	// pending holds the steps a flow declared or must attempt again, in
	// declaration order, and waits the timers it declared, which advance
	// creates once every recorded event is delivered.
	pending      []pendingStep
	pendingWaits []pendingWait
	// openWaits holds the recorded timers this replay reached before they
	// were due, which a flow waiting for an inline step completes when they
	// are.
	openWaits map[worlds.WaitID]pendingWait
	// completedChains holds the chains of the waits this delivery
	// completed, whose timers it arms.
	completedChains map[string]bool
	// armedWaits holds the waits whose timer the delivery queued.
	armedWaits map[string]bool
	// aheadDepth is how many inline steps' results the flow may hand to the
	// runner ahead of their writes, ahead holds those writes in order, and
	// writer sends them. See runahead.go.
	aheadDepth int
	ahead      []*aheadWrite
	writer     *aheadWriter
	// feed pushes the run's events while the flow waits for an inline step,
	// when the World supports it. See feed.go.
	feed *feed
	// observedHooks holds the hooks the runner has looked for a payload of,
	// which may change its decisions when one arrives.
	observedHooks map[worlds.HookID]bool
	// outcomes receives the result of each step the flow runs inline.
	outcomes chan inlineOutcome
	// inline counts the steps the flow runs inline that have not recorded
	// their results.
	inline int
	// ranInline holds the steps whose attempt this delivery started inline,
	// until their result is recorded or their retry is handed to a step
	// delivery.
	ranInline map[worlds.StepID]bool
	// startedAt is when the delivery started. A flow stops running new steps
	// inline after each step's duration.
	startedAt time.Time
	// inlineUntil is halfway from startedAt to the deadline of the
	// delivery's ctx, after which a flow stops running new steps inline, so
	// they and the steps already running can finish before it. It is zero
	// when ctx has no deadline.
	inlineUntil time.Time
}

func newExecution(ctx context.Context, world worlds.World, run worlds.Run, events []worlds.Event, target *worlds.StepID) (*Execution, error) {
	seed, err := newRunSeed(run)
	if err != nil {
		return nil, err
	}
	execution := &Execution{
		clock:     time.UnixMilli(int64(seed.time)).UTC(),
		world:     world,
		ctx:       ctx,
		run:       run,
		events:    events,
		target:    target,
		seed:      seed,
		index:     make(map[string]int),
		reached:   make(map[string]bool),
		settles:   make(map[string]int),
		ranInline: make(map[worlds.StepID]bool),
		openWaits: make(map[worlds.WaitID]pendingWait),

		completedChains: make(map[string]bool),
		observedHooks:   make(map[worlds.HookID]bool),
		inBand:          run.InBandCount,
		startedAt:       time.Now(),
	}
	if deadline, ok := ctx.Deadline(); ok {
		execution.inlineUntil = execution.startedAt.Add(deadline.Sub(execution.startedAt) / 2)
	}
	for index, event := range events {
		execution.trackEvent(index, event)
	}
	return execution, nil
}

func (e *Execution) trackEvent(index int, event worlds.Event) {
	// Indexing an event ID does not settle its correlated operation. The
	// cases below index operation IDs only when their outcome is recorded.
	e.settles[string(event.EventID)] = index
	switch event.EventType {
	case worlds.EventTypeStepCompleted, worlds.EventTypeStepFailed, worlds.EventTypeWaitCompleted, worlds.EventTypeHookReceived, worlds.EventTypeHookDisposed:
		if _, settled := e.settles[event.CorrelationID]; !settled && event.CorrelationID != "" {
			e.settles[event.CorrelationID] = index
		}
		return
	case worlds.EventTypeHookConflict:
		e.settles[event.CorrelationID] = index
		// A conflict also records the declaration, so index it below.
	case worlds.EventTypeStepStarted:
		// An inline step's first step_started declares it.
		if !worlds.StartCreatesStep(event.Data) {
			return
		}
	case worlds.EventTypeStepCreated, worlds.EventTypeWaitCreated, worlds.EventTypeHookCreated, worlds.EventTypeAttrSet:
	default:
		return
	}
	if _, key, ok := strings.Cut(event.CorrelationID, "_"); ok {
		if _, declared := e.index[key]; !declared {
			e.index[key] = index
		}
	}
}

// World returns the World the run is recorded in.
func (e *Execution) World() worlds.World {
	return e.world
}

// Context returns the delivery context with execution metadata, without a
// workflow cancellation context. Internal control steps use it to record outcomes.
func (e *Execution) Context() context.Context {
	return context.WithValue(e.ctx, executionContextKey{}, e)
}

// Run returns the run as loaded at the start of the delivery.
func (e *Execution) Run() worlds.Run {
	return e.run
}

// OpenStartHook reports whether a hook declared with token opens the run's
// start hook, and returns its ID. The first hook declared with the start
// hook's token opens it, and nothing is recorded for it. Whether a hook opens
// it depends only on the hooks declared before it, so every replay decides it
// the same way. A later hook with the token is created as any hook is, and
// conflicts with the start hook.
func (e *Execution) OpenStartHook(token string) (worlds.HookID, bool) {
	if e.startHookBound || token == "" {
		return "", false
	}
	if hook := e.StartHook(); hook == nil || hook.Token != token {
		return "", false
	}
	e.startHookBound = true
	return worlds.StartHookID(e.run.RunID), true
}

// StartHook returns the start hook the run's run_created records, or nil.
// It decodes run_created once per execution.
func (e *Execution) StartHook() *worlds.StartHook {
	if !e.startHookRead && len(e.events) > 0 && e.events[0].EventType == worlds.EventTypeRunCreated {
		e.startHook, e.startHookRead = worlds.RunStartHook(e.events[0].Data), true
	}
	return e.startHook
}

// TargetStepID returns the step a step delivery executes. It reports false
// for a flow, which replays the runner without executing a step.
func (e *Execution) TargetStepID() (worlds.StepID, bool) {
	if e.target == nil {
		return "", false
	}
	return *e.target, true
}

// SetTargetResult records the result of the step a step delivery executes,
// for the Runtime to append once the replay ends. It is meaningful only during
// a step delivery.
func (e *Execution) SetTargetResult(result worlds.DataStepResult) {
	var retryAt *time.Time
	if result.RetryAfter != nil {
		value := *result.RetryAfter
		retryAt = &value
	}
	e.result = &worlds.DataStepResult{Result: bytes.Clone(result.Result), Error: bytes.Clone(result.Error), RetryAfter: retryAt}
}

// GetTargetResult returns the step result recorded by SetTargetResult. It
// reports false when none has been recorded.
func (e *Execution) GetTargetResult() (worlds.DataStepResult, bool) {
	if e.result == nil {
		return worlds.DataStepResult{}, false
	}
	result := *e.result
	result.Result = bytes.Clone(result.Result)
	result.Error = bytes.Clone(result.Error)
	if result.RetryAfter != nil {
		value := *result.RetryAfter
		result.RetryAfter = &value
	}
	return result, true
}

// NextStepID draws the ID of the next step the runner declares: step_ and the
// run's next correlation ULID.
func (e *Execution) NextStepID() worlds.StepID { return worlds.StepID("step_" + e.seed.ulid()) }

// NextWaitID draws the ID of the next timer the runner declares: wait_ and
// the run's next correlation ULID.
func (e *Execution) NextWaitID() worlds.WaitID { return worlds.WaitID("wait_" + e.seed.ulid()) }

// NextHookID draws the ID of the next hook the runner declares: hook_ and the
// run's next correlation ULID, drawing again on the ID of the run's start
// hook, which no other hook may take.
func (e *Execution) NextHookID() worlds.HookID {
	id := worlds.HookID("hook_" + e.seed.ulid())
	if e.StartHook() != nil {
		for id == worlds.StartHookID(e.run.RunID) {
			id = worlds.HookID("hook_" + e.seed.ulid())
		}
	}
	return id
}

// NextHookToken draws the token of the next hook the runner declares without
// one.
func (e *Execution) NextHookToken() string { return e.seed.hookToken() }

// Now returns the replay clock, in whole milliseconds: the run's creation
// time, read from its ID, moved forward to the creation time of each outcome
// delivered to workflow code, so every replay reads the same time at the same
// point.
func (e *Execution) Now() time.Time { return e.clock }

// AdvanceClock moves the replay clock forward to at, the creation time of an
// event delivered to workflow code, in whole milliseconds. It never moves the
// clock back.
func (e *Execution) AdvanceClock(at time.Time) {
	if at = at.Truncate(time.Millisecond).UTC(); at.After(e.clock) {
		e.clock = at
	}
}

// RandReader returns a reader of bytes drawn from the run's seed, each the
// floor of a drawn value times 256.
func (e *Execution) RandReader() io.Reader { return e.seed }

// declared marks the operation with id as reached and returns the index of
// its recorded declaration, matched by ULID so an operation of another kind
// recorded with it is reported too. Because IDs come from the run's seed, a
// runner must declare its operations in the same order on every replay.
func (e *Execution) declared(id string) (int, bool) {
	_, key, _ := strings.Cut(id, "_")
	e.reached[key] = true
	index, ok := e.index[key]
	return index, ok
}

// unreached reports the earliest recorded declaration that the replay has
// not reached. A runner that returns with such an operation left over has
// diverged from its event log.
func (e *Execution) unreached() (string, bool) {
	lowest := -1
	for key, index := range e.index {
		if !e.reached[key] && (lowest < 0 || index < lowest) {
			lowest = index
		}
	}
	if lowest < 0 {
		return "", false
	}
	return e.events[lowest].CorrelationID, true
}

// InBandCount returns the count that fences a write made by this replay, for
// EventsCreateOptions.InBandCount. It is nil in a step delivery, whose writes
// are out-of-band, so only a flow, the run's orchestrator, fences its writes.
func (e *Execution) InBandCount() *int {
	if e.target != nil {
		return nil
	}
	count := e.inBand
	return &count
}

// Settled reports whether the step, timer, or hook with id has settled as far
// as the replay has walked the event log. It never advances the walk, so the
// answer is the same at the same point of every replay.
func (e *Execution) Settled(id string) bool {
	index, ok := e.settles[id]
	return ok && index < e.horizon
}

// SettledEvent returns the event that settled id, when Settled reports true.
func (e *Execution) SettledEvent(id string) (worlds.Event, bool) {
	if !e.Settled(id) {
		return worlds.Event{}, false
	}
	return e.events[e.settles[id]], true
}

// DeliveredEventID identifies the last event delivered to workflow code.
func (e *Execution) DeliveredEventID() worlds.EventID {
	if e.horizon == 0 {
		return ""
	}
	return e.events[e.horizon-1].EventID
}

// exit marks replay as suspended and returns cause for the caller to panic with.
func (e *Execution) exit(cause error) error {
	e.suspended = true
	return cause
}

// Suspend marks replay as suspended and panics to return control to the runtime.
func (e *Execution) Suspend() {
	panic(e.exit(errReplaySuspended))
}

// IsSuspended reports whether the runtime stopped this replay before
// the workflow body finished. Deferred cleanup should be skipped.
func (e *Execution) IsSuspended() bool { return e.suspended }

// AddRollback registers fn for workflow failure. Callbacks run in registration
// order, and registrations are rebuilt on every replay.
func (e *Execution) AddRollback(fn func(context.Context, *Execution) error) {
	e.rollbacks = append(e.rollbacks, fn)
}

// queue queues input for this run, addressed to its deployment. A storage
// error ends the replay so the queue retries the delivery.
func (e *Execution) queue(input worlds.QueueInput) worlds.QueueResult {
	result, err := e.world.Queue(e.ctx, e.addressed(input))
	if err != nil {
		panic(e.exit(deliveryError{cause: err}))
	}
	return result
}

// merge adds result.Events and latest, the event just appended, to the events
// the Execution holds, skipping latest when it is already among them. It does
// not read storage.
func (e *Execution) merge(result worlds.EventResult, latest worlds.Event) {
	e.mergeEvents(result.Events)
	for _, event := range e.events {
		if event.EventID == latest.EventID {
			return
		}
	}
	e.trackEvent(len(e.events), latest)
	e.events = append(e.events, latest)
}

// mergeEvents adds the events the Execution does not hold yet, which follow
// the ones it holds, to its log.
func (e *Execution) mergeEvents(events []worlds.Event) {
	held := len(e.events)
	for _, event := range events {
		if slot, err := worlds.EventSlot(event.EventID); err == nil && int(slot) <= held {
			continue
		}
		e.trackEvent(len(e.events), event)
		e.events = append(e.events, event)
	}
}

// reload reads the events after the ones the Execution holds, when a write's
// report of the events before it was incomplete.
func (e *Execution) reload() {
	page, err := e.world.EventsList(e.ctx, worlds.EventsListInput{RunID: e.run.RunID, Pagination: worlds.Pagination{Cursor: string(worlds.EventIDForSlot(uint64(len(e.events))))}})
	for err == nil {
		e.mergeEvents(page.Data)
		if !page.HasMore {
			return
		}
		page, err = e.world.EventsList(e.ctx, worlds.EventsListInput{RunID: e.run.RunID, Pagination: worlds.Pagination{Cursor: page.Cursor}})
	}
	panic(e.exit(deliveryError{cause: err}))
}

// supersede marks the flow superseded and ends the replay, because the World
// refused an in-band write of it.
func (e *Execution) supersede() {
	e.superseded = true
	panic(e.exit(errReplaySuperseded))
}

// CreateEvent appends event to the run as a write of this replay and adds it,
// with the events others appended since, to the events the Execution holds. A
// flow's write is in-band, fenced by the run's in-band count: when another
// flow wrote first, the World refuses it and CreateEvent ends the replay so
// the Runtime delivers the flow's message again. A step delivery's write is
// out-of-band. Any other World error ends the replay so the queue retries the
// delivery, instead of reaching runner code.
//
// mapError, when not nil, sees the World's error first. It may act on it and
// return it, return another error, or return nil to accept it, and CreateEvent
// then adds nothing.
func (e *Execution) CreateEvent(event worlds.EventsCreateEvent, mapError func(error) error) worlds.EventResult {
	if e.superseded {
		panic(e.exit(errReplaySuperseded))
	}
	e.drainAhead()
	eventCount := len(e.events)
	inBand := e.InBandCount()
	if inBand != nil {
		event.OccurredAt = time.Now().UTC().Truncate(time.Millisecond)
	}
	result, err := e.world.EventsCreate(e.ctx, worlds.EventsCreateInput{
		RunID:   e.run.RunID,
		Event:   event,
		Options: worlds.EventsCreateOptions{EventCount: &eventCount, InBandCount: inBand},
	})
	if err != nil {
		if mapError != nil {
			err = mapError(err)
		}
		switch {
		case err == nil:
			return worlds.EventResult{}
		case errors.Is(err, worlds.ErrInBandSuperseded):
			e.supersede()
		default:
			panic(e.exit(deliveryError{cause: err}))
		}
	}
	if inBand != nil {
		e.inBand = result.InBandCount
	}
	data := event.Data
	if result.Data != nil {
		data = result.Data
	}
	e.merge(result, worlds.Event{RunID: e.run.RunID, EventID: result.EventID, EventType: result.EventType, CorrelationID: event.CorrelationID, Data: data, CreatedAt: result.CreatedAt})
	if result.MoreEvents {
		e.reload()
	}
	return result
}

// OnAdvance registers workflow work that becomes runnable as results arrive.
func (e *Execution) OnAdvance(fn func()) {
	e.continuations = append(e.continuations, fn)
	fn()
}

// advance delivers one recorded event and runs newly ready continuations.
// Once every recorded event is delivered, a flow creates the steps and
// timers the runner declared, and waits for a step it runs inline to record
// its result, which it then delivers, so the runner continues without
// replaying. advance reports false when no event can arrive in this
// delivery.
func (e *Execution) advance() bool {
	if e.horizon == len(e.events) {
		e.createDeclared()
	}
	for e.horizon == len(e.events) {
		if e.inline == 0 {
			// A flow suspends from a prefix of the run's log.
			e.drainAhead()
			if e.horizon < len(e.events) {
				break
			}
			return false
		}
		if err := e.awaitInline(); err != nil {
			if errors.Is(err, errReplaySuperseded) {
				panic(e.exit(errReplaySuperseded))
			}
			panic(e.exit(deliveryError{cause: err}))
		}
	}
	e.horizon++
	switch event := e.events[e.horizon-1]; event.EventType {
	case worlds.EventTypeStepCompleted, worlds.EventTypeStepFailed, worlds.EventTypeWaitCompleted, worlds.EventTypeHookCreated, worlds.EventTypeHookConflict:
		// The walk delivers a step's result, a timer, and a hook's
		// registration outcome as it passes them, and a hook's payload when
		// a consumer takes it.
		e.AdvanceClock(event.CreatedAt)
	}
	for _, fn := range e.continuations {
		fn()
	}
	return true
}

// Wait advances the walk by one event, waiting for a step the flow runs
// inline when every recorded event is delivered, and suspends the replay when
// no event can arrive in this delivery.
func (e *Execution) Wait() {
	if !e.advance() {
		e.Suspend()
	}
}

// hold adds step to the steps advance creates or starts once every recorded
// event is delivered.
func (e *Execution) hold(step pendingStep) {
	e.pending = append(e.pending, step)
}

// holdWait adds a timer to the ones advance creates once every recorded
// event is delivered.
func (e *Execution) holdWait(wait pendingWait) {
	e.pendingWaits = append(e.pendingWaits, wait)
}

// createDeclared records the steps and timers the runner declared, in one
// in-band write, and starts the steps it runs inline. Each new step's mode is
// fixed here: an inline step, and while fewer than its limit run inline, a
// new step, is created by its first step_started and runs in this flow. The
// rest are recorded with step_created and queued as step deliveries once the
// write commits, and never run inline later. The write holds the inline
// starts first, then the step_created events, then the timers'
// wait_created.
func (e *Execution) createDeclared() {
	if len(e.pending) == 0 && len(e.pendingWaits) == 0 {
		return
	}
	pending, waits := e.pending, e.pendingWaits
	e.pending, e.pendingWaits = nil, nil
	if e.superseded {
		return
	}
	var starts, created, failed, timers []worlds.EventsCreateEvent
	var inline, background []pendingStep
	running := e.inline
	for _, step := range pending {
		switch {
		case step.failure != nil:
			// A step whose input did not encode fails as it is created.
			created = append(created, e.stepEvent(worlds.EventTypeStepCreated, step.stepID, worlds.DataStepCreated{StepName: step.name, WorkflowName: e.run.WorkflowName, Inline: true, CreatorMessageID: e.delivery.MessageID}))
			failed = append(failed, e.stepEvent(worlds.EventTypeStepFailed, step.stepID, worlds.DataStepResult{StepName: step.name, Attempt: 1, Error: e.encodeError(step.failure)}))
			continue
		case step.exhausted:
			// An inline step whose attempts all started fails without
			// another.
			failed = append(failed, e.stepEvent(worlds.EventTypeStepFailed, step.stepID, worlds.DataStepResult{StepName: step.name, Attempt: step.attempt, Error: e.encodeError(worlds.FatalError{Cause: errStepAttemptsExhausted})}))
			continue
		case step.recovered:
			// An inline step whose attempt a delivery that stopped started
			// runs again here.
			starts = append(starts, e.stepEvent(worlds.EventTypeStepStarted, step.stepID, worlds.DataStepStarted{StepName: step.name, Attempt: step.attempt}))
			inline = append(inline, step)
			continue
		}
		runInline := step.explicit || running < step.limit && time.Since(e.startedAt) < step.duration &&
			(e.inlineUntil.IsZero() || time.Now().Before(e.inlineUntil))
		if runInline {
			running++
			starts = append(starts, e.stepEvent(worlds.EventTypeStepStarted, step.stepID, worlds.DataStepStarted{StepName: step.name, WorkflowName: e.run.WorkflowName, Attempt: 1, Input: step.input, Inline: true}))
			inline = append(inline, step)
			continue
		}
		created = append(created, e.stepEvent(worlds.EventTypeStepCreated, step.stepID, worlds.DataStepCreated{StepName: step.name, WorkflowName: e.run.WorkflowName, Input: step.input, CreatorMessageID: e.delivery.MessageID}))
		background = append(background, step)
	}
	for _, wait := range waits {
		timers = append(timers, e.stepEvent(worlds.EventTypeWaitCreated, worlds.StepID(wait.waitID), worlds.DataWaitCreated{ResumeAt: wait.dueAt, CreatorMessageID: e.delivery.MessageID}))
	}
	events := append(append(append(starts, created...), failed...), timers...)
	if len(events) == 0 {
		return
	}
	var committed map[string]bool
	if len(e.ahead) > 0 && len(created)+len(failed)+len(timers) == 0 && e.canRunAhead() {
		// Behind writes ahead, a boundary that only starts inline steps
		// runs ahead too.
		committed = e.writeBatchAhead(events)
	} else {
		committed = e.writeBatch(events)
	}
	for _, step := range background {
		if committed[string(step.stepID)] {
			message := stepMessage(step.stepID, step.name, 1, false)
			if len(step.input) <= worlds.MaxStepMessageInput {
				message.StepInput = step.input
			}
			if step.maxRetries == 0 {
				message.Retention = worlds.StepRetentionOnce
			}
			e.queue(message)
		}
	}
	for _, wait := range waits {
		if committed[string(wait.waitID)] {
			// The flow arms the timers of its chain when it ends.
			e.openWaits[wait.waitID] = wait
		}
	}
	if e.outcomes == nil {
		e.outcomes = make(chan inlineOutcome)
	}
	for _, step := range inline {
		if !committed[string(step.stepID)] {
			continue
		}
		e.inline++
		e.ranInline[step.stepID] = true
		go func() {
			result, err := step.run(step.attempt)
			e.outcomes <- inlineOutcome{step: step, result: result, err: err}
		}()
	}
}

// stepEvent returns the event of eventType for the step, wait, or hook id
// with data.
func (e *Execution) stepEvent(eventType worlds.EventType, id worlds.StepID, data any) worlds.EventsCreateEvent {
	encoded, err := json.Marshal(data)
	if err != nil {
		panic(e.exit(deliveryError{cause: err}))
	}
	return worlds.EventsCreateEvent{EventType: eventType, CorrelationID: string(id), Data: encoded}
}

// writeBatch appends events as one in-band write of the flow and adds them,
// with the events others appended before them, to the log. It returns the
// correlation IDs whose every event was appended. A refusal of the whole
// write ends the replay: a fence refusal so the Runtime delivers the flow's
// message again, and anything else so the queue retries it. A finished run's
// refusal of an event ends it too.
func (e *Execution) writeBatch(events []worlds.EventsCreateEvent) map[string]bool {
	e.drainAhead()
	now := time.Now().UTC().Truncate(time.Millisecond)
	for index := range events {
		events[index].OccurredAt = now
	}
	eventCount := len(e.events)
	result, err := e.world.EventsCreateBatch(e.ctx, worlds.EventsCreateBatchInput{RunID: e.run.RunID, InBandCount: e.InBandCount(), EventCount: &eventCount, Events: events})
	switch {
	case errors.Is(err, worlds.ErrInBandSuperseded):
		e.supersede()
	case err != nil:
		panic(e.exit(deliveryError{cause: err}))
	}
	e.inBand = result.InBandCount
	e.mergeEvents(result.Events)
	if result.MoreEvents {
		e.reload()
	}
	committed := make(map[string]bool, len(events))
	var refusal error
	for index, item := range result.Results {
		id := events[index].CorrelationID
		if item.Err != nil {
			committed[id] = false
			if refusal == nil {
				refusal = item.Err
			}
			continue
		}
		if _, seen := committed[id]; !seen {
			committed[id] = true
		}
		e.merge(worlds.EventResult{}, worlds.Event{RunID: e.run.RunID, EventID: item.EventID, EventType: item.EventType, CorrelationID: id, Data: events[index].Data, CreatedAt: item.CreatedAt})
	}
	if refusal != nil {
		// What committed is in the log for the next delivery, and what was
		// refused, such as a write to a finished run, is not retried here.
		panic(e.exit(deliveryError{cause: refusal}))
	}
	return committed
}

// LogPollInterval is how often a flow that waits for a step it runs inline
// reads the events others appended to the run's log, so a payload, a
// cancellation, or another step's result reaches the runner while the step
// runs, when its World does not push them (see FeedPollInterval).
var LogPollInterval = 2 * time.Second

// awaitInline waits for a step running inline to finish and records its
// result. Meanwhile it completes the timers that fall due, and takes what
// others append to the log from the World's feed or by reading it, returning as soon as either
// adds an event for the walk to deliver.
func (e *Execution) awaitInline() error {
	e.subscribe()
	e.catchUp()
	held := len(e.events)
	for len(e.events) == held {
		wait := time.Now().Add(e.pollInterval())
		var due *pendingWait
		for _, open := range e.openWaits {
			if open.dueAt.Before(wait) {
				wait, due = open.dueAt, &open
			}
		}
		timer := time.NewTimer(time.Until(wait))
		select {
		case outcome := <-e.outcomes:
			timer.Stop()
			e.inline--
			if outcome.err != nil {
				delete(e.ranInline, outcome.step.stepID)
				return outcome.err
			}
			e.confirmAhead(false)
			if outcome.result.RetryAfter == nil && e.canRunAhead() {
				// The runner reads the result now, and the write lands
				// behind the flow's other writes ahead.
				e.writeAhead(outcome.step, outcome.result)
				return nil
			}
			return e.completeInlineStep(outcome.step, outcome.result)
		case event := <-e.feedEvents():
			timer.Stop()
			if !e.superseded {
				e.accept(event)
			}
			continue
		case <-timer.C:
		}
		if e.superseded {
			continue
		}
		e.drainAhead()
		if len(e.events) != held {
			continue
		}
		if due != nil {
			e.completeWait(*due)
			continue
		}
		e.reload()
	}
	return nil
}

// recordNext waits for a step running inline to finish and records its
// result.
func (e *Execution) recordNext() error {
	outcome := <-e.outcomes
	e.inline--
	if outcome.err != nil {
		delete(e.ranInline, outcome.step.stepID)
		return outcome.err
	}
	return e.completeInlineStep(outcome.step, outcome.result)
}

// completeInlineStep records the result of a step that ran inline as an
// in-band write. A failed attempt that may retry is handed to a step
// delivery: its step_retrying names this flow's message as the one that
// moved it, and the step's message is queued once it commits.
func (e *Execution) completeInlineStep(step pendingStep, result worlds.DataStepResult) error {
	if e.superseded {
		return errReplaySuperseded
	}
	event, err := e.recordStepResult(step.stepID, step.attempt, result, true)
	if err != nil {
		return err
	}
	if event.EventType == worlds.EventTypeStepRetrying {
		delete(e.ranInline, step.stepID)
		message := stepMessage(step.stepID, step.name, step.attempt+1, false)
		message.NotBefore = result.RetryAfter
		if _, err := e.world.Queue(e.ctx, e.addressed(message)); err != nil {
			return err
		}
	}
	return nil
}

// addressed returns input addressed to the run's workflow and deployment.
func (e *Execution) addressed(input worlds.QueueInput) worlds.QueueInput {
	input.RunID = e.run.RunID
	input.WorkflowName = e.run.WorkflowName
	input.DeploymentID = e.run.DeploymentID
	input.RunSpecVersion = worlds.SpecVersion
	return input
}

// recordStepResult appends the result of attempt of the started step stepID
// and adds it with the events others appended since to the log: in-band for
// a step the flow ran inline, and out-of-band for a step delivery's. A
// terminal run records a retry as the step's final failure. It returns the
// recorded event, or the zero Event when the run ended before the result
// could be recorded or the step already has its result.
func (e *Execution) recordStepResult(stepID worlds.StepID, attempt int, result worlds.DataStepResult, inBand bool) (worlds.Event, error) {
	eventType := worlds.EventTypeStepCompleted
	if len(result.Error) != 0 {
		eventType = worlds.EventTypeStepFailed
		if result.RetryAfter != nil {
			eventType = worlds.EventTypeStepRetrying
		}
	}
	result.StepName, result.Attempt, result.WorkflowName = e.stepName(stepID, nil), 0, ""
	if eventType == worlds.EventTypeStepCompleted {
		result.WorkflowName = e.run.WorkflowName
	} else {
		result.Attempt = attempt
	}
	if eventType == worlds.EventTypeStepRetrying && inBand {
		result.CreatorMessageID = e.delivery.MessageID
	}
	payload, err := json.Marshal(result)
	if err != nil {
		return worlds.Event{}, err
	}
	eventCount := len(e.events)
	input := worlds.EventsCreateInput{
		RunID:   e.run.RunID,
		Event:   worlds.EventsCreateEvent{EventType: eventType, CorrelationID: string(stepID), Data: payload},
		Options: worlds.EventsCreateOptions{EventCount: &eventCount},
	}
	if inBand {
		e.drainAhead()
		eventCount = len(e.events)
		input.Options.EventCount = &eventCount
		count := e.inBand
		input.Options.InBandCount = &count
		input.Event.OccurredAt = time.Now().UTC().Truncate(time.Millisecond)
	}
	created, err := e.world.EventsCreate(e.ctx, input)
	if errors.Is(err, worlds.ErrRunTerminal) && eventType == worlds.EventTypeStepRetrying {
		// A terminal run permits the started step's final failure, but no retry.
		result.RetryAfter, result.CreatorMessageID = nil, ""
		if input.Event.Data, err = json.Marshal(result); err != nil {
			return worlds.Event{}, err
		}
		input.Event.EventType = worlds.EventTypeStepFailed
		created, err = e.world.EventsCreate(e.ctx, input)
	}
	switch {
	case errors.Is(err, worlds.ErrInBandSuperseded):
		e.superseded = true
		return worlds.Event{}, errReplaySuperseded
	case errors.Is(err, worlds.ErrRunTerminal) || errors.Is(err, worlds.ErrStepNotStartable):
		// The first writer of a step's result wins.
		delete(e.ranInline, stepID)
		return worlds.Event{}, nil
	case err != nil:
		return worlds.Event{}, err
	}
	if inBand {
		e.inBand = created.InBandCount
		if input.Event.EventType != worlds.EventTypeStepRetrying {
			delete(e.ranInline, stepID)
		}
	}
	event := worlds.Event{RunID: e.run.RunID, EventID: created.EventID, EventType: input.Event.EventType, CorrelationID: string(stepID), Data: input.Event.Data, CreatedAt: created.CreatedAt}
	e.merge(created, event)
	if created.MoreEvents {
		e.reload()
	}
	return event, nil
}

// stepName returns the name of step stepID, from created when it is not yet
// recorded, or from the event that created it.
func (e *Execution) stepName(stepID worlds.StepID, created *worlds.DataStepCreated) string {
	if created != nil {
		return created.StepName
	}
	return stepNameIn(e.events, stepID)
}

// stepNameIn returns the name the step_created, or creating step_started, of
// step stepID among events records, or "" when none does.
func stepNameIn(events []worlds.Event, stepID worlds.StepID) string {
	creation, ok := stepCreation(events, stepID)
	if !ok {
		return ""
	}
	return creation.name
}

// stepCreationEvent is the event that created a step: its step_created, or
// an inline step's first step_started.
type stepCreationEvent struct {
	index  int
	name   string
	input  []byte
	inline bool
}

// stepCreation returns the event among events that created step stepID.
func stepCreation(events []worlds.Event, stepID worlds.StepID) (stepCreationEvent, bool) {
	for index, event := range events {
		if event.CorrelationID != string(stepID) {
			continue
		}
		switch event.EventType {
		case worlds.EventTypeStepCreated:
			var data worlds.DataStepCreated
			_ = json.Unmarshal(event.Data, &data)
			return stepCreationEvent{index: index, name: data.StepName, input: data.Input, inline: data.Inline}, true
		case worlds.EventTypeStepStarted:
			if worlds.StartCreatesStep(event.Data) {
				var data worlds.DataStepStarted
				_ = json.Unmarshal(event.Data, &data)
				return stepCreationEvent{index: index, name: data.StepName, input: data.Input, inline: true}, true
			}
		}
	}
	return stepCreationEvent{}, false
}

// finishSteps waits for every step the flow runs inline to finish and records
// their results, once the runner has returned or stopped. A superseded flow
// records none.
func (e *Execution) finishSteps() (err error) {
	defer e.unsubscribe()
	defer func() {
		if recovered := recover(); recovered != nil {
			err = replayExit(recovered)
		}
	}()
	var errs []error
	for e.inline > 0 {
		if err := e.recordNext(); err != nil && !errors.Is(err, errReplaySuperseded) {
			errs = append(errs, err)
		}
	}
	if !e.superseded {
		e.drainAhead()
	} else {
		e.abandonAhead()
	}
	return errors.Join(errs...)
}

// EncodePayload encodes value as a payload of the run, encrypted when the
// run's payloads are.
func (e *Execution) EncodePayload(value any) ([]byte, error) {
	return worlds.EncodePayload(value, worlds.PayloadOptions{Keys: e.keys})
}

// DecodePayload decodes a payload of the run into value, a pointer.
func (e *Execution) DecodePayload(data []byte, value any) error {
	return worlds.DecodePayload(data, e.keys, value)
}

// encodeError encodes err as an error payload of the run.
func (e *Execution) encodeError(err error) []byte {
	// An Error value always encodes.
	data, _ := worlds.EncodeErrorPayload(err, worlds.PayloadOptions{Keys: e.keys})
	return data
}

// decodeError decodes an error payload of the run.
func (e *Execution) decodeError(data []byte) *worlds.Error {
	return worlds.DecodeErrorPayload(data, e.keys)
}

// stepArgs is a step's input as it is recorded: its argument list.
type stepArgs[In any] struct {
	Args [1]In `json:"args"`
}

// runFailedData returns the Data of a run_failed event for err.
func (e *Execution) runFailedData(err error) []byte {
	// An Error value always encodes.
	data, _ := worlds.EncodeRunFailed(err, worlds.PayloadOptions{Keys: e.keys})
	return data
}

// StreamFrame returns chunk as a frame of the run's stream, encrypted when
// the run's payloads are.
func (e *Execution) StreamFrame(chunk []byte) ([]byte, error) {
	return worlds.EncodeStreamFrame(chunk, e.keys, nil)
}

// WriteWebhookResponse writes response as the response to request, a manual
// webhook's request to the run, as WriteWebhookResponse does with the run's
// keys.
func (e *Execution) WriteWebhookResponse(ctx context.Context, request worlds.WebhookRequest, response worlds.WebhookResponse) error {
	return worlds.WriteWebhookResponse(ctx, e.world, e.run.RunID, request, response, e.keys)
}
