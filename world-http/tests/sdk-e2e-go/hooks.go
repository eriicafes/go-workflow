package sdke2e

import (
	"context"
	"time"

	workflow "github.com/eriicafes/go-workflow"
)

type hookPayload struct {
	Message    string `json:"message"`
	CustomData string `json:"customData"`
	Done       *bool  `json:"done,omitempty"`
}

// customData is the metadata the hook fixtures record with their hooks.
type customData struct {
	CustomData string `json:"customData"`
}

type hookWorkflow struct {
	workflow.Struct[[]hookPayload]
	workflow.Args
	Token      string
	CustomData string
}

func hookRun(ctx context.Context, input hookWorkflow) ([]hookPayload, error) {
	hook := workflow.CreateHook[hookPayload](ctx, workflow.HookOption{Token: input.Token, Metadata: customData{input.CustomData}})
	defer hook.Dispose(ctx)
	payloads := []hookPayload{}
	for payload, err := range hook.Values(ctx) {
		if err != nil {
			return nil, err
		}
		payloads = append(payloads, payload)
		if payload.Done != nil && *payload.Done {
			break
		}
	}
	return payloads, nil
}

type messagePayload struct {
	Message    string `json:"message"`
	CustomData string `json:"customData"`
}

type cleanupResult struct {
	Message             string `json:"message"`
	CustomData          string `json:"customData"`
	HookCleanupTestData string `json:"hookCleanupTestData"`
}

type hookCleanupTestWorkflow struct {
	workflow.Struct[cleanupResult]
	workflow.Args
	Token      string
	CustomData string
}

func hookCleanupTestRun(ctx context.Context, input hookCleanupTestWorkflow) (cleanupResult, error) {
	hook := workflow.CreateHook[messagePayload](ctx, workflow.HookOption{Token: input.Token, Metadata: customData{input.CustomData}})
	defer hook.Dispose(ctx)
	payload, err := hook.Next(ctx)
	if err != nil {
		return cleanupResult{}, err
	}
	return cleanupResult{Message: payload.Message, CustomData: payload.CustomData, HookCleanupTestData: "workflow_completed"}, nil
}

type disposeResult struct {
	Message             string `json:"message"`
	CustomData          string `json:"customData"`
	Disposed            bool   `json:"disposed"`
	HookDisposeTestData string `json:"hookDisposeTestData"`
}

type hookDisposeTestWorkflow struct {
	workflow.Struct[disposeResult]
	workflow.Args
	Token      string
	CustomData string
}

func hookDisposeTestRun(ctx context.Context, input hookDisposeTestWorkflow) (disposeResult, error) {
	hook := workflow.CreateHook[messagePayload](ctx, workflow.HookOption{Token: input.Token, Metadata: customData{input.CustomData}})
	payload, err := hook.Next(ctx)
	if err != nil {
		return disposeResult{}, err
	}
	// Releases the token before the sleep, so another run can claim it.
	hook.Dispose(ctx)
	if err := workflow.Sleep(ctx, 5*time.Second); err != nil {
		return disposeResult{}, err
	}
	return disposeResult{Message: payload.Message, CustomData: payload.CustomData, Disposed: true, HookDisposeTestData: "workflow_completed"}, nil
}

type conflictResult struct {
	Token                   string            `json:"token"`
	CustomData              string            `json:"customData"`
	ConflictRunID           *string           `json:"conflictRunId"`
	ConflictStatus          string            `json:"conflictStatus,omitempty"`
	StepResult              *conflictStepData `json:"stepResult,omitempty"`
	HookGetConflictTestData string            `json:"hookGetConflictTestData"`
}

type hookGetConflictWorkflow struct {
	workflow.Struct[conflictResult]
	workflow.Args
	Token      string
	CustomData string
}

func hookGetConflictRun(ctx context.Context, input hookGetConflictWorkflow) (conflictResult, error) {
	hook := workflow.CreateHook[struct{}](ctx, workflow.HookOption{Token: input.Token, Metadata: customData{input.CustomData}})
	defer hook.Dispose(ctx)
	conflict, err := hook.GetConflict(ctx)
	if err != nil {
		return conflictResult{}, err
	}
	result := conflictResult{Token: input.Token, CustomData: input.CustomData}
	if conflict != nil {
		status, err := conflict.Status(ctx)
		if err != nil {
			return conflictResult{}, err
		}
		id := conflict.ID()
		result.ConflictRunID, result.ConflictStatus = &id, string(status)
		result.HookGetConflictTestData = "hook_token_conflict_detected"
		return result, nil
	}
	result.HookGetConflictTestData = "hook_registered_without_payload"
	return result, nil
}

type hookGetConflictWithPriorStepWorkflow struct {
	workflow.Struct[conflictResult]
	workflow.Args
	Token      string
	CustomData string
}

func hookGetConflictWithPriorStepRun(ctx context.Context, input hookGetConflictWithPriorStepWorkflow) (conflictResult, error) {
	hook := workflow.CreateHook[struct{}](ctx, workflow.HookOption{Token: input.Token, Metadata: customData{input.CustomData}})
	defer hook.Dispose(ctx)
	step := workflow.Background(ctx, hookGetConflictStep, input.CustomData)
	conflict, err := hook.GetConflict(ctx)
	if err != nil {
		return conflictResult{}, err
	}
	stepResult, err := step.Value(ctx)
	if err != nil {
		return conflictResult{}, err
	}
	return conflictResult{
		Token:                   input.Token,
		CustomData:              input.CustomData,
		ConflictRunID:           runID(conflict),
		StepResult:              &stepResult,
		HookGetConflictTestData: "prior_step_completed_after_registration",
	}, nil
}

type hookGetConflictWithParallelStepWorkflow struct {
	workflow.Struct[conflictResult]
	workflow.Args
	Token      string
	CustomData string
}

func hookGetConflictWithParallelStepRun(ctx context.Context, input hookGetConflictWithParallelStepWorkflow) (conflictResult, error) {
	hook := workflow.CreateHook[struct{}](ctx, workflow.HookOption{Token: input.Token, Metadata: customData{input.CustomData}})
	defer hook.Dispose(ctx)
	// Go has no concurrent workflow branches, so the step starts first and
	// the claim is awaited while it runs.
	step := workflow.Background(ctx, hookGetConflictStep, input.CustomData)
	owner, err := hook.GetConflict(ctx)
	if err != nil {
		return conflictResult{}, err
	}
	stepResult, err := step.Value(ctx)
	if err != nil {
		return conflictResult{}, err
	}
	return conflictResult{
		Token:                   input.Token,
		CustomData:              input.CustomData,
		ConflictRunID:           runID(owner),
		StepResult:              &stepResult,
		HookGetConflictTestData: "parallel_step_completed_with_registration",
	}, nil
}

type parallelConflictResult struct {
	Token                   string    `json:"token"`
	CustomData              string    `json:"customData"`
	StepAResult             timedStep `json:"stepAResult"`
	StepBResult             timedStep `json:"stepBResult"`
	HookGetConflictTestData string    `json:"hookGetConflictTestData"`
}

type hookGetConflictThenStepParallelWorkflow struct {
	workflow.Struct[parallelConflictResult]
	workflow.Args
	Token      string
	CustomData string
}

func hookGetConflictThenStepParallelRun(ctx context.Context, input hookGetConflictThenStepParallelWorkflow) (parallelConflictResult, error) {
	hook := workflow.CreateHook[struct{}](ctx, workflow.HookOption{Token: input.Token, Metadata: customData{input.CustomData}})
	defer hook.Dispose(ctx)
	// Step B follows the claim, which settles while step A runs.
	if _, err := hook.GetConflict(ctx); err != nil {
		return parallelConflictResult{}, err
	}
	stepB := workflow.Background(ctx, hookGetConflictTimedStep, timedDelay{Label: "B", DelayMs: 100})
	stepA, err := workflow.Step(ctx, hookGetConflictTimedStep, timedDelay{Label: "A", DelayMs: 10_000})
	if err != nil {
		return parallelConflictResult{}, err
	}
	stepBResult, err := stepB.Value(ctx)
	if err != nil {
		return parallelConflictResult{}, err
	}
	return parallelConflictResult{
		Token:                   input.Token,
		CustomData:              input.CustomData,
		StepAResult:             stepA,
		StepBResult:             stepBResult,
		HookGetConflictTestData: "registration_then_step_runs_in_parallel",
	}, nil
}

type retentionResult struct {
	Role           string  `json:"role"`
	ConflictRunID  *string `json:"conflictRunId,omitempty"`
	ConflictStatus string  `json:"conflictStatus,omitempty"`
}

type hookMinRetentionWorkflow struct {
	workflow.Struct[retentionResult]
	workflow.Args
	Token          string
	MinRetentionMs int
}

func hookMinRetentionRun(ctx context.Context, input hookMinRetentionWorkflow) (retentionResult, error) {
	// Left undisposed, so the token stays reserved after the run ends.
	hook := workflow.CreateHook[struct{}](ctx, workflow.HookOption{Token: input.Token, MinRetention: time.Duration(input.MinRetentionMs) * time.Millisecond})
	conflict, err := hook.GetConflict(ctx)
	if err != nil {
		return retentionResult{}, err
	}
	if conflict != nil {
		status, err := conflict.Status(ctx)
		if err != nil {
			return retentionResult{}, err
		}
		return retentionResult{Role: "duplicate", ConflictRunID: runID(conflict), ConflictStatus: string(status)}, nil
	}
	return retentionResult{Role: "owner"}, nil
}

type mutexResult struct {
	Role          string  `json:"role"`
	ConflictRunID *string `json:"conflictRunId,omitempty"`
	WorkEndedAt   int64   `json:"workEndedAt,omitempty"`
}

type hookClaimOnlyMutexWorkflow struct {
	workflow.Struct[mutexResult]
	workflow.Args
	Token  string
	HoldMs int
}

func hookClaimOnlyMutexRun(ctx context.Context, input hookClaimOnlyMutexWorkflow) (mutexResult, error) {
	hook := workflow.CreateHook[struct{}](ctx, workflow.HookOption{Token: input.Token})
	defer hook.Dispose(ctx)
	conflict, err := hook.GetConflict(ctx)
	if err != nil {
		return mutexResult{}, err
	}
	if conflict != nil {
		return mutexResult{Role: "duplicate", ConflictRunID: runID(conflict)}, nil
	}
	work, err := workflow.Step(ctx, hookGetConflictTimedStep, timedDelay{Label: "A", DelayMs: input.HoldMs})
	if err != nil {
		return mutexResult{}, err
	}
	return mutexResult{Role: "owner", WorkEndedAt: work.EndedAt}, nil
}

type valuePayload struct {
	Value string `json:"value"`
}

type adoptResult struct {
	Role          string  `json:"role"`
	ConflictRunID *string `json:"conflictRunId,omitempty"`
	Adopted       any     `json:"adopted,omitempty"`
	Marker        string  `json:"marker,omitempty"`
	Value         string  `json:"value,omitempty"`
}

type hookAdoptOwnerResultWorkflow struct {
	workflow.Struct[adoptResult]
	workflow.Args
	Token  string
	Marker string
}

func hookAdoptOwnerResultRun(ctx context.Context, input hookAdoptOwnerResultWorkflow) (adoptResult, error) {
	hook := workflow.CreateHook[valuePayload](ctx, workflow.HookOption{Token: input.Token})
	defer hook.Dispose(ctx)
	conflict, err := hook.GetConflict(ctx)
	if err != nil {
		return adoptResult{}, err
	}
	if conflict != nil {
		adopted, err := workflow.Step(ctx, awaitWorkflowResult, conflict.ID())
		if err != nil {
			return adoptResult{}, err
		}
		return adoptResult{Role: "duplicate", ConflictRunID: runID(conflict), Adopted: adopted}, nil
	}
	payload, err := hook.Next(ctx)
	if err != nil {
		return adoptResult{}, err
	}
	return adoptResult{Role: "owner", Marker: input.Marker, Value: payload.Value}, nil
}

type signalPayload struct {
	Message string `json:"message"`
}

type signalResult struct {
	Role        string `json:"role"`
	ForwardedTo string `json:"forwardedTo,omitempty"`
	Received    string `json:"received,omitempty"`
}

type hookSignalOwnerWorkflow struct {
	workflow.Struct[signalResult]
	workflow.Args
	Token   string
	Message string
}

func hookSignalOwnerRun(ctx context.Context, input hookSignalOwnerWorkflow) (signalResult, error) {
	hook := workflow.CreateHook[signalPayload](ctx, workflow.HookOption{Token: input.Token})
	defer hook.Dispose(ctx)
	conflict, err := hook.GetConflict(ctx)
	if err != nil {
		return signalResult{}, err
	}
	if conflict != nil {
		if _, err := workflow.Step(ctx, forwardPayloadToOwner, tokenMessage{Token: input.Token, Message: input.Message}); err != nil {
			return signalResult{}, err
		}
		return signalResult{Role: "duplicate", ForwardedTo: conflict.ID()}, nil
	}
	payload, err := hook.Next(ctx)
	if err != nil {
		return signalResult{}, err
	}
	return signalResult{Role: "owner", Received: payload.Message}, nil
}

type reuseResult struct {
	Received      []string `json:"received"`
	ConflictRound *int     `json:"conflictRound"`
}

type hookTokenReuseLoopWorkflow struct {
	workflow.Struct[reuseResult]
	workflow.Args
	Token  string
	Rounds int
}

func hookTokenReuseLoopRun(ctx context.Context, input hookTokenReuseLoopWorkflow) (reuseResult, error) {
	received := []string{}
	for round := range input.Rounds {
		hook := workflow.CreateHook[signalPayload](ctx, workflow.HookOption{Token: input.Token})
		conflict, err := hook.GetConflict(ctx)
		if err != nil {
			return reuseResult{}, err
		}
		if conflict != nil {
			return reuseResult{Received: received, ConflictRound: &round}, nil
		}
		payload, err := hook.Next(ctx)
		if err != nil {
			return reuseResult{}, err
		}
		received = append(received, payload.Message)
		hook.Dispose(ctx)
	}
	return reuseResult{Received: received}, nil
}

type sleepHookPayload struct {
	Type string `json:"type"`
	ID   *int   `json:"id,omitempty"`
	Done *bool  `json:"done,omitempty"`
}

type hookWithSleepWorkflow struct {
	workflow.Struct[[]processed]
	workflow.Args
	Token string
}

func hookWithSleepRun(ctx context.Context, input hookWithSleepWorkflow) ([]processed, error) {
	hook := workflow.CreateHook[sleepHookPayload](ctx, workflow.HookOption{Token: input.Token})
	defer hook.Dispose(ctx)
	// A sleep that does not end during the test.
	workflow.Timer(ctx, 24*time.Hour)
	results := []processed{}
	for payload, err := range hook.Values(ctx) {
		if err != nil {
			return nil, err
		}
		result, err := workflow.Step(ctx, processPayload, payload)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
		if payload.Done != nil && *payload.Done {
			break
		}
	}
	return results, nil
}

type finalStepResult struct {
	Seen        []int      `json:"seen"`
	FinalResult *processed `json:"finalResult"`
}

type hookWithSleepFinalStepWorkflow struct {
	workflow.Struct[finalStepResult]
	workflow.Args
	Token string
}

func hookWithSleepFinalStepRun(ctx context.Context, input hookWithSleepFinalStepWorkflow) (finalStepResult, error) {
	hook := workflow.CreateHook[sleepHookPayload](ctx, workflow.HookOption{Token: input.Token})
	defer hook.Dispose(ctx)
	workflow.Timer(ctx, 24*time.Hour)
	result := finalStepResult{Seen: []int{}}
	for payload, err := range hook.Values(ctx) {
		if err != nil {
			return finalStepResult{}, err
		}
		if payload.ID != nil {
			result.Seen = append(result.Seen, *payload.ID)
		}
		if payload.Done != nil && *payload.Done {
			final, err := workflow.Step(ctx, processPayload, payload)
			if err != nil {
				return finalStepResult{}, err
			}
			result.FinalResult = &final
			break
		}
	}
	return result, nil
}

type interleaved struct {
	A      int    `json:"a"`
	B      int    `json:"b"`
	C      int    `json:"c"`
	D      int    `json:"d"`
	E      int    `json:"e"`
	F      int    `json:"f"`
	Winner string `json:"winner"`
	G      int    `json:"g"`
	H      int    `json:"h"`
}

type deltaPayload struct {
	Delta int `json:"delta"`
}

type retainedInterleavingWorkflow struct {
	workflow.Struct[interleaved]
	workflow.Args
	Token string
}

func retainedInterleavingRun(ctx context.Context, input retainedInterleavingWorkflow) (interleaved, error) {
	var out interleaved
	var err error
	if out.A, err = workflow.Step(ctx, add, pair{1, 2}); err != nil {
		return out, err
	}
	if out.B, err = workflow.Step(ctx, unwrapValue, box{out.A}); err != nil {
		return out, err
	}
	c := workflow.Background(ctx, add, pair{out.B, 10})
	d := workflow.Background(ctx, add, pair{out.B, 20})
	if out.C, err = c.Value(ctx); err != nil {
		return out, err
	}
	if out.D, err = d.Value(ctx); err != nil {
		return out, err
	}
	e := workflow.Background(ctx, unwrapValue, box{out.C})
	f := workflow.Background(ctx, add, pair{out.D, 1})
	if out.E, err = e.Value(ctx); err != nil {
		return out, err
	}
	if out.F, err = f.Value(ctx); err != nil {
		return out, err
	}
	if out.Winner, err = raceSleep(ctx, workflow.Background(ctx, delayMsStep, delayed{100, "step"}), 30*time.Second); err != nil {
		return out, err
	}
	if err := workflow.Sleep(ctx, time.Second); err != nil {
		return out, err
	}
	hook := workflow.CreateHook[deltaPayload](ctx, workflow.HookOption{Token: input.Token})
	defer hook.Dispose(ctx)
	g := workflow.Background(ctx, add, pair{out.E + out.F, 100})
	if err := workflow.Join(ctx, hook, g); err != nil {
		return out, err
	}
	delta, err := hook.Next(ctx)
	if err != nil {
		return out, err
	}
	if out.G, err = g.Value(ctx); err != nil {
		return out, err
	}
	if out.H, err = workflow.Step(ctx, add, pair{out.G, delta.Delta}); err != nil {
		return out, err
	}
	return out, nil
}

type raceOutcome struct {
	First  string `json:"first"`
	Second string `json:"second"`
}

type hookRaceAfterLostRaceWorkflow struct {
	workflow.Struct[raceOutcome]
	workflow.Args
	Token string
}

func hookRaceAfterLostRaceRun(ctx context.Context, input hookRaceAfterLostRaceWorkflow) (raceOutcome, error) {
	hook := workflow.CreateHook[valuePayload](ctx, workflow.HookOption{Token: input.Token})
	defer hook.Dispose(ctx)
	first, err := raceHook(ctx, hook, time.Second, "sleep")
	if err != nil {
		return raceOutcome{}, err
	}
	if first != "sleep" {
		first = "hook"
	}
	second, err := raceHook(ctx, hook, 30*time.Second, "timeout")
	return raceOutcome{First: first, Second: second}, err
}

type lostRaces struct {
	Lost      []string `json:"lost"`
	Delivered string   `json:"delivered"`
	Next      string   `json:"next"`
}

type hookRaceLoopAfterLostRacesWorkflow struct {
	workflow.Struct[lostRaces]
	workflow.Args
	Token string
}

func hookRaceLoopAfterLostRacesRun(ctx context.Context, input hookRaceLoopAfterLostRacesWorkflow) (lostRaces, error) {
	hook := workflow.CreateHook[valuePayload](ctx, workflow.HookOption{Token: input.Token})
	defer hook.Dispose(ctx)
	var result lostRaces
	for range 2 {
		lost, err := raceHook(ctx, hook, time.Second, "sleep")
		if err != nil {
			return lostRaces{}, err
		}
		result.Lost = append(result.Lost, lost)
	}
	var err error
	if result.Delivered, err = raceHook(ctx, hook, 30*time.Second, "timeout"); err != nil {
		return lostRaces{}, err
	}
	result.Next, err = raceHook(ctx, hook, 30*time.Second, "timeout")
	return result, err
}

type hookRaceOncePromiseWorkflow struct {
	workflow.Struct[raceOutcome]
	workflow.Args
	Token string
}

func hookRaceOncePromiseRun(ctx context.Context, input hookRaceOncePromiseWorkflow) (raceOutcome, error) {
	hook := workflow.CreateHook[valuePayload](ctx, workflow.HookOption{Token: input.Token})
	defer hook.Dispose(ctx)
	first, err := raceHook(ctx, hook, time.Second, "timeout")
	if err != nil {
		return raceOutcome{}, err
	}
	if _, err := workflow.Step(ctx, hookRaceReminderStep, struct{}{}); err != nil {
		return raceOutcome{}, err
	}
	second, err := raceHook(ctx, hook, 30*time.Second, "timeout")
	return raceOutcome{First: first, Second: second}, err
}

// raceHook returns the hook's next payload value when it arrives before a
// sleep of d, and timeout otherwise.
func raceHook(ctx context.Context, hook *workflow.Hook[valuePayload], d time.Duration, timeout string) (string, error) {
	timer := workflow.Timer(ctx, d)
	winner, err := workflow.Select(ctx, hook, timer)
	if err != nil {
		return "", err
	}
	if winner == timer {
		return timeout, nil
	}
	payload, err := hook.Next(ctx)
	return payload.Value, err
}

// raceSleep returns the step's value when it settles before a sleep of d,
// and "sleep" otherwise.
func raceSleep(ctx context.Context, step *workflow.Result[string], d time.Duration) (string, error) {
	timer := workflow.Timer(ctx, d)
	winner, err := workflow.Select(ctx, step, timer)
	if err != nil {
		return "", err
	}
	if winner == timer {
		return "sleep", nil
	}
	return step.Value(ctx)
}

func runID(run *workflow.Run[any]) *string {
	if run == nil {
		return nil
	}
	id := run.ID()
	return &id
}
