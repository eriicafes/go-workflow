package sdke2e

import (
	"context"
	"errors"
	"fmt"
	"time"

	workflow "github.com/eriicafes/go-workflow"
)

type supersedeResult struct {
	Role     string `json:"role"`
	Attempt  int    `json:"attempt"`
	Received string `json:"received"`
}

type hookSupersedeOwnerWorkflow struct {
	workflow.StructArgs[supersedeResult]
	Token string
}

func hookSupersedeOwnerRun(ctx context.Context, input hookSupersedeOwnerWorkflow) (supersedeResult, error) {
	for attempt := range 5 {
		result, claimed, err := supersedeOwner(ctx, input.Token, attempt)
		if err != nil || claimed {
			return result, err
		}
	}
	return supersedeResult{}, fmt.Errorf("Could not claim %s after cancelling the owner", input.Token)
}

// supersedeOwner claims token and waits for its payload, or cancels the run
// that holds it.
func supersedeOwner(ctx context.Context, token string, attempt int) (supersedeResult, bool, error) {
	hook := workflow.CreateHook[signalPayload](ctx, workflow.HookOption{Token: token})
	defer hook.Dispose(ctx)
	conflict, err := hook.GetConflict(ctx)
	if err != nil {
		return supersedeResult{}, false, err
	}
	if conflict == nil {
		payload, err := hook.Next(ctx)
		return supersedeResult{Role: "owner", Attempt: attempt, Received: payload.Message}, true, err
	}
	return supersedeResult{}, false, conflict.Cancel(ctx)
}

type forceClaimed struct {
	Role            string  `json:"role"`
	Received        string  `json:"received,omitempty"`
	ClaimedByRunID  string  `json:"claimedByRunId,omitempty"`
	ClaimedByHookID *string `json:"claimedByHookId,omitempty"`
	Token           string  `json:"token,omitempty"`
}

type hookForceClaimVictimWorkflow struct {
	workflow.StructArgs[forceClaimed]
	Token string
}

func hookForceClaimVictimRun(ctx context.Context, input hookForceClaimVictimWorkflow) (forceClaimed, error) {
	return awaitUnlessClaimed(ctx, workflow.HookOption{Token: input.Token})
}

type hookForceClaimVictimWorkflowForced struct {
	workflow.StructArgs[forceClaimed]
	Token string
}

func hookForceClaimVictimForcedRun(ctx context.Context, input hookForceClaimVictimWorkflowForced) (forceClaimed, error) {
	return awaitUnlessClaimed(ctx, workflow.HookOption{Token: input.Token, Force: true})
}

type claimerResult struct {
	Role     string  `json:"role"`
	Conflict *string `json:"conflict"`
	Received string  `json:"received"`
}

type hookForceClaimClaimerWorkflow struct {
	workflow.StructArgs[claimerResult]
	Token string
}

func hookForceClaimClaimerRun(ctx context.Context, input hookForceClaimClaimerWorkflow) (claimerResult, error) {
	// Not disposed: the run's completion releases the token, and the test
	// expects no hook_disposed in the claimer's log.
	hook := workflow.CreateHook[signalPayload](ctx, workflow.HookOption{Token: input.Token, Force: true})
	conflict, err := hook.GetConflict(ctx)
	if err != nil {
		return claimerResult{}, err
	}
	payload, err := hook.Next(ctx)
	if err != nil {
		return claimerResult{}, err
	}
	return claimerResult{Role: "claimer", Conflict: runID(conflict), Received: payload.Message}, nil
}

type numberPayload struct {
	N int `json:"n"`
}

type iteratingResult struct {
	Received       []int   `json:"received"`
	ClaimedByRunID *string `json:"claimedByRunId"`
}

type hookForceClaimIteratingVictimWorkflow struct {
	workflow.StructArgs[iteratingResult]
	Token string
}

func hookForceClaimIteratingVictimRun(ctx context.Context, input hookForceClaimIteratingVictimWorkflow) (iteratingResult, error) {
	hook := workflow.CreateHook[numberPayload](ctx, workflow.HookOption{Token: input.Token})
	defer hook.Dispose(ctx)
	received := []int{}
	for payload, err := range hook.Values(ctx) {
		var claimed *workflow.HookForceClaimedError
		if errors.As(err, &claimed) {
			by := string(claimed.RunID)
			return iteratingResult{Received: received, ClaimedByRunID: &by}, nil
		}
		if err != nil {
			return iteratingResult{}, err
		}
		received = append(received, payload.N)
	}
	return iteratingResult{Received: received}, nil
}

type collected struct {
	Received []int `json:"received"`
}

type hookForceClaimCollectorWorkflow struct {
	workflow.StructArgs[collected]
	Token string
	Count int
}

func hookForceClaimCollectorRun(ctx context.Context, input hookForceClaimCollectorWorkflow) (collected, error) {
	hook := workflow.CreateHook[numberPayload](ctx, workflow.HookOption{Token: input.Token, Force: true})
	defer hook.Dispose(ctx)
	received := []int{}
	for payload, err := range hook.Values(ctx) {
		if err != nil {
			return collected{}, err
		}
		received = append(received, payload.N)
		if len(received) >= input.Count {
			break
		}
	}
	return collected{Received: received}, nil
}

type receivedMessage struct {
	Received string `json:"received"`
}

type hookForceClaimRetainedVictimWorkflow struct {
	workflow.StructArgs[receivedMessage]
	Token string
}

func hookForceClaimRetainedVictimRun(ctx context.Context, input hookForceClaimRetainedVictimWorkflow) (receivedMessage, error) {
	// Left undisposed, so the token outlives the run.
	hook := workflow.CreateHook[signalPayload](ctx, workflow.HookOption{Token: input.Token, MinRetention: time.Hour})
	payload, err := hook.Next(ctx)
	return receivedMessage{Received: payload.Message}, err
}

type firstOutcome struct {
	OK             bool   `json:"ok"`
	Message        string `json:"message,omitempty"`
	ClaimedByRunID string `json:"claimedByRunId,omitempty"`
}

type ownHookResult struct {
	First  firstOutcome `json:"first"`
	Second string       `json:"second"`
}

type hookForceClaimOwnHookWorkflow struct {
	workflow.StructArgs[ownHookResult]
	Token string
}

func hookForceClaimOwnHookRun(ctx context.Context, input hookForceClaimOwnHookWorkflow) (ownHookResult, error) {
	first := workflow.CreateHook[signalPayload](ctx, workflow.HookOption{Token: input.Token})
	if _, err := first.GetConflict(ctx); err != nil {
		return ownHookResult{}, err
	}
	second := workflow.CreateHook[signalPayload](ctx, workflow.HookOption{Token: input.Token, Force: true})
	defer second.Dispose(ctx)
	payload, err := second.Next(ctx)
	if err != nil {
		return ownHookResult{}, err
	}
	var outcome firstOutcome
	firstPayload, err := first.Next(ctx)
	var claimed *workflow.HookForceClaimedError
	switch {
	case errors.As(err, &claimed):
		outcome.ClaimedByRunID = string(claimed.RunID)
	case err != nil:
		return ownHookResult{}, err
	default:
		outcome = firstOutcome{OK: true, Message: firstPayload.Message}
	}
	first.Dispose(ctx)
	return ownHookResult{First: outcome, Second: payload.Message}, nil
}

type tolerantResult struct {
	Role             string  `json:"role"`
	Received         string  `json:"received,omitempty"`
	ConflictingRunID *string `json:"conflictingRunId,omitempty"`
}

type hookForceClaimTolerantClaimerWorkflow struct {
	workflow.StructArgs[tolerantResult]
	Token string
}

func hookForceClaimTolerantClaimerRun(ctx context.Context, input hookForceClaimTolerantClaimerWorkflow) (tolerantResult, error) {
	hook := workflow.CreateHook[signalPayload](ctx, workflow.HookOption{Token: input.Token, Force: true})
	defer hook.Dispose(ctx)
	payload, err := hook.Next(ctx)
	var conflict *workflow.HookConflictError
	if errors.As(err, &conflict) {
		id := string(conflict.RunID)
		return tolerantResult{Role: "refused", ConflictingRunID: &id}, nil
	}
	if err != nil {
		return tolerantResult{}, err
	}
	return tolerantResult{Role: "claimer", Received: payload.Message}, nil
}

// awaitUnlessClaimed awaits one payload on a hook created with option, and
// reports a takeover of its token instead of failing.
func awaitUnlessClaimed(ctx context.Context, option workflow.HookOption) (forceClaimed, error) {
	hook := workflow.CreateHook[signalPayload](ctx, option)
	defer hook.Dispose(ctx)
	payload, err := hook.Next(ctx)
	var claimed *workflow.HookForceClaimedError
	if errors.As(err, &claimed) {
		result := forceClaimed{Role: "force_claimed", ClaimedByRunID: string(claimed.RunID), Token: claimed.Token}
		if claimed.HookID != "" {
			hookID := string(claimed.HookID)
			result.ClaimedByHookID = &hookID
		}
		return result, nil
	}
	if err != nil {
		return forceClaimed{}, err
	}
	return forceClaimed{Role: "owner", Received: payload.Message}, nil
}
