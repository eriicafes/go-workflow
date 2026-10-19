package workflow

import (
	"context"
	"errors"

	"github.com/eriicafes/go-workflow/worlds"
)

// Go starts a background step and adds its result to group.
// A completed group does not track the new step.
func Go[In, Out any](ctx context.Context, g group, step func(context.Context, In) (Out, error), input In, options ...StepOptions) *Result[Out] {
	result := Background(ctx, step, input, options...)
	g.Add(result)
	return result
}

type group interface {
	Add(...promise)
}

// WaitGroup waits for all its members, ignoring operation failures.
// Its zero value is ready to use. After completion, Add ignores new members.
// Create a new WaitGroup to wait for more work.
type WaitGroup struct{ groupState }

// Wait waits for every member to settle.
func (g *WaitGroup) Wait(ctx context.Context) error {
	_, err := Select(ctx, g)
	return err
}

func (g *WaitGroup) done(ctx context.Context) (bool, error, error) {
	settled, _, err := g.groupState.done(ctx)
	return settled, nil, err
}

// ErrGroup waits for all its members and returns their first recorded failure.
// Its zero value is ready to use without cancellation. ErrGroupWithContext
// enables cancellation. After completion, the outcome stays fixed and Add ignores
// new members. Create a new ErrGroup to wait for more work.
type ErrGroup struct{ groupState }

// ErrGroupWithContext returns an ErrGroup and a durable cancellation context.
// The group begins observing members when first awaited. Its context is
// cancelled on the first observed failure or when Wait returns.
// It returns ErrNoWorkflow outside workflow context.
func ErrGroupWithContext(ctx context.Context) (*ErrGroup, context.Context, error) {
	groupCtx, cancel, err := WithCancelCause(ctx)
	if err != nil {
		return nil, nil, err
	}
	return &ErrGroup{groupState: groupState{cancel: cancel}}, groupCtx, nil
}

// Wait waits for every member, returns the first failure, and cancels the
// context returned by ErrGroupWithContext, including on success.
func (g *ErrGroup) Wait(ctx context.Context) error {
	_, err := Select(ctx, g)
	if g.cancel != nil && !g.cancelled {
		g.cancelled = true
		err = errors.Join(err, g.cancel(err))
	}
	return err
}

// groupState holds the shared settlement state of WaitGroup and ErrGroup.
type groupState struct {
	members    []promise
	execution  *worlds.Execution
	settled    bool
	id         string
	err        error
	inspectErr error
	cancel     func(error) error
	cancelled  bool
	observing  bool
}

// Add includes existing results, hooks, webhooks, or groups in the wait.
// It does not change their cancellation contexts and ignores new members after settlement.
func (g *groupState) Add(members ...promise) {
	if !g.settled {
		g.members = append(g.members, members...)
	}
}

func (g *groupState) settlementID() (string, bool) { return g.id, g.id != "" }

func (g *groupState) done(ctx context.Context) (bool, error, error) {
	if g.execution == nil {
		g.execution, _ = workflowExecutionFromContext(ctx)
		if g.cancel != nil {
			g.execution.OnAdvance(func() { g.poll(ctx) })
		}
	}
	if !g.settled {
		g.settled = g.poll(ctx)
	}
	return g.settled, g.err, g.inspectErr
}

// poll observes members without completing the group until it is awaited.
// The cancellation callback can re-enter replay, so only one observation runs
// at a time.
func (g *groupState) poll(ctx context.Context) bool {
	if g.settled || g.observing {
		return g.settled
	}
	g.observing = true
	defer func() { g.observing = false }()

	allSettled := true
	var latest, firstFailure uint64
	var failure error
	// Checking a member can run a continuation that adds more work to the group.
	// Read the current length each iteration so that work is checked too.
	for i := 0; i < len(g.members); i++ {
		member := g.members[i]
		settled, memberErr, err := member.done(ctx)
		if err != nil {
			g.inspectErr = err
			return false
		}
		if !settled {
			allSettled = false
			continue
		}
		var slot uint64
		if id, ok := member.settlementID(); ok {
			if event, found := g.execution.SettledEvent(id); found {
				slot, _ = worlds.EventSlot(event.EventID)
				if slot > latest {
					latest, g.id = slot, string(event.EventID)
				}
			}
		}
		if memberErr != nil && (failure == nil || slot < firstFailure) {
			failure, firstFailure = memberErr, slot
		}
	}
	if g.err == nil {
		g.err = failure
	}
	if g.err != nil && g.cancel != nil && !g.cancelled {
		g.cancelled = true
		if err := g.cancel(g.err); err != nil {
			g.inspectErr = errors.Join(g.err, err)
		}
	}
	return allSettled
}
