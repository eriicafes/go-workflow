// Package worlds defines the World contract that go-workflow records runs in,
// the Client, and the HTTP server that exposes a World to clients. Package
// core executes deliveries against a World.
//
// Each run is an append-only event log, and the runs, steps, and hooks returned
// by Storage are views derived from those events.
package worlds

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"
)

// World is the backend a run is recorded in. It provides event storage, a
// delivery queue, and streams. The runtime, the client, and HTTPHandler all
// operate on a World.
type World interface {
	Storage
	Queue
	Streamer
	// BaseURL returns the absolute public URL where HTTPHandler is mounted,
	// including its workflow path prefix and without a trailing slash.
	BaseURL() string
	// Analytics returns the Analytics interface when analytics are enabled,
	// otherwise nil.
	Analytics() Analytics
	// Capabilities reports the optional behavior this World implements.
	Capabilities() Capabilities
	// DeploymentID returns the deployment whose code this World's deliveries
	// execute, or "" when the World has no deployments. Runs created through
	// the World's Client record it, and the Runtime enforces it only when
	// Capabilities reports DeploymentAffinity.
	DeploymentID() string
}

// Capabilities reports optional World behavior. A capability being false does
// not remove a method from World.
type Capabilities struct {
	// Analytics reports that the World collects analytics, so Analytics
	// returns them rather than nil.
	Analytics bool `cbor:"analytics"`
	// SetAttributes reports that the World stores attribute writes. Without
	// it, they are successful no-ops.
	SetAttributes bool `cbor:"setAttributes"`
	// DeploymentAffinity reports that the World's deployments are atomic and
	// its queue can address a delivery to one. Runs created through the
	// World's Client are then pinned to World.DeploymentID, and the Runtime
	// replays a run only on the deployment it is pinned to, re-routing other
	// deliveries there.
	DeploymentAffinity bool `cbor:"deploymentAffinity"`
	// DeleteData reports that the World implements RunsDeleteDataMany, which
	// is optional.
	DeleteData bool `cbor:"deleteData"`
	// StartHook reports that EventsCreateRun creates a run's StartHook with
	// it.
	StartHook bool `cbor:"startHook"`
	// RegionalRunIDs reports that the World implements RegionalRunIDs, so
	// clients ask it for new run IDs (Client.RunsNewID).
	RegionalRunIDs bool `cbor:"regionalRunIds"`
	// MaxQueueDelay is the furthest NotBefore the World's queue accepts
	// from now. The runtime reaches a later timer in hops. Zero means no
	// limit.
	MaxQueueDelay time.Duration `cbor:"maxQueueDelay"`
	// HookResumeDedup reports that the World deduplicates a hook_received
	// by its EventsCreateOptions.ResumeID.
	HookResumeDedup bool `cbor:"hookResumeDedup"`
}

// LatestDeployments is implemented by a World with deployment affinity that
// tracks its newest deployment. Runs started with LatestDeployment are pinned
// to LatestDeploymentID instead of the deployment that starts them, so a chain
// of runs that each start the next moves to new code as it is deployed.
type LatestDeployments interface {
	// LatestDeploymentID returns the newest deployment this World's
	// deployment knows of, or "" when it tracks none.
	LatestDeploymentID(context.Context) (string, error)
}

// Region is the region a run's data lives in and its deliveries are routed
// to. Its values belong to the World that uses it, and the empty Region
// leaves it to the World. Only a World that implements RegionalRunIDs uses
// it; the others ignore it.
type Region string

// RegionalRunIDs is implemented by a World whose run IDs carry the region the
// run's data lives in.
type RegionalRunIDs interface {
	// RegionRunID returns id tagged with region, or with the World's own
	// region for the empty Region. It is deterministic, so a child run's ID
	// derived from its start step stays the same across attempts.
	RegionRunID(id RunID, region Region) RunID
	// RegionForRunID returns the region a run's data lives in, from its ID
	// alone, and false when id is not a run ID.
	RegionForRunID(id RunID) (Region, bool)
}

// RegionRunID returns id tagged with region when world implements
// RegionalRunIDs, and id unchanged otherwise.
func RegionRunID(world World, id RunID, region Region) RunID {
	if regional, ok := world.(RegionalRunIDs); ok {
		return regional.RegionRunID(id, region)
	}
	return id
}

// NormalizeBaseURL validates an HTTP handler's public URL and removes its
// trailing slash.
func NormalizeBaseURL(value string) (string, error) {
	parsed, err := url.ParseRequestURI(value)
	if err != nil || parsed.Host == "" || parsed.User != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.ForceQuery || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", fmt.Errorf("workflow: base URL must be an absolute HTTP URL without a query or fragment")
	}
	return strings.TrimRight(value, "/"), nil
}

// Status is the lifecycle state of a run or step.
type Status string

const (
	// StatusPending means the run or step has been created but not started.
	StatusPending Status = "pending"
	// StatusRunning means a delivery has started the run or step.
	StatusRunning Status = "running"
	// StatusCompleted means the run or step finished with an output.
	StatusCompleted Status = "completed"
	// StatusFailed means the run or step finished with an error.
	StatusFailed Status = "failed"
	// StatusCancelled means the run or step was cancelled.
	StatusCancelled Status = "cancelled"
)

// Terminal reports whether s is completed, failed, or cancelled. A run or
// step in a terminal status accepts no further lifecycle events.
func (s Status) Terminal() bool {
	return s == StatusCompleted || s == StatusFailed || s == StatusCancelled
}

// ResolveData selects whether Storage returns the payloads of runs, steps,
// and hooks, which are run and step Input and Output, and hook Metadata. The
// zero value behaves like ResolveDataAll.
type ResolveData string

const (
	// ResolveDataAll returns payloads.
	ResolveDataAll ResolveData = "all"
	// ResolveDataNone omits payloads.
	ResolveDataNone ResolveData = "none"
)

// EventsResolveData selects which event payloads Storage returns. The zero
// value behaves like EventsResolveDataAll.
type EventsResolveData string

const (
	// EventsResolveDataAll returns every event's Data.
	EventsResolveDataAll EventsResolveData = "all"
	// EventsResolveDataNone omits payloads from event Data and keeps its
	// metadata, as ResolveEventData describes.
	EventsResolveDataNone EventsResolveData = "none"
	// EventsResolveDataSkipStepInputs requests event Data without the step
	// input recorded in step_created events. A World may treat it like
	// EventsResolveDataAll.
	EventsResolveDataSkipStepInputs EventsResolveData = "skip-step-inputs"
)

// SortOrder is the order in which a list returns its items. The zero value
// behaves like SortOrderAscending.
type SortOrder string

const (
	// SortOrderAscending returns the oldest items first.
	SortOrderAscending SortOrder = "asc"
	// SortOrderDescending returns the newest items first.
	SortOrderDescending SortOrder = "desc"
)

// RunID identifies a run. Generated run IDs have the prefix "wrun_".
type RunID string

// StepID identifies a step within its run. Generated step IDs have the
// prefix "step_".
type StepID string

// HookID identifies a hook. Generated hook IDs have the prefix "hook_".
type HookID string

// WaitID identifies a wait, the record of a timer. Generated wait IDs have
// the prefix "wait_".
type WaitID string

// EventID identifies an event within its run's event log. It encodes the
// event's position in the log (see EventIDForSlot and EventSlot).
type EventID string

// NewRunID returns a new run ID: wrun_ and a ULID.
func NewRunID() RunID { return RunID("wrun_" + ulid.Make().String()) }

// NewChildRunID returns the run ID of the run started by step stepID: wrun_
// and the step ID's ULID, so every attempt of the step derives the same ID.
func NewChildRunID(stepID StepID) RunID {
	return RunID("wrun_" + strings.TrimPrefix(string(stepID), "step_"))
}

// NewStreamName returns a new stream name: strm_ and a ULID.
func NewStreamName() string { return "strm_" + ulid.Make().String() }

// NewWriterID returns a new stream writer ID: wrtr_ and a ULID.
func NewWriterID() string { return "wrtr_" + ulid.Make().String() }

// HookTokenAlphabet is Nano ID's URL alphabet, whose 64 characters each take
// six bits of a random byte.
const HookTokenAlphabet = "useandom-26T198340PX75pxJACKVERYMINDBUSHWOLF_GQZbfghjklqvwyzrict"

// NewHookToken returns a new hook token: 21 random characters of Nano ID's
// URL alphabet.
func NewHookToken() string {
	var random [21]byte
	if _, err := rand.Read(random[:]); err != nil {
		panic(err)
	}
	token := make([]byte, len(random))
	for index, value := range random {
		token[index] = HookTokenAlphabet[value&63]
	}
	return string(token)
}

type (
	// Pagination selects one page of a list.
	Pagination struct {
		// Cursor is the Cursor of the previous Page. Empty starts at the
		// first item.
		Cursor string
		// Limit is the maximum number of items to return. Zero or less
		// requests every remaining item, though a World may still return
		// fewer and set HasMore.
		Limit     int
		SortOrder SortOrder
	}
	// Page is one page of a list.
	Page[T any] struct {
		Data []T
		// Cursor continues the list after this page when passed back in
		// Pagination.
		Cursor string
		// HasMore reports whether items remain after this page.
		HasMore bool
	}
)

// WakeRun queues a flow of a run that completes the selected pending waits
// before it replays the run. correlationIDs holds wait IDs, and an empty list
// selects every pending wait. It returns the number of waits the flow
// completes. It queues the flow even when that number is zero while the run
// is not terminal, so retrying a call whose queueing failed still wakes the
// run.
func WakeRun(ctx context.Context, world World, runID RunID, correlationIDs []string) (int, error) {
	pending, err := PendingWaits(ctx, world, runID)
	if err != nil {
		return 0, err
	}
	var selected []string
	for _, waitID := range pending {
		if len(correlationIDs) == 0 || slices.Contains(correlationIDs, waitID) {
			selected = append(selected, waitID)
		}
	}
	queue, err := RunDelivery(ctx, world, runID)
	if err != nil {
		return 0, err
	}
	if len(selected) == 0 {
		run, err := world.RunsGet(ctx, RunsGetInput{RunID: runID, ResolveData: ResolveDataNone})
		if err != nil || run.Status.Terminal() {
			return 0, err
		}
	}
	queue.CompleteWaits = selected
	_, err = world.Queue(ctx, queue)
	if err != nil {
		return 0, err
	}
	return len(selected), nil
}

// FirstFlow returns the delivery that starts the run input creates, pinned
// to its deployment, keyed by the run so a retried start queues it once, and
// carrying the run's creation.
func FirstFlow(input EventsCreateRunInput) QueueInput {
	run := input
	return QueueInput{RunID: input.RunID, WorkflowName: input.WorkflowName, IdempotencyKey: string(input.RunID), DeploymentID: input.DeploymentID, RunSpecVersion: SpecVersion, RunInput: &run}
}

// StartRun creates the run input describes and queues its first flow at the
// same time, as the Workflow SDK's start does. A failure to queue fails the
// start. A creation that fails retryably does not: the queued flow's
// run_started creates the run. Nor does a run that exists already, which the
// queued flow's run_started may have created first.
func StartRun(ctx context.Context, world World, input EventsCreateRunInput) error {
	created := make(chan error, 1)
	go func() {
		_, err := world.EventsCreateRun(ctx, input)
		created <- err
	}()
	_, queueErr := world.Queue(ctx, FirstFlow(input))
	createErr := <-created
	if queueErr != nil {
		return queueErr
	}
	if createErr != nil && (IsRetryableWorldError(createErr) || errors.Is(createErr, ErrRunAlreadyExists)) {
		return nil
	}
	return createErr
}

// IsRetryableWorldError reports whether a World's err is a transient one, a
// throttle, a deadline, a network failure, or a server error, that the same
// write could succeed after.
func IsRetryableWorldError(err error) bool {
	var throttle *ThrottleError
	var network net.Error
	var status interface{ HTTPStatus() int }
	switch {
	case errors.As(err, &throttle), errors.Is(err, context.DeadlineExceeded), errors.As(err, &network):
		return true
	case errors.As(err, &status):
		return status.HTTPStatus() >= 500
	}
	return false
}

// CreateRunForStart creates the run of a run_started whose data carries the
// run's creation, when the run does not exist: the first flow of a start
// whose run_created never landed. A World that implements EventsCreate calls
// it for every run_started and stores the event without its data.
func CreateRunForStart(ctx context.Context, world World, runID RunID, data []byte) error {
	if len(data) == 0 {
		return nil
	}
	var created DataRunCreated
	if json.Unmarshal(data, &created) != nil || created.WorkflowName == "" {
		return nil
	}
	if _, err := world.RunsGet(ctx, RunsGetInput{RunID: runID, ResolveData: ResolveDataNone}); !errors.Is(err, ErrRunNotFound) {
		return nil
	}
	_, err := world.EventsCreateRun(ctx, EventsCreateRunInput{RunID: runID, WorkflowName: created.WorkflowName, Input: created.Input, Attributes: created.Attributes, DeploymentID: created.DeploymentID, EncryptionPublicKey: created.EncryptionPublicKey, ExecutionContext: created.ExecutionContext, StartHook: created.StartHook})
	if errors.Is(err, ErrRunAlreadyExists) {
		return nil
	}
	return err
}

// PendingWaits returns the IDs of run runID's waits that have not completed,
// in the order they were created.
func PendingWaits(ctx context.Context, world World, runID RunID) ([]string, error) {
	var created []string
	completed := make(map[string]struct{})
	var cursor Pagination
	for {
		page, err := world.EventsList(ctx, EventsListInput{
			RunID:      runID,
			Pagination: cursor,
		})
		if err != nil {
			return nil, err
		}
		for _, event := range page.Data {
			switch event.EventType {
			case EventTypeWaitCreated:
				created = append(created, event.CorrelationID)
			case EventTypeWaitCompleted:
				completed[event.CorrelationID] = struct{}{}
			}
		}
		if !page.HasMore {
			break
		}
		cursor = Pagination{Cursor: page.Cursor}
	}
	pending := created[:0]
	for _, waitID := range created {
		if _, ok := completed[waitID]; !ok {
			pending = append(pending, waitID)
		}
	}
	return pending, nil
}

// resumeQueueAttempts is how often ResumeHook tries to queue a recorded payload's run.
const resumeQueueAttempts = 3

// NewResumeID returns the ID of a new logical resume of a hook.
func NewResumeID() string {
	return ulid.MustNew(ulid.Now(), rand.Reader).String()
}

// ResumeDigest returns the hex SHA-256 of a resume's payload, which a World
// that deduplicates resumes compares a repeat of the resume with.
func ResumeDigest(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

// ResumeHook appends payload to hook as a hook_received event and queues a
// delivery so the hook's run resumes, addressed to the run's deployment. When
// the World refuses the payload with ErrHookNotActive, it still queues the
// delivery, because an earlier resume may have recorded its payload without
// queueing one, and returns the error.
//
// resumeID names the logical resume, NewResumeID when empty. On a World that
// deduplicates resumes it is sent with the payload's digest, so a retry of
// the same resume, by this call or its caller, records the payload once, and
// a write that fails retryably is sent again.
//
// When queueing fails after the payload is recorded, it returns an error
// wrapping ErrResumeNotQueued.
func ResumeHook(ctx context.Context, world World, hook Hook, payload []byte, resumeID string) (EventResult, error) {
	data, err := EncodeHookReceived(payload, hook.ResumeContext.WorkflowName)
	if err != nil {
		return EventResult{}, err
	}
	input := EventsCreateInput{
		RunID: hook.RunID,
		Event: EventsCreateEvent{EventType: EventTypeHookReceived, CorrelationID: string(hook.HookID), Data: data},
	}
	dedup := world.Capabilities().HookResumeDedup || hook.ResumeDedup
	if dedup {
		if resumeID == "" {
			resumeID = NewResumeID()
		}
		input.Options.ResumeID, input.Options.ResumePayloadDigest = resumeID, ResumeDigest(payload)
	}
	result, err := world.EventsCreate(ctx, input)
	for attempt := 1; dedup && err != nil && IsRetryableWorldError(err) && attempt < resumeQueueAttempts; attempt++ {
		// The same resume converges on one event, so an uncertain write
		// is sent again.
		timer := time.NewTimer(time.Duration(attempt) * 100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return result, errors.Join(err, ctx.Err())
		case <-timer.C:
		}
		result, err = world.EventsCreate(ctx, input)
	}
	if err != nil && !errors.Is(err, ErrHookNotActive) {
		return result, err
	}
	// The hook carries the context of its run, so resuming it reads no run.
	// A wake carries no key, so it is never absorbed by a flow that is
	// already ending.
	queue := QueueInput{RunID: hook.RunID, WorkflowName: hook.ResumeContext.WorkflowName, RunSpecVersion: SpecVersion}
	if world.Capabilities().DeploymentAffinity {
		queue.DeploymentID = hook.ResumeContext.DeploymentID
	}
	_, queueErr := world.Queue(ctx, queue)
retry:
	for attempt := 1; queueErr != nil && attempt < resumeQueueAttempts; attempt++ {
		timer := time.NewTimer(time.Duration(attempt) * 100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			queueErr = errors.Join(queueErr, ctx.Err())
			break retry
		case <-timer.C:
		}
		_, queueErr = world.Queue(ctx, queue)
	}
	if queueErr == nil {
		return result, err
	}
	if err == nil {
		return result, fmt.Errorf("%w: %w", ErrResumeNotQueued, queueErr)
	}
	return result, queueErr
}

// RunDeploymentID returns the deployment run runID is pinned to, so a delivery
// queued from outside the run can be addressed to it. It returns "" without
// reading the run when world does not report DeploymentAffinity.
func RunDeploymentID(ctx context.Context, world World, runID RunID) (string, error) {
	if !world.Capabilities().DeploymentAffinity {
		return "", nil
	}
	run, err := world.RunsGet(ctx, RunsGetInput{RunID: runID, ResolveData: ResolveDataNone})
	return run.DeploymentID, err
}

// Deployment is the deployment a new run starts on. Its zero value is the
// deployment that starts the run.
type Deployment struct {
	id     string
	latest bool
}

// LatestDeployment starts a run on the World's latest deployment.
var LatestDeployment = Deployment{latest: true}

// DeploymentWithID starts a run on the deployment id. An empty id is the
// zero Deployment.
func DeploymentWithID(id string) Deployment { return Deployment{id: id} }

// ID returns the deployment DeploymentWithID named, or "" for the current or
// latest deployment.
func (d Deployment) ID() string { return d.id }

// Latest reports whether d is LatestDeployment.
func (d Deployment) Latest() bool { return d.latest }

// StartDeploymentID returns the deployment a new run is pinned to. On a World
// with DeploymentAffinity, that is the deployment DeploymentWithID named, or
// for LatestDeployment the World's latest one when it implements
// LatestDeployments and knows one. Otherwise it is World.DeploymentID, so on
// Worlds without deployments the option changes nothing.
func StartDeploymentID(ctx context.Context, world World, deployment Deployment) (string, error) {
	if world.Capabilities().DeploymentAffinity {
		if deployment.id != "" {
			return deployment.id, nil
		}
		if tracker, ok := world.(LatestDeployments); ok && deployment.latest {
			id, err := tracker.LatestDeploymentID(ctx)
			if err != nil || id != "" {
				return id, err
			}
		}
	}
	return world.DeploymentID(), nil
}

// RunDelivery returns a delivery for runID addressed as the Runtime addresses
// its own: with the run's workflow name, and to the run's deployment when the
// World reports DeploymentAffinity. It reads the run.
func RunDelivery(ctx context.Context, world World, runID RunID) (QueueInput, error) {
	input := QueueInput{RunID: runID}
	run, err := world.RunsGet(ctx, RunsGetInput{RunID: runID, ResolveData: ResolveDataNone})
	if err != nil {
		return input, err
	}
	if world.Capabilities().DeploymentAffinity {
		input.DeploymentID = run.DeploymentID
	}
	input.WorkflowName = run.WorkflowName
	return input, nil
}

// HookPayloadOptions returns how a payload sent to hook from outside its run
// is written: sealed to the run's public key, or, for a run that published
// none, encrypted with the run's key from client, or unencrypted when the run
// has no key.
func HookPayloadOptions(ctx context.Context, client Client, hook Hook) (PayloadOptions, error) {
	if publicKey := DecodePublicKey(hook.ResumeContext.EncryptionPublicKey); publicKey != nil {
		return PayloadOptions{SealTo: publicKey}, nil
	}
	keys, err := client.RunKeys(ctx, RunKeysInput{RunID: hook.RunID, DeploymentID: hook.ResumeContext.DeploymentID})
	if err != nil {
		return PayloadOptions{}, err
	}
	return PayloadOptions{Keys: keys}, nil
}

// RunIDTime returns the time a run ID was minted at: the time of its ULID,
// with the region tag bit cleared. It reports false for a run ID that is not
// wrun_ and a ULID.
func RunIDTime(id RunID) (time.Time, bool) {
	rest, ok := strings.CutPrefix(string(id), "wrun_")
	if !ok {
		return time.Time{}, false
	}
	value, err := ulid.ParseStrict(rest)
	if err != nil {
		return time.Time{}, false
	}
	value[0] &^= 0x80
	return time.UnixMilli(int64(value.Time())).UTC(), true
}
