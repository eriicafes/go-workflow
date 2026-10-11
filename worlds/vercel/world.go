// Package vercelworld provides an Upstash Redis, Vercel Queues, and QStash
// workflow world.
package vercelworld

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/workflowops"
)

// Config identifies the Redis connection and Vercel Queue topic. QueueURL is
// the regional Vercel Queues API root, including /api/v3.
type Config struct {
	// RedisURL is the optional Upstash rediss:// TCP URL. When set, the world
	// uses TCP; otherwise it uses RedisRESTURL and RedisRESTToken.
	RedisURL string
	// RedisRESTURL is the Upstash Redis HTTPS endpoint used when RedisURL is empty.
	RedisRESTURL string
	// RedisRESTToken authorizes requests to RedisRESTURL.
	RedisRESTToken string
	// QueueURL is the required regional Vercel Queues API root, including /api/v3.
	QueueURL string
	// OIDCToken is the required bearer token used to publish queue messages.
	OIDCToken string
	// Topic is the required Vercel Queue topic that receives workflow tasks.
	Topic string
	// QStash optionally delivers timers longer than Vercel Queue's seven-day
	// delay limit. Vercel Queue remains the transport for all other tasks.
	QStash *QStashConfig
	// Prefix namespaces all Redis keys. Zero defaults to go-workflow.
	Prefix string
	// PollInterval controls how often WaitRun and WaitOperation read Redis. Zero
	// defaults to 1 second.
	PollInterval time.Duration
	// LeaseDuration is the duration of a task claim before another worker may
	// reclaim it. Zero defaults to 5 minutes.
	LeaseDuration time.Duration
	// HTTPClient sends Redis REST, Vercel Queue, and QStash requests. Nil uses
	// http.DefaultClient.
	HTTPClient *http.Client
}

// QStashConfig configures QStash delivery for long timers. WebhookEndpoint is
// the public application base URL; the fixed QStash webhook path is appended
// when publishing and verifying deliveries.
type QStashConfig struct {
	// BaseURL is the QStash API root. An empty value uses the global endpoint.
	BaseURL string
	// Token authorizes publishing and cancelling QStash messages.
	Token string
	// WebhookEndpoint is the HTTPS base URL that serves the workflow HTTP handler.
	WebhookEndpoint string
	// CurrentSigningKey verifies current QStash request signatures.
	CurrentSigningKey string
	// NextSigningKey verifies requests during QStash signing-key rotation.
	NextSigningKey string
}

// World keeps all durable state in Redis and publishes compact task pointers
// to Vercel Queues. Redis scripts fence leases and atomically create commands.
type World struct {
	config Config
	http   *http.Client
	redis  redisStore
}

func New(config Config) (*World, error) {
	if config.QueueURL == "" || config.OIDCToken == "" || config.Topic == "" {
		return nil, errors.New("workflow/vercel: Queue and OIDC configuration is required")
	}
	if config.LeaseDuration <= 0 {
		config.LeaseDuration = 5 * time.Minute
	}
	if config.PollInterval <= 0 {
		config.PollInterval = time.Second
	}
	if config.Prefix == "" {
		config.Prefix = "go-workflow"
	}
	if config.HTTPClient == nil {
		config.HTTPClient = http.DefaultClient
	}
	client, err := newRedisStore(
		config.RedisURL,
		config.RedisRESTURL,
		config.RedisRESTToken,
		config.HTTPClient,
	)
	if err != nil {
		return nil, err
	}
	config.QueueURL = strings.TrimRight(config.QueueURL, "/")
	if config.QStash != nil {
		qstash := *config.QStash
		if qstash.Token == "" || qstash.WebhookEndpoint == "" ||
			qstash.CurrentSigningKey == "" || qstash.NextSigningKey == "" {
			return nil, errors.New("workflow/vercel: QStash token, webhook endpoint, and signing keys are required")
		}
		if qstash.BaseURL == "" {
			qstash.BaseURL = "https://qstash.upstash.io"
		}
		qstash.BaseURL = strings.TrimRight(qstash.BaseURL, "/")
		webhookEndpoint, err := qstashWebhookEndpoint(qstash.WebhookEndpoint)
		if err != nil {
			return nil, err
		}
		qstash.WebhookEndpoint = webhookEndpoint
		config.QStash = &qstash
	}
	return &World{
		config: config,
		http:   config.HTTPClient,
		redis:  client,
	}, nil
}

func (world *World) LeaseDuration() time.Duration {
	return world.config.LeaseDuration
}

// CleanupTimer removes the QStash message recorded for a cancelled long timer.
// Vercel Queue deliveries have no handle and are safely ignored after
// cancellation.
func (world *World) CleanupTimer(ctx context.Context, task workflowops.Task) error {
	record, err := world.hash(ctx, world.taskKey(task.RunID, task.Sequence))
	if err != nil {
		return err
	}
	handle := record["timer"]
	if handle == "" {
		return nil
	}
	if world.config.QStash == nil {
		return errors.New("workflow/vercel: QStash configuration is required to clean a timer")
	}
	if err = world.cancelQStash(ctx, handle); err != nil {
		return err
	}
	return world.redis.HDel(ctx, world.taskKey(task.RunID, task.Sequence), "timer")
}

func (world *World) createRoot(ctx context.Context, request workflowops.RunRequest) (string, error) {
	if request.Sequence != workflowops.RootSequence {
		return "", fmt.Errorf("workflow/vercel: invalid root sequence %d", request.Sequence)
	}
	if request.Kind != "" && request.Kind != workflowops.KindRun {
		return "", fmt.Errorf("workflow/vercel: invalid root kind %q", request.Kind)
	}
	if request.Fingerprint == "" {
		request.Fingerprint = request.Identity
	}
	runID, err := workflowops.NewRunID()
	if err != nil {
		return "", err
	}
	now := stamp(time.Now())
	_, err = world.eval(
		ctx,
		createRunScript,
		[]string{
			world.runKey(runID),
			world.commandKey(runID, workflowops.RootSequence),
			world.taskKey(runID, workflowops.RootSequence),
		},
		request.Identity,
		string(request.Input),
		now,
		runID,
		strconv.Itoa(workflowops.RootSequence),
		runID+":"+strconv.Itoa(workflowops.RootSequence),
		string(workflowops.KindRun),
		string(workflowops.StatusQueued),
	)
	if err != nil {
		return "", err
	}
	rootTask := workflowops.Task{
		RunID:    runID,
		Sequence: workflowops.RootSequence,
	}
	if err = world.enqueueTask(ctx, rootTask, time.Time{}); err != nil {
		return "", err
	}
	return runID, nil
}

func (world *World) CreateRun(
	ctx context.Context,
	lease *workflowops.Lease,
	request workflowops.RunRequest,
) (string, error) {
	if lease == nil {
		return world.createRoot(ctx, request)
	}
	command, err := world.hash(ctx, world.commandKey(request.RunID, request.Sequence))
	if err != nil {
		return "", err
	}
	if len(command) != 0 {
		if !matchesRun(command, request) || command["child"] == "" {
			return "", workflowops.HistoryMismatchError("nested run", request.Sequence)
		}
		return command["child"], nil
	}
	if !allowsCreate(lease, request.RunID) {
		return "", workflowops.HistoryMissingError("nested run command", request.Sequence)
	}
	child, err := workflowops.NewRunID()
	if err != nil {
		return "", err
	}
	now := stamp(time.Now())
	created, err := world.eval(
		ctx,
		createChildScript,
		[]string{
			world.commandKey(request.RunID, request.Sequence),
			world.runKey(child),
			world.commandKey(child, workflowops.RootSequence),
			world.taskKey(child, workflowops.RootSequence),
		},
		string(workflowops.KindRun),
		request.Identity,
		request.Fingerprint,
		string(request.Input),
		child,
		now,
		child+":"+strconv.Itoa(workflowops.RootSequence),
		string(workflowops.KindRun),
		string(workflowops.StatusQueued),
	)
	if err != nil {
		return "", err
	}
	if created == "0" {
		return world.CreateRun(ctx, lease, request)
	}
	childTask := workflowops.Task{
		RunID:    child,
		Sequence: workflowops.RootSequence,
	}
	if err = world.enqueueTask(ctx, childTask, time.Time{}); err != nil {
		return "", err
	}
	return child, nil
}

func (world *World) ClaimTask(ctx context.Context, task workflowops.Task) (*workflowops.Lease, error) {
	run, err := world.hash(ctx, world.runKey(task.RunID))
	if err != nil {
		return nil, err
	}
	if len(run) == 0 {
		return nil, workflowops.ErrRunNotFound
	}
	token, err := workflowops.NewRunID()
	if err != nil {
		return nil, err
	}
	now := time.Now()
	claimed, err := world.eval(
		ctx,
		claimTaskScript,
		[]string{world.taskKey(task.RunID, task.Sequence)},
		stamp(now),
		stamp(now.Add(world.config.LeaseDuration)),
		token,
		string(workflowops.StatusQueued),
		string(workflowops.StatusRunning),
	)
	if err != nil {
		return nil, err
	}
	if claimed != "1" {
		return nil, nil
	}
	return &workflowops.Lease{
		ID:       task.RunID,
		Identity: run["identity"],
		Input:    json.RawMessage(run["input"]),
		Token:    token,
		Sequence: task.Sequence,
	}, nil
}

func (world *World) HeartbeatRun(ctx context.Context, run *workflowops.Lease) error {
	ok, err := world.eval(
		ctx,
		heartbeatScript,
		[]string{world.taskKey(run.ID, run.Sequence)},
		run.Token,
		stamp(time.Now().Add(world.config.LeaseDuration)),
		string(workflowops.StatusRunning),
	)
	if err != nil {
		return err
	}
	if ok != "1" {
		return workflowops.ErrLeaseLost
	}
	return world.resumeLatestWait(ctx, run.ID)
}

func (world *World) CompleteRun(ctx context.Context, run *workflowops.Lease, output json.RawMessage) error {
	if err := world.finishTask(ctx, run, workflowops.StatusCompleted, output, ""); err != nil {
		return err
	}
	if run.Sequence != workflowops.RootSequence {
		return world.resolveDependents(ctx, workflowops.Task{RunID: run.ID, Sequence: run.Sequence})
	}
	if err := world.redis.HSet(
		ctx,
		world.runKey(run.ID),
		"status", string(workflowops.StatusCompleted),
		"output", string(output),
		"updated", stamp(time.Now()),
	); err != nil {
		return err
	}
	return world.resolveDependents(ctx, workflowops.Task{RunID: run.ID, Sequence: run.Sequence})
}

func (world *World) FailRun(ctx context.Context, run *workflowops.Lease, cause error) error {
	if err := world.finishTask(ctx, run, workflowops.StatusFailed, nil, cause.Error()); err != nil {
		return err
	}
	if run.Sequence != workflowops.RootSequence {
		return world.resolveDependents(ctx, workflowops.Task{RunID: run.ID, Sequence: run.Sequence})
	}
	if err := world.redis.HSet(
		ctx,
		world.runKey(run.ID),
		"status", string(workflowops.StatusFailed),
		"error", cause.Error(),
		"updated", stamp(time.Now()),
	); err != nil {
		return err
	}
	return world.resolveDependents(ctx, workflowops.Task{RunID: run.ID, Sequence: run.Sequence})
}

func (world *World) CancelOperations(ctx context.Context, tasks ...workflowops.Task) error {
	pending := append([]workflowops.Task(nil), tasks...)
	changed := make([]workflowops.Task, 0, len(tasks))
	visitedRoots := make(map[string]bool)
	for len(pending) > 0 {
		task := pending[0]
		pending = pending[1:]
		if task.Sequence == workflowops.RootSequence {
			if visitedRoots[task.RunID] {
				continue
			}
			visitedRoots[task.RunID] = true
			children, err := world.childRuns(ctx, task.RunID)
			if err != nil {
				return err
			}
			for _, childID := range children {
				pending = append(pending, workflowops.Task{
					RunID:    childID,
					Sequence: workflowops.RootSequence,
				})
			}
		}
		record, err := world.hash(ctx, world.taskKey(task.RunID, task.Sequence))
		if err != nil {
			return err
		}
		if len(record) == 0 {
			return workflowops.ErrRunNotFound
		}
		status := workflowops.Status(record["status"])
		if status == workflowops.StatusCompleted || status == workflowops.StatusFailed {
			continue
		}
		if status == workflowops.StatusCancelled {
			if err := world.CleanupTimer(ctx, task); err != nil {
				return err
			}
			continue
		}
		completion, err := world.redis.Incr(ctx, world.completionKey())
		if err != nil {
			return err
		}
		if err = world.redis.HSet(
			ctx,
			world.taskKey(task.RunID, task.Sequence),
			"status", string(workflowops.StatusCancelled),
			"error", workflowops.ErrCancelled.Error(),
			"completed", strconv.FormatInt(completion, 10),
			"updated", stamp(time.Now()),
		); err != nil {
			return err
		}
		changed = append(changed, task)
		if task.Sequence == workflowops.RootSequence {
			if err = world.redis.HSet(
				ctx,
				world.runKey(task.RunID),
				"status", string(workflowops.StatusCancelled),
				"error", workflowops.ErrCancelled.Error(),
				"updated", stamp(time.Now()),
			); err != nil {
				return err
			}
		}
		if err := world.CleanupTimer(ctx, task); err != nil {
			return err
		}
	}
	return world.resolveDependents(ctx, changed...)
}

func (world *World) childRuns(ctx context.Context, runID string) ([]string, error) {
	var cursor uint64
	var children []string
	pattern := world.config.Prefix + ":run:" + runID + ":command:*"
	for {
		keys, next, err := world.redis.Scan(ctx, cursor, pattern, 100)
		if err != nil {
			return nil, err
		}
		for _, key := range keys {
			command, err := world.hash(ctx, key)
			if err != nil {
				return nil, err
			}
			if childID := command["child"]; childID != "" {
				children = append(children, childID)
			}
		}
		if next == 0 {
			return children, nil
		}
		cursor = next
	}
}

func (world *World) EnsureOperation(
	ctx context.Context,
	lease *workflowops.Lease,
	request workflowops.OperationRequest,
) (workflowops.Operation, error) {
	command, err := world.hash(ctx, world.commandKey(request.RunID, request.Sequence))
	if err != nil {
		return workflowops.Operation{}, err
	}
	if len(command) != 0 {
		if !matchesOperation(command, request) {
			return workflowops.Operation{}, workflowops.HistoryMismatchError("operation", request.Sequence)
		}
		return world.operation(ctx, request.RunID, request.Sequence)
	}
	if !allowsCreate(lease, request.RunID) {
		return workflowops.Operation{}, workflowops.HistoryMissingError("command", request.Sequence)
	}
	due := time.Time{}
	if request.DueAt != nil {
		due = *request.DueAt
	}
	key := request.RunID + ":" + strconv.Itoa(request.Sequence)
	created, err := world.eval(
		ctx,
		createOperationScript,
		[]string{
			world.commandKey(request.RunID, request.Sequence),
			world.taskKey(request.RunID, request.Sequence),
		},
		string(request.Kind),
		request.Identity,
		request.Fingerprint,
		string(request.Input),
		request.RunID,
		strconv.Itoa(request.Sequence),
		key,
		stamp(time.Now()),
		stamp(due),
		string(workflowops.StatusQueued),
	)
	if err != nil {
		return workflowops.Operation{}, err
	}
	if created == "0" {
		return world.EnsureOperation(ctx, lease, request)
	}
	if err := world.enqueueTask(ctx, workflowops.Task{RunID: request.RunID, Sequence: request.Sequence}, due); err != nil {
		return workflowops.Operation{}, err
	}
	return workflowops.Operation{
		RunID:          request.RunID,
		Sequence:       request.Sequence,
		Status:         workflowops.StatusQueued,
		DueAt:          due,
		IdempotencyKey: key,
	}, nil
}

func (world *World) EnsureWait(
	ctx context.Context,
	lease *workflowops.Lease,
	request workflowops.WaitRequest,
) (workflowops.Operation, error) {
	input, err := json.Marshal(request.Dependencies)
	if err != nil {
		return workflowops.Operation{}, err
	}
	command, err := world.hash(ctx, world.commandKey(request.RunID, request.Sequence))
	if err != nil {
		return workflowops.Operation{}, err
	}
	if len(command) != 0 {
		if !matchesWait(command, request, input) {
			return workflowops.Operation{}, workflowops.HistoryMismatchError("wait", request.Sequence)
		}
		return world.operation(ctx, request.RunID, request.Sequence)
	}
	if !allowsCreate(lease, request.RunID) {
		return workflowops.Operation{}, workflowops.HistoryMissingError("wait command", request.Sequence)
	}
	keys := []string{world.commandKey(request.RunID, request.Sequence), world.taskKey(request.RunID, request.Sequence)}
	for _, dependency := range request.Dependencies {
		keys = append(keys, world.dependentsKey(dependency))
	}
	created, err := world.eval(
		ctx,
		createWaitScript,
		keys,
		string(request.Kind),
		request.Fingerprint,
		string(input),
		request.RunID,
		strconv.Itoa(request.Sequence),
		request.RunID+":"+strconv.Itoa(request.Sequence),
		stamp(time.Now()),
		string(workflowops.StatusWaiting),
		request.RunID+":"+strconv.Itoa(request.Sequence),
	)
	if err != nil {
		return workflowops.Operation{}, err
	}
	if created == "0" {
		return world.EnsureWait(ctx, lease, request)
	}
	if resolved, err := world.resolveWait(ctx, workflowops.Task{RunID: request.RunID, Sequence: request.Sequence}); err != nil {
		return workflowops.Operation{}, err
	} else if resolved {
		if err = world.resolveDependents(ctx, workflowops.Task{RunID: request.RunID, Sequence: request.Sequence}); err != nil {
			return workflowops.Operation{}, err
		}
	}
	return world.operation(ctx, request.RunID, request.Sequence)
}

func (world *World) SuspendRun(ctx context.Context, run *workflowops.Lease) error {
	if run.Sequence != workflowops.RootSequence {
		return workflowops.ErrLeaseLost
	}
	ok, err := world.eval(
		ctx,
		suspendRunScript,
		[]string{world.taskKey(run.ID, workflowops.RootSequence), world.runKey(run.ID)},
		run.Token,
		stamp(time.Now()),
		string(workflowops.StatusRunning),
		string(workflowops.StatusWaiting),
	)
	if err != nil {
		return err
	}
	if ok != "1" {
		return workflowops.ErrLeaseLost
	}
	return nil
}

func (world *World) FinishOperation(
	ctx context.Context,
	operation workflowops.Operation,
	output json.RawMessage,
	cause error,
) error {
	status, message := workflowops.StatusCompleted, ""
	if cause != nil {
		status, message, output = workflowops.StatusFailed, cause.Error(), nil
	}
	run := &workflowops.Lease{
		ID:       operation.RunID,
		Sequence: operation.Sequence,
		Token:    operation.Token,
	}
	if err := world.finishTask(ctx, run, status, output, message); err != nil {
		return err
	}
	return world.resolveDependents(ctx, workflowops.Task{RunID: operation.RunID, Sequence: operation.Sequence})
}

func (world *World) WaitRun(ctx context.Context, runID string) (json.RawMessage, error) {
	ticker := time.NewTicker(world.config.PollInterval)
	defer ticker.Stop()
	for {
		run, err := world.hash(ctx, world.runKey(runID))
		if err != nil {
			return nil, err
		}
		if len(run) == 0 {
			return nil, workflowops.ErrRunNotFound
		}
		if workflowops.Status(run["status"]) == workflowops.StatusCompleted {
			return json.RawMessage(run["output"]), nil
		}
		if workflowops.Status(run["status"]) == workflowops.StatusFailed {
			return nil, errors.New(run["error"])
		}
		if workflowops.Status(run["status"]) == workflowops.StatusCancelled {
			return nil, workflowops.ErrCancelled
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

func (world *World) WaitOperation(ctx context.Context, task workflowops.Task) (json.RawMessage, error) {
	ticker := time.NewTicker(world.config.PollInterval)
	defer ticker.Stop()
	for {
		operation, err := world.operation(ctx, task.RunID, task.Sequence)
		if err != nil {
			return nil, err
		}
		if operation.Status == workflowops.StatusCompleted {
			item, err := world.hash(ctx, world.taskKey(task.RunID, task.Sequence))
			if err != nil {
				return nil, err
			}
			return json.RawMessage(item["output"]), nil
		}
		if operation.Status == workflowops.StatusFailed {
			return nil, errors.New(operation.Error)
		}
		if operation.Status == workflowops.StatusCancelled {
			return nil, workflowops.ErrCancelled
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}
func (world *World) AppendEvent(ctx context.Context, task workflowops.Task, data []byte) error {
	_, err := world.eval(
		ctx,
		appendEventScript,
		[]string{world.eventKey(task.RunID, task.Sequence), world.eventsKey(task.RunID)},
		base64.StdEncoding.EncodeToString(data),
		strconv.Itoa(task.Sequence),
		stamp(time.Now()),
	)
	return err
}
func (world *World) Events(ctx context.Context, runID string, after int64) ([]workflow.Event, error) {
	sequences, err := world.redis.ZRangeByScore(ctx, world.eventsKey(runID), "-inf", "+inf")
	if err != nil {
		return nil, err
	}
	events := make([]workflow.Event, 0, len(sequences))
	for _, sequence := range sequences {
		id, err := strconv.ParseInt(sequence, 10, 64)
		if err != nil || id <= after {
			continue
		}
		event, err := world.hash(ctx, world.eventKey(runID, int(id)))
		if err != nil {
			return nil, err
		}
		data, err := base64.StdEncoding.DecodeString(event["data"])
		if err != nil {
			return nil, err
		}
		created, _ := time.Parse(time.RFC3339Nano, event["created"])
		events = append(events, workflow.Event{
			ID:        id,
			Data:      data,
			CreatedAt: created,
		})
	}
	return events, nil
}

func (world *World) operation(ctx context.Context, runID string, sequence int) (workflowops.Operation, error) {
	task, err := world.hash(ctx, world.taskKey(runID, sequence))
	if err != nil {
		return workflowops.Operation{}, err
	}
	if len(task) == 0 {
		return workflowops.Operation{}, fmt.Errorf("workflow: operation %d not found", sequence)
	}
	operation := workflowops.Operation{
		RunID:          runID,
		Sequence:       sequence,
		Status:         workflowops.Status(task["status"]),
		Token:          task["token"],
		Error:          task["error"],
		IdempotencyKey: task["idempotency"],
	}
	if value := task["due"]; value != "" {
		operation.DueAt, _ = time.Parse(time.RFC3339Nano, value)
	}
	if value := task["lease"]; value != "" {
		operation.LeaseUntil, _ = time.Parse(time.RFC3339Nano, value)
	}
	return operation, nil
}

func (world *World) finishTask(
	ctx context.Context,
	run *workflowops.Lease,
	status workflowops.Status,
	output json.RawMessage,
	message string,
) error {
	ok, err := world.eval(
		ctx,
		finishTaskScript,
		[]string{world.taskKey(run.ID, run.Sequence), world.completionKey()},
		run.Token,
		string(status),
		string(output),
		message,
		stamp(time.Now()),
		string(workflowops.StatusRunning),
	)
	if err != nil {
		return err
	}
	if ok != "1" {
		return workflowops.ErrLeaseLost
	}
	return nil
}

// reconcileWaits is reserved for recovery or operator-initiated repair. Normal
// task transitions use resolveDependents and never scan every task key.
func (world *World) reconcileWaits(ctx context.Context) error {
	var cursor uint64
	for {
		keys, next, err := world.redis.Scan(ctx, cursor, world.config.Prefix+":run:*:task:*", 100)
		if err != nil {
			return err
		}
		for _, key := range keys {
			task, err := world.hash(ctx, key)
			if err != nil || workflowops.Status(task["status"]) != workflowops.StatusWaiting {
				if err != nil {
					return err
				}
				continue
			}
			runID, sequence := task["run"], readSequence(task["sequence"])
			command, err := world.hash(ctx, world.commandKey(runID, sequence))
			if err != nil {
				return err
			}
			kind := workflowops.Kind(command["kind"])
			if !workflowops.IsWaitKind(kind) {
				continue
			}
			var dependencies []workflowops.Task
			if err = json.Unmarshal([]byte(command["input"]), &dependencies); err != nil {
				return err
			}
			states, err := world.waitStates(ctx, dependencies)
			if err != nil {
				return err
			}
			resolution := workflowops.ResolveWait(kind, states)
			if resolution.Pending {
				continue
			}
			status := workflowops.StatusCompleted
			message := ""
			if resolution.Err != nil {
				status = workflowops.StatusFailed
				message = resolution.Err.Error()
			}
			resolved, err := world.eval(
				ctx,
				resolveWaitScript,
				[]string{key, world.taskKey(runID, workflowops.RootSequence), world.runKey(runID), world.completionKey()},
				string(status),
				string(resolution.Output),
				message,
				stamp(time.Now()),
				string(workflowops.StatusWaiting),
				string(workflowops.StatusQueued),
			)
			if err != nil {
				return err
			}
			if resolved == "1" {
				task := workflowops.Task{
					RunID:    runID,
					Sequence: workflowops.RootSequence,
				}
				if err = world.enqueueTask(ctx, task, time.Time{}); err != nil {
					return err
				}
			}
		}
		if next == 0 {
			return nil
		}
		cursor = next
	}
}

func (world *World) resolveDependents(ctx context.Context, changed ...workflowops.Task) error {
	for len(changed) > 0 {
		task := changed[0]
		changed = changed[1:]
		waits, err := world.redis.SMembers(ctx, world.dependentsKey(task))
		if err != nil {
			return err
		}
		for _, value := range waits {
			index := strings.LastIndex(value, ":")
			if index < 1 {
				continue
			}
			sequence, err := strconv.Atoi(value[index+1:])
			if err != nil {
				return err
			}
			wait := workflowops.Task{RunID: value[:index], Sequence: sequence}
			resolved, err := world.resolveWait(ctx, wait)
			if err != nil {
				return err
			}
			if resolved {
				changed = append(changed, wait)
			}
		}
	}
	return nil
}

func (world *World) resolveWait(ctx context.Context, wait workflowops.Task) (bool, error) {
	command, err := world.hash(ctx, world.commandKey(wait.RunID, wait.Sequence))
	if err != nil {
		return false, err
	}
	var dependencies []workflowops.Task
	if err = json.Unmarshal([]byte(command["input"]), &dependencies); err != nil {
		return false, err
	}
	states, err := world.waitStates(ctx, dependencies)
	if err != nil {
		return false, err
	}
	resolution := workflowops.ResolveWait(workflowops.Kind(command["kind"]), states)
	if resolution.Pending {
		return false, nil
	}
	status, message := workflowops.StatusCompleted, ""
	if resolution.Err != nil {
		status, message = workflowops.StatusFailed, resolution.Err.Error()
	}
	resolved, err := world.eval(ctx, resolveWaitScript, []string{world.taskKey(wait.RunID, wait.Sequence), world.taskKey(wait.RunID, workflowops.RootSequence), world.runKey(wait.RunID), world.completionKey()}, string(status), string(resolution.Output), message, stamp(time.Now()), string(workflowops.StatusWaiting), string(workflowops.StatusQueued))
	if err != nil || resolved != "1" {
		return false, err
	}
	if err = world.enqueueTask(ctx, workflowops.Task{RunID: wait.RunID, Sequence: workflowops.RootSequence}, time.Time{}); err != nil {
		return false, err
	}
	return true, nil
}

// resumeLatestWait closes the race between checking a barrier and releasing
// the root lease. It reads only this run's command keys, not every wait.
func (world *World) resumeLatestWait(ctx context.Context, runID string) error {
	var cursor uint64
	latest := workflowops.Task{RunID: runID, Sequence: workflowops.RootSequence}
	found := false
	pattern := world.config.Prefix + ":run:" + runID + ":command:*"
	for {
		keys, next, err := world.redis.Scan(ctx, cursor, pattern, 100)
		if err != nil {
			return err
		}
		for _, key := range keys {
			sequenceText := key[strings.LastIndex(key, ":")+1:]
			sequence, err := strconv.Atoi(sequenceText)
			if err != nil {
				return err
			}
			command, err := world.hash(ctx, key)
			if err != nil {
				return err
			}
			if workflowops.IsWaitKind(workflowops.Kind(command["kind"])) && (!found || sequence > latest.Sequence) {
				latest.Sequence, found = sequence, true
			}
		}
		if next == 0 {
			break
		}
		cursor = next
	}
	if !found {
		return nil
	}
	task, err := world.hash(ctx, world.taskKey(runID, latest.Sequence))
	if err != nil {
		return err
	}
	status := workflowops.Status(task["status"])
	if status == workflowops.StatusWaiting {
		_, err = world.resolveWait(ctx, latest)
		return err
	}
	if status != workflowops.StatusCompleted && status != workflowops.StatusFailed {
		return nil
	}
	resumed, err := world.eval(
		ctx,
		resumeRunScript,
		[]string{world.taskKey(runID, workflowops.RootSequence), world.runKey(runID)},
		stamp(time.Now()),
		string(workflowops.StatusWaiting),
		string(workflowops.StatusQueued),
	)
	if err != nil || resumed != "1" {
		return err
	}
	return world.enqueueTask(ctx, workflowops.Task{RunID: runID, Sequence: workflowops.RootSequence}, time.Time{})
}

func (world *World) waitStates(ctx context.Context, dependencies []workflowops.Task) ([]workflowops.Dependency, error) {
	states := make([]workflowops.Dependency, len(dependencies))
	for index, dependency := range dependencies {
		task, err := world.hash(ctx, world.taskKey(dependency.RunID, dependency.Sequence))
		if err != nil {
			return nil, err
		}
		states[index] = workflowops.Dependency{
			Task:       dependency,
			Found:      len(task) != 0,
			Status:     workflowops.Status(task["status"]),
			Output:     json.RawMessage(task["output"]),
			Error:      task["error"],
			Completion: completion(task["completed"]),
		}
	}
	return states, nil
}

func (world *World) enqueueTask(ctx context.Context, task workflowops.Task, due time.Time) error {
	const maxVercelQueueDelay = 7 * 24 * time.Hour

	delay := time.Duration(0)
	if !due.IsZero() {
		delay = max(time.Until(due), 0)
	}
	if delay > maxVercelQueueDelay {
		if world.config.QStash == nil {
			return errors.New("workflow/vercel: timer over seven days requires QStash configuration")
		}
		return world.enqueueQStash(ctx, task, due)
	}
	body, err := json.Marshal(task)
	if err != nil {
		return err
	}
	queueURL := world.config.QueueURL + "/topic/" + url.PathEscape(world.config.Topic)
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		queueURL,
		bytes.NewReader(body),
	)
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+world.config.OIDCToken)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Vqs-Idempotency-Key", task.RunID+":"+strconv.Itoa(task.Sequence))
	if delay > 0 {
		request.Header.Set("Vqs-Delay-Seconds", strconv.Itoa(int(delay.Round(time.Second).Seconds())))
		// Vercel Queue's default retention is one day. A delayed message must
		// remain retained until it becomes visible.
		request.Header.Set("Vqs-Retention-Seconds", strconv.Itoa(int(maxVercelQueueDelay.Seconds())))
	}
	response, err := world.http.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		data, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return fmt.Errorf("workflow/vercel: publish queue task: %s: %s", response.Status, strings.TrimSpace(string(data)))
	}
	return nil
}

func (world *World) enqueueQStash(ctx context.Context, task workflowops.Task, due time.Time) error {
	body, err := json.Marshal(task)
	if err != nil {
		return err
	}
	config := world.config.QStash
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		config.BaseURL+"/v2/publish/"+url.PathEscape(config.WebhookEndpoint+workflow.APIPrefix+"/webhook/qstash"),
		bytes.NewReader(body),
	)
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+config.Token)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Upstash-Not-Before", strconv.FormatInt(due.Unix(), 10))
	response, err := world.http.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		data, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return fmt.Errorf("workflow/vercel: publish QStash timer: %s: %s", response.Status, strings.TrimSpace(string(data)))
	}
	var published struct {
		MessageID string `json:"messageId"`
	}
	if err = json.NewDecoder(response.Body).Decode(&published); err != nil {
		return fmt.Errorf("workflow/vercel: decode QStash timer: %w", err)
	}
	if published.MessageID == "" {
		return errors.New("workflow/vercel: QStash timer response has no message ID")
	}
	recorded, err := world.recordTimer(ctx, task, published.MessageID)
	if err != nil {
		cleanupErr := world.cancelQStash(ctx, published.MessageID)
		if cleanupErr != nil {
			return errors.Join(err, cleanupErr)
		}
		return err
	}
	if recorded {
		return nil
	}
	return world.cancelQStash(ctx, published.MessageID)
}

func (world *World) recordTimer(ctx context.Context, task workflowops.Task, handle string) (bool, error) {
	recorded, err := world.eval(
		ctx,
		recordTimerScript,
		[]string{world.taskKey(task.RunID, task.Sequence)},
		string(workflowops.StatusQueued),
		handle,
	)
	return recorded == "1", err
}

func (world *World) cancelQStash(ctx context.Context, messageID string) error {
	config := world.config.QStash
	values := url.Values{"messageIds": []string{messageID}}
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodDelete,
		config.BaseURL+"/v2/messages?"+values.Encode(),
		nil,
	)
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+config.Token)
	response, err := world.http.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotFound {
		return nil
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		data, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return fmt.Errorf("workflow/vercel: cancel QStash timer: %s: %s", response.Status, strings.TrimSpace(string(data)))
	}
	return nil
}

func (world *World) eval(ctx context.Context, script string, keys []string, args ...string) (string, error) {
	values := make([]any, len(args))
	for index, argument := range args {
		values[index] = argument
	}
	result, err := world.redis.Eval(ctx, script, keys, values...)
	if err != nil {
		return "", err
	}
	switch value := result.(type) {
	case int64:
		return strconv.FormatInt(value, 10), nil
	case string:
		return value, nil
	default:
		return "", fmt.Errorf("workflow/vercel: unexpected Lua result %T", result)
	}
}
func (world *World) hash(ctx context.Context, key string) (map[string]string, error) {
	return world.redis.HGetAll(ctx, key)
}
func (world *World) runKey(runID string) string { return world.config.Prefix + ":run:" + runID }
func (world *World) commandKey(runID string, sequence int) string {
	return world.config.Prefix + ":run:" + runID + ":command:" + strconv.Itoa(sequence)
}
func (world *World) taskKey(runID string, sequence int) string {
	return world.config.Prefix + ":run:" + runID + ":task:" + strconv.Itoa(sequence)
}
func (world *World) dependentsKey(task workflowops.Task) string {
	return world.config.Prefix + ":dependents:" + task.RunID + ":" + strconv.Itoa(task.Sequence)
}
func (world *World) eventKey(runID string, sequence int) string {
	return world.config.Prefix + ":run:" + runID + ":event:" + strconv.Itoa(sequence)
}
func (world *World) eventsKey(runID string) string {
	return world.config.Prefix + ":run:" + runID + ":events"
}

func (world *World) completionKey() string {
	return world.config.Prefix + ":completion"
}
func stamp(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func completion(value string) uint64 {
	result, _ := strconv.ParseUint(value, 10, 64)
	return result
}

func allowsCreate(lease *workflowops.Lease, runID string) bool {
	return lease != nil && lease.ID == runID && lease.Sequence == workflowops.RootSequence
}

func matchesOperation(command map[string]string, request workflowops.OperationRequest) bool {
	return workflowops.MatchesOperation(
		workflowops.Kind(command["kind"]),
		command["identity"],
		command["fingerprint"],
		json.RawMessage(command["input"]),
		request,
	)
}

func matchesRun(command map[string]string, request workflowops.RunRequest) bool {
	return workflowops.MatchesRun(
		workflowops.Kind(command["kind"]),
		command["identity"],
		command["fingerprint"],
		json.RawMessage(command["input"]),
		request,
	)
}

func matchesWait(command map[string]string, request workflowops.WaitRequest, input json.RawMessage) bool {
	return workflowops.MatchesWait(
		workflowops.Kind(command["kind"]),
		command["identity"],
		command["fingerprint"],
		json.RawMessage(command["input"]),
		request,
		input,
	)
}

func readSequence(value string) int {
	sequence, _ := strconv.Atoi(value)
	return sequence
}

const createRunScript = `
redis.call('HSET', KEYS[1],
  'identity', ARGV[1], 'input', ARGV[2], 'status', ARGV[8],
  'created', ARGV[3], 'updated', ARGV[3])
redis.call('HSET', KEYS[2],
  'kind', ARGV[7], 'identity', ARGV[1], 'fingerprint', ARGV[1], 'input', ARGV[2])
redis.call('HSET', KEYS[3],
  'run', ARGV[4], 'sequence', ARGV[5], 'status', ARGV[8],
  'idempotency', ARGV[6], 'attempt', '0',
  'created', ARGV[3], 'updated', ARGV[3])
return 1
`

const createChildScript = `
if redis.call('EXISTS', KEYS[1]) == 1 then
  return 0
end
redis.call('HSET', KEYS[1],
  'kind', ARGV[1], 'identity', ARGV[2], 'fingerprint', ARGV[3],
  'input', ARGV[4], 'child', ARGV[5])
redis.call('HSET', KEYS[2],
  'identity', ARGV[2], 'input', ARGV[4], 'status', ARGV[9],
  'created', ARGV[6], 'updated', ARGV[6])
redis.call('HSET', KEYS[3],
  'kind', ARGV[8], 'identity', ARGV[2], 'fingerprint', ARGV[2], 'input', ARGV[4])
redis.call('HSET', KEYS[4],
  'run', ARGV[5], 'sequence', '-1', 'status', ARGV[9],
  'idempotency', ARGV[7], 'attempt', '0',
  'created', ARGV[6], 'updated', ARGV[6])
return 1
`

const createOperationScript = `
if redis.call('EXISTS', KEYS[1]) == 1 then
  return 0
end
redis.call('HSET', KEYS[1],
  'kind', ARGV[1], 'identity', ARGV[2], 'fingerprint', ARGV[3], 'input', ARGV[4])
redis.call('HSET', KEYS[2],
  'run', ARGV[5], 'sequence', ARGV[6], 'status', ARGV[10],
  'idempotency', ARGV[7], 'attempt', '0',
  'created', ARGV[8], 'updated', ARGV[8])
if ARGV[9] ~= '' then
  redis.call('HSET', KEYS[2], 'due', ARGV[9])
end
return 1
`

const createWaitScript = `
if redis.call('EXISTS', KEYS[1]) == 1 then
  return 0
end
redis.call('HSET', KEYS[1],
  'kind', ARGV[1], 'identity', ARGV[1], 'fingerprint', ARGV[2], 'input', ARGV[3])
redis.call('HSET', KEYS[2],
  'run', ARGV[4], 'sequence', ARGV[5], 'status', ARGV[8],
  'idempotency', ARGV[6], 'attempt', '0',
  'created', ARGV[7], 'updated', ARGV[7])
for index = 3, #KEYS do
  redis.call('SADD', KEYS[index], ARGV[9])
end
return 1
`

const resumeRunScript = `
if redis.call('HGET', KEYS[1], 'status') ~= ARGV[2] then
  return 0
end
redis.call('HSET', KEYS[1], 'status', ARGV[3], 'updated', ARGV[1])
redis.call('HSET', KEYS[2], 'status', ARGV[3], 'updated', ARGV[1])
return 1
`

const claimTaskScript = `
local status = redis.call('HGET', KEYS[1], 'status')
local due = redis.call('HGET', KEYS[1], 'due')
local lease = redis.call('HGET', KEYS[1], 'lease')
local ready = status == ARGV[4] and (not due or due == '' or due <= ARGV[1])
local expired = status == ARGV[5] and lease and lease < ARGV[1]
if ready or expired then
  redis.call('HSET', KEYS[1], 'status', ARGV[5], 'token', ARGV[3], 'lease', ARGV[2])
  redis.call('HINCRBY', KEYS[1], 'attempt', 1)
  return 1
end
return 0
`

const heartbeatScript = `
if redis.call('HGET', KEYS[1], 'status') == ARGV[3]
  and redis.call('HGET', KEYS[1], 'token') == ARGV[1] then
  redis.call('HSET', KEYS[1], 'lease', ARGV[2])
  return 1
end
return 0
`

const finishTaskScript = `
if redis.call('HGET', KEYS[1], 'status') ~= ARGV[6]
  or redis.call('HGET', KEYS[1], 'token') ~= ARGV[1] then
  return 0
end
local completed = redis.call('INCR', KEYS[2])
redis.call('HSET', KEYS[1], 'status', ARGV[2], 'updated', ARGV[5], 'completed', completed)
if ARGV[3] ~= '' then
  redis.call('HSET', KEYS[1], 'output', ARGV[3])
end
if ARGV[4] ~= '' then
  redis.call('HSET', KEYS[1], 'error', ARGV[4])
end
redis.call('HDEL', KEYS[1], 'token', 'lease')
return 1
`

const suspendRunScript = `
if redis.call('HGET', KEYS[1], 'status') ~= ARGV[3]
  or redis.call('HGET', KEYS[1], 'token') ~= ARGV[1] then
  return 0
end
redis.call('HSET', KEYS[1], 'status', ARGV[4], 'updated', ARGV[2])
redis.call('HDEL', KEYS[1], 'token', 'lease')
redis.call('HSET', KEYS[2], 'status', ARGV[4], 'updated', ARGV[2])
return 1
`

const resolveWaitScript = `
if redis.call('HGET', KEYS[1], 'status') ~= ARGV[5] then
  return 0
end
local completed = redis.call('INCR', KEYS[4])
redis.call('HSET', KEYS[1], 'status', ARGV[1], 'updated', ARGV[4], 'completed', completed)
if ARGV[2] ~= '' then
  redis.call('HSET', KEYS[1], 'output', ARGV[2])
end
if ARGV[3] ~= '' then
  redis.call('HSET', KEYS[1], 'error', ARGV[3])
end
if redis.call('HGET', KEYS[2], 'status') == ARGV[5] then
  redis.call('HSET', KEYS[2], 'status', ARGV[6], 'updated', ARGV[4])
  redis.call('HSET', KEYS[3], 'status', ARGV[6], 'updated', ARGV[4])
end
return 1
`

const appendEventScript = `
if redis.call('EXISTS', KEYS[1]) == 1 then
  return 0
end
redis.call('HSET', KEYS[1], 'data', ARGV[1], 'created', ARGV[3])
redis.call('ZADD', KEYS[2], ARGV[2], ARGV[2])
return 1
`

const recordTimerScript = `
if redis.call('HGET', KEYS[1], 'status') ~= ARGV[1] then
  return 0
end
redis.call('HSET', KEYS[1], 'timer', ARGV[2])
return 1
`
