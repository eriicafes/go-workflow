// Package awsworld provides a DynamoDB, SQS, and EventBridge Scheduler world.
package awsworld

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	awsv2 "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	sch "github.com/aws/aws-sdk-go-v2/service/scheduler/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/workflowops"
)

// Config identifies the AWS resources used by the world. Scheduler is needed
// only when a queue worker can execute timers beyond SQS's 15-minute delivery
// delay. It is always optional for a World used only by HTTP.
type Config struct {
	// TableName is the required DynamoDB table that stores workflow state.
	TableName string
	// QueueURL is the required SQS queue URL for immediate task delivery.
	QueueURL string
	// Scheduler configures long-timer delivery. It is optional for HTTP-only
	// Worlds and for queue workers that never schedule beyond 15 minutes.
	Scheduler *SchedulerConfig
	// PollInterval controls how often WaitRun and WaitOperation read DynamoDB.
	// Zero defaults to 1 second.
	PollInterval time.Duration
	// LeaseDuration is the duration of a task claim before another worker may
	// reclaim it. Zero defaults to 5 minutes.
	LeaseDuration time.Duration
}

// SchedulerConfig configures EventBridge Scheduler for durable timers longer
// than SQS's 15-minute delay. Leave Scheduler nil when that support is not
// needed.
type SchedulerConfig struct {
	// Client creates one-time EventBridge schedules. It is required when this
	// configuration is used to schedule a timer longer than 15 minutes.
	Client *scheduler.Client
	// Group is the EventBridge Scheduler group. An empty value uses default.
	Group string
	// TargetARN is the SQS queue ARN supplied to EventBridge Scheduler. It is
	// required for a timer longer than 15 minutes.
	TargetARN string
	// RoleARN is the IAM role EventBridge Scheduler assumes to send to TargetARN.
	// It is required for a timer longer than 15 minutes.
	RoleARN string
}

// World uses a single DynamoDB table for run, command, task, and event
// records. SQS notifies a worker of each task. Scheduler delivers long timers.
type World struct {
	dynamo *dynamodb.Client
	sqs    *sqs.Client
	config Config
}

// New preserves the caller's AWS client configuration. Scheduler is optional
// unless this world executes workflow timers longer than SQS's 15-minute
// delay. In that case, provide a complete SchedulerConfig.
func New(config Config, dynamoClient *dynamodb.Client, queueClient *sqs.Client) (*World, error) {
	if config.TableName == "" || config.QueueURL == "" {
		return nil, errors.New("workflow/aws: table name and queue URL are required")
	}
	if dynamoClient == nil || queueClient == nil {
		return nil, errors.New("workflow/aws: DynamoDB and SQS clients are required")
	}
	if config.LeaseDuration <= 0 {
		config.LeaseDuration = 5 * time.Minute
	}
	if config.PollInterval <= 0 {
		config.PollInterval = time.Second
	}
	return &World{
		dynamo: dynamoClient,
		sqs:    queueClient,
		config: config,
	}, nil
}

func (world *World) LeaseDuration() time.Duration {
	return world.config.LeaseDuration
}

// CleanupTimer removes the provider timer recorded for a cancelled task. SQS
// delay messages have no timer handle, so cancellation does not call a
// scheduler for short timers.
func (world *World) CleanupTimer(ctx context.Context, task workflowops.Task) error {
	record, found, err := world.item(ctx, taskKey(task.RunID, task.Sequence))
	if err != nil || !found {
		return err
	}
	handle := record["timer"]
	if handle == "" {
		return nil
	}
	if err := world.deleteTimer(ctx, handle); err != nil {
		return err
	}
	_, err = world.dynamo.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:        awsv2.String(world.config.TableName),
		Key:              taskKey(task.RunID, task.Sequence),
		UpdateExpression: awsv2.String("REMOVE #timer"),
		ExpressionAttributeNames: map[string]string{
			"#timer": "timer",
		},
	})
	return err
}

func (world *World) deleteTimer(ctx context.Context, handle string) error {
	schedulerConfig := world.config.Scheduler
	if schedulerConfig == nil || schedulerConfig.Client == nil {
		return errors.New("workflow/aws: EventBridge Scheduler is required to clean a timer")
	}
	group := schedulerConfig.Group
	if group == "" {
		group = "default"
	}
	_, err := schedulerConfig.Client.DeleteSchedule(ctx, &scheduler.DeleteScheduleInput{
		Name:      awsv2.String(handle),
		GroupName: awsv2.String(group),
	})
	var notFound *sch.ResourceNotFoundException
	if errors.As(err, &notFound) {
		return nil
	}
	return err
}

func (world *World) createRoot(ctx context.Context, request workflowops.RunRequest) (string, error) {
	if request.Sequence != workflowops.RootSequence {
		return "", fmt.Errorf("workflow/aws: invalid root sequence %d", request.Sequence)
	}
	if request.Kind != "" && request.Kind != workflowops.KindRun {
		return "", fmt.Errorf("workflow/aws: invalid root kind %q", request.Kind)
	}
	if request.Fingerprint == "" {
		request.Fingerprint = request.Identity
	}
	runID, err := workflowops.NewRunID()
	if err != nil {
		return "", err
	}
	now := time.Now()
	_, err = world.dynamo.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: []ddb.TransactWriteItem{
			put(world.config.TableName, runKey(runID), map[string]string{
				"identity": request.Identity,
				"input":    string(request.Input),
				"status":   string(workflowops.StatusQueued),
				"created":  stamp(now),
				"updated":  stamp(now),
			}),
			put(world.config.TableName, commandKey(runID, workflowops.RootSequence), map[string]string{
				"kind":        string(workflowops.KindRun),
				"identity":    request.Identity,
				"fingerprint": request.Identity,
				"input":       string(request.Input),
			}),
			put(
				world.config.TableName,
				taskKey(runID, workflowops.RootSequence),
				taskFields(runID, workflowops.RootSequence, now),
			),
		},
	})
	if err != nil {
		return "", fmt.Errorf("workflow/aws: create run: %w", err)
	}
	rootTask := workflowops.Task{
		RunID:    runID,
		Sequence: workflowops.RootSequence,
	}
	if err := world.enqueueTask(ctx, rootTask, time.Time{}); err != nil {
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
	command, found, err := world.item(ctx, commandKey(request.RunID, request.Sequence))
	if err != nil {
		return "", err
	}
	if found {
		if !matchesRun(command, request) || command["child"] == "" {
			return "", workflowops.HistoryMismatchError("nested run", request.Sequence)
		}
		return command["child"], nil
	}
	if !allowsCreate(lease, request.RunID) {
		return "", workflowops.HistoryMissingError("nested run command", request.Sequence)
	}
	childID, err := workflowops.NewRunID()
	if err != nil {
		return "", err
	}
	now := time.Now()
	_, err = world.dynamo.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: []ddb.TransactWriteItem{
			leaseCheck(world.config.TableName, lease),
			put(world.config.TableName, commandKey(request.RunID, request.Sequence), map[string]string{
				"kind":        string(workflowops.KindRun),
				"identity":    request.Identity,
				"fingerprint": request.Fingerprint,
				"input":       string(request.Input),
				"child":       childID,
			}),
			put(world.config.TableName, runKey(childID), map[string]string{
				"identity": request.Identity,
				"input":    string(request.Input),
				"status":   string(workflowops.StatusQueued),
				"created":  stamp(now),
				"updated":  stamp(now),
			}),
			put(world.config.TableName, commandKey(childID, workflowops.RootSequence), map[string]string{
				"kind":        string(workflowops.KindRun),
				"identity":    request.Identity,
				"fingerprint": request.Identity,
				"input":       string(request.Input),
			}),
			put(
				world.config.TableName,
				taskKey(childID, workflowops.RootSequence),
				taskFields(childID, workflowops.RootSequence, now),
			),
		},
	})
	if err != nil {
		if conditional(err) {
			if err := world.validateLease(ctx, lease); err != nil {
				return "", err
			}
			return world.CreateRun(ctx, lease, request)
		}
		return "", fmt.Errorf("workflow/aws: create child run: %w", err)
	}
	childTask := workflowops.Task{
		RunID:    childID,
		Sequence: workflowops.RootSequence,
	}
	if err := world.enqueueTask(ctx, childTask, time.Time{}); err != nil {
		return "", err
	}
	return childID, nil
}

// ClaimTask claims a queue-delivered task. A cancelled task returns
// ErrCancelled. A completed or actively leased task returns nil so duplicate
// queue delivery is safe to acknowledge.
func (world *World) ClaimTask(ctx context.Context, task workflowops.Task) (*workflowops.Lease, error) {
	run, found, err := world.item(ctx, runKey(task.RunID))
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, workflowops.ErrRunNotFound
	}
	if workflowops.Status(run["status"]) == workflowops.StatusCancelled {
		return nil, workflowops.ErrCancelled
	}
	token, err := workflowops.NewRunID()
	if err != nil {
		return nil, err
	}
	now := time.Now()
	until := now.Add(world.config.LeaseDuration)
	_, err = world.dynamo.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: awsv2.String(world.config.TableName),
		Key:       taskKey(task.RunID, task.Sequence),
		UpdateExpression: awsv2.String(
			"SET #status=:running, #token=:token, #lease=:lease, #attempt=if_not_exists(#attempt,:zero)+:one",
		),
		ConditionExpression: awsv2.String(
			"(#status=:queued AND (attribute_not_exists(#due) OR #due<=:now)) OR (#status=:running AND #lease<:now)",
		),
		ExpressionAttributeNames: map[string]string{
			"#status":  "status",
			"#token":   "token",
			"#lease":   "lease",
			"#attempt": "attempt",
			"#due":     "due",
		},
		ExpressionAttributeValues: map[string]ddb.AttributeValue{
			":running": avs(string(workflowops.StatusRunning)),
			":queued":  avs(string(workflowops.StatusQueued)),
			":token":   avs(token),
			":lease":   avs(stamp(until)),
			":now":     avs(stamp(now)),
			":zero":    avn(0),
			":one":     avn(1),
		},
	})
	if err != nil {
		if conditional(err) {
			record, found, getErr := world.item(ctx, taskKey(task.RunID, task.Sequence))
			if getErr != nil {
				return nil, getErr
			}
			if found && workflowops.Status(record["status"]) == workflowops.StatusCancelled {
				return nil, workflowops.ErrCancelled
			}
			return nil, nil
		}
		return nil, err
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
	_, err := world.dynamo.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:           awsv2.String(world.config.TableName),
		Key:                 taskKey(run.ID, run.Sequence),
		UpdateExpression:    awsv2.String("SET #lease=:lease"),
		ConditionExpression: awsv2.String("#status=:running AND #token=:token"),
		ExpressionAttributeNames: map[string]string{
			"#lease":  "lease",
			"#status": "status",
			"#token":  "token",
		},
		ExpressionAttributeValues: map[string]ddb.AttributeValue{
			":lease":   avs(stamp(time.Now().Add(world.config.LeaseDuration))),
			":running": avs(string(workflowops.StatusRunning)),
			":token":   avs(run.Token),
		},
	})
	if conditional(err) {
		record, found, getErr := world.item(ctx, taskKey(run.ID, run.Sequence))
		if getErr != nil {
			return getErr
		}
		if found && workflowops.Status(record["status"]) == workflowops.StatusCancelled {
			return workflowops.ErrCancelled
		}
		return workflowops.ErrLeaseLost
	}
	return err
}
func (world *World) CompleteRun(ctx context.Context, run *workflowops.Lease, output json.RawMessage) error {
	if err := world.finishTask(ctx, run, workflowops.StatusCompleted, output, ""); err != nil {
		return err
	}
	if run.Sequence != workflowops.RootSequence {
		return world.resolveDependents(ctx, workflowops.Task{RunID: run.ID, Sequence: run.Sequence})
	}
	_, err := world.dynamo.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:        awsv2.String(world.config.TableName),
		Key:              runKey(run.ID),
		UpdateExpression: awsv2.String("SET #status=:status, #output=:output, #updated=:updated"),
		ExpressionAttributeNames: map[string]string{
			"#status":  "status",
			"#output":  "output",
			"#updated": "updated",
		},
		ExpressionAttributeValues: map[string]ddb.AttributeValue{
			":status":  avs(string(workflowops.StatusCompleted)),
			":output":  avs(string(output)),
			":updated": avs(stamp(time.Now())),
		},
	})
	if err != nil {
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
	_, err := world.dynamo.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:        awsv2.String(world.config.TableName),
		Key:              runKey(run.ID),
		UpdateExpression: awsv2.String("SET #status=:status, #error=:error, #updated=:updated"),
		ExpressionAttributeNames: map[string]string{
			"#status":  "status",
			"#error":   "error",
			"#updated": "updated",
		},
		ExpressionAttributeValues: map[string]ddb.AttributeValue{
			":status":  avs(string(workflowops.StatusFailed)),
			":error":   avs(cause.Error()),
			":updated": avs(stamp(time.Now())),
		},
	})
	if err != nil {
		return err
	}
	return world.resolveDependents(ctx, workflowops.Task{RunID: run.ID, Sequence: run.Sequence})
}

func (world *World) CancelOperations(ctx context.Context, tasks ...workflowops.Task) error {
	pending := append([]workflowops.Task(nil), tasks...)
	changed := make([]workflowops.Task, 0, len(tasks))
	visitedRoots := make(map[string]bool)
	visitedTasks := make(map[workflowops.Task]bool)
	for len(pending) > 0 {
		task := pending[0]
		pending = pending[1:]
		if visitedTasks[task] {
			continue
		}
		visitedTasks[task] = true

		if task.Sequence == workflowops.RootSequence {
			if visitedRoots[task.RunID] {
				continue
			}
			visitedRoots[task.RunID] = true
			cancelled, err := world.cancelTask(ctx, task)
			if err != nil {
				return err
			}
			if cancelled {
				changed = append(changed, task)
				if err := world.cancelRun(ctx, task.RunID); err != nil {
					return err
				}
			}
			all, err := world.runTasks(ctx, task.RunID)
			if err != nil {
				return err
			}
			pending = append(pending, all...)
			children, err := world.childRuns(ctx, task.RunID)
			if err != nil {
				return err
			}
			for _, childID := range children {
				pending = append(pending, workflowops.Task{RunID: childID, Sequence: workflowops.RootSequence})
			}
			if err := world.CleanupTimer(ctx, task); err != nil {
				return err
			}
			continue
		}

		cancelled, err := world.cancelTask(ctx, task)
		if err != nil {
			return err
		}
		if cancelled {
			changed = append(changed, task)
		}
		if err := world.CleanupTimer(ctx, task); err != nil {
			return err
		}
	}
	return world.resolveDependents(ctx, changed...)
}

func (world *World) cancelTask(ctx context.Context, task workflowops.Task) (bool, error) {
	record, found, err := world.item(ctx, taskKey(task.RunID, task.Sequence))
	if err != nil {
		return false, err
	}
	if !found {
		return false, workflowops.ErrRunNotFound
	}
	status := workflowops.Status(record["status"])
	if status == workflowops.StatusCompleted || status == workflowops.StatusFailed || status == workflowops.StatusCancelled {
		return false, nil
	}
	completion, err := world.nextCompletion(ctx)
	if err != nil {
		return false, err
	}
	_, err = world.dynamo.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: awsv2.String(world.config.TableName),
		Key:       taskKey(task.RunID, task.Sequence),
		UpdateExpression: awsv2.String(
			"SET #status=:cancelled, #error=:error, #completed=:completed, #updated=:updated REMOVE #token, #lease",
		),
		ConditionExpression: awsv2.String("#status=:status"),
		ExpressionAttributeNames: map[string]string{
			"#status": "status", "#error": "error", "#completed": "completed",
			"#updated": "updated", "#token": "token", "#lease": "lease",
		},
		ExpressionAttributeValues: map[string]ddb.AttributeValue{
			":status": avs(string(status)), ":cancelled": avs(string(workflowops.StatusCancelled)),
			":error": avs(workflowops.ErrCancelled.Error()), ":completed": avs(strconv.FormatUint(completion, 10)),
			":updated": avs(stamp(time.Now())),
		},
	})
	if conditional(err) {
		return false, nil
	}
	return err == nil, err
}

func (world *World) cancelRun(ctx context.Context, runID string) error {
	_, err := world.dynamo.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:        awsv2.String(world.config.TableName),
		Key:              runKey(runID),
		UpdateExpression: awsv2.String("SET #status=:cancelled, #error=:error, #updated=:updated"),
		ExpressionAttributeNames: map[string]string{
			"#status": "status", "#error": "error", "#updated": "updated",
		},
		ExpressionAttributeValues: map[string]ddb.AttributeValue{
			":cancelled": avs(string(workflowops.StatusCancelled)),
			":error":     avs(workflowops.ErrCancelled.Error()),
			":updated":   avs(stamp(time.Now())),
		},
	})
	return err
}

func (world *World) childRuns(ctx context.Context, runID string) ([]string, error) {
	children := make([]string, 0)
	var start map[string]ddb.AttributeValue
	for {
		result, err := world.dynamo.Query(ctx, &dynamodb.QueryInput{
			TableName:              awsv2.String(world.config.TableName),
			ExclusiveStartKey:      start,
			KeyConditionExpression: awsv2.String("pk=:pk AND begins_with(sk,:command)"),
			ExpressionAttributeValues: map[string]ddb.AttributeValue{
				":pk":      avs(runPartitionKey(runID)),
				":command": avs("COMMAND#"),
			},
		})
		if err != nil {
			return nil, err
		}
		for _, item := range result.Items {
			if childID := read(item, "child"); childID != "" {
				children = append(children, childID)
			}
		}
		if len(result.LastEvaluatedKey) == 0 {
			return children, nil
		}
		start = result.LastEvaluatedKey
	}
}

func (world *World) runTasks(ctx context.Context, runID string) ([]workflowops.Task, error) {
	tasks := make([]workflowops.Task, 0)
	var start map[string]ddb.AttributeValue
	for {
		result, err := world.dynamo.Query(ctx, &dynamodb.QueryInput{
			TableName:              awsv2.String(world.config.TableName),
			ExclusiveStartKey:      start,
			KeyConditionExpression: awsv2.String("pk=:pk AND begins_with(sk,:task)"),
			ExpressionAttributeValues: map[string]ddb.AttributeValue{
				":pk":   avs(runPartitionKey(runID)),
				":task": avs("TASK#"),
			},
		})
		if err != nil {
			return nil, err
		}
		for _, item := range result.Items {
			tasks = append(tasks, workflowops.Task{
				RunID:    runID,
				Sequence: readInt(item, "sequence"),
			})
		}
		if len(result.LastEvaluatedKey) == 0 {
			return tasks, nil
		}
		start = result.LastEvaluatedKey
	}
}

func (world *World) validateLease(ctx context.Context, lease *workflowops.Lease) error {
	record, found, err := world.item(ctx, taskKey(lease.ID, lease.Sequence))
	if err != nil {
		return err
	}
	if !found {
		return workflowops.ErrLeaseLost
	}
	if workflowops.Status(record["status"]) == workflowops.StatusCancelled {
		return workflowops.ErrCancelled
	}
	if workflowops.Status(record["status"]) != workflowops.StatusRunning || record["token"] != lease.Token {
		return workflowops.ErrLeaseLost
	}
	return nil
}

func (world *World) EnsureOperation(
	ctx context.Context,
	lease *workflowops.Lease,
	request workflowops.OperationRequest,
) (workflowops.Operation, error) {
	command, found, err := world.item(ctx, commandKey(request.RunID, request.Sequence))
	if err != nil {
		return workflowops.Operation{}, err
	}
	if found {
		if !matchesOperation(command, request) {
			return workflowops.Operation{}, workflowops.HistoryMismatchError("operation", request.Sequence)
		}
		return world.operation(ctx, request.RunID, request.Sequence)
	}
	if !allowsCreate(lease, request.RunID) {
		return workflowops.Operation{}, workflowops.HistoryMissingError("command", request.Sequence)
	}
	now, due := time.Now(), time.Time{}
	if request.DueAt != nil {
		due = *request.DueAt
	}
	task := taskFields(request.RunID, request.Sequence, now)
	if !due.IsZero() {
		task["due"] = stamp(due)
	}
	_, err = world.dynamo.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: []ddb.TransactWriteItem{
			leaseCheck(world.config.TableName, lease),
			put(world.config.TableName, commandKey(request.RunID, request.Sequence), map[string]string{
				"kind":        string(request.Kind),
				"identity":    request.Identity,
				"fingerprint": request.Fingerprint,
				"input":       string(request.Input),
			}),
			put(world.config.TableName, taskKey(request.RunID, request.Sequence), task),
		},
	})
	if err != nil {
		if conditional(err) {
			if err := world.validateLease(ctx, lease); err != nil {
				return workflowops.Operation{}, err
			}
			return world.EnsureOperation(ctx, lease, request)
		}
		return workflowops.Operation{}, err
	}
	if err := world.enqueueTask(ctx, workflowops.Task{RunID: request.RunID, Sequence: request.Sequence}, due); err != nil {
		return workflowops.Operation{}, err
	}
	return workflowops.Operation{
		RunID:          request.RunID,
		Sequence:       request.Sequence,
		Status:         workflowops.StatusQueued,
		DueAt:          due,
		IdempotencyKey: task["idempotency"],
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
	command, found, err := world.item(ctx, commandKey(request.RunID, request.Sequence))
	if err != nil {
		return workflowops.Operation{}, err
	}
	if found {
		if !matchesWait(command, request, input) {
			return workflowops.Operation{}, workflowops.HistoryMismatchError("wait", request.Sequence)
		}
		return world.operation(ctx, request.RunID, request.Sequence)
	}
	if !allowsCreate(lease, request.RunID) {
		return workflowops.Operation{}, workflowops.HistoryMissingError("wait command", request.Sequence)
	}
	if len(request.Dependencies) > 97 {
		return workflowops.Operation{}, errors.New("workflow/aws: a wait may depend on at most 97 tasks")
	}
	now := time.Now()
	task := taskFields(request.RunID, request.Sequence, now)
	task["status"] = string(workflowops.StatusWaiting)
	wait := workflowops.Task{RunID: request.RunID, Sequence: request.Sequence}
	items := []ddb.TransactWriteItem{
		leaseCheck(world.config.TableName, lease),
		put(world.config.TableName, commandKey(request.RunID, request.Sequence), map[string]string{
			"kind":        string(request.Kind),
			"identity":    string(request.Kind),
			"fingerprint": request.Fingerprint,
			"input":       string(input),
		}),
		put(world.config.TableName, taskKey(request.RunID, request.Sequence), task),
	}
	for _, dependency := range request.Dependencies {
		items = append(items, put(
			world.config.TableName,
			dependentWaitKey(dependency, wait),
			map[string]string{
				"waitRun":      wait.RunID,
				"waitSequence": strconv.Itoa(wait.Sequence),
			},
		))
	}
	_, err = world.dynamo.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: items,
	})
	if err != nil {
		if conditional(err) {
			if err := world.validateLease(ctx, lease); err != nil {
				return workflowops.Operation{}, err
			}
			return world.EnsureWait(ctx, lease, request)
		}
		return workflowops.Operation{}, err
	}
	resolved, err := world.resolveWait(ctx, workflowops.Task{RunID: request.RunID, Sequence: request.Sequence})
	if err != nil {
		return workflowops.Operation{}, err
	}
	if resolved {
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
	_, err := world.dynamo.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:           awsv2.String(world.config.TableName),
		Key:                 taskKey(run.ID, workflowops.RootSequence),
		UpdateExpression:    awsv2.String("SET #status=:waiting, #updated=:updated REMOVE #token, #lease"),
		ConditionExpression: awsv2.String("#status=:running AND #token=:token"),
		ExpressionAttributeNames: map[string]string{
			"#status": "status", "#updated": "updated", "#token": "token", "#lease": "lease",
		},
		ExpressionAttributeValues: map[string]ddb.AttributeValue{
			":waiting": avs(string(workflowops.StatusWaiting)),
			":running": avs(string(workflowops.StatusRunning)),
			":token":   avs(run.Token),
			":updated": avs(stamp(time.Now())),
		},
	})
	if conditional(err) {
		return workflowops.ErrLeaseLost
	}
	if err != nil {
		return err
	}
	_, err = world.dynamo.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:        awsv2.String(world.config.TableName),
		Key:              runKey(run.ID),
		UpdateExpression: awsv2.String("SET #status=:waiting, #updated=:updated"),
		ExpressionAttributeNames: map[string]string{
			"#status":  "status",
			"#updated": "updated",
		},
		ExpressionAttributeValues: map[string]ddb.AttributeValue{
			":waiting": avs(string(workflowops.StatusWaiting)),
			":updated": avs(stamp(time.Now())),
		},
	})
	if err != nil {
		return err
	}
	return world.resumeLatestWait(ctx, run.ID)
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
		run, found, err := world.item(ctx, runKey(runID))
		if err != nil {
			return nil, err
		}
		if !found {
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
			item, _, err := world.item(ctx, taskKey(task.RunID, task.Sequence))
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
	_, err := world.dynamo.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: awsv2.String(world.config.TableName),
		Item: merge(eventKey(task.RunID, task.Sequence), map[string]string{
			"run":      task.RunID,
			"sequence": strconv.Itoa(task.Sequence),
			"data":     string(data),
			"created":  stamp(time.Now()),
		}),
		ConditionExpression: awsv2.String("attribute_not_exists(pk)"),
	})
	if conditional(err) {
		return nil
	}
	return err
}

func (world *World) Events(ctx context.Context, runID string, after int64) ([]workflow.Event, error) {
	result, err := world.dynamo.Query(ctx, &dynamodb.QueryInput{
		TableName:              awsv2.String(world.config.TableName),
		KeyConditionExpression: awsv2.String("pk=:pk AND begins_with(sk,:event)"),
		ExpressionAttributeValues: map[string]ddb.AttributeValue{
			":pk":    avs(runPartitionKey(runID)),
			":event": avs("EVENT#"),
		},
	})
	if err != nil {
		return nil, err
	}
	events := make([]workflow.Event, 0, len(result.Items))
	for _, item := range result.Items {
		id := int64(readInt(item, "sequence"))
		if id > after {
			created, _ := time.Parse(time.RFC3339Nano, read(item, "created"))
			events = append(events, workflow.Event{
				ID:        id,
				Data:      []byte(read(item, "data")),
				CreatedAt: created,
			})
		}
	}
	return events, nil
}

func (world *World) operation(ctx context.Context, runID string, sequence int) (workflowops.Operation, error) {
	task, found, err := world.item(ctx, taskKey(runID, sequence))
	if err != nil {
		return workflowops.Operation{}, err
	}
	if !found {
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
	completed, err := world.nextCompletion(ctx)
	if err != nil {
		return err
	}
	names := map[string]string{
		"#status":    "status",
		"#token":     "token",
		"#lease":     "lease",
		"#updated":   "updated",
		"#completed": "completed",
	}
	values := map[string]ddb.AttributeValue{
		":status":    avs(string(status)),
		":token":     avs(run.Token),
		":running":   avs(string(workflowops.StatusRunning)),
		":updated":   avs(stamp(time.Now())),
		":completed": avs(strconv.FormatUint(completed, 10)),
	}
	set := "SET #status=:status, #updated=:updated, #completed=:completed"
	if output != nil {
		names["#output"] = "output"
		values[":output"] = avs(string(output))
		set += ", #output=:output"
	}
	if message != "" {
		names["#error"] = "error"
		values[":error"] = avs(message)
		set += ", #error=:error"
	}
	_, err = world.dynamo.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                 awsv2.String(world.config.TableName),
		Key:                       taskKey(run.ID, run.Sequence),
		UpdateExpression:          awsv2.String(set + " REMOVE #token, #lease"),
		ConditionExpression:       awsv2.String("#status=:running AND #token=:token"),
		ExpressionAttributeNames:  names,
		ExpressionAttributeValues: values,
	})
	if conditional(err) {
		return workflowops.ErrLeaseLost
	}
	return err
}

func (world *World) resolveDependents(ctx context.Context, changed ...workflowops.Task) error {
	for len(changed) > 0 {
		dependency := changed[0]
		changed = changed[1:]
		var start map[string]ddb.AttributeValue
		for {
			page, err := world.dynamo.Query(ctx, &dynamodb.QueryInput{
				TableName:              awsv2.String(world.config.TableName),
				ExclusiveStartKey:      start,
				KeyConditionExpression: awsv2.String("pk=:pk AND begins_with(sk,:wait)"),
				ExpressionAttributeValues: map[string]ddb.AttributeValue{
					":pk":   avs(dependencyPartitionKey(dependency)),
					":wait": avs("WAIT#"),
				},
			})
			if err != nil {
				return err
			}
			for _, edge := range page.Items {
				wait := workflowops.Task{
					RunID:    read(edge, "waitRun"),
					Sequence: readInt(edge, "waitSequence"),
				}
				if wait.RunID == "" {
					continue
				}
				resolved, err := world.resolveWait(ctx, wait)
				if err != nil {
					return err
				}
				if resolved {
					changed = append(changed, wait)
				}
			}
			if len(page.LastEvaluatedKey) == 0 {
				break
			}
			start = page.LastEvaluatedKey
		}
	}
	return nil
}

func (world *World) resolveWait(ctx context.Context, wait workflowops.Task) (bool, error) {
	command, found, err := world.item(ctx, commandKey(wait.RunID, wait.Sequence))
	if err != nil || !found {
		return false, err
	}
	kind := workflowops.Kind(command["kind"])
	if !workflowops.IsWaitKind(kind) {
		return false, nil
	}
	var dependencies []workflowops.Task
	if err := json.Unmarshal([]byte(command["input"]), &dependencies); err != nil {
		return false, err
	}
	states, err := world.waitStates(ctx, dependencies)
	if err != nil {
		return false, err
	}
	resolution := workflowops.ResolveWait(kind, states)
	if resolution.Pending {
		return false, nil
	}
	status, message := workflowops.StatusCompleted, ""
	if resolution.Err != nil {
		status, message = workflowops.StatusFailed, resolution.Err.Error()
	}
	return world.transitionWait(ctx, wait, status, resolution.Output, message)
}

// resumeLatestWait closes the race between evaluating a wait and releasing the
// root lease. It reads just this run's command partition, never all waits.
func (world *World) resumeLatestWait(ctx context.Context, runID string) error {
	latest := workflowops.Task{RunID: runID, Sequence: workflowops.RootSequence}
	found := false
	var start map[string]ddb.AttributeValue
	for {
		result, err := world.dynamo.Query(ctx, &dynamodb.QueryInput{
			TableName:              awsv2.String(world.config.TableName),
			ExclusiveStartKey:      start,
			KeyConditionExpression: awsv2.String("pk=:pk AND begins_with(sk,:command)"),
			ExpressionAttributeValues: map[string]ddb.AttributeValue{
				":pk":      avs(runPartitionKey(runID)),
				":command": avs("COMMAND#"),
			},
		})
		if err != nil {
			return err
		}
		for _, command := range result.Items {
			kind := workflowops.Kind(read(command, "kind"))
			sequence, err := strconv.Atoi(strings.TrimPrefix(read(command, "sk"), "COMMAND#"))
			if err != nil {
				return err
			}
			sequence--
			if workflowops.IsWaitKind(kind) && (!found || sequence > latest.Sequence) {
				latest.Sequence, found = sequence, true
			}
		}
		if len(result.LastEvaluatedKey) == 0 {
			break
		}
		start = result.LastEvaluatedKey
	}
	if !found {
		return nil
	}
	task, found, err := world.item(ctx, taskKey(runID, latest.Sequence))
	if err != nil || !found {
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
	_, err = world.dynamo.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:           awsv2.String(world.config.TableName),
		Key:                 taskKey(runID, workflowops.RootSequence),
		UpdateExpression:    awsv2.String("SET #status=:queued, #updated=:updated"),
		ConditionExpression: awsv2.String("#status=:waiting"),
		ExpressionAttributeNames: map[string]string{
			"#status": "status", "#updated": "updated",
		},
		ExpressionAttributeValues: map[string]ddb.AttributeValue{
			":waiting": avs(string(workflowops.StatusWaiting)),
			":queued":  avs(string(workflowops.StatusQueued)),
			":updated": avs(stamp(time.Now())),
		},
	})
	if conditional(err) {
		return nil
	}
	if err != nil {
		return err
	}
	_, err = world.dynamo.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:        awsv2.String(world.config.TableName),
		Key:              runKey(runID),
		UpdateExpression: awsv2.String("SET #status=:queued, #updated=:updated"),
		ExpressionAttributeNames: map[string]string{
			"#status": "status", "#updated": "updated",
		},
		ExpressionAttributeValues: map[string]ddb.AttributeValue{
			":queued":  avs(string(workflowops.StatusQueued)),
			":updated": avs(stamp(time.Now())),
		},
	})
	if err != nil {
		return err
	}
	return world.enqueueTask(ctx, workflowops.Task{RunID: runID, Sequence: workflowops.RootSequence}, time.Time{})
}

func (world *World) waitStates(ctx context.Context, dependencies []workflowops.Task) ([]workflowops.Dependency, error) {
	keys := make([]map[string]ddb.AttributeValue, len(dependencies))
	for index, dependency := range dependencies {
		keys[index] = taskKey(dependency.RunID, dependency.Sequence)
	}
	items, err := world.items(ctx, keys...)
	if err != nil {
		return nil, err
	}
	states := make([]workflowops.Dependency, len(dependencies))
	for index, dependency := range dependencies {
		task, found := items[itemID(taskKey(dependency.RunID, dependency.Sequence))]
		states[index] = workflowops.Dependency{
			Task:       dependency,
			Found:      found,
			Status:     workflowops.Status(task["status"]),
			Output:     json.RawMessage(task["output"]),
			Error:      task["error"],
			Completion: completion(task["completed"]),
		}
	}
	return states, nil
}

func (world *World) transitionWait(
	ctx context.Context,
	task workflowops.Task,
	status workflowops.Status,
	output json.RawMessage,
	message string,
) (bool, error) {
	now := stamp(time.Now())
	completed, err := world.nextCompletion(ctx)
	if err != nil {
		return false, err
	}
	names := map[string]string{
		"#status":    "status",
		"#updated":   "updated",
		"#completed": "completed",
	}
	values := map[string]ddb.AttributeValue{
		":waiting":   avs(string(workflowops.StatusWaiting)),
		":status":    avs(string(status)),
		":updated":   avs(now),
		":completed": avs(strconv.FormatUint(completed, 10)),
	}
	set := "SET #status=:status, #updated=:updated, #completed=:completed"
	if output != nil {
		names["#output"] = "output"
		values[":output"] = avs(string(output))
		set += ", #output=:output"
	}
	if message != "" {
		names["#error"] = "error"
		values[":error"] = avs(message)
		set += ", #error=:error"
	}
	_, err = world.dynamo.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                 awsv2.String(world.config.TableName),
		Key:                       taskKey(task.RunID, task.Sequence),
		UpdateExpression:          awsv2.String(set),
		ConditionExpression:       awsv2.String("#status=:waiting"),
		ExpressionAttributeNames:  names,
		ExpressionAttributeValues: values,
	})
	if conditional(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	_, err = world.dynamo.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:           awsv2.String(world.config.TableName),
		Key:                 taskKey(task.RunID, workflowops.RootSequence),
		UpdateExpression:    awsv2.String("SET #status=:queued, #updated=:updated"),
		ConditionExpression: awsv2.String("#status=:waiting"),
		ExpressionAttributeNames: map[string]string{
			"#status":  "status",
			"#updated": "updated",
		},
		ExpressionAttributeValues: map[string]ddb.AttributeValue{
			":waiting": avs(string(workflowops.StatusWaiting)),
			":queued":  avs(string(workflowops.StatusQueued)),
			":updated": avs(now),
		},
	})
	if conditional(err) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	_, err = world.dynamo.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:        awsv2.String(world.config.TableName),
		Key:              runKey(task.RunID),
		UpdateExpression: awsv2.String("SET #status=:queued, #updated=:updated"),
		ExpressionAttributeNames: map[string]string{
			"#status":  "status",
			"#updated": "updated",
		},
		ExpressionAttributeValues: map[string]ddb.AttributeValue{
			":queued":  avs(string(workflowops.StatusQueued)),
			":updated": avs(now),
		},
	})
	if err != nil {
		return false, err
	}
	if err := world.enqueueTask(ctx, workflowops.Task{RunID: task.RunID, Sequence: workflowops.RootSequence}, time.Time{}); err != nil {
		return false, err
	}
	return true, nil
}

func (world *World) enqueueTask(ctx context.Context, task workflowops.Task, due time.Time) error {
	delay := time.Duration(0)
	if !due.IsZero() {
		delay = max(time.Until(due), 0)
	}
	body, err := json.Marshal(task)
	if err != nil {
		return err
	}
	if delay <= 15*time.Minute {
		_, err = world.sqs.SendMessage(ctx, &sqs.SendMessageInput{
			QueueUrl:     awsv2.String(world.config.QueueURL),
			MessageBody:  awsv2.String(string(body)),
			DelaySeconds: int32(delay.Round(time.Second).Seconds()),
		})
		return err
	}
	schedulerConfig := world.config.Scheduler
	if schedulerConfig == nil ||
		schedulerConfig.Client == nil ||
		schedulerConfig.TargetARN == "" ||
		schedulerConfig.RoleARN == "" {
		return errors.New("workflow/aws: timer over fifteen minutes requires EventBridge Scheduler target, role, and client")
	}
	group := schedulerConfig.Group
	if group == "" {
		group = "default"
	}
	name := "go-workflow-" + task.RunID + "-" + strconv.Itoa(task.Sequence)
	_, err = schedulerConfig.Client.CreateSchedule(ctx, &scheduler.CreateScheduleInput{
		Name:               awsv2.String(name),
		GroupName:          awsv2.String(group),
		ScheduleExpression: awsv2.String("at(" + due.UTC().Format("2006-01-02T15:04:05") + ")"),
		FlexibleTimeWindow: &sch.FlexibleTimeWindow{Mode: sch.FlexibleTimeWindowModeOff},
		Target: &sch.Target{
			Arn:     awsv2.String(schedulerConfig.TargetARN),
			RoleArn: awsv2.String(schedulerConfig.RoleARN),
			Input:   awsv2.String(string(body)),
		},
		ActionAfterCompletion: sch.ActionAfterCompletionDelete,
	})
	if err != nil {
		return err
	}
	if err = world.recordTimer(ctx, task, name); err == nil {
		return nil
	}
	cleanupErr := world.deleteTimer(ctx, name)
	if cleanupErr != nil {
		return errors.Join(err, cleanupErr)
	}
	if conditional(err) {
		return nil
	}
	return err
}

func (world *World) recordTimer(ctx context.Context, task workflowops.Task, handle string) error {
	_, err := world.dynamo.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:           awsv2.String(world.config.TableName),
		Key:                 taskKey(task.RunID, task.Sequence),
		UpdateExpression:    awsv2.String("SET #timer=:timer"),
		ConditionExpression: awsv2.String("#status=:queued"),
		ExpressionAttributeNames: map[string]string{
			"#timer":  "timer",
			"#status": "status",
		},
		ExpressionAttributeValues: map[string]ddb.AttributeValue{
			":timer":  avs(handle),
			":queued": avs(string(workflowops.StatusQueued)),
		},
	})
	return err
}

func (world *World) nextCompletion(ctx context.Context) (uint64, error) {
	result, err := world.dynamo.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:        awsv2.String(world.config.TableName),
		Key:              key("GO_WORKFLOW", "COMPLETION"),
		UpdateExpression: awsv2.String("ADD #value :one"),
		ExpressionAttributeNames: map[string]string{
			"#value": "value",
		},
		ExpressionAttributeValues: map[string]ddb.AttributeValue{
			":one": avn(1),
		},
		ReturnValues: ddb.ReturnValueUpdatedNew,
	})
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(read(result.Attributes, "value"), 10, 64)
}

func (world *World) item(ctx context.Context, key map[string]ddb.AttributeValue) (map[string]string, bool, error) {
	result, err := world.dynamo.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      awsv2.String(world.config.TableName),
		Key:            key,
		ConsistentRead: awsv2.Bool(true),
	})
	if err != nil {
		return nil, false, err
	}
	if len(result.Item) == 0 {
		return nil, false, nil
	}
	value := map[string]string{}
	for name, attribute := range result.Item {
		value[name] = readAttr(attribute)
	}
	return value, true, nil
}

// items reads task records in DynamoDB's 100-key batches. Dependency state is
// immutable for wait resolution, so a single consistent batch per wait avoids
// one request per dependency while preserving the storage boundary here.
func (world *World) items(ctx context.Context, keys ...map[string]ddb.AttributeValue) (map[string]map[string]string, error) {
	items := make(map[string]map[string]string, len(keys))
	for len(keys) > 0 {
		count := min(len(keys), 100)
		request := map[string]ddb.KeysAndAttributes{
			world.config.TableName: {
				Keys:           keys[:count],
				ConsistentRead: awsv2.Bool(true),
			},
		}
		keys = keys[count:]
		for len(request) > 0 {
			result, err := world.dynamo.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{
				RequestItems: request,
			})
			if err != nil {
				return nil, err
			}
			for _, item := range result.Responses[world.config.TableName] {
				value := make(map[string]string, len(item))
				for name, attribute := range item {
					value[name] = readAttr(attribute)
				}
				items[itemID(item)] = value
			}
			request = result.UnprocessedKeys
		}
	}
	return items, nil
}

func itemID(item map[string]ddb.AttributeValue) string {
	return read(item, "pk") + "\x00" + read(item, "sk")
}

func stamp(value time.Time) string {
	return value.UTC().Format(time.RFC3339Nano)
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

func completion(value string) uint64 {
	result, _ := strconv.ParseUint(value, 10, 64)
	return result
}

func conditional(err error) bool {
	var failed *ddb.ConditionalCheckFailedException
	if errors.As(err, &failed) {
		return true
	}
	var cancelled *ddb.TransactionCanceledException
	return errors.As(err, &cancelled)
}
