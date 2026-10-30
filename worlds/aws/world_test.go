package awsworld

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	awsv2 "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	sch "github.com/aws/aws-sdk-go-v2/service/scheduler/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/eriicafes/go-workflow/workflowtest"
	"github.com/eriicafes/go-workflow/worlds"
)

var errUnsupported = errors.New("fake: unsupported operation")

const (
	testBaseURL  = "https://workflows.example.com/v1/workflow"
	testQueueURL = "https://sqs.us-east-1.amazonaws.com/000000000000/workflow"
)

func TestNew(t *testing.T) {
	if _, err := New(Config{BaseURL: testBaseURL, TableName: "t", QueueURL: testQueueURL}, nil, nil); err == nil {
		t.Fatal("New without clients succeeded")
	}
	if _, err := newWorld(Config{BaseURL: "not a url", TableName: "t", QueueURL: testQueueURL}, newFakeDynamo(), &fakeSQS{}, nil); err == nil {
		t.Fatal("invalid base URL accepted")
	}
	if _, err := newWorld(Config{BaseURL: testBaseURL, QueueURL: testQueueURL}, newFakeDynamo(), &fakeSQS{}, nil); err == nil {
		t.Fatal("missing table accepted")
	}
	if _, err := newWorld(Config{BaseURL: testBaseURL, TableName: "t", QueueURL: testQueueURL, Scheduler: &SchedulerConfig{TargetARN: "arn"}}, newFakeDynamo(), &fakeSQS{}, newFakeScheduler()); err == nil {
		t.Fatal("incomplete scheduler config accepted")
	}
	world := testWorld(t, newFakeDynamo(), &fakeSQS{}, nil)
	if world.BaseURL() != testBaseURL {
		t.Fatalf("BaseURL = %q", world.BaseURL())
	}
}

func TestQueueRouting(t *testing.T) {
	ctx := context.Background()
	dynamo, queue, schedules := newFakeDynamo(), &fakeSQS{}, newFakeScheduler()
	world := testWorld(t, dynamo, queue, schedules)

	now, err := world.Queue(ctx, worlds.QueueInput{RunID: "wrun_a"})
	if err != nil {
		t.Fatal(err)
	}
	soon := time.Now().Add(10 * time.Minute)
	short, err := world.Queue(ctx, worlds.QueueInput{RunID: "wrun_a", NotBefore: &soon})
	if err != nil {
		t.Fatal(err)
	}
	sent := queue.sent()
	if len(sent) != 2 || sent[0].DelaySeconds != 0 || sent[1].DelaySeconds < 595 || sent[1].DelaySeconds > 600 || awsv2.ToString(sent[0].QueueUrl) != testQueueURL {
		t.Fatalf("sent = %#v", sent)
	}
	var message queueMessage
	if err := json.Unmarshal([]byte(awsv2.ToString(sent[1].MessageBody)), &message); err != nil || message.ID != short.MessageID || message.Input.RunID != "wrun_a" || message.Input.NotBefore == nil {
		t.Fatalf("body = %s err=%v", awsv2.ToString(sent[1].MessageBody), err)
	}
	if !dynamo.has(now.MessageID) || !dynamo.has(short.MessageID) {
		t.Fatal("queue records missing")
	}

	later := time.Now().Add(2 * time.Hour)
	long, err := world.Queue(ctx, worlds.QueueInput{RunID: "wrun_a", NotBefore: &later})
	if err != nil {
		t.Fatal(err)
	}
	if len(queue.sent()) != 2 {
		t.Fatal("long delay sent to SQS")
	}
	schedule := schedules.schedules[scheduleName(long.MessageID)]
	if schedule == nil {
		t.Fatalf("no schedule for %s", long.MessageID)
	}
	if awsv2.ToString(schedule.ScheduleExpression) != scheduleExpression(later) || schedule.FlexibleTimeWindow.Mode != sch.FlexibleTimeWindowModeOff || schedule.ActionAfterCompletion != sch.ActionAfterCompletionDelete || awsv2.ToString(schedule.GroupName) != "default" || awsv2.ToString(schedule.Target.RoleArn) == "" {
		t.Fatalf("schedule = %#v", schedule)
	}
	if err := json.Unmarshal([]byte(awsv2.ToString(schedule.Target.Input)), &message); err != nil || message.ID != long.MessageID {
		t.Fatalf("schedule input = %s err=%v", awsv2.ToString(schedule.Target.Input), err)
	}
	if read(dynamo.items[itemID(queueKey(long.MessageID))], "schedule") != scheduleName(long.MessageID) {
		t.Fatal("schedule name not stored on the queue record")
	}
}

func TestQueueWithoutScheduler(t *testing.T) {
	dynamo, queue := newFakeDynamo(), &fakeSQS{}
	world := testWorld(t, dynamo, queue, nil)
	later := time.Now().Add(time.Hour)
	if _, err := world.Queue(context.Background(), worlds.QueueInput{RunID: "wrun_a", NotBefore: &later}); err == nil {
		t.Fatal("long delay without scheduler succeeded")
	}
	if dynamo.count() != 0 || len(queue.sent()) != 0 {
		t.Fatal("rejected message left state behind")
	}
}

func TestQueuePublishFailure(t *testing.T) {
	ctx := context.Background()
	dynamo, queue, schedules := newFakeDynamo(), &fakeSQS{err: errors.New("sqs down")}, newFakeScheduler()
	world := testWorld(t, dynamo, queue, schedules)
	if _, err := world.Queue(ctx, worlds.QueueInput{RunID: "wrun_a"}); err == nil {
		t.Fatal("queue succeeded while SQS failed")
	}
	schedules.err = errors.New("scheduler down")
	later := time.Now().Add(time.Hour)
	if _, err := world.Queue(ctx, worlds.QueueInput{RunID: "wrun_a", NotBefore: &later}); err == nil {
		t.Fatal("queue succeeded while Scheduler failed")
	}
	if dynamo.count() != 0 {
		t.Fatalf("failed publishes left %d queue records", dynamo.count())
	}
}

func TestQueueCancel(t *testing.T) {
	ctx := context.Background()
	dynamo, schedules := newFakeDynamo(), newFakeScheduler()
	world := testWorld(t, dynamo, &fakeSQS{}, schedules)
	if err := world.QueueCancel(ctx, worlds.QueueCancelInput{MessageID: "missing"}); !errors.Is(err, worlds.ErrQueueMessageNotFound) {
		t.Fatalf("cancel missing = %v", err)
	}
	later := time.Now().Add(time.Hour)
	long, err := world.Queue(ctx, worlds.QueueInput{RunID: "wrun_a", NotBefore: &later})
	if err != nil {
		t.Fatal(err)
	}
	short, err := world.Queue(ctx, worlds.QueueInput{RunID: "wrun_a"})
	if err != nil {
		t.Fatal(err)
	}
	if err := world.QueueCancel(ctx, worlds.QueueCancelInput{MessageID: long.MessageID}); err != nil {
		t.Fatal(err)
	}
	if schedules.count() != 0 || dynamo.has(long.MessageID) {
		t.Fatal("cancel left the schedule or record")
	}
	if err := world.QueueCancel(ctx, worlds.QueueCancelInput{MessageID: short.MessageID}); err != nil {
		t.Fatal(err)
	}
	if err := world.QueueCancel(ctx, worlds.QueueCancelInput{MessageID: short.MessageID}); !errors.Is(err, worlds.ErrQueueMessageNotFound) {
		t.Fatalf("second cancel = %v", err)
	}
	// A schedule that already fired and deleted itself still cancels cleanly.
	fired, err := world.Queue(ctx, worlds.QueueInput{RunID: "wrun_a", NotBefore: &later})
	if err != nil {
		t.Fatal(err)
	}
	delete(schedules.schedules, scheduleName(fired.MessageID))
	if err := world.QueueCancel(ctx, worlds.QueueCancelInput{MessageID: fired.MessageID}); err != nil {
		t.Fatalf("cancel after schedule fired: %v", err)
	}
}

var tableSequence atomic.Int64

func TestConformance(t *testing.T) {
	client := dynamoClient(t)
	workflowtest.Conformance(t, func(t *testing.T) worlds.World {
		return dynamoWorld(t, client, newTable(t, client))
	})
}

func TestConformanceShared(t *testing.T) {
	client := dynamoClient(t)
	workflowtest.ConformanceShared(t, func(t *testing.T) (worlds.World, worlds.World) {
		table := newTable(t, client)
		return dynamoWorld(t, client, table), dynamoWorld(t, client, table)
	})
}

func TestDynamoLimits(t *testing.T) {
	client := dynamoClient(t)
	testLimits(t, dynamoWorld(t, client, newTable(t, client)))
}

// fakeDynamo stores items by key and understands only the conditions the
// queue code uses. Transactions and updates fail.
type fakeDynamo struct {
	mu          sync.Mutex
	items       map[string]map[string]ddb.AttributeValue
	unprocessed int
	batchCalls  int
	getErr      error
	// getDelay delays the next GetItem once.
	getDelay time.Duration
}

func newFakeDynamo() *fakeDynamo {
	return &fakeDynamo{items: map[string]map[string]ddb.AttributeValue{}}
}

func (f *fakeDynamo) GetItem(_ context.Context, input *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	f.mu.Lock()
	delay := f.getDelay
	f.getDelay = 0
	f.mu.Unlock()
	time.Sleep(delay)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.getErr != nil {
		return nil, f.getErr
	}
	return &dynamodb.GetItemOutput{Item: f.items[itemID(input.Key)]}, nil
}

func (f *fakeDynamo) PutItem(_ context.Context, input *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id := itemID(input.Item)
	if awsv2.ToString(input.ConditionExpression) == "attribute_not_exists(pk)" && f.items[id] != nil {
		return nil, &ddb.ConditionalCheckFailedException{}
	}
	f.items[id] = input.Item
	return &dynamodb.PutItemOutput{}, nil
}

func (f *fakeDynamo) DeleteItem(_ context.Context, input *dynamodb.DeleteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id := itemID(input.Key)
	old := f.items[id]
	if awsv2.ToString(input.ConditionExpression) == "attribute_exists(pk)" && old == nil {
		return nil, &ddb.ConditionalCheckFailedException{}
	}
	delete(f.items, id)
	output := &dynamodb.DeleteItemOutput{}
	if input.ReturnValues == ddb.ReturnValueAllOld {
		output.Attributes = old
	}
	return output, nil
}

// BatchGetItem leaves the first unprocessed keys of each call unprocessed
// while f.unprocessed is positive, to exercise the retry loop.
func (f *fakeDynamo) BatchGetItem(_ context.Context, input *dynamodb.BatchGetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.BatchGetItemOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.batchCalls++
	output := &dynamodb.BatchGetItemOutput{Responses: map[string][]map[string]ddb.AttributeValue{}, UnprocessedKeys: map[string]ddb.KeysAndAttributes{}}
	for table, request := range input.RequestItems {
		if len(request.Keys) > 100 {
			return nil, errors.New("fake: more than 100 keys")
		}
		keys := request.Keys
		if f.unprocessed > 0 {
			skip := min(f.unprocessed, len(keys))
			f.unprocessed -= skip
			output.UnprocessedKeys[table] = ddb.KeysAndAttributes{Keys: keys[:skip], ConsistentRead: request.ConsistentRead}
			keys = keys[skip:]
		}
		for _, key := range keys {
			if item := f.items[itemID(key)]; item != nil {
				output.Responses[table] = append(output.Responses[table], item)
			}
		}
	}
	if len(output.UnprocessedKeys) == 0 {
		output.UnprocessedKeys = nil
	}
	return output, nil
}

func (f *fakeDynamo) UpdateItem(context.Context, *dynamodb.UpdateItemInput, ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	return nil, errUnsupported
}

// Query returns no items; the queue tests only need an empty event log.
func (f *fakeDynamo) Query(context.Context, *dynamodb.QueryInput, ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	return &dynamodb.QueryOutput{}, nil
}

func (f *fakeDynamo) TransactWriteItems(context.Context, *dynamodb.TransactWriteItemsInput, ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
	return nil, errUnsupported
}

func (f *fakeDynamo) has(id string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.items[itemID(queueKey(id))] != nil
}

func (f *fakeDynamo) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.items)
}

type fakeSQS struct {
	mu         sync.Mutex
	messages   []*sqs.SendMessageInput
	visibility []*sqs.ChangeMessageVisibilityInput
	err        error
}

func (f *fakeSQS) ChangeMessageVisibility(_ context.Context, input *sqs.ChangeMessageVisibilityInput, _ ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.visibility = append(f.visibility, input)
	return &sqs.ChangeMessageVisibilityOutput{}, nil
}

func (f *fakeSQS) SendMessage(_ context.Context, input *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	f.messages = append(f.messages, input)
	return &sqs.SendMessageOutput{}, nil
}

func (f *fakeSQS) sent() []*sqs.SendMessageInput {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*sqs.SendMessageInput(nil), f.messages...)
}

type fakeScheduler struct {
	mu        sync.Mutex
	schedules map[string]*scheduler.CreateScheduleInput
	deleted   []string
	err       error
}

func newFakeScheduler() *fakeScheduler {
	return &fakeScheduler{schedules: map[string]*scheduler.CreateScheduleInput{}}
}

func (f *fakeScheduler) CreateSchedule(_ context.Context, input *scheduler.CreateScheduleInput, _ ...func(*scheduler.Options)) (*scheduler.CreateScheduleOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	f.schedules[awsv2.ToString(input.Name)] = input
	return &scheduler.CreateScheduleOutput{}, nil
}

func (f *fakeScheduler) DeleteSchedule(_ context.Context, input *scheduler.DeleteScheduleInput, _ ...func(*scheduler.Options)) (*scheduler.DeleteScheduleOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	name := awsv2.ToString(input.Name)
	f.deleted = append(f.deleted, name)
	if _, ok := f.schedules[name]; !ok {
		return nil, &sch.ResourceNotFoundException{}
	}
	delete(f.schedules, name)
	return &scheduler.DeleteScheduleOutput{}, nil
}

func (f *fakeScheduler) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.schedules)
}

// testWorld builds a World over fakes. A nil scheduler leaves Scheduler
// unconfigured.
func testWorld(t *testing.T, dynamo dynamoAPI, queue sqsAPI, schedules *fakeScheduler) *World {
	t.Helper()
	config := Config{BaseURL: testBaseURL + "/", TableName: "workflow", QueueURL: testQueueURL}
	var schedulerClient schedulerAPI
	if schedules != nil {
		config.Scheduler = &SchedulerConfig{TargetARN: "arn:aws:sqs:us-east-1:000000000000:workflow", RoleARN: "arn:aws:iam::000000000000:role/scheduler"}
		schedulerClient = schedules
	}
	world, err := newWorld(config, dynamo, queue, schedulerClient)
	if err != nil {
		t.Fatal(err)
	}
	return world
}

// dynamoClient returns a client for the DynamoDB Local instance at
// DYNAMODB_ENDPOINT, defaulting to http://localhost:8000.
func dynamoClient(t *testing.T) *dynamodb.Client {
	t.Helper()
	endpoint := cmp.Or(os.Getenv("DYNAMODB_ENDPOINT"), "http://localhost:8000")
	client := dynamodb.New(dynamodb.Options{
		Region:       "us-east-1",
		BaseEndpoint: awsv2.String(endpoint),
		Credentials: awsv2.CredentialsProviderFunc(func(context.Context) (awsv2.Credentials, error) {
			return awsv2.Credentials{AccessKeyID: "test", SecretAccessKey: "test"}, nil
		}),
		// DynamoDB Local occasionally closes a connection mid-response, which
		// the SDK retries as a transient error.
		RetryMaxAttempts: 5,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := client.ListTables(ctx, &dynamodb.ListTablesInput{Limit: awsv2.Int32(1)}); err != nil {
		t.Fatalf("DynamoDB Local is not reachable at %s (set DYNAMODB_ENDPOINT): %v", endpoint, err)
	}
	return client
}

// newTable creates a fresh table that is deleted when the test ends.
func newTable(t *testing.T, client *dynamodb.Client) string {
	t.Helper()
	name := fmt.Sprintf("go_workflow_test_%d_%d", time.Now().UnixNano(), tableSequence.Add(1))
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := CreateTable(ctx, client, name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = client.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{TableName: awsv2.String(name)})
	})
	return name
}

func dynamoWorld(t *testing.T, client dynamoAPI, table string) *World {
	t.Helper()
	world, err := newWorld(Config{
		BaseURL:   testBaseURL,
		TableName: table,
		QueueURL:  testQueueURL,
		Scheduler: &SchedulerConfig{TargetARN: "arn:aws:sqs:us-east-1:000000000000:workflow", RoleARN: "arn:aws:iam::000000000000:role/scheduler"},
	}, client, &fakeSQS{}, newFakeScheduler())
	if err != nil {
		t.Fatal(err)
	}
	return world
}

func testLimits(t *testing.T, world *World) {
	ctx := context.Background()
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: "wrun_limits", WorkflowName: "limits"}); err != nil {
		t.Fatal(err)
	}
	_, err := world.EventsCreate(ctx, worlds.EventsCreateInput{RunID: "wrun_limits", Event: worlds.EventsCreateEvent{EventType: worlds.EventTypeWaitCreated, CorrelationID: "w", Data: make([]byte, maxItemBytes)}})
	if !errors.Is(err, worlds.ErrEventInvalid) {
		t.Fatalf("oversized event error = %v", err)
	}
	batch := make([]worlds.EventsCreateEvent, maxTransactItems)
	for index := range batch {
		batch[index] = worlds.EventsCreateEvent{EventType: worlds.EventTypeWaitCreated, CorrelationID: fmt.Sprint(index)}
	}
	if _, err := world.EventsCreateBatch(ctx, worlds.EventsCreateBatchInput{RunID: "wrun_limits", Events: batch}); !errors.Is(err, worlds.ErrEventInvalid) {
		t.Fatalf("oversized batch error = %v", err)
	}
	if events, err := world.EventsList(ctx, worlds.EventsListInput{RunID: "wrun_limits"}); err != nil || len(events.Data) != 1 {
		t.Fatalf("events after rejected writes = %#v err=%v", events, err)
	}
	chunks := make([][]byte, maxChunksPerWrite+1)
	if err := world.StreamsWriteMulti(ctx, worlds.StreamsWriteMultiInput{RunID: "wrun_limits", Name: "s", Chunks: chunks}); err == nil {
		t.Fatal("oversized stream write succeeded")
	}
}
