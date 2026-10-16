package awsworld

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"time"

	awsv2 "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/eriicafes/go-workflow/worlds"
)

type runCreatedData struct {
	WorkflowName string            `json:"workflowName"`
	Input        json.RawMessage   `json:"input"`
	Attributes   map[string]string `json:"attributes"`
	DeploymentID string            `json:"deploymentId,omitempty"`
}

type attrSetData struct {
	Changes []worlds.RunsAttributeChange `json:"changes"`
}

type stepCreatedData struct {
	StepName string          `json:"stepName"`
	Input    json.RawMessage `json:"input"`
}

type stepResultData struct {
	Output     json.RawMessage `json:"output"`
	Error      json.RawMessage `json:"error"`
	RetryAfter *time.Time      `json:"retryAfter"`
}

type hookCreatedData struct {
	Token        string          `json:"token"`
	Metadata     []byte          `json:"metadata"`
	Webhook      *worlds.Webhook `json:"webhook"`
	MinRetention time.Duration   `json:"minRetention"`
}

type waitCreatedData struct {
	DueAt          time.Time `json:"dueAt"`
	Cancel         bool      `json:"cancel"`
	QueueMessageID string    `json:"queueMessageId"`
}

// runView is the META item of a run partition.
type runView struct {
	worlds.Run
	eventCount       int
	startedEventID   worlds.EventID
	cancelledEventID worlds.EventID
}

// stepView is a STEP# item. The event IDs answer idempotent replays.
type stepView struct {
	worlds.Step
	cancelledEventID worlds.EventID
}

// tokenView is a TOKEN# item. It holds the hook owning a token and, once the
// hook completed with retention, when ownership lapses.
type tokenView struct {
	hookID      worlds.HookID
	runID       worlds.RunID
	retainUntil *time.Time
}

func (t *tokenView) expired(now time.Time) bool {
	return t != nil && t.retainUntil != nil && !t.retainUntil.After(now)
}

// indexQuery selects one partition of the list or list_group index,
// optionally bounded by creation time.
type indexQuery struct {
	index     string
	partition string
	from, to  string
	status    []worlds.Status
}

// indexPage lists base items through an index. The indexes project only
// keys and status, so matching keys are read back from the table with
// consistent reads, and status is checked again on the fresh item. Pages are
// filled across DynamoDB pages until Limit matches are found.
func (w *World) indexPage(ctx context.Context, query indexQuery, pagination worlds.Pagination, match func(map[string]ddb.AttributeValue) bool) ([]map[string]ddb.AttributeValue, string, bool, error) {
	start, err := cursorKey(pagination.Cursor)
	if err != nil {
		return nil, "", false, err
	}
	pk, sk := query.index+"_pk", query.index+"_sk"
	names := map[string]string{"#pk": pk}
	values := map[string]ddb.AttributeValue{":pk": avs(query.partition)}
	condition := "#pk = :pk"
	switch {
	case query.from != "" && query.to != "":
		condition += " AND #sk BETWEEN :from AND :to"
		values[":from"], values[":to"] = avs(query.from), avs(query.to)
	case query.from != "":
		condition += " AND #sk >= :from"
		values[":from"] = avs(query.from)
	case query.to != "":
		condition += " AND #sk <= :to"
		values[":to"] = avs(query.to)
	}
	if strings.Contains(condition, "#sk") {
		names["#sk"] = sk
	}
	input := &dynamodb.QueryInput{
		TableName:                 awsv2.String(w.config.TableName),
		IndexName:                 awsv2.String(query.index),
		KeyConditionExpression:    awsv2.String(condition),
		ExpressionAttributeNames:  names,
		ExpressionAttributeValues: values,
		ScanIndexForward:          awsv2.Bool(pagination.SortOrder != worlds.SortOrderDescending),
		ExclusiveStartKey:         start,
	}
	if pagination.Limit > 0 {
		input.Limit = awsv2.Int32(int32(min(pagination.Limit+1, 1000)))
	}
	if len(query.status) > 0 {
		placeholders := make([]string, len(query.status))
		for index, status := range query.status {
			placeholders[index] = fmt.Sprintf(":status%d", index)
			values[placeholders[index]] = avs(string(status))
		}
		names["#status"] = "status"
		input.FilterExpression = awsv2.String("#status IN (" + strings.Join(placeholders, ", ") + ")")
	}
	limit := pagination.Limit
	var result []map[string]ddb.AttributeValue
	for {
		page, err := w.dynamo.Query(ctx, input)
		if err != nil {
			return nil, "", false, err
		}
		keys := make([]map[string]ddb.AttributeValue, len(page.Items))
		for index, item := range page.Items {
			keys[index] = key(read(item, "pk"), read(item, "sk"))
		}
		fresh, err := w.items(ctx, keys...)
		if err != nil {
			return nil, "", false, err
		}
		for _, key := range keys {
			item, ok := fresh[itemID(key)]
			if !ok || !statusMatches(item, query.status) || (match != nil && !match(item)) {
				continue
			}
			result = append(result, item)
		}
		if limit > 0 && len(result) > limit {
			cursor := keyCursor(result[limit-1], "pk", "sk", pk, sk)
			return result[:limit], cursor, true, nil
		}
		if len(page.LastEvaluatedKey) == 0 {
			return result, "", false, nil
		}
		input.ExclusiveStartKey = page.LastEvaluatedKey
	}
}

func indexSortKey(createdAt time.Time, id string) string { return stamp(createdAt) + "#" + id }

func runItem(view runView) map[string]ddb.AttributeValue {
	run := view.Run
	attributes := make(map[string]ddb.AttributeValue, len(run.Attributes))
	for name, value := range run.Attributes {
		attributes[name] = avs(value)
	}
	item := merge(runKey(string(run.RunID)), map[string]ddb.AttributeValue{
		"runId":         avs(string(run.RunID)),
		"workflowName":  avs(run.WorkflowName),
		"status":        avs(string(run.Status)),
		"attributes":    &ddb.AttributeValueMemberM{Value: attributes},
		"createdAt":     avs(stamp(run.CreatedAt)),
		"updatedAt":     avs(stamp(run.UpdatedAt)),
		"eventCount":    avn(int64(view.eventCount)),
		"inBandCount":   avn(int64(run.InBandCount)),
		"list_pk":       avs("RUNS"),
		"list_sk":       avs(indexSortKey(run.CreatedAt, string(run.RunID))),
		"list_group_pk": avs("RUNS#" + run.WorkflowName),
		"list_group_sk": avs(indexSortKey(run.CreatedAt, string(run.RunID))),
	})
	setBytes(item, "input", run.Input)
	setBytes(item, "output", run.Output)
	setBytes(item, "error", run.Error)
	setTime(item, "startedAt", run.StartedAt)
	setTime(item, "completedAt", run.CompletedAt)
	setString(item, "deploymentId", run.DeploymentID)
	setString(item, "startedEventId", string(view.startedEventID))
	setString(item, "cancelledEventId", string(view.cancelledEventID))
	return item
}

func runFromItem(item map[string]ddb.AttributeValue) runView {
	attributes := map[string]string{}
	if value, ok := item["attributes"].(*ddb.AttributeValueMemberM); ok {
		for name, value := range value.Value {
			attributes[name] = readAttr(value)
		}
	}
	return runView{
		Run: worlds.Run{
			RunID:        worlds.RunID(read(item, "runId")),
			WorkflowName: read(item, "workflowName"),
			Status:       worlds.Status(read(item, "status")),
			Input:        readBytes(item, "input"),
			Output:       readBytes(item, "output"),
			Error:        readBytes(item, "error"),
			Attributes:   attributes,
			DeploymentID: read(item, "deploymentId"),
			InBandCount:  int(readInt(item, "inBandCount")),
			CreatedAt:    readTime(item, "createdAt"),
			UpdatedAt:    readTime(item, "updatedAt"),
			StartedAt:    readTimePtr(item, "startedAt"),
			CompletedAt:  readTimePtr(item, "completedAt"),
		},
		eventCount:       int(readInt(item, "eventCount")),
		startedEventID:   worlds.EventID(read(item, "startedEventId")),
		cancelledEventID: worlds.EventID(read(item, "cancelledEventId")),
	}
}

// runUpdate converts the new view into an update of the mutable META
// attributes. Keys, index keys and input never change after creation, so the
// update leaves them, and the indexes, untouched unless status changes.
func runUpdate(view runView) (set map[string]ddb.AttributeValue, remove []string) {
	item := runItem(view)
	for _, name := range []string{"pk", "sk", "runId", "workflowName", "input", "deploymentId", "createdAt", "list_pk", "list_sk", "list_group_pk", "list_group_sk"} {
		delete(item, name)
	}
	for _, name := range []string{"output", "error", "startedAt", "completedAt", "startedEventId", "cancelledEventId"} {
		if _, ok := item[name]; !ok {
			remove = append(remove, name)
		}
	}
	return item, remove
}

func stepItem(view stepView) map[string]ddb.AttributeValue {
	step := view.Step
	item := merge(stepKey(string(step.RunID), string(step.StepID)), map[string]ddb.AttributeValue{
		"runId":     avs(string(step.RunID)),
		"stepId":    avs(string(step.StepID)),
		"stepName":  avs(step.StepName),
		"status":    avs(string(step.Status)),
		"attempt":   avn(int64(step.Attempt)),
		"createdAt": avs(stamp(step.CreatedAt)),
		"updatedAt": avs(stamp(step.UpdatedAt)),
	})
	setBytes(item, "input", step.Input)
	setBytes(item, "output", step.Output)
	setBytes(item, "error", step.Error)
	setTime(item, "startedAt", step.StartedAt)
	setTime(item, "completedAt", step.CompletedAt)
	setTime(item, "retryAfter", step.RetryAfter)
	setString(item, "cancelledEventId", string(view.cancelledEventID))
	return item
}

func stepFromItem(item map[string]ddb.AttributeValue) stepView {
	return stepView{
		Step: worlds.Step{
			RunID:       worlds.RunID(read(item, "runId")),
			StepID:      worlds.StepID(read(item, "stepId")),
			StepName:    read(item, "stepName"),
			Status:      worlds.Status(read(item, "status")),
			Input:       readBytes(item, "input"),
			Output:      readBytes(item, "output"),
			Error:       readBytes(item, "error"),
			Attempt:     int(readInt(item, "attempt")),
			CreatedAt:   readTime(item, "createdAt"),
			UpdatedAt:   readTime(item, "updatedAt"),
			StartedAt:   readTimePtr(item, "startedAt"),
			CompletedAt: readTimePtr(item, "completedAt"),
			RetryAfter:  readTimePtr(item, "retryAfter"),
		},
		cancelledEventID: worlds.EventID(read(item, "cancelledEventId")),
	}
}

func hookItem(hook worlds.Hook) (map[string]ddb.AttributeValue, error) {
	item := merge(hookKey(string(hook.HookID)), map[string]ddb.AttributeValue{
		"runId":         avs(string(hook.RunID)),
		"hookId":        avs(string(hook.HookID)),
		"token":         avs(hook.Token),
		"createdAt":     avs(stamp(hook.CreatedAt)),
		"updatedAt":     avs(stamp(hook.UpdatedAt)),
		"list_pk":       avs("HOOKS"),
		"list_sk":       avs(indexSortKey(hook.CreatedAt, string(hook.HookID))),
		"list_group_pk": avs("HOOKS#" + string(hook.RunID)),
		"list_group_sk": avs(indexSortKey(hook.CreatedAt, string(hook.HookID))),
	})
	if hook.Webhook != nil {
		webhook, err := json.Marshal(hook.Webhook)
		if err != nil {
			return nil, err
		}
		item["webhook"] = avs(string(webhook))
	}
	setBytes(item, "metadata", hook.Metadata)
	setString(item, "receivedEventId", string(hook.ReceivedEventID))
	setTime(item, "receivedAt", hook.ReceivedAt)
	setTime(item, "retainUntil", hook.RetainUntil)
	if hook.MinRetention != 0 {
		item["minRetention"] = avn(int64(hook.MinRetention))
	}
	return item, nil
}

func hookFromItem(item map[string]ddb.AttributeValue) (worlds.Hook, error) {
	hook := worlds.Hook{
		RunID:           worlds.RunID(read(item, "runId")),
		HookID:          worlds.HookID(read(item, "hookId")),
		Token:           read(item, "token"),
		Metadata:        readBytes(item, "metadata"),
		ReceivedEventID: worlds.EventID(read(item, "receivedEventId")),
		CreatedAt:       readTime(item, "createdAt"),
		UpdatedAt:       readTime(item, "updatedAt"),
		ReceivedAt:      readTimePtr(item, "receivedAt"),
		MinRetention:    time.Duration(readInt(item, "minRetention")),
		RetainUntil:     readTimePtr(item, "retainUntil"),
	}
	if value := read(item, "webhook"); value != "" {
		hook.Webhook = new(worlds.Webhook)
		if err := json.Unmarshal([]byte(value), hook.Webhook); err != nil {
			return worlds.Hook{}, fmt.Errorf("workflow/aws: decode hook %s webhook: %w", hook.HookID, err)
		}
	}
	return hook, nil
}

func tokenItem(token string, view tokenView) map[string]ddb.AttributeValue {
	item := merge(tokenKey(token), map[string]ddb.AttributeValue{
		"hookId": avs(string(view.hookID)),
		"runId":  avs(string(view.runID)),
	})
	setTime(item, "retainUntil", view.retainUntil)
	return item
}

func tokenFromItem(item map[string]ddb.AttributeValue) *tokenView {
	return &tokenView{hookID: worlds.HookID(read(item, "hookId")), runID: worlds.RunID(read(item, "runId")), retainUntil: readTimePtr(item, "retainUntil")}
}

// tokenCondition requires a token item to still be in the state seen when it
// was read, so concurrent claims of one token have exactly one winner.
func tokenCondition(seen *tokenView) (string, map[string]ddb.AttributeValue) {
	if seen == nil {
		return "attribute_not_exists(pk)", nil
	}
	values := map[string]ddb.AttributeValue{":seenHook": avs(string(seen.hookID))}
	if seen.retainUntil == nil {
		return "hookId = :seenHook AND attribute_not_exists(retainUntil)", values
	}
	values[":seenRetain"] = avs(stamp(*seen.retainUntil))
	return "hookId = :seenHook AND retainUntil = :seenRetain", values
}

func eventItem(event worlds.Event, slot uint64) map[string]ddb.AttributeValue {
	item := merge(eventKey(string(event.RunID), slot), map[string]ddb.AttributeValue{
		"runId":     avs(string(event.RunID)),
		"eventId":   avs(string(event.EventID)),
		"eventType": avs(string(event.EventType)),
		"createdAt": avs(stamp(event.CreatedAt)),
	})
	setString(item, "correlationId", event.CorrelationID)
	setBytes(item, "data", event.Data)
	return item
}

func eventFromItem(item map[string]ddb.AttributeValue) worlds.Event {
	return worlds.Event{
		RunID:         worlds.RunID(read(item, "runId")),
		EventID:       worlds.EventID(read(item, "eventId")),
		EventType:     worlds.EventType(read(item, "eventType")),
		CorrelationID: read(item, "correlationId"),
		Data:          readBytes(item, "data"),
		CreatedAt:     readTime(item, "createdAt"),
	}
}

func stripRun(run worlds.Run, resolve worlds.ResolveData) worlds.Run {
	if resolve == worlds.ResolveDataNone {
		run.Input, run.Output = nil, nil
	}
	return run
}

func stripStep(step worlds.Step, resolve worlds.ResolveData) worlds.Step {
	if resolve == worlds.ResolveDataNone {
		step.Input, step.Output = nil, nil
	}
	return step
}

func stripHook(hook worlds.Hook, resolve worlds.ResolveData) worlds.Hook {
	if resolve == worlds.ResolveDataNone {
		hook.Metadata = nil
	}
	return hook
}

func stripEvents(events []worlds.Event, resolve worlds.EventsResolveData) []worlds.Event {
	if resolve == worlds.EventsResolveDataNone {
		for index := range events {
			events[index].Data = nil
		}
	}
	return events
}

// encodeCursor and decodeCursor carry a DynamoDB key (a LastEvaluatedKey or
// ExclusiveStartKey) or a sort position as an opaque string.
func encodeCursor(values map[string]string) string {
	data, _ := json.Marshal(values)
	return base64.RawURLEncoding.EncodeToString(data)
}

func decodeCursor(cursor string) (map[string]string, error) {
	data, err := base64.RawURLEncoding.DecodeString(cursor)
	var values map[string]string
	if err == nil {
		err = json.Unmarshal(data, &values)
	}
	if err != nil {
		return nil, fmt.Errorf("workflow/aws: invalid cursor %q", cursor)
	}
	return values, nil
}

func keyCursor(item map[string]ddb.AttributeValue, names ...string) string {
	values := make(map[string]string, len(names))
	for _, name := range names {
		values[name] = read(item, name)
	}
	return encodeCursor(values)
}

func cursorKey(cursor string) (map[string]ddb.AttributeValue, error) {
	if cursor == "" {
		return nil, nil
	}
	values, err := decodeCursor(cursor)
	if err != nil {
		return nil, err
	}
	key := make(map[string]ddb.AttributeValue, len(values))
	for name, value := range values {
		key[name] = avs(value)
	}
	return key, nil
}

// sortedPage pages values already sorted ascending by sortKey. The cursor is
// the sort key of the last returned value, so pages stay stable while values
// are added.
func sortedPage[T any](values []T, sortKey func(T) string, pagination worlds.Pagination) (worlds.Page[T], error) {
	if pagination.SortOrder == worlds.SortOrderDescending {
		slices.Reverse(values)
	}
	if pagination.Cursor != "" {
		position, err := decodeCursor(pagination.Cursor)
		if err != nil {
			return worlds.Page[T]{}, err
		}
		after := position["after"]
		start := len(values)
		for index, value := range values {
			key := sortKey(value)
			if (pagination.SortOrder == worlds.SortOrderDescending && key < after) || (pagination.SortOrder != worlds.SortOrderDescending && key > after) {
				start = index
				break
			}
		}
		values = values[start:]
	}
	result := worlds.Page[T]{Data: values}
	if pagination.Limit > 0 && len(values) > pagination.Limit {
		result.Data, result.HasMore = values[:pagination.Limit], true
		result.Cursor = encodeCursor(map[string]string{"after": sortKey(values[pagination.Limit-1])})
	}
	return result, nil
}

func statusMatches(item map[string]ddb.AttributeValue, statuses []worlds.Status) bool {
	if len(statuses) == 0 {
		return true
	}
	return slices.Contains(statuses, worlds.Status(read(item, "status")))
}

// backoff returns an exponentially growing delay with jitter.
func backoff(attempt int, base, limit time.Duration) time.Duration {
	delay := base << min(attempt, 16)
	if delay <= 0 || delay > limit {
		delay = limit
	}
	return delay/2 + rand.N(delay/2+1)
}

func sleep(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
