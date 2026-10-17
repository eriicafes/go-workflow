package awsworld

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	awsv2 "github.com/aws/aws-sdk-go-v2/aws"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/eriicafes/go-workflow/worlds"
)

func TestKeys(t *testing.T) {
	tests := []struct {
		key    map[string]ddb.AttributeValue
		pk, sk string
	}{
		{runKey("wrun_1"), "RUN#wrun_1", "META"},
		{eventKey("wrun_1", 7), "RUN#wrun_1", "EVENT#00000000000000000000000007"},
		{stepKey("wrun_1", "step_a"), "RUN#wrun_1", "STEP#step_a"},
		{waitKey("wrun_1", "wait_a"), "RUN#wrun_1", "WAIT#wait_a"},
		{streamKey("wrun_1", "out#1"), "RUN#wrun_1", "STREAM#b3V0IzE"},
		{chunkKey("wrun_1", "out", 3), "RUN#wrun_1", "CHUNK#b3V0#0000000003"},
		{hookKey("hook_a"), "HOOK#hook_a", "META"},
		{tokenKey("tok"), "TOKEN#tok", "META"},
		{queueKey("01ABC"), "QUEUE#01ABC", "META"},
	}
	for _, test := range tests {
		if read(test.key, "pk") != test.pk || read(test.key, "sk") != test.sk {
			t.Errorf("key = %s/%s, want %s/%s", read(test.key, "pk"), read(test.key, "sk"), test.pk, test.sk)
		}
	}
	// A '#' in a stream name must not place another stream's chunks inside
	// this stream's key range.
	if other := chunkSortKey("a#0000000001", 0); other > chunkSortKey("a", 0) && other < chunkSortKey("a", 9999999999) {
		t.Fatalf("chunk key %s collides with stream a", other)
	}
	if eventSortKey(9) >= eventSortKey(10) {
		t.Fatal("event sort keys do not sort by slot")
	}
}

func TestAttributes(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 600, time.UTC)
	item := merge(key("p", "s"), map[string]ddb.AttributeValue{"n": avn(42), "b": avb([]byte("bin")), "t": avbool(true)})
	setString(item, "s", "value")
	setString(item, "empty", "")
	setBytes(item, "nobytes", nil)
	setTime(item, "at", &now)
	setTime(item, "never", nil)
	if read(item, "s") != "value" || readInt(item, "n") != 42 || string(readBytes(item, "b")) != "bin" || !readBool(item, "t") {
		t.Fatalf("item = %#v", item)
	}
	if _, ok := item["empty"]; ok {
		t.Fatal("empty string stored")
	}
	if _, ok := item["nobytes"]; ok {
		t.Fatal("nil bytes stored")
	}
	if at := readTimePtr(item, "at"); at == nil || !at.Equal(now) {
		t.Fatalf("time = %v", at)
	}
	if readTimePtr(item, "never") != nil || readBytes(item, "missing") != nil || readBool(item, "missing") {
		t.Fatal("missing attributes read as present")
	}
	if a, b := stamp(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)), stamp(time.Date(2026, 1, 1, 0, 0, 0, 500, time.UTC)); len(a) != len(b) || a >= b {
		t.Fatalf("stamps not fixed width and ordered: %s %s", a, b)
	}
}

func TestConditional(t *testing.T) {
	code := func(value string) ddb.CancellationReason { return ddb.CancellationReason{Code: awsv2.String(value)} }
	conditionFailed := &ddb.TransactionCanceledException{CancellationReasons: []ddb.CancellationReason{code("None"), code("ConditionalCheckFailed")}}
	conflict := &ddb.TransactionCanceledException{CancellationReasons: []ddb.CancellationReason{code("TransactionConflict")}}
	validation := &ddb.TransactionCanceledException{CancellationReasons: []ddb.CancellationReason{code("ValidationError")}}
	if !conditional(&ddb.ConditionalCheckFailedException{}) || !conditional(fmt.Errorf("wrapped: %w", conditionFailed)) {
		t.Fatal("condition failures not detected")
	}
	if conditional(conflict) || conditional(validation) || conditional(errors.New("other")) || conditional(nil) {
		t.Fatal("non-condition failure detected as conditional")
	}
	if !contended(conflict) || !contended(conditionFailed) || contended(validation) {
		t.Fatal("contended mismatch")
	}
}

func TestItemSize(t *testing.T) {
	small := merge(key("p", "s"), map[string]ddb.AttributeValue{"data": avb(make([]byte, 1000))})
	if err := checkItemSize(small); err != nil {
		t.Fatal(err)
	}
	large := merge(key("p", "s"), map[string]ddb.AttributeValue{"data": avb(make([]byte, maxItemBytes))})
	if err := checkItemSize(large); !errors.Is(err, errWriteTooLarge) {
		t.Fatalf("large item error = %v", err)
	}
	nested := map[string]ddb.AttributeValue{"m": &ddb.AttributeValueMemberM{Value: map[string]ddb.AttributeValue{"k": avs("vvvv")}}}
	if size := itemSize(nested); size != 1+3+1+4 {
		t.Fatalf("nested size = %d", size)
	}
	world := testWorld(t, newFakeDynamo(), &fakeSQS{}, nil)
	items := make([]ddb.TransactWriteItem, maxTransactItems+1)
	if err := world.transact(context.Background(), items); !errors.Is(err, errWriteTooLarge) {
		t.Fatalf("oversized transaction error = %v", err)
	}
	if err := world.transact(context.Background(), []ddb.TransactWriteItem{put("t", large)}); !errors.Is(err, errWriteTooLarge) {
		t.Fatalf("oversized put error = %v", err)
	}
}

func TestBatchGetChunksAndRetries(t *testing.T) {
	dynamo := newFakeDynamo()
	world := testWorld(t, dynamo, &fakeSQS{}, nil)
	var keys []map[string]ddb.AttributeValue
	for index := range 250 {
		item := merge(key(fmt.Sprintf("P#%d", index), "META"), map[string]ddb.AttributeValue{"n": avn(int64(index))})
		dynamo.items[itemID(item)] = item
		keys = append(keys, key(fmt.Sprintf("P#%d", index), "META"))
	}
	keys = append(keys, key("P#0", "META"), key("P#missing", "META"))
	dynamo.unprocessed = 30
	items, err := world.items(context.Background(), keys...)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 250 || readInt(items[itemID(key("P#249", "META"))], "n") != 249 {
		t.Fatalf("items = %d", len(items))
	}
	if dynamo.batchCalls < 4 {
		t.Fatalf("batch calls = %d, want chunked calls plus retries", dynamo.batchCalls)
	}
}

func TestUpdateExpression(t *testing.T) {
	expression, names, values := updateExpression(map[string]ddb.AttributeValue{"status": avs("running"), "count": avn(2), ":seen": avn(1)}, []string{"output"})
	if expression != "SET #a0 = :a0, #a1 = :a1 REMOVE #r0" || names["#a0"] != "count" || names["#a1"] != "status" || names["#r0"] != "output" {
		t.Fatalf("expression = %q names = %v", expression, names)
	}
	if _, ok := values[":seen"]; !ok || len(values) != 3 {
		t.Fatalf("values = %v", values)
	}
	usedNames, usedValues := usedOnly(map[string]string{"#a": "a", "#b": "b"}, map[string]ddb.AttributeValue{":x": avs("x"), ":xy": avs("xy")}, "SET #a = :xy")
	if len(usedNames) != 1 || usedNames["#a"] != "a" || len(usedValues) != 1 || usedValues[":xy"] == nil {
		t.Fatalf("used = %v %v", usedNames, usedValues)
	}
}

func TestTokenCondition(t *testing.T) {
	if condition, values := tokenCondition(nil); condition != "attribute_not_exists(pk)" || values != nil {
		t.Fatalf("absent token condition = %q %v", condition, values)
	}
	if condition, values := tokenCondition(&tokenView{hookID: "hook_a"}); !strings.Contains(condition, "attribute_not_exists(tokenRetentionUntil)") || read(values, ":seenHook") != "hook_a" {
		t.Fatalf("held token condition = %q %v", condition, values)
	}
	until := time.Now()
	if condition, values := tokenCondition(&tokenView{hookID: "hook_a", retainUntil: &until}); !strings.Contains(condition, "tokenRetentionUntil = :seenRetain") || read(values, ":seenRetain") != stamp(until) {
		t.Fatalf("retained token condition = %q %v", condition, values)
	}
	past := time.Now().Add(-time.Second)
	if !(&tokenView{retainUntil: &past}).expired(time.Now()) || (&tokenView{}).expired(time.Now()) || (*tokenView)(nil).expired(time.Now()) {
		t.Fatal("token expiry mismatch")
	}
	if !sameToken(nil, nil) || sameToken(nil, &tokenView{}) || !sameToken(&tokenView{hookID: "a", retainUntil: &until}, &tokenView{hookID: "a", retainUntil: &until}) || sameToken(&tokenView{hookID: "a"}, &tokenView{hookID: "a", retainUntil: &until}) {
		t.Fatal("sameToken mismatch")
	}
}

func TestViewItems(t *testing.T) {
	now := time.Now().UTC()
	run := runView{Run: worlds.Run{RunID: "wrun_1", WorkflowName: "flow", Status: worlds.StatusCompleted, Input: []byte("in"), Output: []byte("out"), Attributes: map[string]string{"k": "v"}, CreatedAt: now, UpdatedAt: now, CompletedAt: &now}, eventCount: 3, startedEventID: worlds.EventIDForSlot(2)}
	decoded := runFromItem(runItem(run))
	if decoded.RunID != run.RunID || string(decoded.Output) != "out" || decoded.Attributes["k"] != "v" || decoded.eventCount != 3 || decoded.startedEventID != run.startedEventID || decoded.StartedAt != nil || !decoded.CompletedAt.Equal(now) {
		t.Fatalf("run = %#v", decoded)
	}
	set, remove := runUpdate(run)
	if _, ok := set["input"]; ok {
		t.Fatal("update rewrites input")
	}
	if _, ok := set["list_pk"]; ok {
		t.Fatal("update rewrites index keys")
	}
	if strings.Join(remove, ",") != "error,startedAt,cancelledEventId" {
		t.Fatalf("remove = %v", remove)
	}
	hook := worlds.Hook{RunID: "wrun_1", HookID: "hook_a", Token: "tok", Metadata: []byte("m"), Webhook: &worlds.Webhook{Method: "POST", Manual: true}, CreatedAt: now, UpdatedAt: now, MinRetention: time.Second, TokenRetentionUntil: &now}
	item, err := hookItem(hook)
	if err != nil {
		t.Fatal(err)
	}
	if read(item, "list_group_pk") != "HOOKS#wrun_1" || read(item, "list_pk") != "HOOKS" {
		t.Fatalf("hook index keys = %v", item)
	}
	back, err := hookFromItem(item)
	if err != nil || back.Webhook == nil || !back.Webhook.Manual || back.MinRetention != time.Second || !back.TokenRetentionUntil.Equal(now) || string(back.Metadata) != "m" {
		t.Fatalf("hook = %#v err=%v", back, err)
	}
	step := stepView{Step: worlds.Step{RunID: "wrun_1", StepID: "step_a", Status: worlds.StatusRunning, Attempt: 2, CreatedAt: now, UpdatedAt: now, RetryAfter: &now}}
	if got := stepFromItem(stepItem(step)); got.Attempt != 2 || !got.RetryAfter.Equal(now) {
		t.Fatalf("step = %#v", got)
	}
	event := worlds.Event{RunID: "wrun_1", EventID: worlds.EventIDForSlot(4), EventType: worlds.EventTypeWaitCreated, CorrelationID: "w", Data: []byte("{}"), CreatedAt: now}
	if got := eventFromItem(eventItem(event, 4)); got.EventID != event.EventID || got.CorrelationID != "w" || string(got.Data) != "{}" || !got.CreatedAt.Equal(now) {
		t.Fatalf("event = %#v", got)
	}
}
