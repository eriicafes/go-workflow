package awsworld

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"strconv"
	"time"

	awsv2 "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// errWriteTooLarge marks writes that exceed DynamoDB's item or transaction
// limits and can never succeed as issued.
var errWriteTooLarge = errors.New("workflow/aws: write exceeds DynamoDB limits")

// dynamoAPI is the subset of the DynamoDB client used by World.
type dynamoAPI interface {
	GetItem(context.Context, *dynamodb.GetItemInput, ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	PutItem(context.Context, *dynamodb.PutItemInput, ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	UpdateItem(context.Context, *dynamodb.UpdateItemInput, ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
	DeleteItem(context.Context, *dynamodb.DeleteItemInput, ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
	Query(context.Context, *dynamodb.QueryInput, ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
	BatchGetItem(context.Context, *dynamodb.BatchGetItemInput, ...func(*dynamodb.Options)) (*dynamodb.BatchGetItemOutput, error)
	TransactWriteItems(context.Context, *dynamodb.TransactWriteItemsInput, ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error)
}

const (
	// listIndex lists every run (RUNS) and every hook (HOOKS) by creation time.
	listIndex = "list"
	// listGroupIndex lists runs of one workflow (RUNS#<workflow>) and hooks
	// of one run (HOOKS#<runID>) by creation time.
	listGroupIndex = "list_group"

	// maxItemBytes is DynamoDB's item size limit.
	maxItemBytes = 400 << 10
	// maxTransactItems is DynamoDB's TransactWriteItems action limit.
	maxTransactItems = 100
)

// CreateTable creates the DynamoDB table name in the layout World expects, with
// on-demand billing, and waits until it is active. The table has string keys pk
// and sk and two global secondary indexes, list (list_pk, list_sk) and
// list_group (list_group_pk, list_group_sk), which project only their keys and
// status.
//
// CreateTable suits tests and DynamoDB Local. Production deployments usually
// declare the same table with infrastructure as code, such as the module in
// worlds/aws/terraform.
func CreateTable(ctx context.Context, client *dynamodb.Client, name string) error {
	index := func(name string) ddb.GlobalSecondaryIndex {
		return ddb.GlobalSecondaryIndex{
			IndexName: awsv2.String(name),
			KeySchema: []ddb.KeySchemaElement{
				{AttributeName: awsv2.String(name + "_pk"), KeyType: ddb.KeyTypeHash},
				{AttributeName: awsv2.String(name + "_sk"), KeyType: ddb.KeyTypeRange},
			},
			Projection: &ddb.Projection{ProjectionType: ddb.ProjectionTypeInclude, NonKeyAttributes: []string{"status"}},
		}
	}
	attributes := make([]ddb.AttributeDefinition, 0, 6)
	for _, attribute := range []string{"pk", "sk", "list_pk", "list_sk", "list_group_pk", "list_group_sk"} {
		attributes = append(attributes, ddb.AttributeDefinition{AttributeName: awsv2.String(attribute), AttributeType: ddb.ScalarAttributeTypeS})
	}
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            awsv2.String(name),
		BillingMode:          ddb.BillingModePayPerRequest,
		AttributeDefinitions: attributes,
		KeySchema: []ddb.KeySchemaElement{
			{AttributeName: awsv2.String("pk"), KeyType: ddb.KeyTypeHash},
			{AttributeName: awsv2.String("sk"), KeyType: ddb.KeyTypeRange},
		},
		GlobalSecondaryIndexes: []ddb.GlobalSecondaryIndex{index(listIndex), index(listGroupIndex)},
	})
	if err != nil {
		return fmt.Errorf("workflow/aws: create table: %w", err)
	}
	return dynamodb.NewTableExistsWaiter(client).Wait(ctx, &dynamodb.DescribeTableInput{TableName: awsv2.String(name)}, 5*time.Minute)
}

func (w *World) item(ctx context.Context, key map[string]ddb.AttributeValue) (map[string]ddb.AttributeValue, bool, error) {
	result, err := w.dynamo.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      awsv2.String(w.config.TableName),
		Key:            key,
		ConsistentRead: awsv2.Bool(true),
	})
	if err != nil {
		return nil, false, err
	}
	if len(result.Item) == 0 {
		return nil, false, nil
	}
	return result.Item, true, nil
}

// items reads keys with consistent BatchGetItem calls in DynamoDB's 100-key
// chunks, retrying UnprocessedKeys. Results are keyed by itemID.
func (w *World) items(ctx context.Context, keys ...map[string]ddb.AttributeValue) (map[string]map[string]ddb.AttributeValue, error) {
	items := make(map[string]map[string]ddb.AttributeValue, len(keys))
	seen := make(map[string]bool, len(keys))
	unique := make([]map[string]ddb.AttributeValue, 0, len(keys))
	for _, key := range keys {
		if id := itemID(key); !seen[id] {
			seen[id] = true
			unique = append(unique, key)
		}
	}
	keys = unique
	for len(keys) > 0 {
		count := min(len(keys), 100)
		request := map[string]ddb.KeysAndAttributes{
			w.config.TableName: {Keys: keys[:count], ConsistentRead: awsv2.Bool(true)},
		}
		keys = keys[count:]
		for attempt := 0; len(request) > 0; attempt++ {
			if attempt > 0 {
				if err := sleep(ctx, backoff(attempt, 10*time.Millisecond, 500*time.Millisecond)); err != nil {
					return nil, err
				}
			}
			result, err := w.dynamo.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{RequestItems: request})
			if err != nil {
				return nil, err
			}
			for _, item := range result.Responses[w.config.TableName] {
				items[itemID(item)] = item
			}
			request = result.UnprocessedKeys
		}
	}
	return items, nil
}

var placeholderPattern = regexp.MustCompile(`[#:][A-Za-z0-9_]+`)

func (w *World) transact(ctx context.Context, items []ddb.TransactWriteItem) error {
	if len(items) > maxTransactItems {
		return fmt.Errorf("%w: %d transaction items, over the limit of %d", errWriteTooLarge, len(items), maxTransactItems)
	}
	for _, item := range items {
		if item.Put != nil {
			if err := checkItemSize(item.Put.Item); err != nil {
				return err
			}
		}
	}
	_, err := w.dynamo.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: items})
	return err
}

// query runs a Query and follows LastEvaluatedKey until visit returns false
// or the result set ends.
func (w *World) query(ctx context.Context, input *dynamodb.QueryInput, visit func(map[string]ddb.AttributeValue) bool) error {
	input.TableName = awsv2.String(w.config.TableName)
	for {
		result, err := w.dynamo.Query(ctx, input)
		if err != nil {
			return err
		}
		for _, item := range result.Items {
			if !visit(item) {
				return nil
			}
		}
		if len(result.LastEvaluatedKey) == 0 {
			return nil
		}
		input.ExclusiveStartKey = result.LastEvaluatedKey
	}
}

func runPartitionKey(runID string) string { return "RUN#" + runID }

func runKey(runID string) map[string]ddb.AttributeValue {
	return key(runPartitionKey(runID), "META")
}

func eventSortKey(slot uint64) string { return fmt.Sprintf("EVENT#%026d", slot) }

func eventKey(runID string, slot uint64) map[string]ddb.AttributeValue {
	return key(runPartitionKey(runID), eventSortKey(slot))
}

func stepKey(runID, stepID string) map[string]ddb.AttributeValue {
	return key(runPartitionKey(runID), "STEP#"+stepID)
}

func waitKey(runID, correlationID string) map[string]ddb.AttributeValue {
	return key(runPartitionKey(runID), "WAIT#"+correlationID)
}

// Stream names are encoded so a name containing '#' cannot fall inside the
// chunk key range of another stream.
func streamName(name string) string { return base64.RawURLEncoding.EncodeToString([]byte(name)) }

func streamKey(runID, name string) map[string]ddb.AttributeValue {
	return key(runPartitionKey(runID), "STREAM#"+streamName(name))
}

func chunkSortKey(name string, index int) string {
	return fmt.Sprintf("CHUNK#%s#%010d", streamName(name), index)
}

func chunkKey(runID, name string, index int) map[string]ddb.AttributeValue {
	return key(runPartitionKey(runID), chunkSortKey(name, index))
}

func hookKey(hookID string) map[string]ddb.AttributeValue { return key("HOOK#"+hookID, "META") }

func tokenKey(token string) map[string]ddb.AttributeValue { return key("TOKEN#"+token, "META") }

func queueKey(id string) map[string]ddb.AttributeValue { return key("QUEUE#"+id, "META") }

func key(pk, sk string) map[string]ddb.AttributeValue {
	return map[string]ddb.AttributeValue{"pk": avs(pk), "sk": avs(sk)}
}

// put returns a transactional create that fails when the item already exists.
func put(tableName string, item map[string]ddb.AttributeValue) ddb.TransactWriteItem {
	return ddb.TransactWriteItem{Put: &ddb.Put{
		TableName:           awsv2.String(tableName),
		Item:                item,
		ConditionExpression: awsv2.String("attribute_not_exists(pk)"),
	}}
}

func merge(key map[string]ddb.AttributeValue, values map[string]ddb.AttributeValue) map[string]ddb.AttributeValue {
	result := make(map[string]ddb.AttributeValue, len(key)+len(values))
	maps.Copy(result, key)
	maps.Copy(result, values)
	return result
}

func avs(value string) ddb.AttributeValue { return &ddb.AttributeValueMemberS{Value: value} }

func avn(value int64) ddb.AttributeValue {
	return &ddb.AttributeValueMemberN{Value: strconv.FormatInt(value, 10)}
}

func avb(value []byte) ddb.AttributeValue { return &ddb.AttributeValueMemberB{Value: value} }

func avbool(value bool) ddb.AttributeValue { return &ddb.AttributeValueMemberBOOL{Value: value} }

// setString, setBytes and setTime add optional attributes only when present,
// so absent values read back as their zero value.
func setString(item map[string]ddb.AttributeValue, name, value string) {
	if value != "" {
		item[name] = avs(value)
	}
}

func setBytes(item map[string]ddb.AttributeValue, name string, value []byte) {
	if len(value) != 0 {
		item[name] = avb(value)
	}
}

func setTime(item map[string]ddb.AttributeValue, name string, value *time.Time) {
	if value != nil {
		item[name] = avs(stamp(*value))
	}
}

func read(item map[string]ddb.AttributeValue, name string) string {
	return readAttr(item[name])
}

func readInt(item map[string]ddb.AttributeValue, name string) int64 {
	value, _ := strconv.ParseInt(read(item, name), 10, 64)
	return value
}

func readBytes(item map[string]ddb.AttributeValue, name string) []byte {
	if value, ok := item[name].(*ddb.AttributeValueMemberB); ok && len(value.Value) != 0 {
		return value.Value
	}
	return nil
}

func readBool(item map[string]ddb.AttributeValue, name string) bool {
	value, ok := item[name].(*ddb.AttributeValueMemberBOOL)
	return ok && value.Value
}

func readTime(item map[string]ddb.AttributeValue, name string) time.Time {
	return parseStamp(read(item, name))
}

func readTimePtr(item map[string]ddb.AttributeValue, name string) *time.Time {
	if read(item, name) == "" {
		return nil
	}
	value := readTime(item, name)
	return &value
}

func readAttr(value ddb.AttributeValue) string {
	switch value := value.(type) {
	case *ddb.AttributeValueMemberS:
		return value.Value
	case *ddb.AttributeValueMemberN:
		return value.Value
	default:
		return ""
	}
}

// stamp formats a fixed-width UTC timestamp, so string order is time order.
func stamp(value time.Time) string {
	return value.UTC().Format("2006-01-02T15:04:05.000000000Z")
}

func parseStamp(value string) time.Time {
	result, _ := time.Parse(time.RFC3339Nano, value)
	return result.UTC()
}

// conditional reports whether a write failed on a condition expression,
// directly or as a cancelled transaction caused by one.
func conditional(err error) bool {
	var failed *ddb.ConditionalCheckFailedException
	if errors.As(err, &failed) {
		return true
	}
	return cancelledBy(err, "ConditionalCheckFailed")
}

// contended reports whether a transaction lost a race or was throttled and
// may be retried after reading the current state again.
func contended(err error) bool {
	return conditional(err) || cancelledBy(err, "TransactionConflict") || cancelledBy(err, "ThrottlingError")
}

func cancelledBy(err error, code string) bool {
	var cancelled *ddb.TransactionCanceledException
	if !errors.As(err, &cancelled) {
		return false
	}
	for _, reason := range cancelled.CancellationReasons {
		if reason.Code != nil && *reason.Code == code {
			return true
		}
	}
	return false
}

// itemSize estimates DynamoDB's item size as attribute names plus values.
func itemSize(item map[string]ddb.AttributeValue) int {
	size := 0
	for name, value := range item {
		size += len(name) + attributeSize(value)
	}
	return size
}

func attributeSize(value ddb.AttributeValue) int {
	switch value := value.(type) {
	case *ddb.AttributeValueMemberS:
		return len(value.Value)
	case *ddb.AttributeValueMemberB:
		return len(value.Value)
	case *ddb.AttributeValueMemberN:
		return len(value.Value)/2 + 1
	case *ddb.AttributeValueMemberM:
		return 3 + itemSize(value.Value)
	case *ddb.AttributeValueMemberL:
		size := 3
		for _, item := range value.Value {
			size += 1 + attributeSize(item)
		}
		return size
	default:
		return 1
	}
}

func checkItemSize(item map[string]ddb.AttributeValue) error {
	if size := itemSize(item); size > maxItemBytes {
		return fmt.Errorf("%w: item %s %s is %d bytes, over the 400 KB item limit", errWriteTooLarge, read(item, "pk"), read(item, "sk"), size)
	}
	return nil
}

// usedOnly returns the expression names and values referenced by
// expressions, because DynamoDB rejects unused ones.
func usedOnly(names map[string]string, values map[string]ddb.AttributeValue, expressions ...string) (map[string]string, map[string]ddb.AttributeValue) {
	usedNames, usedValues := map[string]string{}, map[string]ddb.AttributeValue{}
	for _, expression := range expressions {
		for _, placeholder := range placeholderPattern.FindAllString(expression, -1) {
			if name, ok := names[placeholder]; ok {
				usedNames[placeholder] = name
			}
			if value, ok := values[placeholder]; ok {
				usedValues[placeholder] = value
			}
		}
	}
	if len(usedNames) == 0 {
		usedNames = nil
	}
	if len(usedValues) == 0 {
		usedValues = nil
	}
	return usedNames, usedValues
}

func itemID(item map[string]ddb.AttributeValue) string {
	return read(item, "pk") + "\x00" + read(item, "sk")
}
