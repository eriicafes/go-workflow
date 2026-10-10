package awsworld

import (
	"fmt"
	"maps"
	"strconv"
	"time"

	awsv2 "github.com/aws/aws-sdk-go-v2/aws"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/eriicafes/go-workflow/workflowops"
)

func runPartitionKey(runID string) string {
	return "RUN#" + runID
}

func runKey(runID string) map[string]ddb.AttributeValue {
	return key(runPartitionKey(runID), "META")
}

func commandKey(runID string, sequence int) map[string]ddb.AttributeValue {
	return key(runPartitionKey(runID), fmt.Sprintf("COMMAND#%010d", sequence+1))
}

func taskKey(runID string, sequence int) map[string]ddb.AttributeValue {
	return key(runPartitionKey(runID), fmt.Sprintf("TASK#%010d", sequence+1))
}

func eventKey(runID string, sequence int) map[string]ddb.AttributeValue {
	return key(runPartitionKey(runID), fmt.Sprintf("EVENT#%010d", sequence))
}

func put(tableName string, key map[string]ddb.AttributeValue, values map[string]string) ddb.TransactWriteItem {
	return ddb.TransactWriteItem{Put: &ddb.Put{
		TableName:           awsv2.String(tableName),
		Item:                merge(key, values),
		ConditionExpression: awsv2.String("attribute_not_exists(pk)"),
	}}
}

func key(pk, sk string) map[string]ddb.AttributeValue {
	return map[string]ddb.AttributeValue{
		"pk": avs(pk),
		"sk": avs(sk),
	}
}

func merge(key map[string]ddb.AttributeValue, values map[string]string) map[string]ddb.AttributeValue {
	result := make(map[string]ddb.AttributeValue, len(key)+len(values))
	maps.Copy(result, key)
	for name, value := range values {
		result[name] = avs(value)
	}
	return result
}

func taskFields(runID string, sequence int, now time.Time) map[string]string {
	return map[string]string{
		"run":         runID,
		"sequence":    strconv.Itoa(sequence),
		"status":      string(workflowops.StatusQueued),
		"idempotency": runID + ":" + strconv.Itoa(sequence),
		"attempt":     "0",
		"created":     stamp(now),
		"updated":     stamp(now),
	}
}

func avs(value string) ddb.AttributeValue {
	return &ddb.AttributeValueMemberS{Value: value}
}

func avn(value int) ddb.AttributeValue {
	return &ddb.AttributeValueMemberN{Value: strconv.Itoa(value)}
}

func read(item map[string]ddb.AttributeValue, name string) string {
	return readAttr(item[name])
}

func readInt(item map[string]ddb.AttributeValue, name string) int {
	value, _ := strconv.Atoi(read(item, name))
	return value
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
