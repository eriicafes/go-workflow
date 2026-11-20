package awsworld

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"time"

	awsv2 "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/eriicafes/go-workflow/worlds"
)

const (
	defaultChunkLimit = 100
	// maxChunksPerWrite keeps a write and its stream update in one
	// TransactWriteItems call, so chunks become visible atomically.
	maxChunksPerWrite = maxTransactItems - 1
	streamPollMin     = 50 * time.Millisecond
	streamPollMax     = 500 * time.Millisecond
)

// streamMeta is a STREAM# item. Readers only see chunks below count, which
// is advanced in the same transaction that writes them.
type streamMeta struct {
	found  bool
	closed bool
	count  int
}

func (w *World) streamMeta(ctx context.Context, runID worlds.RunID, name string) (streamMeta, error) {
	item, found, err := w.item(ctx, streamKey(string(runID), name))
	if err != nil || !found {
		return streamMeta{}, err
	}
	return streamMeta{
		found:  true,
		closed: readBool(item, "closed"),
		count:  int(readInt(item, "count")),
	}, nil
}

// updateStream applies a conditional update to the STREAM# item, creating
// it when missing. It reports false when the condition failed.
func (w *World) updateStream(ctx context.Context, runID worlds.RunID, name, update, condition string, values map[string]ddb.AttributeValue) (bool, error) {
	values[":name"] = avs(name)
	values[":zero"] = avn(0)
	update += ", #name = :name, #count = if_not_exists(#count, :zero)"
	names, values := usedOnly(streamNames, values, update, condition)
	input := &dynamodb.UpdateItemInput{
		TableName:                 awsv2.String(w.config.TableName),
		Key:                       streamKey(string(runID), name),
		UpdateExpression:          awsv2.String(update),
		ExpressionAttributeNames:  names,
		ExpressionAttributeValues: values,
	}
	if condition != "" {
		input.ConditionExpression = awsv2.String(condition)
	}
	_, err := w.dynamo.UpdateItem(ctx, input)
	if conditional(err) {
		return false, nil
	}
	return err == nil, err
}

var streamNames = map[string]string{"#name": "name", "#count": "count", "#closed": "closed"}

// appendChunks writes chunks at count and advances count in one transaction.
func (w *World) appendChunks(ctx context.Context, runID worlds.RunID, name string, count int, chunks [][]byte, condition string) error {
	if len(chunks) > maxChunksPerWrite {
		return fmt.Errorf("workflow/aws: stream write of %d chunks exceeds the limit of %d per write", len(chunks), maxChunksPerWrite)
	}
	values := map[string]ddb.AttributeValue{
		":seen":  avn(int64(count)),
		":count": avn(int64(count + len(chunks))),
		":name":  avs(name),
		":false": avbool(false),
	}
	update := "SET #count = :count, #name = :name, #closed = if_not_exists(#closed, :false)"
	names, values := usedOnly(streamNames, values, update, condition)
	table := awsv2.String(w.config.TableName)
	items := []ddb.TransactWriteItem{{Update: &ddb.Update{
		TableName:                 table,
		Key:                       streamKey(string(runID), name),
		UpdateExpression:          awsv2.String(update),
		ConditionExpression:       awsv2.String(condition),
		ExpressionAttributeNames:  names,
		ExpressionAttributeValues: values,
	}}}
	for index, chunk := range chunks {
		items = append(items, ddb.TransactWriteItem{Put: &ddb.Put{TableName: table, Item: merge(chunkKey(string(runID), name, count+index), map[string]ddb.AttributeValue{"data": avb(chunk)})}})
	}
	return w.transact(ctx, items)
}

func (w *World) StreamsWrite(ctx context.Context, input worlds.StreamsWriteInput) error {
	return w.StreamsWriteMulti(ctx, worlds.StreamsWriteMultiInput{RunID: input.RunID, Name: input.Name, Chunks: [][]byte{input.Chunk}})
}

func (w *World) StreamsWriteMulti(ctx context.Context, input worlds.StreamsWriteMultiInput) error {
	// Concurrent writers race on count and retry.
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		meta, err := w.streamMeta(ctx, input.RunID, input.Name)
		if err != nil {
			return err
		}
		if meta.closed {
			return worlds.ErrStreamClosed
		}
		condition := "attribute_not_exists(pk)"
		if meta.found {
			condition = "#count = :seen AND #closed = :false"
		}
		err = w.appendChunks(ctx, input.RunID, input.Name, meta.count, input.Chunks, condition)
		if !contended(err) {
			return err
		}
		if attempt+1 >= maxAppendAttempts {
			return fmt.Errorf("workflow/aws: stream write still contended after %d attempts: %w", maxAppendAttempts, err)
		}
		if err := sleep(ctx, backoff(attempt, 5*time.Millisecond, 250*time.Millisecond)); err != nil {
			return err
		}
	}
}

func (w *World) StreamsClose(ctx context.Context, input worlds.StreamsCloseInput) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := w.updateStream(ctx, input.RunID, input.Name, "SET #closed = :true", "", map[string]ddb.AttributeValue{":true": avbool(true)})
	return err
}

func (w *World) StreamsGetChunks(ctx context.Context, input worlds.StreamsGetChunksInput) (worlds.StreamChunksResult, error) {
	// Poll the stream with backoff from 50ms to 500ms while waiting for chunks
	// past the cursor.
	start, err := worlds.StreamsStartIndex(ctx, w, input)
	if err != nil {
		return worlds.StreamChunksResult{}, err
	}
	if input.Cursor != "" {
		value, err := strconv.Atoi(input.Cursor)
		if err != nil || value < 0 {
			return worlds.StreamChunksResult{}, fmt.Errorf("workflow/aws: invalid stream cursor %q", input.Cursor)
		}
		start = value
	}
	limit := input.Limit
	if limit <= 0 {
		limit = defaultChunkLimit
	}
	deadline := time.Now().Add(input.Wait)
	delay := streamPollMin
	for {
		if err := ctx.Err(); err != nil {
			return worlds.StreamChunksResult{}, err
		}
		meta, err := w.streamMeta(ctx, input.RunID, input.Name)
		if err != nil {
			return worlds.StreamChunksResult{}, err
		}
		remaining := time.Until(deadline)
		if start < meta.count || meta.closed || input.Wait <= 0 || remaining <= 0 {
			return w.chunks(ctx, input.RunID, input.Name, start, limit, meta)
		}
		if err := sleep(ctx, min(delay, remaining)); err != nil {
			return worlds.StreamChunksResult{}, err
		}
		delay = min(delay*2, streamPollMax)
	}
}

func (w *World) chunks(ctx context.Context, runID worlds.RunID, name string, start, limit int, meta streamMeta) (worlds.StreamChunksResult, error) {
	end := max(min(start+limit, meta.count), start)
	result := worlds.StreamChunksResult{Done: meta.closed, HasMore: end < meta.count, RetryAfter: streamPollMin, Cursor: strconv.Itoa(end)}
	if end == start {
		return result, nil
	}
	err := w.query(ctx, &dynamodb.QueryInput{
		KeyConditionExpression:    awsv2.String("pk = :pk AND sk BETWEEN :from AND :to"),
		ExpressionAttributeValues: map[string]ddb.AttributeValue{":pk": avs(runPartitionKey(string(runID))), ":from": avs(chunkSortKey(name, start)), ":to": avs(chunkSortKey(name, end-1))},
		ConsistentRead:            awsv2.Bool(true),
	}, func(item map[string]ddb.AttributeValue) bool {
		result.Data = append(result.Data, worlds.StreamChunk{Index: start + len(result.Data), Data: readBytes(item, "data")})
		return true
	})
	if err == nil && len(result.Data) != end-start {
		err = fmt.Errorf("workflow/aws: stream %q of run %s is missing chunks %d-%d", name, runID, start, end-1)
	}
	return result, err
}

func (w *World) StreamsGetInfo(ctx context.Context, input worlds.StreamsGetInfoInput) (worlds.StreamInfo, error) {
	meta, err := w.streamMeta(ctx, input.RunID, input.Name)
	if err != nil {
		return worlds.StreamInfo{}, err
	}
	return worlds.StreamInfo{TailIndex: meta.count - 1, Done: meta.closed}, nil
}

func (w *World) StreamsList(ctx context.Context, input worlds.StreamsListInput) ([]string, error) {
	names := []string{}
	err := w.query(ctx, &dynamodb.QueryInput{
		KeyConditionExpression:    awsv2.String("pk = :pk AND begins_with(sk, :prefix)"),
		ExpressionAttributeValues: map[string]ddb.AttributeValue{":pk": avs(runPartitionKey(string(input.RunID))), ":prefix": avs("STREAM#")},
		ConsistentRead:            awsv2.Bool(true),
	}, func(item map[string]ddb.AttributeValue) bool {
		names = append(names, read(item, "name"))
		return true
	})
	sort.Strings(names)
	return names, err
}
