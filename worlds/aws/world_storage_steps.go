package awsworld

import (
	"context"
	"sort"

	awsv2 "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/eriicafes/go-workflow/worlds"
)

func (w *World) StepsGet(ctx context.Context, input worlds.StepsGetInput) (worlds.Step, error) {
	if err := ctx.Err(); err != nil {
		return worlds.Step{}, err
	}
	item, found, err := w.item(ctx, stepKey(string(input.RunID), string(input.StepID)))
	if err != nil {
		return worlds.Step{}, err
	}
	if !found {
		return worlds.Step{}, worlds.ErrStepNotFound
	}
	steps, err := w.resolveSteps(ctx, []stepView{stepFromItem(item)}, input.ResolveData)
	if err != nil {
		return worlds.Step{}, err
	}
	return steps[0], nil
}

func (w *World) StepsList(ctx context.Context, input worlds.StepsListInput) (worlds.Page[worlds.Step], error) {
	// Read the run's STEP# items, order them by creation time, and read the
	// payloads of the requested page only.
	if err := ctx.Err(); err != nil {
		return worlds.Page[worlds.Step]{}, err
	}
	var views []stepView
	err := w.query(ctx, &dynamodb.QueryInput{
		KeyConditionExpression:    awsv2.String("pk = :pk AND begins_with(sk, :prefix)"),
		ExpressionAttributeValues: map[string]ddb.AttributeValue{":pk": avs(runPartitionKey(string(input.RunID))), ":prefix": avs("STEP#")},
		ConsistentRead:            awsv2.Bool(true),
	}, func(item map[string]ddb.AttributeValue) bool {
		views = append(views, stepFromItem(item))
		return true
	})
	if err != nil {
		return worlds.Page[worlds.Step]{}, err
	}
	sortKey := func(view stepView) string { return indexSortKey(view.CreatedAt, string(view.StepID)) }
	sort.Slice(views, func(i, j int) bool { return sortKey(views[i]) < sortKey(views[j]) })
	page, err := sortedPage(views, sortKey, worlds.Pagination{Cursor: input.Pagination.Cursor, Limit: input.Pagination.Limit})
	if err != nil {
		return worlds.Page[worlds.Step]{}, err
	}
	steps, err := w.resolveSteps(ctx, page.Data, input.ResolveData)
	if err != nil {
		return worlds.Page[worlds.Step]{}, err
	}
	return worlds.Page[worlds.Step]{Data: steps, Cursor: page.Cursor, HasMore: page.HasMore}, nil
}
