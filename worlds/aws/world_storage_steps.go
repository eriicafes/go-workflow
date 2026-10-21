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
	return w.resolveStep(ctx, stepFromItem(item), input.ResolveData)
}

func (w *World) StepsList(ctx context.Context, input worlds.StepsListInput) (worlds.Page[worlds.Step], error) {
	// Read the run's STEP# items and order them by creation time.
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
	steps := make([]worlds.Step, len(views))
	for index, view := range views {
		if steps[index], err = w.resolveStep(ctx, view, input.ResolveData); err != nil {
			return worlds.Page[worlds.Step]{}, err
		}
	}
	sortKey := func(step worlds.Step) string { return indexSortKey(step.CreatedAt, string(step.StepID)) }
	sort.Slice(steps, func(i, j int) bool { return sortKey(steps[i]) < sortKey(steps[j]) })
	return sortedPage(steps, sortKey, worlds.Pagination{Cursor: input.Pagination.Cursor, Limit: input.Pagination.Limit})
}
