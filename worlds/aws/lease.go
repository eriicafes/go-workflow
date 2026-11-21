package awsworld

import (
	"context"
	"log"
	"time"

	awsv2 "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddb "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/eriicafes/go-workflow/worlds"
	"github.com/oklog/ulid/v2"
)

const (
	// leaseDuration is how long a run's flow lease lasts without renewal,
	// so a lease whose holder died frees the run after at most this long.
	// The holder renews it every visibilityHeartbeat.
	leaseDuration = 2 * time.Minute
	// leaseRetryDelay is how long a flow that finds its run's lease taken
	// waits before it is delivered again.
	leaseRetryDelay = 2 * time.Second
)

// flowLease is a run's flow lease held by one delivery.
type flowLease struct {
	world  *World
	runID  worlds.RunID
	holder string
	stop   func()
}

// acquireLease takes runID's flow lease, so one serialized flow of the run
// executes at a time, and keeps renewing it until release. It returns nil
// when another delivery holds a lease that has not expired.
func (w *World) acquireLease(ctx context.Context, runID worlds.RunID) (*flowLease, error) {
	holder := ulid.Make().String()
	now := time.Now()
	_, err := w.dynamo.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:                 awsv2.String(w.config.TableName),
		Item:                      leaseItem(runID, holder, now),
		ConditionExpression:       awsv2.String("attribute_not_exists(pk) OR leaseUntil < :now"),
		ExpressionAttributeValues: map[string]ddb.AttributeValue{":now": avn(now.UnixMilli())},
	})
	if conditional(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	lease := &flowLease{world: w, runID: runID, holder: holder}
	renewCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(visibilityHeartbeat)
		defer ticker.Stop()
		for {
			select {
			case <-renewCtx.Done():
				return
			case <-ticker.C:
				lease.renew(renewCtx)
			}
		}
	}()
	lease.stop = func() {
		cancel()
		<-done
	}
	return lease, nil
}

func leaseItem(runID worlds.RunID, holder string, now time.Time) map[string]ddb.AttributeValue {
	until := now.Add(leaseDuration)
	return merge(leaseKey(runID), map[string]ddb.AttributeValue{
		"holder":     avs(holder),
		"leaseUntil": avn(until.UnixMilli()),
		// The table's TTL removes a lease whose holder died.
		"ttl": avn(until.Add(time.Hour).Unix()),
	})
}

func leaseKey(runID worlds.RunID) map[string]ddb.AttributeValue {
	return key("LEASE#"+string(runID), "META")
}

// renew extends the lease while this delivery holds it. A lease lost to
// another delivery after it expired is not taken back: the run's fence keeps
// the overlap safe.
func (l *flowLease) renew(ctx context.Context) {
	until := time.Now().Add(leaseDuration)
	_, err := l.world.dynamo.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                 awsv2.String(l.world.config.TableName),
		Key:                       leaseKey(l.runID),
		UpdateExpression:          awsv2.String("SET leaseUntil = :until, #ttl = :ttl"),
		ConditionExpression:       awsv2.String("holder = :holder"),
		ExpressionAttributeNames:  map[string]string{"#ttl": "ttl"},
		ExpressionAttributeValues: map[string]ddb.AttributeValue{":until": avn(until.UnixMilli()), ":ttl": avn(until.Add(time.Hour).Unix()), ":holder": avs(l.holder)},
	})
	if err != nil && ctx.Err() == nil {
		log.Printf("workflow/aws: renew flow lease of run %s: %v", l.runID, err)
	}
}

// release stops renewing the lease and deletes it while this delivery holds
// it. It is safe to call more than once.
func (l *flowLease) release(ctx context.Context) {
	if l == nil || l.stop == nil {
		return
	}
	l.stop()
	l.stop = nil
	_, err := l.world.dynamo.DeleteItem(context.WithoutCancel(ctx), &dynamodb.DeleteItemInput{
		TableName:                 awsv2.String(l.world.config.TableName),
		Key:                       leaseKey(l.runID),
		ConditionExpression:       awsv2.String("holder = :holder"),
		ExpressionAttributeValues: map[string]ddb.AttributeValue{":holder": avs(l.holder)},
	})
	if err != nil && !conditional(err) {
		// The lease expires on its own.
		log.Printf("workflow/aws: release flow lease of run %s: %v", l.runID, err)
	}
}
