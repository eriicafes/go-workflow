package awsworld

import (
	"context"
	"testing"
	"time"

	awsv2 "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/eriicafes/go-workflow/worlds"
)

func TestQueueDeliversOneFlowOfARunAtATime(t *testing.T) {
	ctx := context.Background()
	dynamo, queue := newFakeDynamo(), &fakeSQS{}
	world := testWorld(t, dynamo, queue, nil)
	other, err := world.acquireLease(ctx, "wrun_a")
	if err != nil || other == nil {
		t.Fatalf("lease = %v, %v", other, err)
	}
	defer other.release(ctx)
	if again, err := world.acquireLease(ctx, "wrun_a"); err != nil || again != nil {
		t.Fatalf("second lease = %v, %v; want none", again, err)
	}

	queued, err := world.Queue(ctx, worlds.QueueInput{RunID: "wrun_a"})
	if err != nil {
		t.Fatal(err)
	}
	runtime := &fakeExecutor{}
	if err := world.deliver(ctx, runtime, lastBody(t, queue)); err != nil {
		t.Fatal(err)
	}
	sent := queue.sent()
	if len(runtime.inputs) != 0 || len(sent) != 2 || sent[1].DelaySeconds != int32(leaseRetryDelay/time.Second) {
		t.Fatalf("%d executions, sends %+v; want the flow sent again shortly", len(runtime.inputs), sent)
	}
	if deliveries := readInt(dynamo.items[itemID(queueKey(queued.MessageID))], "deliveries"); deliveries != 0 {
		t.Fatalf("deliveries = %d, want a held flow uncounted", deliveries)
	}

	// Step deliveries and intermediate hops are never held.
	stepID := worlds.StepID("step_1")
	resumeAt, hopAt := time.Now().Add(time.Hour), time.Now()
	for _, input := range []worlds.QueueInput{
		{RunID: "wrun_a", StepID: &stepID},
		{RunID: "wrun_a", WaitContinuation: &worlds.WaitContinuation{CorrelationID: "wait_1", ResumeAt: &resumeAt, HopAt: &hopAt}},
	} {
		if _, err := world.Queue(ctx, input); err != nil {
			t.Fatal(err)
		}
		if err := world.deliver(ctx, runtime, lastBody(t, queue)); err != nil {
			t.Fatal(err)
		}
	}
	if len(runtime.inputs) != 2 {
		t.Fatalf("%d executions, want the step and the hop", len(runtime.inputs))
	}

	// Once the lease is released, the flow executes and releases it in turn.
	other.release(ctx)
	if err := world.deliver(ctx, runtime, awsv2.ToString(sent[1].MessageBody)); err != nil {
		t.Fatal(err)
	}
	if len(runtime.inputs) != 3 || runtime.inputs[2].Attempt != 1 || runtime.inputs[2].DeliveryCount != 1 {
		t.Fatalf("executions = %+v", runtime.inputs)
	}
	if dynamo.items[itemID(leaseKey("wrun_a"))] != nil {
		t.Fatal("the flow kept the lease")
	}
}

func TestAcquireLeaseTakesAnExpiredLease(t *testing.T) {
	ctx := context.Background()
	dynamo := newFakeDynamo()
	world := testWorld(t, dynamo, &fakeSQS{}, nil)
	dynamo.items[itemID(leaseKey("wrun_a"))] = leaseItem("wrun_a", "dead", time.Now().Add(-2*leaseDuration))
	lease, err := world.acquireLease(ctx, "wrun_a")
	if err != nil || lease == nil {
		t.Fatalf("lease = %v, %v; want the expired lease taken", lease, err)
	}
	lease.renew(ctx)
	if holder := read(dynamo.items[itemID(leaseKey("wrun_a"))], "holder"); holder != lease.holder {
		t.Fatalf("holder = %s", holder)
	}
	// A release by a holder that lost the lease leaves the new holder's.
	dynamo.items[itemID(leaseKey("wrun_a"))] = leaseItem("wrun_a", "newer", time.Now())
	lease.release(ctx)
	lease.release(ctx)
	if read(dynamo.items[itemID(leaseKey("wrun_a"))], "holder") != "newer" {
		t.Fatal("a stale holder released another's lease")
	}
}
