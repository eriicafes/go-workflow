package awsworld

import (
	"context"
	"errors"
	"time"

	awsv2 "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"
	sch "github.com/aws/aws-sdk-go-v2/service/scheduler/types"
)

// schedulerAPI is the subset of the EventBridge Scheduler client used by World.
type schedulerAPI interface {
	CreateSchedule(context.Context, *scheduler.CreateScheduleInput, ...func(*scheduler.Options)) (*scheduler.CreateScheduleOutput, error)
	DeleteSchedule(context.Context, *scheduler.DeleteScheduleInput, ...func(*scheduler.Options)) (*scheduler.DeleteScheduleOutput, error)
}

func (w *World) scheduleGroup() string {
	if w.config.Scheduler == nil || w.config.Scheduler.Group == "" {
		return "default"
	}
	return w.config.Scheduler.Group
}

func (w *World) createSchedule(ctx context.Context, name string, due time.Time, body string) error {
	_, err := w.scheduler.CreateSchedule(ctx, &scheduler.CreateScheduleInput{
		Name:                       awsv2.String(name),
		GroupName:                  awsv2.String(w.scheduleGroup()),
		ScheduleExpression:         awsv2.String(scheduleExpression(due)),
		ScheduleExpressionTimezone: awsv2.String("UTC"),
		FlexibleTimeWindow:         &sch.FlexibleTimeWindow{Mode: sch.FlexibleTimeWindowModeOff},
		Target: &sch.Target{
			Arn:     awsv2.String(w.config.Scheduler.TargetARN),
			RoleArn: awsv2.String(w.config.Scheduler.RoleARN),
			Input:   awsv2.String(body),
		},
		ActionAfterCompletion: sch.ActionAfterCompletionDelete,
	})
	return err
}

// deleteSchedule removes a schedule. A schedule that already fired and was
// deleted, or never existed, counts as removed.
func (w *World) deleteSchedule(ctx context.Context, name string) error {
	_, err := w.scheduler.DeleteSchedule(ctx, &scheduler.DeleteScheduleInput{
		Name:      awsv2.String(name),
		GroupName: awsv2.String(w.scheduleGroup()),
	})
	var notFound *sch.ResourceNotFoundException
	if errors.As(err, &notFound) {
		return nil
	}
	return err
}

// scheduleName derives a schedule name from a queue message ID. ULIDs are 26
// characters, so the name stays well within Scheduler's 64-character limit.
func scheduleName(messageID string) string {
	name := "go-workflow-" + messageID
	if len(name) > 64 {
		name = name[:64]
	}
	return name
}

// scheduleExpression returns a one-time expression for due, rounded up to
// the next second so delivery is never earlier than due.
func scheduleExpression(due time.Time) string {
	due = due.UTC()
	if truncated := due.Truncate(time.Second); !truncated.Equal(due) {
		due = truncated.Add(time.Second)
	}
	return "at(" + due.Format("2006-01-02T15:04:05") + ")"
}
