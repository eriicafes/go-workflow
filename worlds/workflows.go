package worlds

import (
	"reflect"
	"time"
)

// Workflow is satisfied by a workflow: a struct type whose Workflow(Out)
// method declares the workflow's output type, as embedding workflow.Struct
// does.
type Workflow[Out any] interface{ Workflow(Out) }

// WorkflowName returns the name runs of the workflow type In are recorded
// under: its package path and type name.
func WorkflowName[In Workflow[Out], Out any]() string {
	typeOf := reflect.TypeFor[In]()
	for typeOf.Kind() == reflect.Pointer {
		typeOf = typeOf.Elem()
	}
	return typeOf.PkgPath() + "." + typeOf.Name()
}

// QueueRoute routes the deliveries of one workflow's runs to a named queue.
type QueueRoute struct {
	Workflow string
	Queue    string
}

// Route routes the deliveries of runs of the workflow type In to the queue
// named queue.
func Route[In Workflow[Out], Out any](queue string) QueueRoute {
	return QueueRoute{Workflow: WorkflowName[In](), Queue: queue}
}

// Routes returns the queue of each routed workflow, by workflow name, for a
// World's Config.Routes.
func Routes(routes ...QueueRoute) map[string]string {
	byWorkflow := make(map[string]string, len(routes))
	for _, route := range routes {
		byWorkflow[route.Workflow] = route.Queue
	}
	return byWorkflow
}

// WorkflowRetention sets how long one workflow's finished runs are kept.
type WorkflowRetention struct {
	Workflow  string
	Retention time.Duration
}

// Retain keeps finished runs of the workflow type In for retention. Zero
// keeps them forever, whatever a World's default retention is.
func Retain[In Workflow[Out], Out any](retention time.Duration) WorkflowRetention {
	return WorkflowRetention{Workflow: WorkflowName[In](), Retention: retention}
}

// Retentions returns the retention of each listed workflow, by workflow
// name, for a World's Config.Retentions.
func Retentions(retentions ...WorkflowRetention) map[string]time.Duration {
	byWorkflow := make(map[string]time.Duration, len(retentions))
	for _, retention := range retentions {
		byWorkflow[retention.Workflow] = retention.Retention
	}
	return byWorkflow
}

// RetentionFor returns how long finished runs of workflow are kept: its
// entry in retentions, or fallback when it has none. Zero keeps them forever.
func RetentionFor(fallback time.Duration, retentions map[string]time.Duration, workflow string) time.Duration {
	if retention, ok := retentions[workflow]; ok {
		return retention
	}
	return fallback
}

// MaxRetention returns the longest of fallback and retentions, and whether
// any of them is negative, so a World can validate its configuration.
func MaxRetention(fallback time.Duration, retentions map[string]time.Duration) (longest time.Duration, negative bool) {
	longest, negative = fallback, fallback < 0
	for _, retention := range retentions {
		longest = max(longest, retention)
		negative = negative || retention < 0
	}
	return longest, negative
}
