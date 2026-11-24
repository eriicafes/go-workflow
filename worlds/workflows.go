package worlds

import (
	"reflect"
	"runtime"
	"strings"
	"time"
)

// Workflow is satisfied by a workflow: a struct type whose Workflow(Out)
// method declares the workflow's output type, as embedding workflow.Struct
// does.
type Workflow[Out any] interface{ Workflow(Out) }

// WorkflowName returns the name runs of the workflow type In are recorded
// under, in the Workflow SDK's form: "workflow//<package path>//<type name>".
func WorkflowName[In Workflow[Out], Out any]() string {
	typeOf := reflect.TypeFor[In]()
	for typeOf.Kind() == reflect.Pointer {
		typeOf = typeOf.Elem()
	}
	return "workflow//" + typeOf.PkgPath() + "//" + typeOf.Name()
}

// StepName returns the name the step function fn is recorded under, in the
// Workflow SDK's form "step//<package path>//<name>", from fn's Go name. The
// package path ends at the first dot after its last slash, and a nested
// name, such as a closure's or a method's, joins its parts with a slash: the
// closure example.com/app/flows.Publish.func1 is
// "step//example.com/app/flows//Publish/func1". Dots in a generic function's
// type arguments are kept.
func StepName(fn any) string {
	return stepName(runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name())
}

// stepName returns StepName of the function with the Go name function.
func stepName(function string) string {
	// Type arguments, in brackets, may hold slashes and dots of their own.
	head := function
	if bracket := strings.IndexByte(head, '['); bracket >= 0 {
		head = head[:bracket]
	}
	slash := strings.LastIndexByte(head, '/') + 1
	dot := strings.IndexByte(head[slash:], '.')
	if dot < 0 {
		return "step//" + function
	}
	dot += slash
	var name strings.Builder
	depth := 0
	for _, r := range function[dot+1:] {
		switch {
		case r == '[':
			depth++
		case r == ']':
			depth--
		case r == '.' && depth == 0:
			r = '/'
		}
		name.WriteRune(r)
	}
	return "step//" + function[:dot] + "//" + name.String()
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

// Retention is how long a finished run's data is kept, as the run's
// $retention attribute records it. Its zero value is RetainDefault, and
// RetainNone is the only other value.
type Retention struct{ value string }

var (
	// RetainDefault leaves a run's data to the World's retention.
	RetainDefault = Retention{}
	// RetainNone deletes a run's data as soon as the run finishes. The run's
	// metadata, such as its ID, status, and attributes, is kept.
	RetainNone = Retention{value: "0"}
)

// String returns the RetentionAttribute value of r, or "" for
// RetainDefault, which is recorded by leaving the attribute out.
func (r Retention) String() string { return r.value }

// ReadRunRetention returns the retention a run's attributes ask for:
// RetainNone for a RetentionAttribute of "0", and RetainDefault otherwise.
// Any other value, such as another duration, keeps the run's data rather
// than deleting it.
func ReadRunRetention(attributes map[string]string) Retention {
	if attributes[RetentionAttribute] == RetainNone.value {
		return RetainNone
	}
	return RetainDefault
}

// RunDataExpired reports whether run's data is gone at now: its ExpiredAt
// is set and has passed. A World stops returning a run's payloads once it
// has, even before it deletes them.
func RunDataExpired(run Run, now time.Time) bool {
	return run.ExpiredAt != nil && !run.ExpiredAt.After(now)
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
