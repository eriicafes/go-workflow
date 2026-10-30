package worlds

import "reflect"

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
