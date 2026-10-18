// Package check provides Analyzer, a static analyzer that reports runners
// that cannot replay deterministically and calls that misuse the workflow API.
// The workflowcheck command runs it with go vet, and workflowdev runs it before
// every go-dev start or reload.
package check

import (
	"go/ast"
	"go/types"
	"strings"

	"golang.org/x/tools/go/analysis"
)

const workflowPackage = "github.com/eriicafes/go-workflow"

// Analyzer checks that every function passed to workflow.Use is a named runner
// whose input type is a workflow.
//
// In runners, it reports operations that break replay, such as goroutines,
// select, ranging over maps, time.Now, and I/O or randomness outside steps. It
// also reports stream access outside steps, and steps that are not named
// functions or that capture variables instead of taking them as input.
var Analyzer = &analysis.Analyzer{Name: "workflowcheck", Doc: "reports invalid or non-deterministic workflows", Run: run}

func run(pass *analysis.Pass) (any, error) {
	declarations := map[*types.Func]*ast.FuncDecl{}
	for _, file := range pass.Files {
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Recv != nil {
				continue
			}
			if object, ok := pass.TypesInfo.Defs[function.Name].(*types.Func); ok {
				declarations[object] = function
			}
		}
	}
	for function, declaration := range declarations {
		if isWorkflowFunction(function) {
			checkBody(pass, declaration.Body)
		}
	}
	for _, file := range pass.Files {
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok || !isSDKCall(pass, call, "Use") {
				return true
			}
			if len(call.Args) != 1 {
				pass.Reportf(call.Pos(), "workflow.Use requires one workflow function")
				return true
			}
			for _, argument := range call.Args {
				function := namedFunction(pass, argument)
				if function == nil {
					pass.Reportf(argument.Pos(), "workflow.Use requires a named package-level workflow function")
					continue
				}
				validateWorkflow(pass, argument, function)
			}
			return true
		})
	}
	return nil, nil
}

func namedFunction(pass *analysis.Pass, expression ast.Expr) *types.Func {
	switch expression := expression.(type) {
	case *ast.Ident:
		function, _ := pass.TypesInfo.ObjectOf(expression).(*types.Func)
		return function
	case *ast.SelectorExpr:
		function, _ := pass.TypesInfo.ObjectOf(expression.Sel).(*types.Func)
		return function
	default:
		return nil
	}
}

func validateWorkflow(pass *analysis.Pass, position ast.Expr, function *types.Func) {
	signature, ok := function.Type().(*types.Signature)
	if !ok || signature.Params().Len() != 2 || signature.Results().Len() != 2 || !isContextType(signature.Params().At(0).Type()) || !types.Identical(signature.Results().At(1).Type(), types.Universe.Lookup("error").Type()) {
		pass.Reportf(position.Pos(), "workflow function must be func(context.Context, NamedInput) (Output, error)")
		return
	}
	input, output := signature.Params().At(1).Type(), signature.Results().At(0).Type()
	named, ok := input.(*types.Named)
	if !ok || named.Obj().Pkg() == nil || named.Obj().Name() == "" {
		pass.Reportf(position.Pos(), "workflow input must be a named package type")
		return
	}
	methodSet := types.NewMethodSet(input)
	selection := methodSet.Lookup(nil, "Workflow")
	if selection == nil {
		pass.Reportf(position.Pos(), "%s must declare Workflow(%s)", input, output)
		return
	}
	method, ok := selection.Obj().Type().(*types.Signature)
	if !ok || method.Params().Len() != 1 || !types.Identical(method.Params().At(0).Type(), output) || method.Results().Len() != 0 {
		pass.Reportf(position.Pos(), "%s must declare Workflow(%s)", input, output)
	}
}

func isWorkflowFunction(function *types.Func) bool {
	signature, ok := function.Type().(*types.Signature)
	if !ok || signature.Params().Len() != 2 || signature.Results().Len() != 2 {
		return false
	}
	if !isContextType(signature.Params().At(0).Type()) || !types.Identical(signature.Results().At(1).Type(), types.Universe.Lookup("error").Type()) {
		return false
	}
	input, output := signature.Params().At(1).Type(), signature.Results().At(0).Type()
	named, ok := input.(*types.Named)
	if !ok || named.Obj().Pkg() == nil || named.Obj().Name() == "" {
		return false
	}
	selection := types.NewMethodSet(input).Lookup(nil, "Workflow")
	if selection == nil {
		return false
	}
	method, ok := selection.Obj().Type().(*types.Signature)
	return ok && method.Params().Len() == 1 &&
		types.Identical(method.Params().At(0).Type(), output) &&
		method.Results().Len() == 0
}

func isContextType(value types.Type) bool {
	named, ok := value.(*types.Named)
	if !ok || named.Obj().Pkg() == nil {
		return false
	}
	return named.Obj().Pkg().Path() == "context" && named.Obj().Name() == "Context"
}

func checkBody(pass *analysis.Pass, body *ast.BlockStmt) {
	ast.Inspect(body, func(node ast.Node) bool {
		switch node := node.(type) {
		case *ast.FuncLit:
			// Durable step closures are validated independently. Their bodies are
			// allowed to perform step-only operations.
			return false
		case *ast.GoStmt:
			pass.Reportf(node.Pos(), "workflow body must not start a goroutine, use workflow.Background instead")
		case *ast.SelectStmt:
			pass.Reportf(node.Pos(), "workflow body must not select on channels")
		case *ast.RangeStmt:
			if typeOf := pass.TypesInfo.TypeOf(node.X); typeOf != nil {
				if _, mapRange := typeOf.Underlying().(*types.Map); mapRange {
					pass.Reportf(node.Pos(), "workflow body must not range over a map")
				}
			}
		case *ast.CallExpr:
			if isSDKCall(pass, node, "Writer") || isSDKCall(pass, node, "Reader") ||
				isSDKCall(pass, node, "ReaderAt") ||
				isSDKMethod(pass, node, "Writer") || isSDKMethod(pass, node, "Reader") ||
				isSDKMethod(pass, node, "ReaderAt") || isSDKMethod(pass, node, "RespondWith") {
				pass.Reportf(node.Pos(), "workflow stream access and webhook responses require a durable step")
			}
			if banned, ok := bannedCall(pass, node); ok {
				if strings.HasPrefix(banned, "context.With") {
					pass.Reportf(node.Pos(), "workflow body calls %s, use workflow.WithCancel/WithCancelCause and durable timers instead", banned)
				} else if banned == "time.Now" {
					pass.Reportf(node.Pos(), "workflow body calls time.Now, use workflow.Now instead")
				} else {
					pass.Reportf(node.Pos(), "workflow body calls %s, use workflow.Step or workflow.Background instead", banned)
				}
			}
			if isSDKCall(pass, node, "Then") && len(node.Args) == 3 {
				if callback, ok := node.Args[2].(*ast.FuncLit); ok {
					checkBody(pass, callback.Body)
				}
			}
			if isDurableOperation(pass, node) {
				validateStepFunction(pass, node)
			}
		}
		return true
	})
}

func isSDKMethod(pass *analysis.Pass, call *ast.CallExpr, name string) bool {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != name {
		return false
	}
	function, ok := pass.TypesInfo.ObjectOf(selector.Sel).(*types.Func)
	return ok && function.Pkg() != nil && function.Pkg().Path() == workflowPackage
}

func validateStepFunction(pass *analysis.Pass, call *ast.CallExpr) {
	if len(call.Args) < 2 {
		return
	}
	if closure, ok := call.Args[1].(*ast.FuncLit); ok {
		validateClosure(pass, closure)
		return
	}
	function := namedFunction(pass, call.Args[1])
	if function == nil {
		pass.Reportf(call.Args[1].Pos(), "durable step must be a named function or inline closure, not a wrapper or factory")
	}
}

func validateClosure(pass *analysis.Pass, closure *ast.FuncLit) {
	parameters := map[types.Object]bool{}
	if closure.Type.Params != nil {
		for _, field := range closure.Type.Params.List {
			for _, name := range field.Names {
				if object := pass.TypesInfo.Defs[name]; object != nil {
					parameters[object] = true
				}
			}
		}
	}
	ast.Inspect(closure.Body, func(node ast.Node) bool {
		identifier, ok := node.(*ast.Ident)
		if !ok {
			return true
		}
		object := pass.TypesInfo.ObjectOf(identifier)
		variable, ok := object.(*types.Var)
		if !ok || parameters[variable] || variable.Pos() >= closure.Body.Pos() {
			return true
		}
		pass.Reportf(identifier.Pos(), "durable closure captures %s, pass workflow-relevant state through its explicit input instead", identifier.Name)
		return true
	})
}

func isSDKCall(pass *analysis.Pass, call *ast.CallExpr, name string) bool {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != name {
		return false
	}
	function, ok := pass.TypesInfo.ObjectOf(selector.Sel).(*types.Func)
	return ok && function.Pkg() != nil && function.Pkg().Path() == workflowPackage
}

func isDurableOperation(pass *analysis.Pass, call *ast.CallExpr) bool {
	for _, name := range []string{"Step", "InlineStep", "Background", "Map", "All", "Rollback"} {
		if isSDKCall(pass, call, name) {
			return true
		}
	}
	return false
}

func bannedCall(pass *analysis.Pass, call *ast.CallExpr) (string, bool) {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return "", false
	}
	function, ok := pass.TypesInfo.ObjectOf(selector.Sel).(*types.Func)
	if !ok || function.Pkg() == nil {
		return "", false
	}
	path, name := function.Pkg().Path(), function.Name()
	if path == "context" && (name == "WithCancel" || name == "WithCancelCause" || name == "WithTimeout" || name == "WithTimeoutCause" || name == "WithDeadline" || name == "WithDeadlineCause") {
		return path + "." + name, true
	}
	if path == "time" && name == "Now" {
		return path + "." + name, true
	}
	if path == "os" || path == "io" || path == "net/http" || path == "database/sql" || path == "crypto/rand" || path == "math/rand" || strings.Contains(path, "uuid") {
		return path + "." + name, true
	}
	return "", false
}
