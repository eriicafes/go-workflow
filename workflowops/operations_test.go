package workflowops

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestResolveWait(t *testing.T) {
	completed := func(order uint64, output string) Dependency {
		return Dependency{Found: true, Status: StatusCompleted, Output: json.RawMessage(output), Completion: order}
	}
	failed := func(order uint64, message string) Dependency {
		return Dependency{Found: true, Status: StatusFailed, Error: message, Completion: order}
	}
	pending := Dependency{Found: true, Status: StatusRunning}

	if result := ResolveWait(KindAwait, []Dependency{completed(1, `"ok"`)}); result.Pending || string(result.Output) != `"ok"` || result.Err != nil {
		t.Fatalf("await success = %#v", result)
	}
	if result := ResolveWait(KindAwait, []Dependency{failed(1, "no")}); result.Err == nil || result.Err.Error() != "no" {
		t.Fatalf("await failure = %#v", result)
	}
	if result := ResolveWait(KindAwait, []Dependency{{}}); !errors.Is(result.Err, ErrRunNotFound) {
		t.Fatalf("await missing = %#v", result)
	}
	if result := ResolveWait(KindRace, []Dependency{completed(2, `1`), completed(1, `2`)}); string(result.Output) != `{"index":1}` {
		t.Fatalf("race = %#v", result)
	}
	if result := ResolveWait(KindJoin, []Dependency{pending, failed(2, "late"), failed(1, "first")}); result.Err == nil || result.Err.Error() != "first" {
		t.Fatalf("join = %#v", result)
	}
	if result := ResolveWait(KindJoinSettled, []Dependency{pending, failed(1, "ignored")}); !result.Pending {
		t.Fatalf("join settled pending = %#v", result)
	}
	if result := ResolveWait(KindJoinSettled, []Dependency{completed(2, `1`), failed(1, "ignored")}); result.Pending || result.Err != nil || string(result.Output) != `{}` {
		t.Fatalf("join settled = %#v", result)
	}
}

func TestCommandMatchingAndRunIDs(t *testing.T) {
	request := OperationRequest{Kind: KindStep, Identity: "step", Fingerprint: "fingerprint", Input: json.RawMessage(`{"input":1}`)}
	command := CommandMetadata{
		Kind:        request.Kind,
		Identity:    request.Identity,
		Fingerprint: request.Fingerprint,
		Input:       request.Input,
	}
	if !command.MatchesOperation(request) {
		t.Fatal("operation metadata did not match its request")
	}
	request.Identity = "changed"
	if command.MatchesOperation(request) {
		t.Fatal("operation metadata matched changed request")
	}
	run := RunRequest{RunID: "parent", Sequence: 1, Kind: KindRun, Identity: "child", Fingerprint: "caller", Input: json.RawMessage(`{"input":2}`)}
	if !(CommandMetadata{
		Kind:        KindRun,
		Identity:    run.Identity,
		Fingerprint: run.Fingerprint,
		Input:       run.Input,
		ChildRunID:  "run_child",
	}).MatchesRun(run) {
		t.Fatal("run metadata did not match its request")
	}
	wait := WaitRequest{Kind: KindJoin, Fingerprint: "join", Dependencies: []Task{{RunID: "run_1", Sequence: 1}}}
	input, err := json.Marshal(wait.Dependencies)
	if err != nil {
		t.Fatal(err)
	}
	command = CommandMetadata{
		Kind:        wait.Kind,
		Identity:    string(wait.Kind),
		Fingerprint: wait.Fingerprint,
		Input:       input,
	}
	if !command.MatchesWait(wait, input) {
		t.Fatal("wait metadata did not match")
	}
	first, err := NewRunID()
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewRunID()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(first, "run_") || len(first) != len("run_")+32 || first == second {
		t.Fatalf("run IDs = %q, %q", first, second)
	}
}
