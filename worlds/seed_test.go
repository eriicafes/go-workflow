package worlds

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"testing"
)

// TestRunSeedVectors checks the generator against values seedrandom, ulid's
// monotonicFactory, and the SDK's seeded Nano ID produce for the same runs.
func TestRunSeedVectors(t *testing.T) {
	raw, err := os.ReadFile("testdata/seed.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		RunID        RunID    `json:"runId"`
		WorkflowName string   `json:"workflowName"`
		DeploymentID string   `json:"deploymentId"`
		Ops          string   `json:"ops"`
		CreatedAt    uint64   `json:"createdAt"`
		Results      []string `json:"results"`
	}
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		created, err := runCreatedAt(c.RunID)
		if err != nil || created != c.CreatedAt {
			t.Fatalf("runCreatedAt(%s) = %d, %v, want %d", c.RunID, created, err, c.CreatedAt)
		}
		seed, err := newRunSeed(Run{RunID: c.RunID, WorkflowName: c.WorkflowName, DeploymentID: c.DeploymentID})
		if err != nil {
			t.Fatal(err)
		}
		for index, op := range strings.Fields(c.Ops) {
			want := c.Results[index]
			switch {
			case op == "f":
				expected, _ := strconv.ParseFloat(want, 64)
				if got := seed.float64(); got != expected {
					t.Fatalf("%s op %d: float64 = %v, want %v", c.RunID, index, got, expected)
				}
			case op == "u":
				if got := seed.ulid(); got != want {
					t.Fatalf("%s op %d: ulid = %s, want %s", c.RunID, index, got, want)
				}
			case op == "n":
				if got := seed.hookToken(); got != want {
					t.Fatalf("%s op %d: hookToken = %s, want %s", c.RunID, index, got, want)
				}
			default:
				size, _ := strconv.Atoi(op[1:])
				got := make([]byte, size)
				_, _ = seed.Read(got)
				if hex.EncodeToString(got) != want {
					t.Fatalf("%s op %d: Read = %x, want %s", c.RunID, index, got, want)
				}
			}
		}
	}
}

func TestRunCreatedAtRejectsOtherIDs(t *testing.T) {
	for _, id := range []RunID{"wrun_test", "run_01JQ8Z5K3M4N5P6Q7R8S9T0VWX", ""} {
		if _, err := runCreatedAt(id); err == nil {
			t.Errorf("runCreatedAt(%q) succeeded", id)
		}
	}
}

func TestIncrementBase32Overflow(t *testing.T) {
	value := []byte("ZZ")
	if incrementBase32(value) || string(value) != "00" {
		t.Fatalf("incrementBase32(ZZ) = %s", value)
	}
	value = []byte("0Z")
	if !incrementBase32(value) || string(value) != "10" {
		t.Fatalf("incrementBase32(0Z) = %s", value)
	}
}

// TestExecutionDrawsFromRunSeed checks that declarations draw the SDK's IDs
// and tokens for the run, in declaration order.
func TestExecutionDrawsFromRunSeed(t *testing.T) {
	run := Run{RunID: "wrun_41JQ8Z5K3M50NP6Q7R8S9T0VWX", WorkflowName: "checkout"}
	execution, err := newExecution(t.Context(), nil, run, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	got := []string{string(execution.NextStepID()), execution.NextHookToken(), string(execution.NextHookID()), execution.NextHookToken(), string(execution.NextWaitID())}
	raw, err := os.ReadFile("testdata/seed.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		RunID   RunID    `json:"runId"`
		Results []string `json:"results"`
	}
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	want := cases[1].Results
	if cases[1].RunID != run.RunID {
		t.Fatalf("vector run = %s", cases[1].RunID)
	}
	for index, prefix := range []string{"step_", "", "hook_", "", "wait_"} {
		if got[index] != prefix+want[index] {
			t.Errorf("draw %d = %s, want %s", index, got[index], prefix+want[index])
		}
	}
}

// TestNextHookIDSkipsTheStartHookID checks that a hook never draws the ID of
// the run's start hook, hook_ and the run ID's ULID.
func TestNextHookIDSkipsTheStartHookID(t *testing.T) {
	run := Run{RunID: "wrun_01JQ8Z5K3M4N5P6Q7R8S9T0VW1", WorkflowName: "w"}
	created, err := json.Marshal(DataRunCreated{StartHook: &StartHook{Token: "start", HookID: StartHookID(run.RunID)}})
	if err != nil {
		t.Fatal(err)
	}
	execution, err := newExecution(t.Context(), nil, run, []Event{{EventType: EventTypeRunCreated, Data: created}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Make the next correlation ULID the run's own.
	copy(execution.seed.last[:], "4N5P6Q7R8S9T0VW0")
	execution.seed.minted = true
	if id := execution.NextHookID(); id != "hook_01JQ8Z5K3M4N5P6Q7R8S9T0VW2" {
		t.Fatalf("NextHookID = %s, want the ID after the start hook's", id)
	}
}
