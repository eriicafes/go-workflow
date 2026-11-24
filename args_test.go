package workflow

import (
	"context"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/internal/devalue"
	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/testworld"
)

type addArgs struct {
	Struct[int]
	Args
	A, B int
	note string
}

// A workflow whose input embeds Args takes its fields as an argument list,
// as a Workflow SDK caller passes them.
func TestArgsAreTheWorkflowsArgumentList(t *testing.T) {
	world := testworld.New(worlds.Runners(Use(func(_ context.Context, input addArgs) (int, error) {
		return input.A + input.B, nil
	})))
	defer world.Close()
	ctx, cancel := context.WithTimeout(NewContext(context.Background(), NewClient(world)), 5*time.Second)
	defer cancel()
	run, err := Start(ctx, addArgs{A: 2, B: 3})
	if err != nil {
		t.Fatal(err)
	}
	if sum, err := run.Value(ctx); err != nil || sum != 5 {
		t.Fatalf("sum = %d, %v, want 5", sum, err)
	}
	stored, err := world.RunsGet(ctx, worlds.RunsGetInput{RunID: worlds.RunID(run.ID())})
	if err != nil {
		t.Fatal(err)
	}
	var list []int
	if err := worlds.DecodePayload(stored.Input, nil, &list); err != nil || len(list) != 2 || list[0] != 2 || list[1] != 3 {
		t.Fatalf("stored input = %v, %v, want the argument list [2 3]", list, err)
	}
}

// Missing arguments leave their fields zero, and extra ones are ignored.
func TestArgsDecodeLikeJavaScriptArguments(t *testing.T) {
	decode := func(raw []byte, value any) error { return worlds.DecodePayload(raw, nil, value) }
	for _, test := range []struct {
		list []any
		want addArgs
	}{
		{[]any{7}, addArgs{A: 7}},
		{[]any{1, 2, 3, "extra"}, addArgs{A: 1, B: 2}},
		{[]any{}, addArgs{}},
	} {
		raw, err := worlds.EncodePayload(test.list, worlds.PayloadOptions{})
		if err != nil {
			t.Fatal(err)
		}
		got, err := decodeArguments[addArgs](raw, decode)
		if err != nil || got.A != test.want.A || got.B != test.want.B {
			t.Errorf("decode %v = %+v, %v, want A=%d B=%d", test.list, got, err, test.want.A, test.want.B)
		}
	}
	// An input without Args is the one argument.
	raw, _ := worlds.EncodePayload(arguments(structWorkflowArg{Topic: "go"}), worlds.PayloadOptions{})
	got, err := decodeArguments[structWorkflowArg](raw, decode)
	if err != nil || got.Topic != "go" {
		t.Fatalf("single argument = %+v, %v", got, err)
	}
	text, _ := devalue.Marshal(arguments(structWorkflowArg{Topic: "go"}))
	if string(text) != `[[1],{"topic":2},"go"]` {
		t.Fatalf("single argument list = %s", text)
	}
}

type structWorkflowArg struct {
	Topic string `json:"topic"`
}
