package vercelworld

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

// scriptCommand runs one Redis command, for tests to inspect keys.
var scriptCommand = newRedisScript(`return redis.call(unpack(ARGV))`)

// scriptKeyTTLs returns key and PTTL pairs for the keys matching ARGV[1].
var scriptKeyTTLs = newRedisScript(`
local out = {}
for _, key in ipairs(redis.call('KEYS', ARGV[1])) do
  table.insert(out, key)
  table.insert(out, redis.call('PTTL', key))
end
return out
`)

// scriptDeleteMatching deletes the keys matching ARGV[1], as Redis does
// when they expire.
var scriptDeleteMatching = newRedisScript(`
local keys = redis.call('KEYS', ARGV[1])
for _, key in ipairs(keys) do redis.call('DEL', key) end
return #keys
`)

func TestRetentionRedisTCP(t *testing.T)  { testRetention(t, redisTCPConfig()) }
func TestRetentionRedisREST(t *testing.T) { testRetention(t, redisRESTConfig()) }

func testRetention(t *testing.T, config Config) {
	t.Run("ExpiresTerminalRuns", func(t *testing.T) {
		config := config
		config.Retention = time.Hour
		worldList, _ := newWorlds(t, config, 1)
		world := worldList[0]
		prefix := world.prefix
		ctx := context.Background()

		done := retentionRun(t, world, "done")
		writeEvent(t, world, done, worlds.EventTypeHookCreated, "hook_done", worlds.DataHookCreated{Token: "token_done"})
		writeEvent(t, world, done, worlds.EventTypeWaitCreated, "wait_done", worlds.DataWaitCreated{ResumeAt: time.Now().Add(time.Hour)})
		if err := world.StreamsWrite(ctx, worlds.StreamsWriteInput{RunID: done, Name: "out", Chunk: []byte("chunk")}); err != nil {
			t.Fatal(err)
		}
		writeEvent(t, world, done, worlds.EventTypeRunCompleted, "", worlds.DataRunCompleted{Output: json.RawMessage(`"ok"`)})
		alive := retentionRun(t, world, "alive")

		ttls := keyTTLs(t, world, prefix+":*"+string(done)+"*")
		for _, kind := range []string{"run", "events", "last", "steps", "steporder", "runhooks", "streams", "corrs", "chunks", "corr"} {
			if !slices.ContainsFunc(keysOf(ttls), func(key string) bool { return strings.HasPrefix(key, prefix+":"+kind+":") }) {
				t.Errorf("no %s key for the finished run", kind)
			}
		}
		ttls[prefix+":hook:hook_done"] = command[int64](t, world, "PTTL", prefix+":hook:hook_done")
		for key, ttl := range ttls {
			if ttl <= 0 || ttl > time.Hour.Milliseconds() {
				t.Errorf("%s expires in %dms, want within the hour", key, ttl)
			}
		}
		for key, ttl := range keyTTLs(t, world, prefix+":*"+string(alive)+"*") {
			if ttl != -1 {
				t.Errorf("%s of a running run expires in %dms", key, ttl)
			}
		}
		if n := command[int64](t, world, "ZCARD", prefix+":expiring"); n != 1 {
			t.Fatalf("%d runs scheduled for removal, want 1", n)
		}

		// A late step result keeps the finished run's expiry.
		writeEvent(t, world, done, worlds.EventTypeStepCompleted, "step_late", worlds.DataStepResult{Result: json.RawMessage(`1`)})
		if ttl := command[int64](t, world, "PTTL", prefix+":run:"+string(done)); ttl <= 0 {
			t.Fatalf("run key expires in %dms after a late write", ttl)
		}
	})

	t.Run("RetainsByWorkflow", func(t *testing.T) {
		config := config
		config.Retention = time.Hour
		config.Retentions = map[string]time.Duration{"short": time.Minute, "kept": 0}
		worldList, _ := newWorlds(t, config, 1)
		world := worldList[0]
		prefix := world.prefix

		within := map[string]time.Duration{"short": time.Minute, "kept": 0, "other": time.Hour}
		for workflow, retention := range within {
			runID := retentionRunOf(t, world, workflow, workflow)
			writeEvent(t, world, runID, worlds.EventTypeRunCompleted, "", worlds.DataRunCompleted{Output: json.RawMessage(`"ok"`)})
			ttl := command[int64](t, world, "PTTL", prefix+":run:"+string(runID))
			switch {
			case retention == 0 && ttl != -1:
				t.Errorf("%s run expires in %dms, want kept", workflow, ttl)
			case retention > 0 && (ttl <= 0 || ttl > retention.Milliseconds()):
				t.Errorf("%s run expires in %dms, want within %v", workflow, ttl, retention)
			}
		}
	})

	t.Run("RemovesExpiredRunsFromIndexes", func(t *testing.T) {
		config := config
		config.Retention = time.Millisecond
		worldList, _ := newWorlds(t, config, 1)
		world := worldList[0]
		prefix := world.prefix
		ctx := context.Background()

		done := retentionRun(t, world, "swept")
		writeEvent(t, world, done, worlds.EventTypeHookCreated, "hook_swept", worlds.DataHookCreated{Token: "token_swept"})
		writeEvent(t, world, done, worlds.EventTypeRunCompleted, "", worlds.DataRunCompleted{Output: json.RawMessage(`"ok"`)})
		time.Sleep(20 * time.Millisecond)
		// Redis has expired the run's keys; delete them for Redis servers
		// whose clock the test cannot advance.
		command[int64](t, world, "DEL", prefix+":hook:hook_swept")
		if _, err := world.redis.Run(ctx, scriptDeleteMatching, prefix+":*"+string(done)+"*"); err != nil {
			t.Fatal(err)
		}

		next := retentionRun(t, world, "next")
		page, err := world.RunsList(ctx, worlds.RunsListInput{})
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Data) != 1 || page.Data[0].RunID != next {
			t.Fatalf("RunsList = %v, want only %s", page.Data, next)
		}
		for _, index := range []string{":runs", ":runs:wf:retention", ":runs:status:completed", ":runs:wfstatus:completed:retention"} {
			if score := command[any](t, world, "ZSCORE", prefix+index, string(done)); score != nil {
				t.Errorf("%s still lists the expired run", index)
			}
		}
		if score := command[any](t, world, "ZSCORE", prefix+":hooks", "hook_swept"); score != nil {
			t.Error("the hook index still lists the expired run's hook")
		}
		if n := command[int64](t, world, "ZCARD", prefix+":expiring"); n != 0 {
			t.Fatalf("%d runs still scheduled for removal", n)
		}
	})
}

// retentionRun creates a started run of the retention workflow with a
// completed step, and a step still running.
func retentionRun(t *testing.T, world *World, name string) worlds.RunID {
	t.Helper()
	return retentionRunOf(t, world, name, "retention")
}

func retentionRunOf(t *testing.T, world *World, name, workflow string) worlds.RunID {
	t.Helper()
	runID := worlds.RunID("wrun_retention_" + name + "_" + time.Now().Format("150405.000000000"))
	if _, err := world.EventsCreateRun(context.Background(), worlds.EventsCreateRunInput{RunID: runID, WorkflowName: workflow, Input: []byte(`"input"`)}); err != nil {
		t.Fatal(err)
	}
	writeEvent(t, world, runID, worlds.EventTypeRunStarted, "", nil)
	writeEvent(t, world, runID, worlds.EventTypeStepCreated, "step_"+name, worlds.DataStepCreated{StepName: "step", Input: json.RawMessage(`1`)})
	writeEvent(t, world, runID, worlds.EventTypeStepStarted, "step_"+name, worlds.DataStepStarted{})
	writeEvent(t, world, runID, worlds.EventTypeStepCompleted, "step_"+name, worlds.DataStepResult{Result: json.RawMessage(`2`)})
	writeEvent(t, world, runID, worlds.EventTypeStepCreated, "step_late", worlds.DataStepCreated{StepName: "late", Input: json.RawMessage(`1`)})
	writeEvent(t, world, runID, worlds.EventTypeStepStarted, "step_late", worlds.DataStepStarted{})
	return runID
}

func writeEvent(t *testing.T, world *World, runID worlds.RunID, eventType worlds.EventType, correlationID string, data any) {
	t.Helper()
	event := worlds.EventsCreateEvent{EventType: eventType, CorrelationID: correlationID}
	if data != nil {
		encoded, err := json.Marshal(data)
		if err != nil {
			t.Fatal(err)
		}
		event.Data = encoded
	}
	if _, err := world.EventsCreate(context.Background(), worlds.EventsCreateInput{RunID: runID, Event: event}); err != nil {
		t.Fatalf("EventsCreate(%s %s): %v", eventType, correlationID, err)
	}
}

func command[T any](t *testing.T, world *World, args ...string) T {
	t.Helper()
	result, err := world.redis.Run(context.Background(), scriptCommand, args...)
	if err != nil {
		t.Fatalf("%v: %v", args, err)
	}
	value, _ := result.(T)
	return value
}

func keyTTLs(t *testing.T, world *World, pattern string) map[string]int64 {
	t.Helper()
	result, err := world.redis.Run(context.Background(), scriptKeyTTLs, pattern)
	if err != nil {
		t.Fatal(err)
	}
	items, _ := result.([]any)
	ttls := map[string]int64{}
	for i := 0; i+1 < len(items); i += 2 {
		ttl, _ := items[i+1].(int64)
		ttls[replyString(items[i])] = ttl
	}
	return ttls
}

func keysOf(ttls map[string]int64) []string {
	keys := make([]string, 0, len(ttls))
	for key := range ttls {
		keys = append(keys, key)
	}
	return keys
}
