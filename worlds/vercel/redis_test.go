package vercelworld

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/eriicafes/go-workflow/worlds"
)

func TestNewRedisStoreTLSOnlyForRediss(t *testing.T) {
	store, err := newRedisStore("rediss://default:password@redis.example:6379", "", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	tcp, ok := store.(tcpRedis)
	if !ok {
		t.Fatalf("store = %T, want tcpRedis", store)
	}
	if config := tcp.client.Options().TLSConfig; config == nil || config.MinVersion < tls.VersionTLS12 {
		t.Fatalf("rediss TLS config = %#v", config)
	}
	store, err = newRedisStore("redis://localhost:6379", "", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	if config := store.(tcpRedis).client.Options().TLSConfig; config != nil {
		t.Fatalf("redis:// TLS config = %#v, want none", config)
	}
}

func TestNewUsesHTTPClientForRedisREST(t *testing.T) {
	client := &http.Client{}
	world, err := New(Config{
		BaseURL:        "https://workflows.example.com/v1/workflow",
		RedisRESTURL:   "https://redis.example",
		RedisRESTToken: "secret",
		QueueURL:       "https://iad1.vercel-queue.com/api/v3",
		Topic:          "workflow",
		HTTPClient:     client,
	})
	if err != nil {
		t.Fatal(err)
	}
	store, ok := world.redis.(restRedis)
	if !ok {
		t.Fatalf("store = %T, want restRedis", world.redis)
	}
	if store.http != client {
		t.Fatal("Redis REST store did not receive Config.HTTPClient")
	}
}

func TestNewValidatesConfig(t *testing.T) {
	valid := Config{BaseURL: "https://example.com/v1/workflow", RedisURL: "redis://localhost:6379", QueueURL: "https://iad1.vercel-queue.com/api/v3", Topic: "workflow"}
	for name, mutate := range map[string]func(*Config){
		"missing base URL":   func(c *Config) { c.BaseURL = "" },
		"relative base URL":  func(c *Config) { c.BaseURL = "/v1/workflow" },
		"missing queue URL":  func(c *Config) { c.QueueURL = "" },
		"missing topic":      func(c *Config) { c.Topic = "" },
		"missing redis":      func(c *Config) { c.RedisURL = "" },
		"QStash without key": func(c *Config) { c.QStash = &QStashConfig{Token: "t"} },
		"QStash over HTTP": func(c *Config) {
			c.BaseURL = "http://example.com/v1/workflow"
			c.QStash = &QStashConfig{Token: "t", CurrentSigningKey: "a", NextSigningKey: "b"}
		},
	} {
		config := valid
		mutate(&config)
		if _, err := New(config); err == nil {
			t.Errorf("%s: New succeeded", name)
		}
	}
	if _, err := New(valid); err != nil {
		t.Fatal(err)
	}
}

func TestRESTRedisCommands(t *testing.T) {
	store, commands := restStub(t, func(command []any) (int, string) {
		switch command[0] {
		case "GET":
			if command[1] == "missing" {
				return 200, `{"result":null}`
			}
			return 200, `{"result":"value"}`
		case "MGET", "HMGET":
			return 200, `{"result":["a",null]}`
		case "HGET":
			return 200, `{"result":"field"}`
		case "LINDEX":
			return 200, `{"result":null}`
		case "LRANGE", "SMEMBERS":
			return 200, `{"result":["x","y"]}`
		case "ZRANGEBYSCORE", "ZREVRANGEBYSCORE":
			return 200, `{"result":["m1","1760000000000001","m2","2.5"]}`
		case "EXISTS":
			return 200, `{"result":1}`
		}
		return 400, `{"error":"ERR unknown command"}`
	})
	ctx := context.Background()
	if value, ok, err := store.Get(ctx, "k"); err != nil || !ok || value != "value" {
		t.Fatalf("GET = %q %v %v", value, ok, err)
	}
	if _, ok, err := store.Get(ctx, "missing"); err != nil || ok {
		t.Fatalf("GET missing = %v %v", ok, err)
	}
	if values, err := store.MGet(ctx, "a", "b"); err != nil || !reflect.DeepEqual(values, []string{"a", ""}) {
		t.Fatalf("MGET = %q %v", values, err)
	}
	if values, err := store.HMGet(ctx, "h", "a", "b"); err != nil || !reflect.DeepEqual(values, []string{"a", ""}) {
		t.Fatalf("HMGET = %q %v", values, err)
	}
	if value, ok, err := store.HGet(ctx, "h", "f"); err != nil || !ok || value != "field" {
		t.Fatalf("HGET = %q %v %v", value, ok, err)
	}
	if _, ok, err := store.LIndex(ctx, "l", -1); err != nil || ok {
		t.Fatalf("LINDEX = %v %v", ok, err)
	}
	if values, err := store.LRange(ctx, "l", 0, -1); err != nil || len(values) != 2 {
		t.Fatalf("LRANGE = %q %v", values, err)
	}
	if values, err := store.SMembers(ctx, "s"); err != nil || len(values) != 2 {
		t.Fatalf("SMEMBERS = %q %v", values, err)
	}
	if exists, err := store.Exists(ctx, "k"); err != nil || !exists {
		t.Fatalf("EXISTS = %v %v", exists, err)
	}
	members, err := store.ZRangeByScore(ctx, "z", "(5", "+inf", false, 3)
	if err != nil || len(members) != 2 || members[0] != (redisZ{"m1", 1760000000000001}) || members[1].Score != 2.5 {
		t.Fatalf("ZRANGEBYSCORE = %#v %v", members, err)
	}
	if _, err := store.ZRangeByScore(ctx, "z", "-inf", "(9", true, -1); err != nil {
		t.Fatal(err)
	}
	want := [][]any{
		{"ZRANGEBYSCORE", "z", "(5", "+inf", "WITHSCORES", "LIMIT", "0", "3"},
		{"ZREVRANGEBYSCORE", "z", "(9", "-inf", "WITHSCORES"},
	}
	if got := (*commands)[len(*commands)-2:]; !reflect.DeepEqual(got, want) {
		t.Fatalf("sorted set commands = %#v", got)
	}
}

func TestRESTRedisEvalShaFallsBackOnNoScript(t *testing.T) {
	script := newRedisScript("return {'ok', 1, {'x', false}}")
	loaded := false
	store, commands := restStub(t, func(command []any) (int, string) {
		switch command[0] {
		case "EVALSHA":
			if !loaded {
				return 400, `{"error":"NOSCRIPT No matching script. Please use EVAL."}`
			}
			return 200, `{"result":["ok",1,["x",null]]}`
		case "EVAL":
			loaded = true
			return 200, `{"result":["ok",1,["x",null]]}`
		}
		return 400, `{"error":"ERR unexpected"}`
	})
	for range 2 {
		result, err := store.Run(context.Background(), script, "prefix", "arg")
		if err != nil {
			t.Fatal(err)
		}
		if want := []any{"ok", int64(1), []any{"x", nil}}; !reflect.DeepEqual(result, want) {
			t.Fatalf("result = %#v, want %#v", result, want)
		}
	}
	want := [][]any{
		{"EVALSHA", script.sha, "0", "prefix", "arg"},
		{"EVAL", script.src, "0", "prefix", "arg"},
		{"EVALSHA", script.sha, "0", "prefix", "arg"},
	}
	if !reflect.DeepEqual(*commands, want) {
		t.Fatalf("commands = %#v", *commands)
	}
}

func TestRESTRedisErrors(t *testing.T) {
	store, _ := restStub(t, func(command []any) (int, string) {
		if command[0] == "GET" {
			return 200, `{"error":"WRONGTYPE Operation against a key"}`
		}
		return 500, `upstream failure`
	})
	if _, _, err := store.Get(context.Background(), "k"); err == nil || !strings.Contains(err.Error(), "WRONGTYPE") {
		t.Fatalf("error reply = %v", err)
	}
	if _, err := store.SMembers(context.Background(), "k"); err == nil || !strings.Contains(err.Error(), "500") {
		t.Fatalf("HTTP failure = %v", err)
	}
	if _, err := store.Run(context.Background(), newRedisScript("return 1"), "p"); err == nil {
		t.Fatal("script failure was not returned")
	}
}

func TestRESTRedisResponseCap(t *testing.T) {
	previous := restResponseMaxBytes
	restResponseMaxBytes = 64
	t.Cleanup(func() { restResponseMaxBytes = previous })
	store, _ := restStub(t, func([]any) (int, string) {
		return 200, `{"result":"` + strings.Repeat("x", 100) + `"}`
	})
	if _, _, err := store.Get(context.Background(), "k"); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("oversized response error = %v", err)
	}
	restResponseMaxBytes = 1 << 10
	if value, _, err := store.Get(context.Background(), "k"); err != nil || len(value) != 100 {
		t.Fatalf("response under the cap = %d bytes, %v", len(value), err)
	}
}

func TestDecodeREST(t *testing.T) {
	value, err := decodeREST(json.RawMessage(`["ok",42,null,["a",-1]]`))
	if err != nil {
		t.Fatal(err)
	}
	if want := []any{"ok", int64(42), nil, []any{"a", int64(-1)}}; !reflect.DeepEqual(value, want) {
		t.Fatalf("decoded = %#v", value)
	}
	if _, err := decodeREST(json.RawMessage(`{"a":1}`)); err == nil {
		t.Fatal("object reply was accepted")
	}
	if _, err := decodeREST(json.RawMessage(`1.5`)); err == nil {
		t.Fatal("fractional integer was accepted")
	}
}

func TestScriptReply(t *testing.T) {
	values, err := scriptReply([]any{"ok", int64(2), "x"}, nil)
	if err != nil || !reflect.DeepEqual(values, []any{int64(2), "x"}) {
		t.Fatalf("ok reply = %#v %v", values, err)
	}
	for code, want := range map[string]error{
		"run_not_found":      worlds.ErrRunNotFound,
		"run_exists":         worlds.ErrRunAlreadyExists,
		"run_terminal":       worlds.ErrRunTerminal,
		"step_not_startable": worlds.ErrStepNotStartable,
		"hook_not_active":    worlds.ErrHookNotActive,
		"hook_conflict":      worlds.ErrHookConflict,
		"invalid":            worlds.ErrEventInvalid,
		"stream_locked":      worlds.ErrStreamLocked,
	} {
		if _, err := scriptReply([]any{"err", code}, nil); !errors.Is(err, want) {
			t.Errorf("%s = %v, want %v", code, err, want)
		}
	}
	if _, err := scriptReply("nope", nil); err == nil {
		t.Fatal("malformed reply accepted")
	}
}

func TestKeysAndCursors(t *testing.T) {
	world := &World{prefix: "p"}
	if got := world.key("run", "wrun_1"); got != "p:run:wrun_1" {
		t.Fatalf("run key = %q", got)
	}
	// A correlation ID containing ":" cannot collide with another run's key.
	if a, b := world.corrKey("a", "b:c"), world.corrKey("a:b", "c"); a == b {
		t.Fatalf("correlation keys collide: %q", a)
	}
	for _, test := range []struct {
		cursor   string
		desc     bool
		min, max string
	}{
		{"", false, "-inf", "+inf"},
		{"", true, "-inf", "+inf"},
		{"1760000000000001", false, "(1760000000000001", "+inf"},
		{"12", true, "-inf", "(12"},
	} {
		min, max, err := scoreRange(test.cursor, test.desc)
		if err != nil || min != test.min || max != test.max {
			t.Errorf("scoreRange(%q, %v) = %q %q %v", test.cursor, test.desc, min, max, err)
		}
	}
	if _, _, err := scoreRange("abc", false); !errors.Is(err, worlds.ErrEventInvalid) {
		t.Fatalf("invalid cursor error = %v", err)
	}
	members := []redisZ{{"a", 1}, {"b", 1760000000000001}, {"c", 3}}
	page, cursor, more := trimPage(members, 2)
	if len(page) != 2 || cursor != "1760000000000001" || !more {
		t.Fatalf("trimPage = %v %q %v", page, cursor, more)
	}
	if page, cursor, more := trimPage(members, 0); len(page) != 3 || cursor != "" || more {
		t.Fatalf("unlimited trimPage = %v %q %v", page, cursor, more)
	}
	count, negative := 3, -2
	for _, test := range []struct {
		options worlds.EventsCreateOptions
		want    string
	}{
		{worlds.EventsCreateOptions{}, "-1"},
		{worlds.EventsCreateOptions{EventCount: &count, SinceCursor: string(worlds.EventIDForSlot(1))}, "3"},
		{worlds.EventsCreateOptions{EventCount: &negative}, "0"},
		{worlds.EventsCreateOptions{SinceCursor: string(worlds.EventIDForSlot(7))}, "7"},
		{worlds.EventsCreateOptions{SinceCursor: "bogus"}, "-1"},
	} {
		if got := deltaStart(test.options); got != test.want {
			t.Errorf("deltaStart(%+v) = %s, want %s", test.options, got, test.want)
		}
	}
}

func TestDescribeEvent(t *testing.T) {
	data, _ := json.Marshal(worlds.DataHookCreated{Token: "tok", Metadata: []byte{0xff}, Webhook: &worlds.Webhook{Manual: true}, MinRetention: 1500})
	raw, err := describeEvent(worlds.EventsCreateEvent{EventType: worlds.EventTypeHookCreated, CorrelationID: "hook_1", Data: data}, parseMicros("1700000000000000"))
	if err != nil {
		t.Fatal(err)
	}
	var desc eventDescriptor
	if err := json.Unmarshal([]byte(raw), &desc); err != nil {
		t.Fatal(err)
	}
	if desc.Hook == nil || desc.Hook.Token != "tok" || desc.Hook.Metadata != "/w==" || !desc.Hook.Manual || desc.Hook.MinRetention != "1500" || desc.Hook.RetentionUs != "2" {
		t.Fatalf("hook descriptor = %+v", desc.Hook)
	}
	var record eventRecord
	if err := json.Unmarshal([]byte(desc.Record), &record); err != nil || record.At != "1700000000000000" || string(record.Data) != string(data) {
		t.Fatalf("record = %+v %v", record, err)
	}
	received, _ := json.Marshal(worlds.WebhookReceivedEventData{Response: &worlds.WebhookResponse{Status: 200}, ResponseTo: worlds.EventIDForSlot(4)})
	raw, _ = describeEvent(worlds.EventsCreateEvent{EventType: worlds.EventTypeHookReceived, Data: received}, parseMicros("1"))
	desc = eventDescriptor{}
	_ = json.Unmarshal([]byte(raw), &desc)
	if desc.Received == nil || desc.Received.Request || !desc.Received.Response || desc.Received.To != 4 {
		t.Fatalf("received descriptor = %+v", desc.Received)
	}
	raw, _ = describeEvent(worlds.EventsCreateEvent{EventType: worlds.EventTypeStepStarted, Data: []byte("not json")}, parseMicros("1"))
	desc = eventDescriptor{}
	_ = json.Unmarshal([]byte(raw), &desc)
	if desc.Result != nil {
		t.Fatalf("unparsable step data produced a result: %+v", desc.Result)
	}
	raw, _ = describeEvent(worlds.EventsCreateEvent{EventType: worlds.EventTypeRunCreated}, parseMicros("1"))
	if !strings.Contains(raw, `"bad":true`) {
		t.Fatalf("run_created descriptor = %s", raw)
	}
}

// restStub answers Upstash REST commands from a function and records them.
func restStub(t *testing.T, answer func(command []any) (int, string)) (restRedis, *[][]any) {
	t.Helper()
	var mu sync.Mutex
	var commands [][]any
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if got := request.Header.Get("Authorization"); got != "Bearer secret" {
			t.Errorf("authorization = %q", got)
		}
		if got := request.Header.Get("Content-Type"); got != "application/json" {
			t.Errorf("content type = %q", got)
		}
		var command []any
		if err := json.NewDecoder(request.Body).Decode(&command); err != nil {
			t.Error(err)
		}
		mu.Lock()
		commands = append(commands, command)
		mu.Unlock()
		status, body := answer(command)
		writer.WriteHeader(status)
		_, _ = writer.Write([]byte(body))
	}))
	t.Cleanup(server.Close)
	store, err := newRedisStore("", server.URL+"/", "secret", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	return store.(restRedis), &commands
}
