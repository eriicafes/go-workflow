package vercelworld

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestNewRedisStoreUsesTCPURL(t *testing.T) {
	store, err := newRedisStore("rediss://default:password@redis.example:6379", "", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := store.(tcpRedis); !ok {
		t.Fatalf("store = %T, want tcpRedis", store)
	}
}

func TestNewUsesTopLevelHTTPClientForRedisREST(t *testing.T) {
	client := &http.Client{}
	world, err := New(Config{
		RedisRESTURL:   "https://redis.example",
		RedisRESTToken: "secret",
		QueueURL:       "https://queue.example/api/v3",
		OIDCToken:      "oidc",
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

func TestRESTRedisSendsRedisCommands(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if got := request.Header.Get("Authorization"); got != "Bearer secret" {
			t.Errorf("authorization = %q, want bearer token", got)
		}
		var command []any
		if err := json.NewDecoder(request.Body).Decode(&command); err != nil {
			t.Fatal(err)
		}
		if command[0] == "HGETALL" {
			_, _ = writer.Write([]byte(`{"result":{"status":"queued"}}`))
			return
		}
		if command[0] == "EVAL" {
			if len(command) != 5 || command[2] != float64(1) || command[3] != "task" {
				t.Errorf("eval command = %#v", command)
			}
			_, _ = writer.Write([]byte(`{"result":1}`))
			return
		}
		t.Errorf("unexpected command %#v", command)
		writer.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()

	store, err := newRedisStore("", server.URL, "secret", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := store.(restRedis); !ok {
		t.Fatalf("store = %T, want restRedis", store)
	}
	fields, err := store.HGetAll(context.Background(), "task")
	if err != nil {
		t.Fatal(err)
	}
	if fields["status"] != "queued" {
		t.Fatalf("status = %q, want queued", fields["status"])
	}
	result, err := store.Eval(context.Background(), "return 1", []string{"task"}, "value")
	if err != nil {
		t.Fatal(err)
	}
	if result != int64(1) {
		t.Fatalf("result = %#v, want int64(1)", result)
	}
}
