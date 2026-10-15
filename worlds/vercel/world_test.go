package vercelworld

import (
	"cmp"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/eriicafes/go-workflow/workflowtest"
	"github.com/eriicafes/go-workflow/worlds"
)

// scriptDeletePrefix removes every key under ARGV[1] after a test.
var scriptDeletePrefix = newRedisScript(`
local cursor = '0'
repeat
  local reply = redis.call('SCAN', cursor, 'MATCH', ARGV[1] .. ':*', 'COUNT', 1000)
  cursor = reply[1]
  for _, key in ipairs(reply[2]) do redis.call('DEL', key) end
until cursor == '0'
return 0
`)

// fakeVQS accepts publishes like the Vercel Queues API.
type fakeVQS struct {
	mu       sync.Mutex
	requests []*http.Request
	bodies   [][]byte
	status   int
}

func (f *fakeVQS) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	body, _ := io.ReadAll(request.Body)
	f.mu.Lock()
	f.requests = append(f.requests, request)
	f.bodies = append(f.bodies, body)
	status := f.status
	f.mu.Unlock()
	if status == 0 {
		status = http.StatusCreated
	}
	writer.WriteHeader(status)
	_, _ = writer.Write([]byte(`{"messageId":"msg_test"}`))
}

func (f *fakeVQS) published() [][]byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([][]byte(nil), f.bodies...)
}

func TestConformanceRedisTCP(t *testing.T) {
	runConformance(t, redisTCPConfig())
}

func TestConformanceRedisREST(t *testing.T) {
	runConformance(t, redisRESTConfig())
}

// newWorldPair returns count Worlds sharing a unique prefix on the backend
// described by config, and removes the prefix's keys after the test.
func newWorlds(t *testing.T, config Config, count int) ([]*World, *fakeVQS) {
	t.Helper()
	queue := &fakeVQS{}
	server := httptest.NewServer(queue)
	t.Cleanup(server.Close)
	config.BaseURL = "http://localhost/v1/workflow"
	config.QueueURL = server.URL + "/api/v3"
	config.Topic = "workflow"
	config.OIDCToken = staticOIDCToken("test-oidc")
	config.Prefix = "gwtest:" + ulid.Make().String()
	result := make([]*World, count)
	for index := range result {
		world, err := New(config)
		if err != nil {
			t.Fatal(err)
		}
		result[index] = world
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := result[0].redis.Exists(ctx, config.Prefix); err != nil {
		t.Fatalf("Redis is not reachable (%s%s): %v", config.RedisURL, config.RedisRESTURL, err)
	}
	t.Cleanup(func() {
		if _, err := result[0].redis.Run(context.Background(), scriptDeletePrefix, config.Prefix); err != nil {
			t.Errorf("clean up Redis keys: %v", err)
		}
		for _, world := range result {
			_ = world.Close()
		}
	})
	return result, queue
}

func runConformance(t *testing.T, config Config) {
	t.Run("Conformance", func(t *testing.T) {
		workflowtest.Conformance(t, func(t *testing.T) worlds.World {
			pair, _ := newWorlds(t, config, 1)
			return pair[0]
		})
	})
	t.Run("ConformanceShared", func(t *testing.T) {
		workflowtest.ConformanceShared(t, func(t *testing.T) (worlds.World, worlds.World) {
			pair, _ := newWorlds(t, config, 2)
			return pair[0], pair[1]
		})
	})
}

// redisTCPConfig targets the Redis at REDIS_URL, defaulting to
// redis://localhost:6379.
func redisTCPConfig() Config {
	return Config{RedisURL: cmp.Or(os.Getenv("REDIS_URL"), "redis://localhost:6379")}
}

// redisRESTConfig targets the Upstash-compatible REST endpoint at
// UPSTASH_REDIS_REST_URL, defaulting to a local serverless-redis-http at
// http://localhost:8079 with token "local".
func redisRESTConfig() Config {
	return Config{
		RedisRESTURL:   cmp.Or(os.Getenv("UPSTASH_REDIS_REST_URL"), "http://localhost:8079"),
		RedisRESTToken: cmp.Or(os.Getenv("UPSTASH_REDIS_REST_TOKEN"), "local"),
	}
}
