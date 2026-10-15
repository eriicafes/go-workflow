// Package vercelworld provides a World backed by Redis (TCP or the Upstash
// REST API), Vercel Queues for delivery, and optionally QStash for timers
// longer than Vercel Queues' seven-day delay limit.
package vercelworld

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/eriicafes/go-workflow/worlds"
)

const (
	defaultPrefix   = "go-workflow"
	qstashRoutePath = "/v1/workflow/qstash"
)

type Config struct {
	// BaseURL is the absolute public URL of HTTP(world), including
	// /v1/workflow. Webhooks created by this World use it as their URL base.
	BaseURL string
	// RedisURL selects the TCP transport (redis:// or rediss:// for TLS). When
	// empty, RedisRESTURL and RedisRESTToken select the Upstash REST transport.
	RedisURL       string
	RedisRESTURL   string
	RedisRESTToken string
	// QueueURL is the regional Vercel Queues API root including /api/v3, for
	// example https://iad1.vercel-queue.com/api/v3.
	QueueURL string
	// OIDCTokenSource returns the Vercel OIDC token for each Queues request.
	// When nil, the token comes from the x-vercel-oidc-token header of the
	// request being served by a handler of this package, then the file named
	// by VERCEL_OIDC_TOKEN_FILE, then VERCEL_OIDC_TOKEN, then OIDCToken.
	OIDCTokenSource func(context.Context) (string, error)
	// OIDCToken is a static fallback token.
	OIDCToken string
	// Topic is the Vercel Queue topic consumed by Queue(world, runners).
	Topic string
	// Prefix namespaces Redis keys. Empty uses go-workflow.
	Prefix string
	// QStash delivers timers longer than seven days. Without it such timers
	// are rejected.
	QStash *QStashConfig
	// HTTPClient sends Redis REST, Vercel Queues, and QStash requests. Nil uses
	// http.DefaultClient.
	HTTPClient *http.Client
}

// World stores the event log and its views in Redis. All mutations are
// single Lua scripts, so any number of instances may share one database.
type World struct {
	config Config
	http   *http.Client
	redis  redisStore
	prefix string
	// qstashURL is the public URL QStash delivers long timers to.
	qstashURL string
}

func New(config Config) (*World, error) {
	if config.BaseURL == "" || config.QueueURL == "" || config.Topic == "" {
		return nil, errors.New("workflow/vercel: base URL, queue URL, and topic are required")
	}
	baseURL, err := worlds.NormalizeBaseURL(config.BaseURL)
	if err != nil {
		return nil, fmt.Errorf("workflow/vercel: invalid base URL: %w", err)
	}
	config.BaseURL = baseURL
	config.QueueURL = strings.TrimRight(config.QueueURL, "/")
	if config.HTTPClient == nil {
		config.HTTPClient = http.DefaultClient
	}
	if config.Prefix == "" {
		config.Prefix = defaultPrefix
	}
	world := &World{config: config, http: config.HTTPClient, prefix: config.Prefix}
	if config.QStash != nil {
		qstash := *config.QStash
		if qstash.Token == "" || qstash.CurrentSigningKey == "" || qstash.NextSigningKey == "" {
			return nil, errors.New("workflow/vercel: QStash token and signing keys are required")
		}
		if qstash.BaseURL == "" {
			qstash.BaseURL = "https://qstash.upstash.io"
		}
		qstash.BaseURL = strings.TrimRight(qstash.BaseURL, "/")
		world.qstashURL, err = qstashDestination(qstash.WebhookEndpoint, baseURL)
		if err != nil {
			return nil, err
		}
		world.config.QStash = &qstash
	}
	world.redis, err = newRedisStore(config.RedisURL, config.RedisRESTURL, config.RedisRESTToken, config.HTTPClient)
	if err != nil {
		return nil, err
	}
	return world, nil
}

// Close releases the Redis connection pool.
func (w *World) Close() error { return w.redis.Close() }

func (w *World) BaseURL() string             { return w.config.BaseURL }
func (*World) SpecVersion() int              { return worlds.SpecVersion }
func (w *World) Analytics() worlds.Analytics { return analytics{world: w} }
func (*World) Capabilities() worlds.Capabilities {
	return worlds.Capabilities{Analytics: true, SetAttributes: true}
}

type oidcTokenKey struct{}

// oidcToken resolves the Vercel OIDC token at request time because Vercel
// rotates it.
func (w *World) oidcToken(ctx context.Context) (string, error) {
	if w.config.OIDCTokenSource != nil {
		return w.config.OIDCTokenSource(ctx)
	}
	if token, _ := ctx.Value(oidcTokenKey{}).(string); token != "" {
		return token, nil
	}
	if path := os.Getenv("VERCEL_OIDC_TOKEN_FILE"); path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return "", fmt.Errorf("workflow/vercel: read OIDC token file: %w", err)
		}
		if token := strings.TrimSpace(string(data)); token != "" {
			return token, nil
		}
		return "", errors.New("workflow/vercel: OIDC token file is empty")
	}
	if token := os.Getenv("VERCEL_OIDC_TOKEN"); token != "" {
		return token, nil
	}
	if w.config.OIDCToken != "" {
		return w.config.OIDCToken, nil
	}
	return "", errors.New("workflow/vercel: no Vercel OIDC token available")
}

// HTTP returns the RPC and webhook endpoints under /v1/workflow. When QStash
// is configured it also mounts the signed long-timer route at
// /v1/workflow/qstash.
func HTTP(world *World) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/", worlds.HTTPHandler(world, nil, ""))
	if world.config.QStash != nil {
		mux.Handle("POST "+qstashRoutePath, qstashHandler(world))
	}
	return withRequestToken(mux)
}

// RPC returns only the CBOR RPC endpoint.
func RPC(world *World) http.Handler { return withRequestToken(worlds.RPCHandler(world)) }

// Webhook returns only the token webhook endpoint.
func Webhook(world *World) http.Handler { return withRequestToken(worlds.WebhookHandler(world)) }

// Queue returns the Vercel Queues push consumer. Deploy it as a function with
// a queue/v2beta trigger for Config.Topic; Vercel does not expose such
// functions publicly, so deliveries are not authenticated here. The handler
// executes the stored queue record, acknowledges the message on success, and
// answers 500 on failure so Vercel redelivers it.
func Queue(world *World, runners worlds.RunnerSet) http.Handler {
	return withRequestToken(&consumer{world: world, runtime: worlds.NewRuntime(world, runners)})
}

// withRequestToken carries a request's x-vercel-oidc-token header to the
// default token source.
func withRequestToken(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if token := request.Header.Get("X-Vercel-Oidc-Token"); token != "" {
			request = request.WithContext(context.WithValue(request.Context(), oidcTokenKey{}, token))
		}
		next.ServeHTTP(writer, request)
	})
}
