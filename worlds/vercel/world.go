// Package vercelworld implements a World on Redis (over TCP or the Upstash REST
// API) and Vercel Queues, for deployments on Vercel Functions. QStash can
// optionally deliver work due more than seven days ahead.
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
	defaultPrefix     = "go-workflow"
	defaultHTTPPrefix = "/v1/workflow"
	// qstashRouteSuffix is the QStash route under the HTTP prefix, among the
	// callbacks a World mounts for its own use.
	qstashRouteSuffix = "/callbacks/qstash"
)

// Config configures the Vercel World.
type Config struct {
	// BaseURL is the absolute public URL of HTTP(world), including
	// /v1/workflow. Webhooks created by this World use it as their URL base.
	// It is required.
	BaseURL string
	// RedisURL selects the TCP transport with redis://, or rediss:// for TLS.
	// When it is empty, RedisRESTURL and RedisRESTToken select the Upstash
	// REST transport. One of the two transports is required.
	RedisURL       string
	RedisRESTURL   string
	RedisRESTToken string
	// QueueURL is the regional Vercel Queues API root including /api/v3, for
	// example https://iad1.vercel-queue.com/api/v3. It is required.
	QueueURL string
	// OIDCToken returns the Vercel OIDC token. It is called for every Vercel
	// Queues request because Vercel rotates the token. Nil uses
	// OIDCTokenFromEnv(DefaultOIDCTokenEnv).
	OIDCToken func(context.Context) (string, error)
	// Topic is the Vercel Queues topic that deliveries are published to and
	// that Queue(world, runners) consumes. It is required.
	Topic string
	// Prefix namespaces Redis keys. Empty uses go-workflow.
	Prefix string
	// QStash, when set, delivers work due more than seven days ahead. Without
	// it, queueing such a delivery returns an error.
	QStash *QStashConfig
	// HTTPClient sends Redis REST, Vercel Queues, and QStash requests. Nil uses
	// http.DefaultClient.
	HTTPClient *http.Client
	// DeploymentID identifies the deployment this World runs in. Runs created
	// through the World are pinned to it, deliveries are addressed to the
	// deployment of their run, and a delivery that reaches another deployment
	// is re-routed to the run's own. Empty uses the VERCEL_DEPLOYMENT_ID
	// environment variable, which Vercel sets in every deployment. With
	// neither, deployment affinity is off.
	DeploymentID string
}

// World is the Vercel World. It is safe for concurrent use, and any number of
// instances may share one Redis database.
type World struct {
	config Config
	http   *http.Client
	redis  redisStore
	prefix string
	// qstashURL is the public URL QStash delivers long timers to.
	qstashURL string
}

// New returns a World for config without contacting Redis or Vercel. It returns
// an error when BaseURL, QueueURL, Topic, or a Redis transport is missing, or
// when QStash lacks its token, its signing keys, or an HTTPS callback URL.
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
	if config.OIDCToken == nil {
		config.OIDCToken = OIDCTokenFromEnv(DefaultOIDCTokenEnv)
	}
	if config.DeploymentID == "" {
		config.DeploymentID = os.Getenv("VERCEL_DEPLOYMENT_ID")
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
		world.qstashURL, err = qstashDestination(baseURL)
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

// Close closes the Redis TCP connection pool. With the REST transport it does
// nothing.
func (w *World) Close() error { return w.redis.Close() }

func (w *World) BaseURL() string             { return w.config.BaseURL }
func (*World) SpecVersion() int              { return worlds.SpecVersion }
func (w *World) DeploymentID() string        { return w.config.DeploymentID }
func (w *World) Analytics() worlds.Analytics { return analytics{world: w} }
func (w *World) Capabilities() worlds.Capabilities {
	return worlds.Capabilities{Analytics: true, SetAttributes: true, DeploymentAffinity: w.config.DeploymentID != ""}
}

// DefaultOIDCTokenEnv is the environment variable Vercel sets to the
// deployment's OIDC token.
const DefaultOIDCTokenEnv = "VERCEL_OIDC_TOKEN"

// OIDCTokenFromEnv returns a Config.OIDCToken that reads the environment
// variable name on every call, so it picks up a rotated token. The returned
// function fails when the variable is unset or empty.
func OIDCTokenFromEnv(name string) func(context.Context) (string, error) {
	return func(context.Context) (string, error) {
		if token := os.Getenv(name); token != "" {
			return token, nil
		}
		return "", fmt.Errorf("workflow/vercel: %s is not set", name)
	}
}

// HTTP returns a handler serving the RPC and webhook endpoints under
// /v1/workflow. It does not authenticate requests. When Config.QStash is set,
// it also serves the QStash callback at /v1/workflow/callbacks/qstash, which
// accepts only requests signed by QStash.
func HTTP(world *World) http.Handler { return HTTPPrefix(world, "") }

// HTTPPrefix is like HTTP but serves the endpoints, and the QStash callback,
// under prefix, such as /internal/workflow. An empty prefix uses /v1/workflow.
// Config.BaseURL must end with the same prefix.
func HTTPPrefix(world *World, prefix string) http.Handler {
	if prefix == "" {
		prefix = defaultHTTPPrefix
	} else {
		prefix = "/" + strings.Trim(prefix, "/")
	}
	mux := http.NewServeMux()
	mux.Handle("/", worlds.HTTPHandler(world, prefix))
	if world.config.QStash != nil {
		mux.Handle("POST "+prefix+qstashRouteSuffix, QStash(world))
	}
	return mux
}

// RPC returns a handler serving only the RPC endpoint.
func RPC(world *World) http.Handler { return worlds.RPCHandler(world) }

// Webhook returns a handler serving only the webhook endpoint, authorized by
// the hook token in its URL.
func Webhook(world *World) http.Handler { return worlds.WebhookHandler(world) }

// Queue returns the Vercel Queues push consumer, which executes deliveries for
// the workflows of runners. It acknowledges a delivery that succeeds and
// answers 500 to one that fails, so Vercel redelivers it.
//
// Deploy it only as a function with a queue/v2beta trigger for Config.Topic.
// Vercel does not expose such functions publicly, so the handler does not
// authenticate requests.
func Queue(world *World, runners worlds.RunnerSet) http.Handler {
	return &consumer{world: world, runtime: worlds.NewRuntime(world, runners)}
}
