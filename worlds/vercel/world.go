// Package vercelworld implements a World on Vercel's managed workflow
// storage and Vercel Queues, for deployments on Vercel Functions. Runs, steps,
// hooks, and streams are stored by Vercel's workflow backend, the one the
// Workflow SDK uses, scoped to the deployment's project and environment.
// Deliveries go through Vercel Queues, in hops of up to 23 hours for work due
// later.
package vercelworld

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

// deploymentRouteSuffix is the route under the HTTP prefix that answers with
// the deployment serving it.
const deploymentRouteSuffix = "/deployment"

// Config configures the Vercel World.
type Config struct {
	// BaseURL is the absolute public URL of HTTP(world), including
	// /v1/workflow. Webhooks created by this World use it as their URL base.
	// It is required.
	BaseURL string
	// QueueURL is the regional Vercel Queues API root including /api/v3.
	// Empty uses https://<region>.vercel-queue.com/api/v3 for the region in
	// VERCEL_REGION, or iad1.
	QueueURL string
	// Topic is the Vercel Queues topic that deliveries of unrouted workflows
	// are published to. It is required.
	Topic string
	// Topics names further Vercel Queues topics by queue name, for Routes.
	Topics map[string]string
	// Routes publishes the deliveries of each workflow named by workflow
	// name, as worlds.Routes builds it, to the topic of its queue in Topics.
	// Queue(world, runners) consumes any topic its function is triggered by,
	// so each topic can have its own trigger and concurrency.
	Routes map[string]string
	// OIDCToken returns the deployment's Vercel OIDC token. It is called for
	// every Vercel Queues request, and every workflow backend request without
	// Token, because Vercel rotates the token. Nil uses
	// OIDCTokenFromEnv(DefaultOIDCTokenEnv).
	OIDCToken func(context.Context) (string, error)
	// Token is a Vercel access token for reaching the workflow backend from
	// outside a deployment, such as a script or a CLI. With it, requests go
	// through Vercel's API proxy for ProjectID, TeamID, and Environment
	// (default production), and the OIDC token is used only for Vercel
	// Queues. Inside a deployment leave it empty.
	Token       string
	ProjectID   string
	TeamID      string
	Environment string
	// BackendURL is the root of the workflow API. Empty uses
	// DefaultBackendURL, or DefaultProxyURL with Token.
	BackendURL string
	// HTTPClient sends workflow backend and Vercel Queues requests. Nil uses
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
// instances may share one project's storage.
type World struct {
	config Config
	http   *http.Client
	api    *backend
	// latestURL is the deployment route LatestDeploymentID asks for the
	// latest deployment, or "" when it reports none. latest guards latestID,
	// the deployment it last read, and latestAt, when it read it.
	latestURL string
	latest    sync.Mutex
	latestID  string
	latestAt  time.Time
	// cursors remembers where stream reads stopped, so the next read of a
	// stream continues from the backend's cursor instead of its start.
	cursors streamCursors
	// socketDeclinedUntil is when stream writers next try the backend's
	// stream socket after it declined one.
	socketMutex         sync.Mutex
	socketDeclinedUntil time.Time
	// streamsOverHTTP keeps stream writes on HTTP, as
	// WORKFLOW_STREAMS_TRANSPORT=http does for the Workflow SDK.
	streamsOverHTTP bool
}

// New returns a World for config without contacting Vercel. It returns an
// error when BaseURL or Topic is missing, when a route names a queue without
// a topic, or when Token is set without ProjectID.
func New(config Config) (*World, error) {
	if config.BaseURL == "" || config.Topic == "" {
		return nil, errors.New("workflow/vercel: base URL and topic are required")
	}
	for workflow, queue := range config.Routes {
		if config.Topics[queue] == "" {
			return nil, fmt.Errorf("workflow/vercel: workflow %s is routed to queue %q, which has no topic", workflow, queue)
		}
	}
	baseURL, err := worlds.NormalizeBaseURL(config.BaseURL)
	if err != nil {
		return nil, fmt.Errorf("workflow/vercel: invalid base URL: %w", err)
	}
	config.BaseURL = baseURL
	if config.Token != "" && config.ProjectID == "" {
		return nil, errors.New("workflow/vercel: a token needs a project ID")
	}
	if config.QueueURL == "" {
		region := os.Getenv("VERCEL_REGION")
		if region == "" || region == "dev1" {
			region = "iad1"
		}
		config.QueueURL = "https://" + region + ".vercel-queue.com/api/v3"
	}
	config.QueueURL = strings.TrimRight(config.QueueURL, "/")
	if config.HTTPClient == nil {
		config.HTTPClient = http.DefaultClient
	}
	if config.OIDCToken == nil {
		config.OIDCToken = OIDCTokenFromEnv(DefaultOIDCTokenEnv)
	}
	if config.DeploymentID == "" {
		config.DeploymentID = os.Getenv("VERCEL_DEPLOYMENT_ID")
	}
	if config.Environment == "" {
		config.Environment = "production"
	}
	if config.BackendURL == "" {
		config.BackendURL = DefaultBackendURL
		if config.Token != "" {
			config.BackendURL = DefaultProxyURL
		}
	}
	userAgent := "go-workflow/vercelworld"
	if config.DeploymentID != "" {
		userAgent += " " + config.DeploymentID
	}
	world := &World{
		config: config,
		http:   config.HTTPClient,
		api: &backend{
			baseURL:   strings.TrimRight(config.BackendURL, "/"),
			proxy:     config.Token != "",
			token:     config.Token,
			oidc:      config.OIDCToken,
			http:      config.HTTPClient,
			project:   config.ProjectID,
			team:      config.TeamID,
			env:       config.Environment,
			userAgent: userAgent,
		},
		streamsOverHTTP: os.Getenv("WORKFLOW_STREAMS_TRANSPORT") == "http",
	}
	if config.DeploymentID != "" && os.Getenv("VERCEL_ENV") == "production" {
		world.latestURL = baseURL + deploymentRouteSuffix
	}
	return world, nil
}

func (w *World) BaseURL() string           { return w.config.BaseURL }
func (*World) SpecVersion() int            { return worlds.SpecVersion }
func (w *World) DeploymentID() string      { return w.config.DeploymentID }
func (*World) Analytics() worlds.Analytics { return nil }
func (w *World) Capabilities() worlds.Capabilities {
	return worlds.Capabilities{SetAttributes: true, DeploymentAffinity: w.config.DeploymentID != ""}
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

// HTTP returns a handler serving the client and webhook endpoints under
// /v1/workflow. Its client endpoints are unauthenticated unless options
// protect them, such as worlds.WithClientToken. It also serves
// GET /v1/workflow/deployment, which answers with the deployment serving it,
// so a World in an older production deployment can find the latest one.
func HTTP(world *World, options ...worlds.HTTPOption) http.Handler {
	return HTTPPrefix(world, "", options...)
}

// HTTPPrefix is like HTTP but serves the endpoints and the deployment route
// under prefix, such as /internal/workflow. An empty prefix uses
// /v1/workflow. Config.BaseURL must end with the same prefix.
func HTTPPrefix(world *World, prefix string, options ...worlds.HTTPOption) http.Handler {
	mux := worlds.HTTPHandler(world, prefix, options...)
	mux.Handle("GET "+worlds.HTTPPrefixPath(prefix)+deploymentRouteSuffix, Deployment(world))
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

// Deployment returns a handler serving only the deployment route, which
// answers with the ID of the deployment serving it. Mount it at
// GET <Config.BaseURL>/deployment.
func Deployment(world *World) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "text/plain; charset=utf-8")
		writer.Header().Set("Cache-Control", "no-store")
		_, _ = io.WriteString(writer, world.config.DeploymentID)
	})
}
