package worlds

import (
	"crypto/subtle"
	"net/http"
	"strings"
)

const defaultHTTPPrefix = "/v1/workflow"

// HTTPPrefixPath returns the path HTTPHandler serves under for prefix: a
// leading slash and no trailing slash, or /v1/workflow when prefix is empty.
func HTTPPrefixPath(prefix string) string {
	if prefix == "" {
		return defaultHTTPPrefix
	}
	return "/" + strings.Trim(prefix, "/")
}

// HTTPOption configures HTTPHandler.
type HTTPOption func(*httpOptions)

type httpOptions struct {
	client func(http.Handler) http.Handler
}

// WithClientToken makes the client endpoints, RPC and the stream socket,
// answer 401 unless a request carries token as a bearer token, such as
// WORKFLOW_HTTP_TOKEN, which world-http sends. An empty token refuses every
// client request, so an unset secret never opens them. Webhooks are unaffected: the hook token in their URL
// authorizes them, since their callers can't send your credentials.
func WithClientToken(token string) HTTPOption {
	want := []byte("Bearer " + token)
	return WithClientMiddleware(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if token == "" || subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), want) != 1 {
				w.Header().Set("WWW-Authenticate", "Bearer")
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			next.ServeHTTP(w, r)
		})
	})
}

// WithClientMiddleware wraps the client endpoints, RPC and the stream socket,
// in middleware, such as a session or IP check, and leaves webhooks reachable
// by their callers.
// Several wrap in order, the first outermost.
func WithClientMiddleware(middleware func(http.Handler) http.Handler) HTTPOption {
	return func(o *httpOptions) {
		if o.client == nil {
			o.client = middleware
			return
		}
		outer := o.client
		o.client = func(next http.Handler) http.Handler { return outer(middleware(next)) }
	}
}

// HTTPHandler returns the mux that exposes world to clients and
// webhook callers. It serves the client endpoints, RPCHandler at
// POST <prefix>/rpc and StreamSocketHandler at
// GET <prefix>/websockets/v1/runs/{runId}/streams/{name}, and WebhookHandler
// at <prefix>/webhooks/{token}. An empty prefix uses
// /v1/workflow. It does not execute deliveries. Worlds can register their own
// routes, such as delivery callbacks, on the returned mux.
//
// The client endpoints can read and change every run and are unauthenticated
// unless options protect them, with WithClientToken or WithClientMiddleware.
func HTTPHandler(world World, prefix string, options ...HTTPOption) *http.ServeMux {
	var o httpOptions
	for _, option := range options {
		option(&o)
	}
	rpc, socket := RPCHandler(world), StreamSocketHandler(world)
	if o.client != nil {
		rpc, socket = o.client(rpc), o.client(socket)
	}
	mux := http.NewServeMux()
	prefix = HTTPPrefixPath(prefix)
	mux.Handle(prefix+"/webhooks/{token}", WebhookHandler(world))
	mux.Handle("POST "+prefix+"/rpc", rpc)
	mux.Handle("GET "+prefix+streamSocketRoute, socket)
	return mux
}

// RPCHandler returns the RPC endpoint for world. It accepts POST requests
// with RPCContentType bodies holding an RPCRequest and does no path routing,
// so mount it at <prefix>/rpc to match the path HTTP clients use.
func RPCHandler(world World) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		handleRPC(w, r, world)
	})
}
