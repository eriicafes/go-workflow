package vercelworld

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/fxamacker/cbor/v2"

	"github.com/eriicafes/go-workflow/worlds"
)

const (
	// DefaultBackendURL is the root of Vercel's workflow API that a
	// deployment reaches with its OIDC token.
	DefaultBackendURL = "https://vercel-workflow.com/api"
	// DefaultProxyURL is the root of Vercel's API proxy for the workflow
	// API, which accepts a Vercel access token from outside a deployment.
	DefaultProxyURL = "https://api.vercel.com/v1/workflow"

	// specVersion is the spec version of every run this World creates,
	// worlds.SpecVersion: one orchestrator per run, whose in-band writes the
	// backend fences, over slot-numbered event IDs in a sealed log. The
	// backend serves such runs only on its /v5 event routes.
	specVersion = worlds.SpecVersion

	// eventsRoute is the version of the run-scoped event routes.
	eventsRoute = "/v5"

	cborContentType  = "application/cbor"
	frameContentType = "application/vnd.workflow.v4-frames"

	// maxResponseBytes caps a buffered backend response.
	maxResponseBytes = 256 << 20
)

var (
	cborEncode cbor.EncMode
	cborDecode cbor.DecMode
)

func init() {
	var err error
	// Times are tag 1 epoch seconds, which the backend reads as dates.
	cborEncode, err = cbor.EncOptions{Time: cbor.TimeUnixMicro, TimeTag: cbor.EncTagRequired, Sort: cbor.SortNone}.EncMode()
	if err != nil {
		panic(err)
	}
	cborDecode, err = cbor.DecOptions{DefaultMapType: reflect.TypeOf(map[string]any(nil)), MaxArrayElements: 1 << 24, MaxMapPairs: 1 << 24, MaxNestedLevels: 64}.DecMode()
	if err != nil {
		panic(err)
	}
}

// backend sends requests to Vercel's workflow API.
type backend struct {
	baseURL   string
	proxy     bool
	token     string
	oidc      func(context.Context) (string, error)
	http      *http.Client
	project   string
	team      string
	env       string
	userAgent string
	// forwardTo is the backend the API proxy is asked to forward to, from
	// VERCEL_WORKFLOW_SERVER_URL, or "" for its default.
	forwardTo string
}

// apiError is a non-2xx answer of the workflow API.
type apiError struct {
	Status  int
	Code    string
	Message string
	// Fields holds the rest of a structured error body, such as the
	// counters of an in-band refusal.
	Fields map[string]any
	// RetryAfter is the wait the answer's Retry-After header asks for, zero
	// for none.
	RetryAfter time.Duration
}

// HTTPStatus is the status of the answer, which tells a server error, worth
// another attempt, from a refusal.
func (e *apiError) HTTPStatus() int { return e.Status }

func (e *apiError) Error() string {
	message := e.Message
	if message == "" {
		message = http.StatusText(e.Status)
	}
	if e.Code != "" {
		return fmt.Sprintf("workflow/vercel: backend %d %s: %s", e.Status, e.Code, message)
	}
	return fmt.Sprintf("workflow/vercel: backend %d: %s", e.Status, message)
}

func statusOf(err error) int {
	var api *apiError
	if errors.As(err, &api) {
		return api.Status
	}
	return 0
}

func codeOf(err error) string {
	var api *apiError
	if errors.As(err, &api) {
		return api.Code
	}
	return ""
}

// withoutRedirects returns a copy of client that does not follow redirects, so
// a request's credentials, the trusted OIDC token header among them, never
// reach the host a redirect names. A 3xx is returned as an *apiError.
func withoutRedirects(client *http.Client) *http.Client {
	copied := *client
	copied.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &copied
}

const (
	// maxRequestRetries is how often an event write is sent again after a
	// transient failure, each after requestRetryBase doubled per retry plus
	// up to requestRetryJitter. The waits ride out a brief failure, and a
	// longer one is left to the queue's redelivery. A read whose answer could
	// not be read or decoded is sent again as often, without the jitter.
	maxRequestRetries  = 2
	requestRetryBase   = 100 * time.Millisecond
	requestRetryJitter = 50 * time.Millisecond

	// maxReadRetries is how often a GET is sent again after a failed
	// connection or a 500, 502, 503 or 504: each after the answer's
	// Retry-After, or readRetryBase doubled per retry, both capped at
	// readRetryMax.
	maxReadRetries = 5
	readRetryMax   = 30 * time.Second

	// throttleBudget is how long one request waits in all for the backend's
	// rate limit to pass. A write of a step's outcome whose context has a
	// deadline waits up to throttleMaxBudget instead, since losing it costs
	// the step's work. No wait runs into the last throttleDeadlineReserve
	// before the context's deadline.
	throttleBudget          = 30 * time.Second
	throttleDeadlineReserve = 15 * time.Second
	throttleMaxBudget       = 15 * time.Minute

	// startHookRunPendingBudget is how long an event write waits, from its
	// first attempt, for a start hook's run to be stored, which the backend
	// reports as a 503 start-hook-run-pending.
	startHookRunPendingBudget = 30 * time.Second

	// minRequestTimeout and maxRequestTimeout bound WORKFLOW_REQUEST_TIMEOUT_MS.
	minRequestTimeout = 10 * time.Second
	maxRequestTimeout = 2 * time.Minute
)

var (
	// defaultRequestTimeout is how long an entity request may take in all,
	// its retries and the reading of its answer included, unless
	// WORKFLOW_REQUEST_TIMEOUT_MS sets another.
	defaultRequestTimeout = 60 * time.Second
	// eventsPhaseTimeout is how long an event request waits for the answer's
	// headers, and for each further part of its body. It bounds silence and
	// not the whole request, since a large event log takes a while to read.
	eventsPhaseTimeout = 60 * time.Second
	// streamPhaseTimeout is the same for a stream request.
	streamPhaseTimeout = 5 * time.Minute

	// readRetryBase is the wait before a GET's first retry.
	readRetryBase = 500 * time.Millisecond
	// throttleMinWait is the shortest wait for a rate limit to pass, and the
	// wait when the answer names none.
	throttleMinWait = time.Second
)

// requestKind selects the deadlines and retries of a backend request.
type requestKind int

const (
	// entityRequest reads or writes runs, steps, hooks, analytics, and a
	// stream's chunks and info. It has requestTimeout in all.
	entityRequest requestKind = iota
	// eventsRequest reads or writes events, within eventsPhaseTimeout per
	// phase.
	eventsRequest
	// streamRequest writes, closes, or lists streams, within
	// streamPhaseTimeout per phase. It is never sent again here.
	streamRequest
)

// errRequestTimeout is the cause of a request cancelled by its own deadline.
var errRequestTimeout = errors.New("workflow/vercel: backend request timed out")

// requestTimeout is the deadline of an entity request:
// WORKFLOW_REQUEST_TIMEOUT_MS when it is set, within 10 seconds to 2 minutes,
// and defaultRequestTimeout otherwise.
func requestTimeout() time.Duration {
	value, err := strconv.Atoi(strings.TrimSpace(os.Getenv("WORKFLOW_REQUEST_TIMEOUT_MS")))
	if err != nil {
		return defaultRequestTimeout
	}
	return min(max(time.Duration(value)*time.Millisecond, minRequestTimeout), maxRequestTimeout)
}

// transientError is a failure that says nothing about whether the backend
// handled the request: it never answered, its answer could not be read, or
// Vercel's firewall answered in its place. timeout marks a request that ran
// past its own deadline.
type transientError struct {
	err     error
	timeout bool
}

func (e *transientError) Error() string { return e.err.Error() }
func (e *transientError) Unwrap() error { return e.err }

// Timeout reports whether the request ran past its own deadline.
func (e *transientError) Timeout() bool { return e.timeout }

// transient reports whether err is a failure worth sending an event write
// again for: no readable answer, or a 5xx. A cancelled ctx is never one.
func transient(ctx context.Context, err error) bool {
	if ctx.Err() != nil {
		return false
	}
	if _, ok := errors.AsType[*transientError](err); ok {
		return true
	}
	status := statusOf(err)
	return status >= 500 && status <= 599
}

// retriesRead reports whether a GET that failed with err is sent again: the
// connection failed before an answer, or the answer is a 500, 502, 503 or
// 504. A request past its own deadline is not.
func retriesRead(ctx context.Context, err error) bool {
	if ctx.Err() != nil {
		return false
	}
	switch statusOf(err) {
	case http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	case 0:
		failed, ok := errors.AsType[*transientError](err)
		return ok && !failed.timeout
	}
	return false
}

// sleep waits for wait, or returns ctx's error when it ends first.
func sleep(ctx context.Context, wait time.Duration) error {
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func retryBackoff(retry int) time.Duration {
	return requestRetryBase<<retry + rand.N(requestRetryJitter)
}

// throttleWaiter accounts the waits of one request for the backend's rate
// limit to pass. A rate limit is a definite refusal, so the request is sent
// again after each wait.
type throttleWaiter struct {
	// stepOutcome gives the waits the budget of a step's outcome.
	stepOutcome bool
	waited      time.Duration
}

// wait waits out one rate limit that asks for after, and reports whether the
// request is to be sent again: false once the waits would pass the budget or
// run into the reserve before ctx's deadline, and when ctx ends first.
func (t *throttleWaiter) wait(ctx context.Context, after time.Duration) bool {
	wait := max(after, throttleMinWait)
	budget := throttleBudget
	if _, ok := ctx.Deadline(); ok && t.stepOutcome {
		budget = throttleMaxBudget
	}
	if t.waited+wait > budget || outlasts(ctx, wait) {
		return false
	}
	t.waited += wait
	return sleep(ctx, wait) == nil
}

// outlasts reports whether a wait would run into the reserve kept before
// ctx's deadline for sending the request again and the work after it.
func outlasts(ctx context.Context, wait time.Duration) bool {
	deadline, ok := ctx.Deadline()
	return ok && time.Until(deadline)-wait < throttleDeadlineReserve
}

// retryEventWrite runs an event write, and runs it again when the backend
// did not take it. A rate limit and a 503 start-hook-run-pending are
// definite refusals, waited out within their budgets for every event. A
// transient failure may have landed the write, so it is sent again only when
// eligible says a repeat of a landed write is refused as a conflict rather
// than recorded twice. stepOutcome gives the rate limit wait the budget of a
// step's outcome.
func retryEventWrite[T any](ctx context.Context, eligible, stepOutcome bool, write func() (T, error)) (T, error) {
	started := time.Now()
	throttle := throttleWaiter{stepOutcome: stepOutcome}
	for retry := 0; ; {
		result, err := write()
		if err == nil {
			return result, nil
		}
		if throttled, ok := errors.AsType[*worlds.ThrottleError](err); ok {
			if !throttle.wait(ctx, throttled.After) {
				return result, err
			}
			continue
		}
		if api, ok := errors.AsType[*apiError](err); ok && api.Status == http.StatusServiceUnavailable && api.Code == "start-hook-run-pending" {
			wait := max(api.RetryAfter, throttleMinWait)
			if time.Since(started)+wait > startHookRunPendingBudget || outlasts(ctx, wait) || sleep(ctx, wait) != nil {
				return result, err
			}
			continue
		}
		if !eligible || retry >= maxRequestRetries || !transient(ctx, err) {
			return result, err
		}
		wait := retryBackoff(retry)
		retry++
		if sleep(ctx, wait) != nil {
			return result, err
		}
	}
}

// recordsStepOutcome reports whether a write of eventType records the outcome
// of a step function that ran, which gets the long rate limit budget: losing
// it runs the function again. A step_completed and a step_retrying always
// do. A step_failed does when its writer says so, since one is also written
// for a step that never ran.
func recordsStepOutcome(eventType worlds.EventType, options worlds.EventsCreateOptions) bool {
	switch eventType {
	case worlds.EventTypeStepCompleted, worlds.EventTypeStepRetrying:
		return true
	case worlds.EventTypeStepFailed:
		return options.AfterStepBody
	}
	return false
}

// retriesTransient reports whether a write of eventType may be sent again
// after a transient failure. The backend refuses a repeat of the others as a
// conflict, but it counts a second step_started as another attempt, records a
// second step_retrying, and delivers a hook_received twice unless it carries
// a resume ID. An in-band write is never sent again: the fence decides it.
func retriesTransient(eventType worlds.EventType, inBand, resume bool) bool {
	if inBand {
		return false
	}
	switch eventType {
	case worlds.EventTypeStepStarted, worlds.EventTypeStepRetrying:
		return false
	case worlds.EventTypeHookReceived:
		return resume
	}
	return true
}

// request sends a request to path, relative to the backend root, and returns
// the response for a 2xx status. Any other status is returned as an
// *apiError after reading its body. A GET other than a stream's is sent again
// up to maxReadRetries times while retriesRead says so. An entity request has
// requestTimeout for all of it, the reading of the answer included.
func (b *backend) request(ctx context.Context, kind requestKind, method, path string, query url.Values, body []byte, header http.Header) (*http.Response, error) {
	if kind != entityRequest {
		return b.attempts(ctx, kind, method, path, query, body, header)
	}
	limit := requestTimeout()
	requestCtx, cancel := context.WithCancelCause(ctx)
	timer := time.AfterFunc(limit, func() { cancel(errRequestTimeout) })
	response, err := b.attempts(requestCtx, kind, method, path, query, body, header)
	if err != nil {
		timer.Stop()
		if context.Cause(requestCtx) == errRequestTimeout && ctx.Err() == nil {
			err = &transientError{err: fmt.Errorf("workflow/vercel: %s %s timed out after %s: %w", method, path, limit, err), timeout: true}
		}
		cancel(nil)
		return nil, err
	}
	response.Body = &deadlineBody{ReadCloser: response.Body, ctx: requestCtx, cancel: cancel, timer: timer}
	return response, nil
}

func (b *backend) attempts(ctx context.Context, kind requestKind, method, path string, query url.Values, body []byte, header http.Header) (*http.Response, error) {
	for retry := 0; ; retry++ {
		response, err := b.send(ctx, kind, method, path, query, body, header)
		if err == nil || method != http.MethodGet || kind == streamRequest || retry >= maxReadRetries || !retriesRead(ctx, err) {
			return response, err
		}
		wait := min(readRetryBase<<retry, readRetryMax)
		if api, ok := errors.AsType[*apiError](err); ok && api.RetryAfter > 0 {
			wait = min(api.RetryAfter, readRetryMax)
		}
		if sleep(ctx, wait) != nil {
			return nil, err
		}
	}
}

// deadlineBody is a response body under its request's deadline. With idle
// set, the deadline is on each read; otherwise timer is the deadline of the
// whole request. Closing it releases the deadline.
type deadlineBody struct {
	io.ReadCloser
	ctx    context.Context
	cancel context.CancelCauseFunc
	timer  *time.Timer
	idle   time.Duration
}

func (body *deadlineBody) Read(data []byte) (int, error) {
	if body.idle > 0 {
		body.timer.Reset(body.idle)
	}
	n, err := body.ReadCloser.Read(data)
	if body.idle > 0 {
		body.timer.Stop()
	}
	if err != nil && err != io.EOF && context.Cause(body.ctx) == errRequestTimeout {
		err = &transientError{err: fmt.Errorf("%w: %w", errRequestTimeout, err), timeout: true}
	}
	return n, err
}

func (body *deadlineBody) Close() error {
	body.timer.Stop()
	err := body.ReadCloser.Close()
	body.cancel(nil)
	return err
}

// send sends one request. An event or stream request waits its phase timeout
// for the answer's headers, and then for each read of the body.
func (b *backend) send(ctx context.Context, kind requestKind, method, path string, query url.Values, body []byte, header http.Header) (*http.Response, error) {
	target := b.baseURL + path
	if len(query) > 0 {
		target += "?" + query.Encode()
	}
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	phase := time.Duration(0)
	switch kind {
	case eventsRequest:
		phase = eventsPhaseTimeout
	case streamRequest:
		phase = streamPhaseTimeout
	}
	requestCtx, cancel := context.WithCancelCause(ctx)
	request, err := http.NewRequestWithContext(requestCtx, method, target, reader)
	if err != nil {
		cancel(nil)
		return nil, err
	}
	for key, values := range header {
		request.Header[key] = values
	}
	request.Header.Set("User-Agent", b.userAgent)
	if err := b.authorize(ctx, request.Header); err != nil {
		cancel(nil)
		return nil, err
	}
	// An entity request's deadline is its caller's, and the timer is idle.
	timer := time.NewTimer(0)
	timer.Stop()
	if phase > 0 {
		timer = time.AfterFunc(phase, func() { cancel(errRequestTimeout) })
	}
	response, err := b.http.Do(request)
	timer.Stop()
	if err != nil {
		failed := &transientError{err: fmt.Errorf("workflow/vercel: %s %s: %w", method, path, err)}
		if context.Cause(requestCtx) == errRequestTimeout && ctx.Err() == nil {
			failed = &transientError{err: fmt.Errorf("workflow/vercel: %s %s sent no answer within %s: %w", method, path, phase, err), timeout: true}
		}
		cancel(nil)
		return nil, failed
	}
	response.Body = &deadlineBody{ReadCloser: response.Body, ctx: requestCtx, cancel: cancel, timer: timer, idle: phase}
	if response.StatusCode >= 200 && response.StatusCode < 300 {
		return response, nil
	}
	defer response.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	api := decodeAPIError(response.StatusCode, response.Header.Get("Content-Type"), data)
	api.RetryAfter = retryAfter(response.Header.Get("Retry-After"))
	err = api
	if response.StatusCode == http.StatusTooManyRequests {
		// A 429 is the backend's rate limit, unless Vercel's firewall answered
		// it with a challenge, which this client cannot solve: a failure of
		// the transport, never waited out as a rate limit.
		if response.Header.Get("X-Vercel-Mitigated") == "challenge" {
			return nil, &transientError{err: err}
		}
		return nil, &worlds.ThrottleError{Cause: err, After: api.RetryAfter}
	}
	// A 425 is a step started before its retry is due, returned as a
	// TooEarlyError with the Retry-After wait.
	if response.StatusCode == http.StatusTooEarly {
		return nil, &worlds.TooEarlyError{RetryAfter: api.RetryAfter}
	}
	return nil, err
}

// retryAfter returns the wait a Retry-After header asks for, in seconds or
// as an HTTP date, or zero when it has none.
func retryAfter(value string) time.Duration {
	if seconds, err := strconv.Atoi(strings.TrimSpace(value)); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	if at, err := http.ParseTime(value); err == nil {
		return max(time.Until(at), 0)
	}
	return 0
}

// authorize sets the credentials of a request: a Vercel access token and the
// project it acts in through the API proxy, or the deployment's OIDC token.
func (b *backend) authorize(ctx context.Context, header http.Header) error {
	if b.proxy {
		header.Set("Authorization", "Bearer "+b.token)
		b.setProject(header)
		return nil
	}
	token, err := b.oidc(ctx)
	if err != nil {
		return err
	}
	header.Set("Authorization", "Bearer "+token)
	header.Set("X-Vercel-Trusted-Oidc-Idp-Token", token)
	return nil
}

// setProject sets the headers that tell the API proxy which project,
// team, and environment a request acts in, and which backend to forward it to
// when one is set.
func (b *backend) setProject(header http.Header) {
	header.Set("X-Vercel-Project-Id", b.project)
	if b.team != "" {
		header.Set("X-Vercel-Team-Id", b.team)
	}
	header.Set("X-Vercel-Environment", b.env)
	if b.forwardTo != "" {
		header.Set("X-Vercel-Workflow-Api-Url", b.forwardTo)
	}
}

// do sends an entity request whose body, when set, is CBOR, and decodes a
// CBOR answer into out when it is not nil. A GET whose answer could not be
// read or decoded is sent again, up to maxRequestRetries times.
func (b *backend) do(ctx context.Context, method, path string, query url.Values, in, out any) error {
	header := http.Header{}
	header.Set("Accept", cborContentType)
	var body []byte
	if in != nil {
		var err error
		if body, err = cborEncode.Marshal(in); err != nil {
			return err
		}
		header.Set("Content-Type", cborContentType)
	}
	for retry := 0; ; retry++ {
		response, err := b.request(ctx, entityRequest, method, path, query, body, header)
		if err != nil {
			return err
		}
		data, err := readAll(response.Body)
		_ = response.Body.Close()
		if err == nil {
			if out == nil {
				return nil
			}
			err = decodeBody(response.Header.Get("Content-Type"), data, out)
		}
		timedOut := false
		if failed, ok := errors.AsType[*transientError](err); ok {
			timedOut = failed.timeout
		}
		if err == nil || method != http.MethodGet || retry >= maxRequestRetries || timedOut || ctx.Err() != nil {
			return err
		}
		if sleep(ctx, requestRetryBase<<retry) != nil {
			return err
		}
	}
}

func readAll(body io.Reader) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(body, maxResponseBytes+1))
	if err != nil {
		if _, ok := errors.AsType[*transientError](err); ok {
			return nil, err
		}
		return nil, &transientError{err: fmt.Errorf("workflow/vercel: read backend response: %w", err)}
	}
	if len(data) > maxResponseBytes {
		return nil, errors.New("workflow/vercel: backend response is too large")
	}
	return data, nil
}

// decodeBody decodes a CBOR or JSON body into out.
func decodeBody(contentType string, data []byte, out any) error {
	if strings.Contains(contentType, "json") {
		if err := json.Unmarshal(data, out); err != nil {
			return fmt.Errorf("workflow/vercel: decode backend response: %w", err)
		}
		return nil
	}
	if err := cborDecode.Unmarshal(data, out); err != nil {
		return fmt.Errorf("workflow/vercel: decode backend response: %w", err)
	}
	return nil
}

func decodeAPIError(status int, contentType string, data []byte) *apiError {
	result := &apiError{Status: status}
	var fields map[string]any
	if strings.Contains(contentType, "cbor") {
		_ = cborDecode.Unmarshal(data, &fields)
	} else if json.Unmarshal(data, &fields) != nil {
		fields = nil
	}
	if fields == nil {
		result.Message = strings.TrimSpace(string(data))
		return result
	}
	result.Fields = fields
	result.Message, _ = fields["message"].(string)
	if code, ok := fields["code"].(string); ok {
		result.Code = code
	} else if code, ok := fields["error"].(string); ok {
		result.Code = code
	}
	return result
}

// wireTime is a timestamp the backend sends as a CBOR date, a string, or epoch
// milliseconds.
type wireTime struct{ time.Time }

func (t *wireTime) UnmarshalCBOR(data []byte) error {
	var value any
	if err := cborDecode.Unmarshal(data, &value); err != nil {
		return err
	}
	parsed, ok := asTime(value)
	if !ok && value != nil {
		return fmt.Errorf("workflow/vercel: invalid time %v", value)
	}
	t.Time = parsed
	return nil
}

func (t *wireTime) UnmarshalJSON(data []byte) error {
	var value any
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	parsed, ok := asTime(value)
	if !ok && value != nil {
		return fmt.Errorf("workflow/vercel: invalid time %v", value)
	}
	t.Time = parsed
	return nil
}

func (t *wireTime) ptr() *time.Time {
	if t == nil || t.IsZero() {
		return nil
	}
	value := t.Time
	return &value
}

// asTime reads a decoded CBOR or JSON value as a time.
func asTime(value any) (time.Time, bool) {
	switch value := value.(type) {
	case time.Time:
		// A CBOR date is float seconds, so round away its float error.
		return value.Round(time.Microsecond).UTC(), true
	case string:
		parsed, err := time.Parse(time.RFC3339Nano, value)
		if err != nil {
			// Analytics answers carry timezone-naive UTC times, such as
			// 2026-07-13 17:09:11.593, which are read as UTC.
			parsed, err = time.Parse("2006-01-02 15:04:05.999999999", strings.Replace(value, "T", " ", 1))
		}
		return parsed.UTC(), err == nil
	case float64:
		return time.UnixMicro(int64(value * 1000)).UTC(), true
	case int64:
		return time.UnixMilli(value).UTC(), true
	case uint64:
		return time.UnixMilli(int64(value)).UTC(), true
	}
	return time.Time{}, false
}

// classify maps a workflow API refusal of an event write to the World
// contract's errors.
func classify(err error, eventType worlds.EventType) error {
	var api *apiError
	if !errors.As(err, &api) {
		return err
	}
	switch {
	case api.Status == http.StatusPreconditionFailed && api.Code == "in-band-superseded":
		// The refusal reports the run's counters as the backend read them.
		superseded := &worlds.InBandSupersededError{Cause: err}
		superseded.EventCount, _ = asInt(api.Fields["seq"])
		superseded.InBandCount, _ = asInt(api.Fields["seqInBand"])
		return superseded
	case api.Status == http.StatusNotFound && api.Code != "":
		switch eventType {
		case worlds.EventTypeHookReceived, worlds.EventTypeHookDisposed:
			return fmt.Errorf("%w: %v", &worlds.HookNotFoundError{}, err)
		case worlds.EventTypeWaitCompleted:
			// A wait that was never created, which the backend refuses.
			if api.Code != "run-not-found" {
				return fmt.Errorf("%w: %v", worlds.ErrEventNotFound, err)
			}
		case worlds.EventTypeStepStarted, worlds.EventTypeStepCompleted, worlds.EventTypeStepFailed, worlds.EventTypeStepRetrying:
			// A step that does not exist.
			if api.Code != "run-not-found" {
				return fmt.Errorf("%w: %v", worlds.ErrStepNotFound, err)
			}
		}
		return fmt.Errorf("%w: %v", worlds.ErrRunNotFound, err)
	case api.Status == http.StatusConflict && api.Code == "start-hook-conflict":
		// Another run owns a start hook's token. The backend names the token
		// and that run at the top level of the refusal.
		conflict := &worlds.HookConflictError{Start: true}
		conflict.Token, _ = api.Fields["token"].(string)
		id, _ := api.Fields["conflictingRunId"].(string)
		conflict.RunID = worlds.RunID(id)
		return conflict
	case api.Status == http.StatusBadRequest && api.Code == "start-hook-admission-rejected":
		// The candidate run is too old to admit a start hook.
		return fmt.Errorf("%w: %v", worlds.ErrStartHookRejected, err)
	case api.Status == http.StatusBadRequest && api.Code == "start-hook-disabled":
		// The backend has start hook admission switched off and recorded
		// nothing for the candidate. That is no verdict on the candidate, which
		// may be admitted once admission is back on.
		return fmt.Errorf("%w: %w", worlds.ErrStartHookDisabled, err)
	case api.Status == http.StatusConflict && api.Code == "start-hook-candidate-cancelled":
		// A cancellation reached a start hook candidate that has no run, and
		// the backend fenced it: no copy of the candidate can create the run.
		// The cancellation is refused as one of a finished run is, and a
		// creation as a candidate that is never admitted.
		if eventType == worlds.EventTypeRunCancelled {
			return fmt.Errorf("%w: %w: %v", worlds.ErrEntityConflict, &worlds.RunExpiredError{}, err)
		}
		return fmt.Errorf("%w: %v", worlds.ErrStartHookRejected, err)
	case api.Status == http.StatusConflict && api.Code == "hook-force-claimed":
		// The backend names the hook that took the token as claimedBy, at the
		// top level of the refusal.
		claimed := &worlds.HookForceClaimedError{}
		if by, ok := api.Fields["claimedBy"].(map[string]any); ok {
			runID, _ := by["runId"].(string)
			hookID, _ := by["hookId"].(string)
			claimed.RunID, claimed.HookID = worlds.RunID(runID), worlds.HookID(hookID)
		}
		return fmt.Errorf("%w: %v", claimed, err)
	case api.Status == http.StatusConflict && (api.Code == "run-already-started" || api.Code == "entity-conflict"):
		// A repeat of an event the log holds, returned as a conflict: a
		// run_started of a running run, or a second creation.
		return fmt.Errorf("%w: %v", worlds.ErrEntityConflict, err)
	case api.Status == http.StatusGone:
		// A 410 is a RunExpiredError: the run finished.
		return fmt.Errorf("%w: %v", &worlds.RunExpiredError{}, err)
	case api.Status == http.StatusConflict:
		if strings.Contains(api.Code, "run") {
			if eventType == worlds.EventTypeRunCreated {
				return fmt.Errorf("%w: %v", worlds.ErrRunAlreadyExists, err)
			}
			// A transition from, or a creation on, a finished run: a 409,
			// returned as a conflict that is also a RunExpiredError, where a
			// finished run's other refusals are a 410's RunExpiredError.
			return fmt.Errorf("%w: %w: %v", worlds.ErrEntityConflict, &worlds.RunExpiredError{}, err)
		}
		switch eventType {
		case worlds.EventTypeRunCreated:
			return fmt.Errorf("%w: %v", worlds.ErrRunAlreadyExists, err)
		case worlds.EventTypeRunCompleted, worlds.EventTypeRunFailed, worlds.EventTypeRunCancelled, worlds.EventTypeStepCreated, worlds.EventTypeWaitCreated, worlds.EventTypeWaitCompleted, worlds.EventTypeAttrSet, worlds.EventTypeHookCreated:
			// The backend refuses a second creation, returned as a
			// conflict.
			return fmt.Errorf("%w: %v", worlds.ErrEntityConflict, err)
		case worlds.EventTypeStepStarted, worlds.EventTypeStepCompleted, worlds.EventTypeStepFailed, worlds.EventTypeStepRetrying:
			// A step that finished, returned as a conflict.
			return fmt.Errorf("%w: %v", worlds.ErrEntityConflict, err)
		case worlds.EventTypeHookReceived, worlds.EventTypeHookDisposed:
			if strings.Contains(api.Code, "run") {
				return fmt.Errorf("%w: %v", &worlds.RunExpiredError{}, err)
			}
			return fmt.Errorf("%w: %v", worlds.ErrHookNotActive, err)
		}
		return fmt.Errorf("%w: %v", &worlds.RunExpiredError{}, err)
	case api.Status == http.StatusBadRequest && api.Code == "validation-error" && (eventType == worlds.EventTypeRunCreated || eventType == worlds.EventTypeRunStarted || eventType == worlds.EventTypeAttrSet):
		// The backend refuses a run's or an attr_set's attributes.
		return fmt.Errorf("%w: %w: %v", worlds.ErrAttributeInvalid, worlds.ErrEventInvalid, err)
	case api.Status == http.StatusBadRequest || api.Status == http.StatusUnprocessableEntity || api.Status == http.StatusRequestEntityTooLarge:
		return fmt.Errorf("%w: %v", worlds.ErrEventInvalid, err)
	}
	return err
}

// notFound maps a 404 of a read to want.
func notFound(err error, want error) error {
	if statusOf(err) == http.StatusNotFound {
		return fmt.Errorf("%w: %v", want, err)
	}
	return err
}

func pathEscape(value string) string { return url.PathEscape(value) }

func itoa(value int) string { return strconv.Itoa(value) }
