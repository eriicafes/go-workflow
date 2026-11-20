package vercelworld

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
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

	// specVersion is the Workflow SDK spec version of every run this World
	// creates: slot-numbered event IDs, a sealed log whose in-band writes are
	// fenced, hook token retention, and forced hook claims. Later versions
	// change the orchestration model, so runs stay on it.
	specVersion = 8

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
}

// apiError is a non-2xx answer of the workflow API.
type apiError struct {
	Status  int
	Code    string
	Message string
	// Fields holds the rest of a structured error body, such as the
	// counters of an in-band refusal.
	Fields map[string]any
}

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

// request sends one request to path, relative to the backend root, and
// returns the response for a 2xx status. Any other status is returned as an
// *apiError after reading its body.
func (b *backend) request(ctx context.Context, method, path string, query url.Values, body []byte, header http.Header) (*http.Response, error) {
	target := b.baseURL + path
	if len(query) > 0 {
		target += "?" + query.Encode()
	}
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	request, err := http.NewRequestWithContext(ctx, method, target, reader)
	if err != nil {
		return nil, err
	}
	for key, values := range header {
		request.Header[key] = values
	}
	request.Header.Set("User-Agent", b.userAgent)
	if err := b.authorize(ctx, request.Header); err != nil {
		return nil, err
	}
	response, err := b.http.Do(request)
	if err != nil {
		return nil, fmt.Errorf("workflow/vercel: %s %s: %w", method, path, err)
	}
	if response.StatusCode >= 200 && response.StatusCode < 300 {
		return response, nil
	}
	defer response.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	return nil, decodeAPIError(response.StatusCode, response.Header.Get("Content-Type"), data)
}

// authorize sets the credentials of a request: a Vercel access token and the
// project it acts in through the API proxy, or the deployment's OIDC token.
func (b *backend) authorize(ctx context.Context, header http.Header) error {
	if b.proxy {
		header.Set("Authorization", "Bearer "+b.token)
		header.Set("X-Vercel-Project-Id", b.project)
		if b.team != "" {
			header.Set("X-Vercel-Team-Id", b.team)
		}
		header.Set("X-Vercel-Environment", b.env)
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

// do sends a request whose body, when set, is CBOR, and decodes a CBOR answer
// into out when it is not nil.
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
	response, err := b.request(ctx, method, path, query, body, header)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	data, err := readAll(response.Body)
	if err != nil || out == nil {
		return err
	}
	return decodeBody(response.Header.Get("Content-Type"), data, out)
}

func readAll(body io.Reader) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(body, maxResponseBytes+1))
	if err != nil {
		return nil, fmt.Errorf("workflow/vercel: read backend response: %w", err)
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
		return fmt.Errorf("%w: %v", worlds.ErrInBandSuperseded, err)
	case api.Status == http.StatusNotFound && api.Code != "":
		switch eventType {
		case worlds.EventTypeHookReceived, worlds.EventTypeHookDisposed:
			return fmt.Errorf("%w: %v", worlds.ErrHookNotActive, err)
		}
		return fmt.Errorf("%w: %v", worlds.ErrRunNotFound, err)
	case api.Status == http.StatusConflict && api.Code == "hook-force-claimed":
		return fmt.Errorf("%w: %v", worlds.ErrHookForceClaimed, err)
	case api.Status == http.StatusConflict, api.Status == http.StatusGone:
		switch eventType {
		case worlds.EventTypeRunCreated:
			return fmt.Errorf("%w: %v", worlds.ErrRunAlreadyExists, err)
		case worlds.EventTypeStepCreated, worlds.EventTypeStepStarted, worlds.EventTypeStepCompleted, worlds.EventTypeStepFailed, worlds.EventTypeStepRetrying:
			if strings.Contains(api.Code, "run") {
				return fmt.Errorf("%w: %v", worlds.ErrRunTerminal, err)
			}
			return fmt.Errorf("%w: %v", worlds.ErrStepNotStartable, err)
		case worlds.EventTypeHookReceived, worlds.EventTypeHookDisposed:
			if strings.Contains(api.Code, "run") {
				return fmt.Errorf("%w: %v", worlds.ErrRunTerminal, err)
			}
			return fmt.Errorf("%w: %v", worlds.ErrHookNotActive, err)
		case worlds.EventTypeHookCreated:
			if strings.Contains(api.Code, "run") {
				return fmt.Errorf("%w: %v", worlds.ErrRunTerminal, err)
			}
			return fmt.Errorf("%w: %v", worlds.ErrHookConflict, err)
		}
		return fmt.Errorf("%w: %v", worlds.ErrRunTerminal, err)
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
