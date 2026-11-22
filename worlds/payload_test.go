package worlds

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/internal/envelope"
)

// sdkVectors are payloads the Workflow SDK encoded, written by
// testdata/payload/generate.mjs.
type sdkVectors struct {
	DeploymentKey []byte            `json:"deploymentKey"`
	ProjectID     string            `json:"projectId"`
	RunID         RunID             `json:"runId"`
	RunKey        []byte            `json:"runKey"`
	PublicKey     []byte            `json:"publicKey"`
	Values        map[string][]byte `json:"values"`
	Shapes        map[string][]byte `json:"shapes"`
	Encrypted     map[string][]byte `json:"encrypted"`
	Compressed    map[string][]byte `json:"compressed"`
	Stream        map[string][]byte `json:"stream"`
}

func loadSDKVectors(t *testing.T) sdkVectors {
	t.Helper()
	data, err := os.ReadFile("testdata/payload/vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var vectors sdkVectors
	if err := json.Unmarshal(data, &vectors); err != nil {
		t.Fatal(err)
	}
	return vectors
}

func TestPayloadKeysMatchTheSDK(t *testing.T) {
	vectors := loadSDKVectors(t)
	runKey, err := DeriveRunKey(vectors.DeploymentKey, vectors.ProjectID, vectors.RunID)
	if err != nil || !bytes.Equal(runKey, vectors.RunKey) {
		t.Fatalf("run key = %x err=%v, want %x", runKey, err, vectors.RunKey)
	}
	keys, err := NewRunKeys(runKey)
	if err != nil || !bytes.Equal(keys.PublicKey, vectors.PublicKey) {
		t.Fatalf("public key = %x err=%v, want %x", keys.PublicKey, err, vectors.PublicKey)
	}
	if got := envelope.DecodePublicKey(envelope.EncodePublicKey(keys.PublicKey)); !bytes.Equal(got, keys.PublicKey) {
		t.Fatalf("public key round trip = %x", got)
	}
}

func TestPayloadDecodesSDKValues(t *testing.T) {
	vectors := loadSDKVectors(t)
	decode := func(name string, target any) {
		t.Helper()
		if err := DecodePayload(vectors.Values[name], nil, target); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}

	var object struct {
		S      string
		N      float64
		I      int
		B      bool
		Z      *int
		Arr    []any
		Nested map[string]int
	}
	decode("object", &object)
	if object.S != "hi" || object.N != 1.5 || object.I != 42 || !object.B || object.Z != nil || !reflect.DeepEqual(object.Arr, []any{1.0, "a"}) || object.Nested["x"] != 1 {
		t.Fatalf("object = %+v", object)
	}
	var data, empty []byte
	decode("bytes", &data)
	decode("emptyBytes", &empty)
	if !bytes.Equal(data, []byte{1, 2, 3}) || empty == nil || len(empty) != 0 {
		t.Fatalf("bytes = %v %v", data, empty)
	}
	var date time.Time
	decode("date", &date)
	if !date.Equal(time.Date(2024, 1, 2, 3, 4, 5, 678e6, time.UTC)) {
		t.Fatalf("date = %v", date)
	}
	var number *big.Int
	decode("bigint", &number)
	if number.String() != "12345678901234567890" {
		t.Fatalf("bigint = %v", number)
	}
	var m map[int]string
	decode("map", &m)
	if !reflect.DeepEqual(m, map[int]string{1: "a", 2: "b"}) {
		t.Fatalf("map = %v", m)
	}
	var u *url.URL
	decode("url", &u)
	if u.String() != "https://example.com/a?b=c" {
		t.Fatalf("url = %v", u)
	}
	var header http.Header
	decode("headers", &header)
	if header.Get("Content-Type") != "text/plain" || header.Get("X-A") != "1" {
		t.Fatalf("headers = %v", header)
	}
	var specials []float64
	decode("specials", &specials)
	if !math.IsNaN(specials[0]) || !math.Signbit(specials[1]) || !math.IsInf(specials[2], 1) || !math.IsInf(specials[3], -1) || specials[4] != 1 {
		t.Fatalf("specials = %v", specials)
	}
	var top string
	decode("string", &top)
	var repeated []any
	decode("repeated", &repeated)
	var holes []*int
	decode("holes", &holes)
	if top != "top" || !reflect.DeepEqual(repeated, []any{"a", "a", map[string]any{"k": "a"}}) || len(holes) != 3 || holes[1] != nil || *holes[2] != 3 {
		t.Fatalf("top=%q repeated=%v holes=%v", top, repeated, holes)
	}
}

func TestPayloadDecodesSDKErrors(t *testing.T) {
	vectors := loadSDKVectors(t)
	var err error
	if decodeErr := DecodePayload(vectors.Values["error"], nil, &err); decodeErr != nil {
		t.Fatal(decodeErr)
	}
	if err.Error() != "outer" || errors.Unwrap(err) != nil && errors.Unwrap(err).Error() != "inner" {
		t.Fatalf("error = %v", err)
	}
	recorded := err.(*Error)
	if recorded.Type != "Error" || recorded.Cause == nil || recorded.Cause.Message != "inner" || recorded.Cause.Cause != nil {
		t.Fatalf("chain = %+v", recorded)
	}
	var fatal error
	if decodeErr := DecodePayload(vectors.Values["fatal"], nil, &fatal); decodeErr != nil {
		t.Fatal(decodeErr)
	}
	var target FatalError
	if !errors.As(fatal, &target) || fatal.Error() != "fatal" {
		t.Fatalf("fatal = %v", fatal)
	}
	var retryable error
	if decodeErr := DecodePayload(vectors.Values["retryable"], nil, &retryable); decodeErr != nil {
		t.Fatal(decodeErr)
	}
	var retry RetryableError
	if !errors.As(retryable, &retry) || retry.After < 24*time.Hour {
		t.Fatalf("retryable = %v after %v", retryable, retry.After)
	}
}

func TestPayloadDecodesSDKShapesAndEnvelopes(t *testing.T) {
	vectors := loadSDKVectors(t)
	keys, err := NewRunKeys(vectors.RunKey)
	if err != nil {
		t.Fatal(err)
	}
	var arguments []struct{ N int }
	if err := DecodePayload(vectors.Shapes["workflowArguments"], nil, &arguments); err != nil || len(arguments) != 1 || arguments[0].N != 1 {
		t.Fatalf("workflow arguments = %+v err=%v", arguments, err)
	}
	var step struct{ Args []struct{ N int } }
	if err := DecodePayload(vectors.Shapes["stepArguments"], nil, &step); err != nil || len(step.Args) != 1 || step.Args[0].N != 1 {
		t.Fatalf("step arguments = %+v err=%v", step, err)
	}
	for _, name := range []string{"encr", "encp"} {
		var value struct{ Secret string }
		if err := DecodePayload(vectors.Encrypted[name], keys, &value); err != nil || value.Secret != "x" {
			t.Fatalf("%s = %+v err=%v", name, value, err)
		}
		if err := DecodePayload(vectors.Encrypted[name], nil, &value); !errors.Is(err, ErrPayloadInvalid) {
			t.Fatalf("%s without a key: %v", name, err)
		}
	}
	for _, name := range []string{"zstd", "gzip"} {
		if envelope.Peek(vectors.Compressed[name]) != envelope.Prefix(name) {
			t.Fatalf("%s vector is %q", name, envelope.Peek(vectors.Compressed[name]))
		}
		text, err := envelope.Unwrap(vectors.Compressed[name], nil)
		plain, _ := envelope.Unwrap(vectors.Compressed["plain"], nil)
		if err != nil || !bytes.Equal(text, plain) {
			t.Fatalf("%s: err=%v", name, err)
		}
	}
	for name, frame := range vectors.Stream {
		chunks, err := DecodeStreamFrames(frame, keys)
		if err != nil || len(chunks) != 1 || string(chunks[0]) != "hi" {
			t.Fatalf("stream %s = %q err=%v", name, chunks, err)
		}
	}
}

type payloadRecord struct {
	Name     string            `json:"name"`
	Count    int               `json:"count,omitempty"`
	Data     []byte            `json:"data"`
	At       time.Time         `json:"at"`
	Big      *big.Int          `json:"big"`
	Link     *url.URL          `json:"link"`
	Header   http.Header       `json:"header"`
	Tags     map[string]string `json:"tags"`
	Scores   map[int]float64   `json:"scores"`
	Err      error             `json:"err"`
	Raw      json.RawMessage   `json:"raw"`
	Skip     string            `json:"-"`
	Embedded `json:"embedded"`
}

type Embedded struct{ Inner string }

func TestPayloadRoundTrips(t *testing.T) {
	keys, err := NewRunKeys(bytes.Repeat([]byte{7}, 32))
	if err != nil {
		t.Fatal(err)
	}
	link, _ := url.Parse("https://example.com/x")
	value := payloadRecord{
		Name: strings.Repeat("long ", 300), Data: []byte{0, 1}, At: time.Date(2024, 5, 6, 7, 8, 9, 0, time.UTC),
		Big: big.NewInt(-5), Link: link, Header: http.Header{"X-Test": {"a", "b"}},
		Tags: map[string]string{"b": "2", "a": "1"}, Scores: map[int]float64{2: 0.5, 1: 1},
		Err: FatalError{Cause: errors.New("nope")}, Raw: json.RawMessage(`{"k":[1,true]}`), Skip: "skipped",
		Embedded: Embedded{Inner: "in"},
	}
	for name, options := range map[string]PayloadOptions{
		"plain":  {},
		"gzip":   {},
		"encr":   {Keys: keys},
		"encp":   {SealTo: keys.PublicKey},
		"stored": {NoCompression: true},
	} {
		codec := ""
		if name == "gzip" {
			codec = "gzip"
		}
		t.Setenv("WORKFLOW_COMPRESSION_CODEC", codec)
		payload, err := EncodePayload(value, options)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		wantLabel := map[string]envelope.Prefix{"plain": envelope.PrefixZstd, "gzip": envelope.PrefixGzip, "encr": envelope.PrefixEncrypted, "encp": envelope.PrefixSealed, "stored": envelope.PrefixDevalue}[name]
		if envelope.Peek(payload) != wantLabel {
			t.Fatalf("%s label = %q", name, envelope.Peek(payload))
		}
		var got payloadRecord
		if err := DecodePayload(payload, keys, &got); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got.Name != value.Name || !bytes.Equal(got.Data, value.Data) || !got.At.Equal(value.At) || got.Big.Cmp(value.Big) != 0 ||
			got.Link.String() != link.String() || !reflect.DeepEqual(got.Header["X-Test"], []string{"a", "b"}) ||
			!reflect.DeepEqual(got.Tags, value.Tags) || !reflect.DeepEqual(got.Scores, value.Scores) || got.Skip != "" ||
			string(got.Raw) != `{"k":[1,true]}` || got.Inner != "in" {
			t.Fatalf("%s round trip = %+v", name, got)
		}
		var fatal FatalError
		if !errors.As(got.Err, &fatal) || got.Err.Error() != "nope" {
			t.Fatalf("%s error = %v", name, got.Err)
		}
	}
}

func TestPayloadStreamFrames(t *testing.T) {
	keys, _ := NewRunKeys(bytes.Repeat([]byte{9}, 32))
	sealer, err := envelope.NewSealer(keys.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	var chunk []byte
	for _, frame := range []struct {
		keys   *RunKeys
		sealer *envelope.Sealer
	}{{nil, nil}, {keys, nil}, {nil, sealer}} {
		encoded, err := EncodeStreamFrame([]byte("chunk"), frame.keys, frame.sealer)
		if err != nil {
			t.Fatal(err)
		}
		chunk = append(chunk, encoded...)
	}
	values, err := DecodeStreamFrames(chunk, keys)
	if err != nil || len(values) != 3 || string(values[2]) != "chunk" {
		t.Fatalf("frames = %q err=%v", values, err)
	}
}

// TestWriteGoPayloadVectors writes payloads Go encoded for
// testdata/payload/verify.mjs to decode with the Workflow SDK, when
// WORKFLOW_PAYLOAD_VECTORS_OUT names the file to write.
func TestWriteGoPayloadVectors(t *testing.T) {
	out := os.Getenv("WORKFLOW_PAYLOAD_VECTORS_OUT")
	if out == "" {
		t.Skip("set WORKFLOW_PAYLOAD_VECTORS_OUT to write Go payload vectors")
	}
	vectors := loadSDKVectors(t)
	keys, err := NewRunKeys(vectors.RunKey)
	if err != nil {
		t.Fatal(err)
	}
	link, _ := url.Parse("https://example.com/a?b=c")
	values := map[string]any{
		"object":     map[string]any{"s": "hi", "n": 1.5, "list": []any{1, "a"}},
		"bytes":      []byte{1, 2, 3},
		"date":       time.Date(2024, 1, 2, 3, 4, 5, 678e6, time.UTC),
		"bigint":     big.NewInt(42),
		"map":        map[int]string{1: "a"},
		"url":        link,
		"headers":    http.Header{"Content-Type": {"text/plain"}},
		"error":      errors.New("boom"),
		"fatal":      FatalError{Cause: errors.New("fatal")},
		"retryable":  RetryableError{Cause: errors.New("later"), After: time.Hour},
		"conflict":   &HookConflictError{Token: "tok", RunID: "wrun_1", Start: true},
		"claimed":    &HookForceClaimedError{Token: "tok", RunID: "wrun_2", HookID: "hook_2"},
		"notFound":   &HookNotFoundError{Token: "tok"},
		"expired":    &RunExpiredError{RunID: "wrun_3", RunStatus: StatusCompleted},
		"streamGone": &StreamExpiredError{RunID: "wrun_4", StreamID: "strm_1"},
		"large":      strings.Repeat("workflow ", 400),
	}
	result := map[string]map[string]string{"plain": {}, "encr": {}, "encp": {}, "stream": {}}
	for name, value := range values {
		if err, ok := value.(error); ok {
			// Encode as an error value, as the runtime does.
			value = struct{ E error }{err}
		}
		for mode, options := range map[string]PayloadOptions{"plain": {}, "encr": {Keys: keys}, "encp": {SealTo: keys.PublicKey}} {
			payload, err := EncodePayload(value, options)
			if err != nil {
				t.Fatal(err)
			}
			result[mode][name] = base64.StdEncoding.EncodeToString(payload)
		}
	}
	sealer, _ := envelope.NewSealer(keys.PublicKey)
	for mode, frame := range map[string]struct {
		keys   *RunKeys
		sealer *envelope.Sealer
	}{"plain": {}, "encr": {keys, nil}, "encp": {nil, sealer}} {
		encoded, err := EncodeStreamFrame([]byte("hi"), frame.keys, frame.sealer)
		if err != nil {
			t.Fatal(err)
		}
		result["stream"][mode] = base64.StdEncoding.EncodeToString(encoded)
	}
	data, _ := json.MarshalIndent(result, "", "\t")
	if err := os.WriteFile(out, data, 0o644); err != nil {
		t.Fatal(err)
	}
}
