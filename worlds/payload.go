package worlds

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"encoding"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/zstd"
)

// Payloads are encoded as the Workflow SDK encodes them, so SDK tooling reads
// a Go run's data as its own. A payload starts with a 4-byte label, and labels
// nest: devalue text (devl), optionally compressed (zstd or gzip), optionally
// encrypted with the run's key (encr) or sealed to the run's public key
// (encp). Values map onto devalue's JavaScript types: []byte is a Uint8Array,
// time.Time a Date, *big.Int a BigInt, *url.URL a URL, http.Header Headers,
// a map with keys other than strings a Map, and an error an Error.

// Payload labels.
const (
	PayloadDevalue   = "devl"
	PayloadEncrypted = "encr"
	PayloadSealed    = "encp"
	PayloadGzip      = "gzip"
	PayloadZstd      = "zstd"
)

const (
	payloadLabelSize = 4
	// payloadCompressMin is the smallest payload worth compressing.
	payloadCompressMin = 1024
	// payloadCompressSavings is the share of its size a compressed payload
	// must save to be kept.
	payloadCompressSavings = 0.05
	payloadNonceSize       = 12
	payloadKeySize         = 32
	// payloadMaxFrame bounds a stream frame.
	payloadMaxFrame = 100_000_000

	payloadScalarInfo     = "workflow/encp/x25519/v1"
	payloadContentKeyInfo = "workflow/encp/aes256gcm/v1"
)

// ErrPayloadInvalid reports a payload that cannot be decoded.
var ErrPayloadInvalid = errors.New("workflow: invalid payload")

// PayloadMarshaler is implemented by a type that chooses its own payload
// encoding: MarshalPayload returns the value to encode in its place.
type PayloadMarshaler interface {
	MarshalPayload() (any, error)
}

// PayloadUnmarshaler is implemented by a type that decodes its own payload:
// UnmarshalPayload calls decode with a pointer to decode the payload into.
type PayloadUnmarshaler interface {
	UnmarshalPayload(decode func(any) error) error
}

// PayloadReduced is a value of a reduced type, devalue's ["Type", value],
// such as a Request or a ReadableStream reference. Decoding into a
// PayloadReduced whose Value holds a pointer decodes the reduced value into
// it; otherwise Value holds it as an any does.
type PayloadReduced struct {
	Type  string
	Value any
}

// PayloadOptions says how EncodePayload protects a payload. SealTo takes
// precedence over Keys; with neither, the payload is not encrypted.
type PayloadOptions struct {
	// Keys encrypts the payload with the run's key (encr).
	Keys *RunKeys
	// SealTo seals the payload to a run's public key (encp).
	SealTo []byte
	// NoCompression stores the payload uncompressed.
	NoCompression bool
}

// EncodePayload encodes value as a payload.
func EncodePayload(value any, options PayloadOptions) ([]byte, error) {
	data, err := MarshalDevalue(value)
	if err != nil {
		return nil, err
	}
	return EncodeDevaluePayload(data, options)
}

// EncodeDevaluePayload encodes devalue text, as MarshalDevalue returns it, as
// a payload, for a value encoded before its run's keys are known.
func EncodeDevaluePayload(data []byte, options PayloadOptions) ([]byte, error) {
	data = labelPayload(PayloadDevalue, data)
	if !options.NoCompression {
		data = compressPayload(data)
	}
	return protectPayload(data, options.Keys, options.SealTo)
}

// DecodePayload decodes a payload into value, a pointer, opening it with
// keys when it is encrypted or sealed. An empty payload leaves value as it is.
func DecodePayload(data []byte, keys *RunKeys, value any) error {
	if len(data) == 0 {
		return nil
	}
	text, err := OpenPayload(data, keys)
	if err != nil {
		return err
	}
	return UnmarshalDevalue(text, value)
}

// OpenPayload removes a payload's encryption and compression and returns
// its devalue text.
func OpenPayload(data []byte, keys *RunKeys) ([]byte, error) {
	for {
		label, body, err := splitPayload(data)
		if err != nil {
			return nil, err
		}
		switch label {
		case PayloadDevalue:
			return body, nil
		case PayloadEncrypted:
			if keys == nil {
				return nil, fmt.Errorf("%w: payload is encrypted and no key is available", ErrPayloadInvalid)
			}
			data, err = openAESGCM(keys.key, body)
		case PayloadSealed:
			if keys == nil {
				return nil, fmt.Errorf("%w: payload is sealed and no key is available", ErrPayloadInvalid)
			}
			data, err = keys.open(body)
		case PayloadZstd:
			data, err = unzstd(body)
		case PayloadGzip:
			data, err = gunzip(body)
		default:
			return nil, fmt.Errorf("%w: unknown payload format %q", ErrPayloadInvalid, label)
		}
		if err != nil {
			return nil, fmt.Errorf("%w: %s: %v", ErrPayloadInvalid, label, err)
		}
	}
}

// PayloadLabel returns a payload's outer label, or "" when it has none.
func PayloadLabel(data []byte) string {
	if label, _, err := splitPayload(data); err == nil {
		return label
	}
	return ""
}

func labelPayload(label string, body []byte) []byte {
	return append([]byte(label), body...)
}

func splitPayload(data []byte) (string, []byte, error) {
	if len(data) < payloadLabelSize {
		return "", nil, fmt.Errorf("%w: %d bytes is too short for a format label", ErrPayloadInvalid, len(data))
	}
	label := string(data[:payloadLabelSize])
	for _, c := range label {
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') {
			return "", nil, fmt.Errorf("%w: no format label", ErrPayloadInvalid)
		}
	}
	return label, data[payloadLabelSize:], nil
}

func protectPayload(data []byte, keys *RunKeys, sealTo []byte) ([]byte, error) {
	switch {
	case sealTo != nil:
		sealer, err := NewPayloadSealer(sealTo)
		if err != nil {
			return nil, err
		}
		return sealer.Seal(data)
	case keys != nil:
		return encryptPayload(keys.key, data)
	}
	return data, nil
}

// compressPayload compresses a labelled payload: payloads of at least 1 KiB,
// kept only when that saves at least 5%. It writes zstd at level 3, or gzip
// with WORKFLOW_COMPRESSION_CODEC=gzip.
// WORKFLOW_DISABLE_COMPRESSION=1 turns it off.
func compressPayload(data []byte) []byte {
	if len(data) < payloadCompressMin || os.Getenv("WORKFLOW_DISABLE_COMPRESSION") == "1" {
		return data
	}
	var label string
	var compressed []byte
	if encoder := newZstdEncoder(); encoder != nil && os.Getenv("WORKFLOW_COMPRESSION_CODEC") != "gzip" {
		label, compressed = PayloadZstd, encoder.EncodeAll(data, nil)
		_ = encoder.Close()
	} else {
		var buffer bytes.Buffer
		writer, _ := gzip.NewWriterLevel(&buffer, gzip.DefaultCompression)
		_, _ = writer.Write(data)
		_ = writer.Close()
		label, compressed = PayloadGzip, buffer.Bytes()
	}
	if float64(payloadLabelSize+len(compressed)) >= float64(len(data))*(1-payloadCompressSavings) {
		return data
	}
	return labelPayload(label, compressed)
}

// newZstdEncoder returns an encoder at zstd level 3, or nil if one cannot be
// made, and payloads are then written with gzip. It is made for the call, as
// unzstd's decoder is, since an encoder's channels cannot be shared across
// synctest bubbles.
func newZstdEncoder() *zstd.Encoder {
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault), zstd.WithEncoderConcurrency(1))
	if err != nil {
		return nil
	}
	return encoder
}

// unzstd decompresses a zstd payload. Its decoder is made for the call, so
// no goroutine outlives it.
func unzstd(data []byte) ([]byte, error) {
	decoder, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
	if err != nil {
		return nil, err
	}
	defer decoder.Close()
	return decoder.DecodeAll(data, nil)
}

func gunzip(data []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	return io.ReadAll(reader)
}

// Keys

// DeriveRunKey returns a run's 32-byte key derived from a 32-byte key, such
// as a deployment key: HKDF-SHA256 with a zero salt and "{scope}|{runID}" as
// info.
func DeriveRunKey(key []byte, scope string, runID RunID) ([]byte, error) {
	if len(key) != payloadKeySize {
		return nil, fmt.Errorf("workflow: encryption key is %d bytes, want %d", len(key), payloadKeySize)
	}
	return hkdf.Key(sha256.New, key, make([]byte, 32), scope+"|"+string(runID), payloadKeySize)
}

// RunKeys holds what a run's key opens and seals payloads with: the key
// itself and the X25519 key pair derived from it.
type RunKeys struct {
	key     []byte
	private *ecdh.PrivateKey
	// PublicKey is the run's X25519 public key, which writers outside the
	// run seal payloads to.
	PublicKey []byte
}

// NewRunKeys returns the keys of a run's 32-byte key.
func NewRunKeys(runKey []byte) (*RunKeys, error) {
	if len(runKey) != payloadKeySize {
		return nil, fmt.Errorf("workflow: run key is %d bytes, want %d", len(runKey), payloadKeySize)
	}
	scalar, err := hkdf.Key(sha256.New, runKey, make([]byte, 32), payloadScalarInfo, payloadKeySize)
	if err != nil {
		return nil, err
	}
	private, err := ecdh.X25519().NewPrivateKey(scalar)
	if err != nil {
		return nil, err
	}
	return &RunKeys{key: slices.Clone(runKey), private: private, PublicKey: private.PublicKey().Bytes()}, nil
}

// EncodePublicKey returns a run's public key as it is published, in
// standard base64.
func EncodePublicKey(publicKey []byte) string {
	return base64.StdEncoding.EncodeToString(publicKey)
}

// DecodePublicKey decodes a published public key. It returns nil when the
// value is empty, not base64, or not 32 bytes, and writers then fall back to
// the run's key.
func DecodePublicKey(value string) []byte {
	key, err := base64.StdEncoding.DecodeString(value)
	if err != nil || len(key) != payloadKeySize {
		return nil
	}
	return key
}

func (k *RunKeys) open(body []byte) ([]byte, error) {
	if len(body) < payloadKeySize {
		return nil, errors.New("sealed payload is too short")
	}
	ephemeral, err := ecdh.X25519().NewPublicKey(body[:payloadKeySize])
	if err != nil {
		return nil, err
	}
	aead, err := sealedContentKey(k.private, ephemeral, body[:payloadKeySize], k.PublicKey)
	if err != nil {
		return nil, err
	}
	return openAEAD(aead, body[payloadKeySize:])
}

// PayloadSealer seals payloads to a run's public key. Its payloads share
// one key exchange, each with a fresh nonce, so a stream writer seals all
// its frames with one exchange.
type PayloadSealer struct {
	ephemeral []byte
	aead      cipher.AEAD
}

// NewPayloadSealer returns a sealer for the run whose public key is given.
func NewPayloadSealer(publicKey []byte) (*PayloadSealer, error) {
	recipient, err := ecdh.X25519().NewPublicKey(publicKey)
	if err != nil {
		return nil, err
	}
	ephemeral, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	public := ephemeral.PublicKey().Bytes()
	aead, err := sealedContentKey(ephemeral, recipient, public, publicKey)
	if err != nil {
		return nil, err
	}
	return &PayloadSealer{ephemeral: public, aead: aead}, nil
}

// Seal returns data sealed as an encp payload.
func (s *PayloadSealer) Seal(data []byte) ([]byte, error) {
	sealed, err := sealAEAD(s.aead, data)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, payloadLabelSize+len(s.ephemeral)+len(sealed))
	out = append(append(append(out, PayloadSealed...), s.ephemeral...), sealed...)
	return out, nil
}

func sealedContentKey(private *ecdh.PrivateKey, public *ecdh.PublicKey, ephemeral, recipient []byte) (cipher.AEAD, error) {
	shared, err := private.ECDH(public)
	if err != nil {
		return nil, err
	}
	info := payloadContentKeyInfo + string(ephemeral) + string(recipient)
	key, err := hkdf.Key(sha256.New, shared, make([]byte, 32), info, payloadKeySize)
	if err != nil {
		return nil, err
	}
	return newAESGCM(key)
}

func encryptPayload(key, data []byte) ([]byte, error) {
	aead, err := newAESGCM(key)
	if err != nil {
		return nil, err
	}
	sealed, err := sealAEAD(aead, data)
	if err != nil {
		return nil, err
	}
	return labelPayload(PayloadEncrypted, sealed), nil
}

func openAESGCM(key, body []byte) ([]byte, error) {
	aead, err := newAESGCM(key)
	if err != nil {
		return nil, err
	}
	return openAEAD(aead, body)
}

func newAESGCM(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

func sealAEAD(aead cipher.AEAD, data []byte) ([]byte, error) {
	nonce := make([]byte, payloadNonceSize, payloadNonceSize+len(data)+aead.Overhead())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	return aead.Seal(nonce, nonce, data, nil), nil
}

func openAEAD(aead cipher.AEAD, body []byte) ([]byte, error) {
	if len(body) < payloadNonceSize+aead.Overhead() {
		return nil, errors.New("encrypted payload is too short")
	}
	return aead.Open(nil, body[:payloadNonceSize], body[payloadNonceSize:], nil)
}

// Streams

// EncodeStreamFrame returns value as one frame of an object stream: a
// big-endian u32 length, then value's payload, encrypted with keys or sealed
// with sealer when set. A Go stream writer's chunk is a []byte, a Uint8Array.
// Stream frames are never compressed.
func EncodeStreamFrame(value any, keys *RunKeys, sealer *PayloadSealer) ([]byte, error) {
	text, err := MarshalDevalue(value)
	if err != nil {
		return nil, err
	}
	payload := labelPayload(PayloadDevalue, text)
	switch {
	case sealer != nil:
		payload, err = sealer.Seal(payload)
	case keys != nil:
		payload, err = encryptPayload(keys.key, payload)
	}
	if err != nil {
		return nil, err
	}
	frame := binary.BigEndian.AppendUint32(make([]byte, 0, 4+len(payload)), uint32(len(payload)))
	return append(frame, payload...), nil
}

// SplitStreamFrames returns the payloads of the frames a stored stream chunk
// holds, for DecodePayload.
func SplitStreamFrames(chunk []byte) ([][]byte, error) {
	var payloads [][]byte
	for len(chunk) > 0 {
		if len(chunk) < 4 {
			return nil, fmt.Errorf("%w: truncated stream frame", ErrPayloadInvalid)
		}
		size := binary.BigEndian.Uint32(chunk)
		if size > payloadMaxFrame || int(size) > len(chunk)-4 {
			return nil, fmt.Errorf("%w: stream frame of %d bytes", ErrPayloadInvalid, size)
		}
		payloads = append(payloads, chunk[4:4+size])
		chunk = chunk[4+size:]
	}
	return payloads, nil
}

// DecodeStreamFrames returns the values of the frames a stored stream chunk
// holds, as bytes: a Uint8Array as is, a string as UTF-8, and any other
// value as JSON.
func DecodeStreamFrames(chunk []byte, keys *RunKeys) ([][]byte, error) {
	payloads, err := SplitStreamFrames(chunk)
	if err != nil {
		return nil, err
	}
	values := make([][]byte, 0, len(payloads))
	for _, payload := range payloads {
		var value any
		if err := DecodePayload(payload, keys, &value); err != nil {
			return nil, err
		}
		switch value := value.(type) {
		case []byte:
			values = append(values, value)
		case string:
			values = append(values, []byte(value))
		default:
			data, err := json.Marshal(value)
			if err != nil {
				return nil, fmt.Errorf("%w: stream value: %v", ErrPayloadInvalid, err)
			}
			values = append(values, data)
		}
	}
	return values, nil
}

// devalue

// devalue's indexes for values that are not stored.
const (
	dvUndefinedIndex = -1
	dvHole           = -2
	dvNaN            = -3
	dvPosInf         = -4
	dvNegInf         = -5
	dvNegZero        = -6
	dvSparse         = -7
)

// A devalue value is decoded to, and encoded from, these nodes: nil (null),
// dvUndefined, bool, string, dvNumber, dvSpecial, []any, *dvObject, and
// dvTyped for a reduced type such as Date or Uint8Array.
type (
	dvUndefined struct{}
	dvNumber    string
	dvSpecial   int
	dvObject    struct {
		keys   []string
		values []any
	}
	dvTyped struct {
		typ   string
		value any
	}
)

func (o *dvObject) set(key string, value any) {
	o.keys, o.values = append(o.keys, key), append(o.values, value)
}

func (o *dvObject) get(key string) (any, bool) {
	for index, name := range o.keys {
		if name == key {
			return o.values[index], true
		}
	}
	return nil, false
}

// MarshalDevalue returns value as devalue text, with reducers for the
// JavaScript types Go values map onto.
func MarshalDevalue(value any) ([]byte, error) {
	node, err := encodeNode(reflect.ValueOf(value), nil)
	if err != nil {
		return nil, err
	}
	var w dvWriter
	root := w.add(node)
	if root < 0 {
		return []byte(strconv.Itoa(root)), nil
	}
	var out bytes.Buffer
	out.WriteByte('[')
	for index, item := range w.values {
		if index > 0 {
			out.WriteByte(',')
		}
		out.Write(item)
	}
	out.WriteByte(']')
	return out.Bytes(), nil
}

// UnmarshalDevalue decodes devalue text into value, a pointer.
func UnmarshalDevalue(data []byte, value any) error {
	target := reflect.ValueOf(value)
	if target.Kind() != reflect.Pointer || target.IsNil() {
		return fmt.Errorf("workflow: decode payload into non-pointer %T", value)
	}
	node, err := parseDevalue(data)
	if err != nil {
		return err
	}
	return decodeNode(node, target.Elem())
}

type dvWriter struct{ values [][]byte }

func (w *dvWriter) add(node any) int {
	switch node := node.(type) {
	case dvUndefined:
		return dvUndefinedIndex
	case dvSpecial:
		return int(node)
	}
	index := len(w.values)
	w.values = append(w.values, nil)
	var out []byte
	switch node := node.(type) {
	case nil:
		out = []byte("null")
	case bool:
		out = strconv.AppendBool(nil, node)
	case string:
		out = quoteString(node)
	case dvNumber:
		out = []byte(node)
	case []any:
		out = append(out, '[')
		for i, item := range node {
			if i > 0 {
				out = append(out, ',')
			}
			out = strconv.AppendInt(out, int64(w.add(item)), 10)
		}
		out = append(out, ']')
	case *dvObject:
		out = append(out, '{')
		for i, key := range node.keys {
			if i > 0 {
				out = append(out, ',')
			}
			out = append(append(out, quoteString(key)...), ':')
			out = strconv.AppendInt(out, int64(w.add(node.values[i])), 10)
		}
		out = append(out, '}')
	case dvTyped:
		out = append(append(append(out, '['), quoteString(node.typ)...), ',')
		out = append(strconv.AppendInt(out, int64(w.add(node.value)), 10), ']')
	}
	w.values[index] = out
	return index
}

func quoteString(s string) []byte {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	_ = encoder.Encode(s)
	return bytes.TrimSuffix(buffer.Bytes(), []byte("\n"))
}

var (
	timeType        = reflect.TypeFor[time.Time]()
	bigIntType      = reflect.TypeFor[big.Int]()
	urlType         = reflect.TypeFor[url.URL]()
	headerType      = reflect.TypeFor[http.Header]()
	errorType       = reflect.TypeFor[error]()
	rawMessageType  = reflect.TypeFor[json.RawMessage]()
	reducedType     = reflect.TypeFor[PayloadReduced]()
	payloadMarshal  = reflect.TypeFor[PayloadMarshaler]()
	payloadUnmarsh  = reflect.TypeFor[PayloadUnmarshaler]()
	jsonMarshaler   = reflect.TypeFor[json.Marshaler]()
	jsonUnmarshaler = reflect.TypeFor[json.Unmarshaler]()
	textMarshaler   = reflect.TypeFor[encoding.TextMarshaler]()
	textUnmarshaler = reflect.TypeFor[encoding.TextUnmarshaler]()
)

// encodeNode returns the node of v. seen holds the pointers being encoded,
// to report cycles.
func encodeNode(v reflect.Value, seen map[uintptr]bool) (any, error) {
	if !v.IsValid() {
		return nil, nil
	}
	if v.Kind() == reflect.Interface {
		if v.IsNil() {
			return nil, nil
		}
		if v.Type() == errorType {
			return encodeErrorNode(v.Interface().(error))
		}
		return encodeNode(v.Elem(), seen)
	}
	if v.Kind() == reflect.Pointer && v.IsNil() {
		return nil, nil
	}
	t := v.Type()
	if t.Implements(payloadMarshal) {
		replacement, err := v.Interface().(PayloadMarshaler).MarshalPayload()
		if err != nil {
			return nil, err
		}
		return encodeNode(reflect.ValueOf(replacement), seen)
	}
	switch t {
	case reducedType:
		reduced := v.Interface().(PayloadReduced)
		value, err := encodeNode(reflect.ValueOf(reduced.Value), seen)
		if err != nil {
			return nil, err
		}
		return dvTyped{reduced.Type, value}, nil
	case timeType:
		return dvTyped{"Date", v.Interface().(time.Time).UTC().Format("2006-01-02T15:04:05.000Z")}, nil
	case bigIntType:
		value := v.Interface().(big.Int)
		return dvTyped{"BigInt", value.String()}, nil
	case urlType:
		value := v.Interface().(url.URL)
		return dvTyped{"URL", value.String()}, nil
	case headerType:
		var entries []any
		header := v.Interface().(http.Header)
		for _, name := range slices.Sorted(mapKeys(header)) {
			for _, value := range header[name] {
				entries = append(entries, []any{strings.ToLower(name), value})
			}
		}
		return dvTyped{"Headers", entries}, nil
	case rawMessageType:
		if v.Len() == 0 {
			return nil, nil
		}
		return jsonNode(v.Bytes())
	}
	if t.Kind() == reflect.Pointer && t.Elem().Kind() == reflect.Struct && (t.Elem() == bigIntType || t.Elem() == urlType || t.Elem() == timeType) {
		return encodeNode(v.Elem(), seen)
	}
	if t.Implements(jsonMarshaler) && t.Kind() != reflect.Interface {
		data, err := v.Interface().(json.Marshaler).MarshalJSON()
		if err != nil {
			return nil, err
		}
		return jsonNode(data)
	}
	if t.Implements(textMarshaler) {
		text, err := v.Interface().(encoding.TextMarshaler).MarshalText()
		if err != nil {
			return nil, err
		}
		return string(text), nil
	}
	switch t.Kind() {
	case reflect.Bool:
		return v.Bool(), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return dvNumber(strconv.FormatInt(v.Int(), 10)), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return dvNumber(strconv.FormatUint(v.Uint(), 10)), nil
	case reflect.Float32, reflect.Float64:
		return floatNode(v.Float(), t.Bits()), nil
	case reflect.String:
		return v.String(), nil
	case reflect.Slice:
		if v.IsNil() {
			return nil, nil
		}
		if t.Elem().Kind() == reflect.Uint8 && !t.Elem().Implements(payloadMarshal) {
			return dvTyped{"Uint8Array", encodeBase64(v.Bytes())}, nil
		}
		fallthrough
	case reflect.Array:
		items := make([]any, v.Len())
		for i := range items {
			item, err := encodeNode(v.Index(i), seen)
			if err != nil {
				return nil, err
			}
			items[i] = item
		}
		return items, nil
	case reflect.Map:
		if v.IsNil() {
			return nil, nil
		}
		return encodeMap(v, seen)
	case reflect.Struct:
		return encodeStruct(v, seen)
	case reflect.Pointer:
		if seen == nil {
			seen = map[uintptr]bool{}
		}
		if seen[v.Pointer()] {
			return nil, fmt.Errorf("workflow: encode payload: cycle through %s", t)
		}
		seen[v.Pointer()] = true
		defer delete(seen, v.Pointer())
		return encodeNode(v.Elem(), seen)
	}
	return nil, fmt.Errorf("workflow: encode payload: unsupported type %s", t)
}

func mapKeys[V any](m map[string]V) func(func(string) bool) {
	return func(yield func(string) bool) {
		for key := range m {
			if !yield(key) {
				return
			}
		}
	}
}

func floatNode(f float64, bits int) any {
	switch {
	case math.IsNaN(f):
		return dvSpecial(dvNaN)
	case math.IsInf(f, 1):
		return dvSpecial(dvPosInf)
	case math.IsInf(f, -1):
		return dvSpecial(dvNegInf)
	case f == 0 && math.Signbit(f):
		return dvSpecial(dvNegZero)
	}
	data, _ := json.Marshal(f)
	if bits == 32 {
		data, _ = json.Marshal(float32(f))
	}
	return dvNumber(data)
}

func encodeBase64(data []byte) string {
	if len(data) == 0 {
		return "."
	}
	return base64.StdEncoding.EncodeToString(data)
}

func decodeBase64(value string) ([]byte, error) {
	if value == "." || value == "" {
		return []byte{}, nil
	}
	data, err := base64.StdEncoding.DecodeString(value)
	if err != nil {
		data, err = base64.RawStdEncoding.DecodeString(strings.TrimRight(value, "="))
	}
	return data, err
}

func encodeMap(v reflect.Value, seen map[uintptr]bool) (any, error) {
	keyType := v.Type().Key()
	type entry struct {
		key  reflect.Value
		text string
	}
	entries := make([]entry, 0, v.Len())
	stringKeys := keyType.Kind() == reflect.String
	for _, key := range v.MapKeys() {
		text := ""
		if stringKeys {
			text = key.String()
		} else {
			text = fmt.Sprint(key.Interface())
		}
		entries = append(entries, entry{key, text})
	}
	slices.SortFunc(entries, func(a, b entry) int {
		if !stringKeys && a.key.CanInt() && b.key.CanInt() {
			return cmpInt(a.key.Int(), b.key.Int())
		}
		return strings.Compare(a.text, b.text)
	})
	if stringKeys {
		object := &dvObject{}
		for _, e := range entries {
			value, err := encodeNode(v.MapIndex(e.key), seen)
			if err != nil {
				return nil, err
			}
			object.set(e.text, value)
		}
		return object, nil
	}
	pairs := make([]any, 0, len(entries))
	for _, e := range entries {
		key, err := encodeNode(e.key, seen)
		if err != nil {
			return nil, err
		}
		value, err := encodeNode(v.MapIndex(e.key), seen)
		if err != nil {
			return nil, err
		}
		pairs = append(pairs, []any{key, value})
	}
	return dvTyped{"Map", pairs}, nil
}

func cmpInt(a, b int64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

func encodeStruct(v reflect.Value, seen map[uintptr]bool) (any, error) {
	object := &dvObject{}
	for _, field := range structFields(v.Type()) {
		value, ok := fieldByIndex(v, field.index)
		if !ok {
			continue
		}
		if field.omitEmpty && isEmptyValue(value) || field.omitZero && value.IsZero() {
			continue
		}
		node, err := encodeNode(value, seen)
		if err != nil {
			return nil, err
		}
		if field.quoted {
			if number, ok := node.(dvNumber); ok {
				node = string(number)
			} else if b, ok := node.(bool); ok {
				node = strconv.FormatBool(b)
			}
		}
		object.set(field.name, node)
	}
	return object, nil
}

func fieldByIndex(v reflect.Value, index []int) (reflect.Value, bool) {
	for i, step := range index {
		if i > 0 && v.Kind() == reflect.Pointer {
			if v.IsNil() {
				return reflect.Value{}, false
			}
			v = v.Elem()
		}
		v = v.Field(step)
	}
	return v, true
}

func isEmptyValue(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64, reflect.Interface, reflect.Pointer:
		return v.IsZero()
	}
	return false
}

// structField is a field as encoding/json names it.
type structField struct {
	name                string
	index               []int
	omitEmpty, omitZero bool
	quoted              bool
	depth               int
	tagged              bool
	fieldType           reflect.Type
}

var structFieldCache sync.Map

// structFields returns the fields of t that encoding/json encodes, by the
// names it gives them.
func structFields(t reflect.Type) []structField {
	if cached, ok := structFieldCache.Load(t); ok {
		return cached.([]structField)
	}
	var all []structField
	var walk func(t reflect.Type, index []int, depth int, visited map[reflect.Type]bool)
	walk = func(t reflect.Type, index []int, depth int, visited map[reflect.Type]bool) {
		if visited[t] {
			return
		}
		visited[t] = true
		defer delete(visited, t)
		for i := range t.NumField() {
			f := t.Field(i)
			tag := f.Tag.Get("json")
			if tag == "-" {
				continue
			}
			name, options, _ := strings.Cut(tag, ",")
			fieldIndex := append(slices.Clone(index), i)
			if f.Anonymous && name == "" {
				ft := f.Type
				if ft.Kind() == reflect.Pointer {
					ft = ft.Elem()
				}
				if ft.Kind() == reflect.Struct {
					walk(ft, fieldIndex, depth+1, visited)
					continue
				}
			}
			if !f.IsExported() {
				continue
			}
			field := structField{name: name, index: fieldIndex, depth: depth, tagged: name != "", fieldType: f.Type}
			if field.name == "" {
				field.name = f.Name
			}
			for option := range strings.SplitSeq(options, ",") {
				switch option {
				case "omitempty":
					field.omitEmpty = true
				case "omitzero":
					field.omitZero = true
				case "string":
					field.quoted = true
				}
			}
			all = append(all, field)
		}
	}
	walk(t, nil, 0, map[reflect.Type]bool{})
	// As encoding/json does, the shallowest field of a name wins, a tagged
	// one among fields of the same depth, and a name with several such
	// fields is dropped.
	var fields []structField
	for _, field := range all {
		best, ambiguous := field, false
		for _, other := range all {
			if other.name != field.name || other.index[0] == field.index[0] && slices.Equal(other.index, field.index) {
				continue
			}
			if other.depth < best.depth || other.depth == best.depth && other.tagged && !best.tagged {
				best, ambiguous = other, false
			} else if other.depth == best.depth && other.tagged == best.tagged {
				ambiguous = true
			}
		}
		if !ambiguous && slices.Equal(best.index, field.index) {
			fields = append(fields, field)
		}
	}
	structFieldCache.Store(t, fields)
	return fields
}

// jsonNode returns the node of JSON text.
func jsonNode(data []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	node, err := readJSONNode(decoder)
	if err != nil {
		return nil, fmt.Errorf("workflow: encode payload: %w", err)
	}
	return node, nil
}

// readJSONNode reads one JSON value as a node, keeping object key order.
func readJSONNode(decoder *json.Decoder) (any, error) {
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	switch token := token.(type) {
	case json.Delim:
		switch token {
		case '[':
			items := []any{}
			for decoder.More() {
				item, err := readJSONNode(decoder)
				if err != nil {
					return nil, err
				}
				items = append(items, item)
			}
			_, err := decoder.Token()
			return items, err
		case '{':
			object := &dvObject{}
			for decoder.More() {
				key, err := decoder.Token()
				if err != nil {
					return nil, err
				}
				value, err := readJSONNode(decoder)
				if err != nil {
					return nil, err
				}
				object.set(key.(string), value)
			}
			_, err := decoder.Token()
			return object, err
		}
	case json.Number:
		return dvNumber(token), nil
	case string, bool, nil:
		return token, nil
	}
	return nil, fmt.Errorf("unexpected JSON token %v", token)
}

// nodeJSON returns a node as JSON text, for types decoded from JSON. Numbers
// keep their text and objects their key order, so JSON written by jsonNode
// reads back as it was, including integers beyond float64's precision.
func nodeJSON(node any) ([]byte, error) {
	var buffer bytes.Buffer
	if err := writeNodeJSON(&buffer, node); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func writeNodeJSON(buffer *bytes.Buffer, node any) error {
	switch value := node.(type) {
	case nil, dvUndefined:
		buffer.WriteString("null")
		return nil
	case dvNumber:
		if !json.Valid([]byte(value)) {
			return fmt.Errorf("%w: number %s", ErrPayloadInvalid, value)
		}
		buffer.WriteString(string(value))
		return nil
	case []any:
		buffer.WriteByte('[')
		for i, item := range value {
			if i > 0 {
				buffer.WriteByte(',')
			}
			if err := writeNodeJSON(buffer, item); err != nil {
				return err
			}
		}
		buffer.WriteByte(']')
		return nil
	case *dvObject:
		buffer.WriteByte('{')
		first := true
		for i, key := range value.keys {
			if _, ok := value.values[i].(dvUndefined); ok {
				continue
			}
			if !first {
				buffer.WriteByte(',')
			}
			first = false
			name, _ := json.Marshal(key)
			buffer.Write(name)
			buffer.WriteByte(':')
			if err := writeNodeJSON(buffer, value.values[i]); err != nil {
				return err
			}
		}
		buffer.WriteByte('}')
		return nil
	}
	// Strings, booleans, special numbers, and the mapped types are written as
	// encoding/json writes their Go values.
	converted, err := nodeAny(node)
	if err != nil {
		return err
	}
	data, err := json.Marshal(converted)
	if err != nil {
		return err
	}
	buffer.Write(data)
	return nil
}

// parseDevalue parses devalue text into its root node.
func parseDevalue(data []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	raw, err := readJSONNode(decoder)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrPayloadInvalid, err)
	}
	if number, ok := raw.(dvNumber); ok {
		index, err := strconv.Atoi(string(number))
		if err != nil || index >= 0 {
			return nil, fmt.Errorf("%w: devalue root %s", ErrPayloadInvalid, number)
		}
		return specialNode(index)
	}
	values, ok := raw.([]any)
	if !ok || len(values) == 0 {
		return nil, fmt.Errorf("%w: devalue text is not an array", ErrPayloadInvalid)
	}
	p := &dvParser{values: values, hydrated: make([]any, len(values)), done: make([]bool, len(values)), active: make([]bool, len(values))}
	return p.hydrate(0)
}

func specialNode(index int) (any, error) {
	switch index {
	case dvUndefinedIndex, dvHole:
		return dvUndefined{}, nil
	case dvNaN, dvPosInf, dvNegInf, dvNegZero:
		return dvSpecial(index), nil
	}
	return nil, fmt.Errorf("%w: devalue index %d", ErrPayloadInvalid, index)
}

type dvParser struct {
	values   []any
	hydrated []any
	done     []bool
	active   []bool
}

func (p *dvParser) index(node any) (int, error) {
	number, ok := node.(dvNumber)
	if !ok {
		return 0, fmt.Errorf("%w: devalue reference is not a number", ErrPayloadInvalid)
	}
	index, err := strconv.Atoi(string(number))
	if err != nil {
		return 0, fmt.Errorf("%w: devalue reference %s", ErrPayloadInvalid, number)
	}
	return index, nil
}

func (p *dvParser) ref(node any) (any, error) {
	index, err := p.index(node)
	if err != nil {
		return nil, err
	}
	return p.hydrate(index)
}

func (p *dvParser) hydrate(index int) (any, error) {
	if index < 0 {
		return specialNode(index)
	}
	if index >= len(p.values) {
		return nil, fmt.Errorf("%w: devalue index %d out of range", ErrPayloadInvalid, index)
	}
	if p.done[index] {
		return p.hydrated[index], nil
	}
	if p.active[index] {
		return nil, fmt.Errorf("%w: circular devalue reference", ErrPayloadInvalid)
	}
	p.active[index] = true
	defer func() { p.active[index] = false }()
	node, err := p.build(p.values[index])
	if err != nil {
		return nil, err
	}
	p.hydrated[index], p.done[index] = node, true
	return node, nil
}

func (p *dvParser) build(raw any) (any, error) {
	switch raw := raw.(type) {
	case []any:
		if len(raw) > 0 {
			if typ, ok := raw[0].(string); ok {
				return p.buildTyped(typ, raw[1:])
			}
		}
		if len(raw) > 0 {
			if index, err := p.index(raw[0]); err == nil && index == dvSparse {
				return p.buildSparse(raw[1:])
			}
		}
		items := make([]any, len(raw))
		for i, ref := range raw {
			item, err := p.ref(ref)
			if err != nil {
				return nil, err
			}
			items[i] = item
		}
		return items, nil
	case *dvObject:
		object := &dvObject{}
		for i, key := range raw.keys {
			value, err := p.ref(raw.values[i])
			if err != nil {
				return nil, err
			}
			object.set(key, value)
		}
		return object, nil
	}
	return raw, nil
}

func (p *dvParser) buildSparse(rest []any) (any, error) {
	if len(rest) == 0 {
		return nil, fmt.Errorf("%w: sparse array without length", ErrPayloadInvalid)
	}
	length, err := p.index(rest[0])
	if err != nil || length < 0 || length > 1<<24 {
		return nil, fmt.Errorf("%w: sparse array length", ErrPayloadInvalid)
	}
	items := make([]any, length)
	for i := range items {
		items[i] = dvUndefined{}
	}
	for i := 1; i+1 < len(rest); i += 2 {
		at, err := p.index(rest[i])
		if err != nil || at < 0 || at >= length {
			return nil, fmt.Errorf("%w: sparse array index", ErrPayloadInvalid)
		}
		if items[at], err = p.ref(rest[i+1]); err != nil {
			return nil, err
		}
	}
	return items, nil
}

// buildTyped builds ["type", ...]. Except for devalue's own null-prototype
// objects and boxed primitives, the second entry is the reduced value's
// index, or the value itself when it is not a number.
func (p *dvParser) buildTyped(typ string, rest []any) (any, error) {
	switch typ {
	case "null":
		object := &dvObject{}
		for i := 0; i+1 < len(rest); i += 2 {
			key, ok := rest[i].(string)
			if !ok {
				return nil, fmt.Errorf("%w: object key is not a string", ErrPayloadInvalid)
			}
			value, err := p.ref(rest[i+1])
			if err != nil {
				return nil, err
			}
			object.set(key, value)
		}
		return object, nil
	case "Object":
		if len(rest) == 0 {
			return nil, fmt.Errorf("%w: empty boxed value", ErrPayloadInvalid)
		}
		return p.ref(rest[0])
	case "Set":
		if len(rest) != 1 {
			// devalue's own Set: every entry is a reference.
			items := make([]any, len(rest))
			for i, ref := range rest {
				item, err := p.ref(ref)
				if err != nil {
					return nil, err
				}
				items[i] = item
			}
			return dvTyped{"Set", items}, nil
		}
	}
	if len(rest) == 0 {
		return dvTyped{typ, dvUndefined{}}, nil
	}
	var value any
	if _, ok := rest[0].(dvNumber); ok {
		var err error
		if value, err = p.ref(rest[0]); err != nil {
			return nil, err
		}
	} else {
		value = rest[0]
	}
	// devalue's own typed arrays reference an ArrayBuffer.
	if inner, ok := value.(dvTyped); ok && inner.typ == "ArrayBuffer" && typ != "ArrayBuffer" {
		value = inner.value
	}
	if typ == "Map" && len(rest) > 1 {
		// devalue's own Map: key and value references in turn.
		var pairs []any
		for i := 0; i+1 < len(rest); i += 2 {
			key, err := p.ref(rest[i])
			if err != nil {
				return nil, err
			}
			item, err := p.ref(rest[i+1])
			if err != nil {
				return nil, err
			}
			pairs = append(pairs, []any{key, item})
		}
		value = pairs
	}
	return dvTyped{typ, value}, nil
}

// decodeNode stores node in v.
func decodeNode(node any, v reflect.Value) error {
	if _, ok := node.(dvUndefined); ok {
		return nil
	}
	if v.Kind() != reflect.Pointer && v.CanAddr() && v.Addr().Type().Implements(payloadUnmarsh) {
		return v.Addr().Interface().(PayloadUnmarshaler).UnmarshalPayload(func(target any) error {
			pointer := reflect.ValueOf(target)
			if pointer.Kind() != reflect.Pointer || pointer.IsNil() {
				return fmt.Errorf("workflow: decode payload into non-pointer %T", target)
			}
			return decodeNode(node, pointer.Elem())
		})
	}
	if node == nil {
		switch v.Kind() {
		case reflect.Interface, reflect.Pointer, reflect.Map, reflect.Slice:
			v.SetZero()
		}
		return nil
	}
	t := v.Type()
	switch t {
	case reducedType:
		typed, ok := node.(dvTyped)
		if !ok {
			return fmt.Errorf("%w: cannot decode %s as a reduced value", ErrPayloadInvalid, nodeKind(node))
		}
		reduced := v.Addr().Interface().(*PayloadReduced)
		reduced.Type = typed.typ
		if target := reflect.ValueOf(reduced.Value); target.Kind() == reflect.Pointer && !target.IsNil() {
			return decodeNode(typed.value, target.Elem())
		}
		value, err := nodeAny(typed.value)
		reduced.Value = value
		return err
	case timeType:
		return decodeTime(node, v)
	case bigIntType:
		value, err := decodeBigInt(node)
		if err != nil {
			return err
		}
		v.Set(reflect.ValueOf(*value))
		return nil
	case urlType:
		text, err := typedString(node, "URL")
		if err != nil {
			return err
		}
		parsed, err := url.Parse(text)
		if err != nil {
			return fmt.Errorf("%w: URL: %v", ErrPayloadInvalid, err)
		}
		v.Set(reflect.ValueOf(*parsed))
		return nil
	case headerType:
		header, err := decodeHeaders(node)
		if err != nil {
			return err
		}
		v.Set(reflect.ValueOf(header))
		return nil
	case rawMessageType:
		data, err := nodeJSON(node)
		if err != nil {
			return err
		}
		v.SetBytes(data)
		return nil
	case errorType:
		err, decodeErr := decodeErrorNode(node)
		if decodeErr != nil {
			return decodeErr
		}
		v.Set(reflect.ValueOf(err))
		return nil
	}
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			v.Set(reflect.New(t.Elem()))
		}
		return decodeNode(node, v.Elem())
	}
	if v.CanAddr() && t.Kind() != reflect.Interface {
		pointer := v.Addr()
		if pointer.Type().Implements(jsonUnmarshaler) {
			data, err := nodeJSON(node)
			if err != nil {
				return err
			}
			return pointer.Interface().(json.Unmarshaler).UnmarshalJSON(data)
		}
		if text, ok := node.(string); ok && pointer.Type().Implements(textUnmarshaler) {
			return pointer.Interface().(encoding.TextUnmarshaler).UnmarshalText([]byte(text))
		}
	}
	switch t.Kind() {
	case reflect.Interface:
		value, err := nodeAny(node)
		if err != nil {
			return err
		}
		if value == nil {
			v.SetZero()
			return nil
		}
		rv := reflect.ValueOf(value)
		if !rv.Type().AssignableTo(t) {
			return fmt.Errorf("%w: cannot store %T in %s", ErrPayloadInvalid, value, t)
		}
		v.Set(rv)
		return nil
	case reflect.Bool:
		switch value := node.(type) {
		case bool:
			v.SetBool(value)
			return nil
		case string:
			parsed, err := strconv.ParseBool(value)
			if err == nil {
				v.SetBool(parsed)
				return nil
			}
		}
	case reflect.String:
		switch value := node.(type) {
		case string:
			v.SetString(value)
			return nil
		case dvNumber:
			if t == reflect.TypeFor[json.Number]() {
				v.SetString(string(value))
				return nil
			}
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		text, ok := numberText(node)
		if ok {
			n, err := strconv.ParseInt(text, 10, t.Bits())
			if err != nil {
				f, ferr := strconv.ParseFloat(text, 64)
				if ferr != nil || f != math.Trunc(f) || v.OverflowInt(int64(f)) {
					return fmt.Errorf("%w: %s does not fit %s", ErrPayloadInvalid, text, t)
				}
				n = int64(f)
			}
			v.SetInt(n)
			return nil
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		text, ok := numberText(node)
		if ok {
			n, err := strconv.ParseUint(text, 10, t.Bits())
			if err != nil {
				f, ferr := strconv.ParseFloat(text, 64)
				if ferr != nil || f < 0 || f != math.Trunc(f) || v.OverflowUint(uint64(f)) {
					return fmt.Errorf("%w: %s does not fit %s", ErrPayloadInvalid, text, t)
				}
				n = uint64(f)
			}
			v.SetUint(n)
			return nil
		}
	case reflect.Float32, reflect.Float64:
		switch value := node.(type) {
		case dvSpecial:
			v.SetFloat(specialFloat(value))
			return nil
		}
		if text, ok := numberText(node); ok {
			f, err := strconv.ParseFloat(text, t.Bits())
			if err != nil {
				return fmt.Errorf("%w: %s does not fit %s", ErrPayloadInvalid, text, t)
			}
			v.SetFloat(f)
			return nil
		}
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			if data, ok, err := bytesOf(node); ok {
				if err != nil {
					return err
				}
				v.SetBytes(reflect.ValueOf(data).Convert(reflect.SliceOf(t.Elem())).Bytes())
				return nil
			}
		}
		items, ok := arrayItems(node)
		if !ok {
			break
		}
		slice := reflect.MakeSlice(t, len(items), len(items))
		for i, item := range items {
			if err := decodeNode(item, slice.Index(i)); err != nil {
				return err
			}
		}
		v.Set(slice)
		return nil
	case reflect.Array:
		items, ok := arrayItems(node)
		if !ok {
			break
		}
		for i := range v.Len() {
			if i < len(items) {
				if err := decodeNode(items[i], v.Index(i)); err != nil {
					return err
				}
			} else {
				v.Index(i).SetZero()
			}
		}
		return nil
	case reflect.Map:
		return decodeMap(node, v)
	case reflect.Struct:
		object, ok := node.(*dvObject)
		if !ok {
			break
		}
		fields := structFields(t)
		for i, key := range object.keys {
			field := findField(fields, key)
			if field == nil {
				continue
			}
			target := v
			for j, step := range field.index {
				if j > 0 && target.Kind() == reflect.Pointer {
					if target.IsNil() {
						target.Set(reflect.New(target.Type().Elem()))
					}
					target = target.Elem()
				}
				target = target.Field(step)
			}
			value := object.values[i]
			if field.quoted {
				if text, ok := value.(string); ok && target.Kind() != reflect.String {
					value = dvNumber(text)
					if target.Kind() == reflect.Bool {
						value = text == "true"
					}
				}
			}
			if err := decodeNode(value, target); err != nil {
				return fmt.Errorf("%s: %w", key, err)
			}
		}
		return nil
	}
	return fmt.Errorf("%w: cannot decode %s into %s", ErrPayloadInvalid, nodeKind(node), t)
}

func findField(fields []structField, key string) *structField {
	for i := range fields {
		if fields[i].name == key {
			return &fields[i]
		}
	}
	for i := range fields {
		if strings.EqualFold(fields[i].name, key) {
			return &fields[i]
		}
	}
	return nil
}

func nodeKind(node any) string {
	switch node := node.(type) {
	case dvTyped:
		return node.typ
	case *dvObject:
		return "object"
	case []any:
		return "array"
	case dvNumber, dvSpecial:
		return "number"
	}
	return fmt.Sprintf("%T", node)
}

func numberText(node any) (string, bool) {
	switch value := node.(type) {
	case dvNumber:
		return string(value), true
	case dvTyped:
		if value.typ == "BigInt" {
			if text, ok := value.value.(string); ok {
				return text, true
			}
		}
	}
	return "", false
}

func specialFloat(value dvSpecial) float64 {
	switch value {
	case dvNaN:
		return math.NaN()
	case dvPosInf:
		return math.Inf(1)
	case dvNegInf:
		return math.Inf(-1)
	}
	return math.Copysign(0, -1)
}

func arrayItems(node any) ([]any, bool) {
	switch value := node.(type) {
	case []any:
		return value, true
	case dvTyped:
		if value.typ == "Set" {
			items, ok := value.value.([]any)
			return items, ok
		}
	}
	return nil, false
}

// bytesOf returns the bytes of a Uint8Array, ArrayBuffer, or base64 string.
func bytesOf(node any) ([]byte, bool, error) {
	var text string
	switch value := node.(type) {
	case dvTyped:
		switch value.typ {
		case "Uint8Array", "ArrayBuffer", "Uint8ClampedArray", "Int8Array", "DataViewBytes":
			s, ok := value.value.(string)
			if !ok {
				return nil, true, fmt.Errorf("%w: %s is not base64", ErrPayloadInvalid, value.typ)
			}
			text = s
		default:
			return nil, false, nil
		}
	case string:
		text = value
	default:
		return nil, false, nil
	}
	data, err := decodeBase64(text)
	if err != nil {
		return nil, true, fmt.Errorf("%w: bytes: %v", ErrPayloadInvalid, err)
	}
	return data, true, nil
}

func typedString(node any, typ string) (string, error) {
	switch value := node.(type) {
	case string:
		return value, nil
	case dvTyped:
		if text, ok := value.value.(string); ok && value.typ == typ {
			return text, nil
		}
	}
	return "", fmt.Errorf("%w: cannot decode %s as %s", ErrPayloadInvalid, nodeKind(node), typ)
}

func decodeTime(node any, v reflect.Value) error {
	text, err := typedString(node, "Date")
	if err != nil {
		return err
	}
	if text == "." {
		return fmt.Errorf("%w: invalid Date", ErrPayloadInvalid)
	}
	parsed, err := time.Parse(time.RFC3339Nano, text)
	if err != nil {
		return fmt.Errorf("%w: Date: %v", ErrPayloadInvalid, err)
	}
	v.Set(reflect.ValueOf(parsed))
	return nil
}

func decodeBigInt(node any) (*big.Int, error) {
	text, ok := numberText(node)
	if !ok {
		if s, isString := node.(string); isString {
			text, ok = s, true
		}
	}
	value, parsed := new(big.Int).SetString(text, 10)
	if !ok || !parsed {
		return nil, fmt.Errorf("%w: cannot decode %s as BigInt", ErrPayloadInvalid, nodeKind(node))
	}
	return value, nil
}

func decodeHeaders(node any) (http.Header, error) {
	typed, ok := node.(dvTyped)
	var entries []any
	switch {
	case ok && typed.typ == "Headers":
		entries, ok = typed.value.([]any)
	case !ok:
		if object, isObject := node.(*dvObject); isObject {
			header := http.Header{}
			for i, key := range object.keys {
				if text, isString := object.values[i].(string); isString {
					header.Add(key, text)
				}
			}
			return header, nil
		}
	}
	if !ok {
		return nil, fmt.Errorf("%w: cannot decode %s as Headers", ErrPayloadInvalid, nodeKind(node))
	}
	header := http.Header{}
	for _, entry := range entries {
		pair, isPair := entry.([]any)
		if !isPair || len(pair) != 2 {
			return nil, fmt.Errorf("%w: Headers entry", ErrPayloadInvalid)
		}
		name, nameOK := pair[0].(string)
		value, valueOK := pair[1].(string)
		if !nameOK || !valueOK {
			return nil, fmt.Errorf("%w: Headers entry", ErrPayloadInvalid)
		}
		header.Add(name, value)
	}
	return header, nil
}

func decodeMap(node any, v reflect.Value) error {
	t := v.Type()
	if v.IsNil() {
		v.Set(reflect.MakeMap(t))
	}
	switch value := node.(type) {
	case *dvObject:
		for i, key := range value.keys {
			keyValue := reflect.New(t.Key()).Elem()
			if err := decodeMapKey(key, keyValue); err != nil {
				return err
			}
			item := reflect.New(t.Elem()).Elem()
			if err := decodeNode(value.values[i], item); err != nil {
				return fmt.Errorf("%s: %w", key, err)
			}
			v.SetMapIndex(keyValue, item)
		}
		return nil
	case dvTyped:
		if value.typ != "Map" {
			break
		}
		pairs, _ := value.value.([]any)
		for _, entry := range pairs {
			pair, ok := entry.([]any)
			if !ok || len(pair) != 2 {
				return fmt.Errorf("%w: Map entry", ErrPayloadInvalid)
			}
			key := reflect.New(t.Key()).Elem()
			if err := decodeNode(pair[0], key); err != nil {
				return err
			}
			item := reflect.New(t.Elem()).Elem()
			if err := decodeNode(pair[1], item); err != nil {
				return err
			}
			v.SetMapIndex(key, item)
		}
		return nil
	}
	return fmt.Errorf("%w: cannot decode %s into %s", ErrPayloadInvalid, nodeKind(node), t)
}

func decodeMapKey(key string, v reflect.Value) error {
	if v.Kind() == reflect.String {
		v.SetString(key)
		return nil
	}
	if v.CanAddr() && v.Addr().Type().Implements(textUnmarshaler) {
		return v.Addr().Interface().(encoding.TextUnmarshaler).UnmarshalText([]byte(key))
	}
	return decodeNode(dvNumber(key), v)
}

// nodeAny returns node as the Go value an any holds: objects as
// map[string]any, arrays as []any, numbers as float64, and the mapped
// JavaScript types as their Go types.
func nodeAny(node any) (any, error) {
	switch value := node.(type) {
	case nil, dvUndefined:
		return nil, nil
	case bool, string:
		return value, nil
	case dvNumber:
		f, err := strconv.ParseFloat(string(value), 64)
		if err != nil {
			return nil, fmt.Errorf("%w: number %s", ErrPayloadInvalid, value)
		}
		return f, nil
	case dvSpecial:
		return specialFloat(value), nil
	case []any:
		items := make([]any, len(value))
		for i, item := range value {
			converted, err := nodeAny(item)
			if err != nil {
				return nil, err
			}
			items[i] = converted
		}
		return items, nil
	case *dvObject:
		object := make(map[string]any, len(value.keys))
		for i, key := range value.keys {
			if _, ok := value.values[i].(dvUndefined); ok {
				continue
			}
			converted, err := nodeAny(value.values[i])
			if err != nil {
				return nil, err
			}
			object[key] = converted
		}
		return object, nil
	case dvTyped:
		switch value.typ {
		case "Date":
			var t time.Time
			if err := decodeTime(value, reflect.ValueOf(&t).Elem()); err != nil {
				return nil, err
			}
			return t, nil
		case "BigInt":
			return decodeBigInt(value)
		case "URL":
			text, err := typedString(value, "URL")
			if err != nil {
				return nil, err
			}
			return url.Parse(text)
		case "Headers":
			return decodeHeaders(value)
		case "Map":
			pairs, _ := value.value.([]any)
			result := make(map[any]any, len(pairs))
			for _, entry := range pairs {
				pair, ok := entry.([]any)
				if !ok || len(pair) != 2 {
					return nil, fmt.Errorf("%w: Map entry", ErrPayloadInvalid)
				}
				key, err := nodeAny(pair[0])
				if err != nil {
					return nil, err
				}
				if key != nil && !reflect.TypeOf(key).Comparable() {
					return nil, fmt.Errorf("%w: Map key %T", ErrPayloadInvalid, key)
				}
				item, err := nodeAny(pair[1])
				if err != nil {
					return nil, err
				}
				result[key] = item
			}
			return result, nil
		case "Set":
			items, _ := value.value.([]any)
			return nodeAny(items)
		}
		if data, ok, err := bytesOf(value); ok {
			return data, err
		}
		if isErrorType(value.typ) {
			return decodeErrorNode(value)
		}
		inner, err := nodeAny(value.value)
		if err != nil {
			return nil, err
		}
		return PayloadReduced{Type: value.typ, Value: inner}, nil
	}
	return nil, fmt.Errorf("%w: unexpected node %T", ErrPayloadInvalid, node)
}

func isErrorType(typ string) bool {
	return typ == "Error" || strings.HasSuffix(typ, "Error") || typ == "DOMException"
}

// EncodeErrorPayload encodes err as an Error value.
func EncodeErrorPayload(err error, options PayloadOptions) ([]byte, error) {
	node, encodeErr := encodeErrorNode(err)
	if encodeErr != nil {
		return nil, encodeErr
	}
	var w dvWriter
	w.add(node)
	text := append([]byte{'['}, bytes.Join(w.values, []byte{','})...)
	data := labelPayload(PayloadDevalue, append(text, ']'))
	if !options.NoCompression {
		data = compressPayload(data)
	}
	return protectPayload(data, options.Keys, options.SealTo)
}

// DecodeErrorPayload decodes an error payload. A payload that cannot be
// read becomes an Error saying so.
func DecodeErrorPayload(data []byte, keys *RunKeys) *Error {
	if len(data) == 0 {
		return nil
	}
	var err error
	if decodeErr := DecodePayload(data, keys, &err); decodeErr != nil {
		return &Error{message: decodeErr.Error()}
	}
	if recorded, ok := err.(*Error); ok {
		return recorded
	}
	return NewError(err)
}

// EncryptionKeys is an optional World interface that resolves run keys. A
// run's payloads are encrypted with the 32-byte key it returns. A nil key, or
// a World that does not implement it, leaves them unencrypted. deploymentID
// is the run's deployment, and the same run must always resolve to the same
// key.
type EncryptionKeys interface {
	EncryptionKeyForRun(ctx context.Context, runID RunID, deploymentID string) ([]byte, error)
}

type runKeysCacheKey struct {
	provider EncryptionKeys
	runID    RunID
}

var (
	runKeysCache     sync.Map
	runKeysCacheSize atomic.Int64
)

// runKeysCacheMax bounds the run keys a process remembers.
const runKeysCacheMax = 10_000

// RunKeysFor returns the keys of a run's payloads from world, or nil when
// they are not encrypted. A run's keys are cached, since they never change.
func RunKeysFor(ctx context.Context, world any, runID RunID, deploymentID string) (*RunKeys, error) {
	provider, ok := world.(EncryptionKeys)
	if !ok {
		return nil, nil
	}
	cacheKey := runKeysCacheKey{provider, runID}
	if cached, ok := runKeysCache.Load(cacheKey); ok {
		return cached.(*RunKeys), nil
	}
	key, err := provider.EncryptionKeyForRun(ctx, runID, deploymentID)
	if err != nil {
		return nil, fmt.Errorf("workflow: encryption key of run %s: %w", runID, err)
	}
	var keys *RunKeys
	if key != nil {
		if keys, err = NewRunKeys(key); err != nil {
			return nil, err
		}
	}
	if runKeysCacheSize.Add(1) > runKeysCacheMax {
		runKeysCache.Clear()
		runKeysCacheSize.Store(1)
	}
	runKeysCache.Store(cacheKey, keys)
	return keys, nil
}

// ConfiguredKeyScope is the HKDF scope a World derives run keys in from a
// configured EncryptionKey.
const ConfiguredKeyScope = "go-workflow"

// CheckEncryptionKey validates a World's configured EncryptionKey: empty, or
// 32 bytes.
func CheckEncryptionKey(key []byte) error {
	if len(key) != 0 && len(key) != payloadKeySize {
		return fmt.Errorf("workflow: encryption key is %d bytes, want %d", len(key), payloadKeySize)
	}
	return nil
}

// ConfiguredRunKey returns the key of runID's payloads in a World configured
// with key, derived with DeriveRunKey in ConfiguredKeyScope, or nil when key
// is empty and payloads are not encrypted.
func ConfiguredRunKey(key []byte, runID RunID) ([]byte, error) {
	if len(key) == 0 {
		return nil, nil
	}
	return DeriveRunKey(key, ConfiguredKeyScope, runID)
}
