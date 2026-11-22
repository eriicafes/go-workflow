package worlds

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/eriicafes/go-workflow/internal/devalue"
	"github.com/eriicafes/go-workflow/internal/envelope"
)

// Payloads are encoded as the Workflow SDK encodes them, so SDK tooling reads
// a Go run's data as its own: devalue text in the envelope internal/envelope
// writes, optionally compressed and encrypted with the run's keys. Values map
// onto devalue's JavaScript types, and errors are recorded with sdkerror.

// ErrPayloadInvalid reports a payload that cannot be decoded.
var ErrPayloadInvalid = devalue.ErrInvalid

// PayloadMarshaler is implemented by a type that chooses its own payload
// encoding: MarshalPayload returns the value to encode in its place.
type PayloadMarshaler = devalue.Marshaler

// PayloadUnmarshaler is implemented by a type that decodes its own payload:
// UnmarshalPayload calls decode with a pointer to decode the payload into.
type PayloadUnmarshaler = devalue.Unmarshaler

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

// formatOptions returns options as envelope.Options, compressing with the
// codec the environment chooses unless options turn compression off.
func (options PayloadOptions) formatOptions() envelope.Options {
	formatted := envelope.Options{Keys: options.Keys, SealTo: options.SealTo}
	if !options.NoCompression {
		formatted.Compression = compressionCodec()
	}
	return formatted
}

// compressionCodec returns the codec payloads are compressed with: zstd, or
// gzip with WORKFLOW_COMPRESSION_CODEC=gzip. WORKFLOW_DISABLE_COMPRESSION=1
// turns compression off.
func compressionCodec() envelope.Codec {
	switch {
	case os.Getenv("WORKFLOW_DISABLE_COMPRESSION") == "1":
		return envelope.CodecNone
	case os.Getenv("WORKFLOW_COMPRESSION_CODEC") == "gzip":
		return envelope.CodecGzip
	}
	return envelope.CodecZstd
}

// Payloads

// EncodePayload encodes value as a payload.
func EncodePayload(value any, options PayloadOptions) ([]byte, error) {
	data, err := devalue.Marshal(value)
	if err != nil {
		return nil, err
	}
	return EncodeDevaluePayload(data, options)
}

// EncodeDevaluePayload encodes devalue text, as devalue.Marshal returns it, as
// a payload, for a value encoded before its run's keys are known.
func EncodeDevaluePayload(data []byte, options PayloadOptions) ([]byte, error) {
	return envelope.Wrap(data, options.formatOptions())
}

// DecodePayload decodes a payload into value, a pointer, opening it with
// keys when it is encrypted or sealed. An empty payload leaves value as it is.
func DecodePayload(data []byte, keys *RunKeys, value any) error {
	if len(data) == 0 {
		return nil
	}
	text, err := envelope.Unwrap(data, keys)
	if err != nil {
		return err
	}
	return devalue.Unmarshal(text, value)
}

// EncodeErrorPayload encodes err as an Error value.
func EncodeErrorPayload(err error, options PayloadOptions) ([]byte, error) {
	text, encodeErr := devalue.MarshalError(err)
	if encodeErr != nil {
		return nil, encodeErr
	}
	return envelope.Wrap(text, options.formatOptions())
}

// DecodeErrorPayload decodes an error payload. A payload that cannot be
// read becomes an Error saying so.
func DecodeErrorPayload(data []byte, keys *RunKeys) *Error {
	if len(data) == 0 {
		return nil
	}
	var err error
	if decodeErr := DecodePayload(data, keys, &err); decodeErr != nil {
		return &Error{Message: decodeErr.Error()}
	}
	if recorded, ok := err.(*Error); ok {
		return recorded
	}
	return NewError(err)
}

// Streams

// EncodeStreamFrame returns value as one frame of an object stream: a
// big-endian u32 length, then value's payload, encrypted with keys or sealed
// with sealer when set. A Go stream writer's chunk is a []byte, a Uint8Array.
// Stream frames are never compressed.
func EncodeStreamFrame(value any, keys *RunKeys, sealer *envelope.Sealer) ([]byte, error) {
	text, err := devalue.Marshal(value)
	if err != nil {
		return nil, err
	}
	payload := envelope.Encode(envelope.PrefixDevalue, text)
	switch {
	case sealer != nil:
		payload, err = sealer.Seal(payload)
	case keys != nil:
		payload, err = keys.Encrypt(payload)
	}
	if err != nil {
		return nil, err
	}
	return envelope.AppendFrame(make([]byte, 0, 4+len(payload)), payload), nil
}

// SplitStreamFrames returns the payloads of the frames a stored stream chunk
// holds, for DecodePayload.
func SplitStreamFrames(chunk []byte) ([][]byte, error) { return envelope.SplitFrames(chunk) }

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

// Keys

// RunKeys holds what a run's key opens and seals payloads with: the key
// itself and the X25519 key pair derived from it.
type RunKeys = envelope.RunKeys

// NewRunKeys returns the keys of a run's 32-byte key.
func NewRunKeys(runKey []byte) (*RunKeys, error) { return envelope.NewRunKeys(runKey) }

// DeriveRunKey returns a run's 32-byte key derived from a 32-byte key, such
// as a deployment key: HKDF-SHA256 with a zero salt and "{scope}|{runID}" as
// info.
func DeriveRunKey(key []byte, scope string, runID RunID) ([]byte, error) {
	return envelope.DeriveRunKey(key, scope, string(runID))
}

// CheckEncryptionKey validates a World's configured EncryptionKey: empty, or
// 32 bytes.
func CheckEncryptionKey(key []byte) error {
	if len(key) != 0 && len(key) != envelope.KeySize {
		return fmt.Errorf("workflow: encryption key is %d bytes, want %d", len(key), envelope.KeySize)
	}
	return nil
}

// RunKeysFor returns the keys of a run's payloads from world, or nil when
// they are not encrypted. A run's keys are cached, since they never change.
func RunKeysFor(ctx context.Context, world World, runID RunID, deploymentID string) (*RunKeys, error) {
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

// runKeysCacheKey is a run's keys' entry in runKeysCache, by the World that
// resolved them.
type runKeysCacheKey struct {
	provider EncryptionKeys
	runID    RunID
}

// runKeysCache holds resolved run keys, and runKeysCacheSize counts its
// entries.
var (
	runKeysCache     sync.Map
	runKeysCacheSize atomic.Int64
)

// runKeysCacheMax bounds the run keys a process remembers.
const runKeysCacheMax = 10_000
