// Package envelope wraps serialized data in the layers the Workflow SDK
// stores it in, each marked by a 4-byte format prefix. Prefixes nest:
// devalue text (devl), optionally compressed (zstd or gzip), optionally
// encrypted with a run's key (encr) or sealed to a run's public key (encp).
// The package reads no configuration of its own; callers choose compression
// and keys.
package envelope

import (
	"encoding/binary"
	"fmt"

	"github.com/eriicafes/go-workflow/internal/devalue"
)

// Prefix is a format prefix.
type Prefix string

// Format prefixes.
const (
	PrefixDevalue   Prefix = "devl"
	PrefixEncrypted Prefix = "encr"
	PrefixSealed    Prefix = "encp"
	PrefixGzip      Prefix = "gzip"
	PrefixZstd      Prefix = "zstd"
)

// prefixSize is the length of a format prefix.
const prefixSize = 4

// maxFrame bounds a stream frame.
const maxFrame = 100_000_000

// maxLayers bounds the layers Unwrap removes: encryption, compression, and
// devalue text.
const maxLayers = 3

// ErrInvalid reports data that cannot be decoded. It is devalue's, so one
// sentinel covers every layer of a payload.
var ErrInvalid = devalue.ErrInvalid

// Encode returns body behind prefix.
func Encode(prefix Prefix, body []byte) []byte {
	return append([]byte(prefix), body...)
}

// Decode splits data into its prefix and body.
func Decode(data []byte) (Prefix, []byte, error) {
	if len(data) < prefixSize {
		return "", nil, fmt.Errorf("%w: %d bytes is too short for a format prefix", ErrInvalid, len(data))
	}
	prefix := string(data[:prefixSize])
	for _, c := range prefix {
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') {
			return "", nil, fmt.Errorf("%w: no format prefix", ErrInvalid)
		}
	}
	return Prefix(prefix), data[prefixSize:], nil
}

// Peek returns data's outer prefix, or "" when it has none.
func Peek(data []byte) Prefix {
	if prefix, _, err := Decode(data); err == nil {
		return prefix
	}
	return ""
}

// Options says how Wrap protects and compresses devalue text. SealTo takes
// precedence over Keys; with neither, the data is not encrypted.
type Options struct {
	// Keys encrypts the data with the run's key (encr).
	Keys *RunKeys
	// SealTo seals the data to a run's public key (encp).
	SealTo []byte
	// Compression is the codec to compress with. The zero value does not
	// compress.
	Compression Codec
}

// Wrap returns devalue text behind its prefixes: devl, then compressed with
// options.Compression, then encrypted or sealed.
func Wrap(text []byte, options Options) ([]byte, error) {
	data := Compress(Encode(PrefixDevalue, text), options.Compression)
	switch {
	case options.SealTo != nil:
		sealer, err := NewSealer(options.SealTo)
		if err != nil {
			return nil, err
		}
		return sealer.Seal(data)
	case options.Keys != nil:
		return options.Keys.Encrypt(data)
	}
	return data, nil
}

// Unwrap removes data's encryption and compression and returns its devalue
// text, opening it with keys when it is encrypted or sealed.
func Unwrap(data []byte, keys *RunKeys) ([]byte, error) {
	for layer := 0; ; layer++ {
		if layer == maxLayers {
			return nil, fmt.Errorf("%w: payload nests more than %d layers", ErrInvalid, maxLayers)
		}
		prefix, body, err := Decode(data)
		if err != nil {
			return nil, err
		}
		switch prefix {
		case PrefixDevalue:
			return body, nil
		case PrefixEncrypted:
			if keys == nil {
				return nil, fmt.Errorf("%w: payload is encrypted and no key is available", ErrInvalid)
			}
			data, err = keys.decrypt(body)
		case PrefixSealed:
			if keys == nil {
				return nil, fmt.Errorf("%w: payload is sealed and no key is available", ErrInvalid)
			}
			data, err = keys.open(body)
		case PrefixZstd:
			data, err = unzstd(body)
		case PrefixGzip:
			data, err = gunzip(body)
		default:
			return nil, fmt.Errorf("%w: unknown payload format %q", ErrInvalid, prefix)
		}
		if err != nil {
			return nil, fmt.Errorf("%w: %s: %v", ErrInvalid, prefix, err)
		}
	}
}

// AppendFrame appends data to frames as one frame of an object stream: a
// big-endian u32 length, then data.
func AppendFrame(frames, data []byte) []byte {
	return append(binary.BigEndian.AppendUint32(frames, uint32(len(data))), data...)
}

// SplitFrames returns the data of the frames a stream chunk holds.
func SplitFrames(chunk []byte) ([][]byte, error) {
	var frames [][]byte
	for len(chunk) > 0 {
		if len(chunk) < 4 {
			return nil, fmt.Errorf("%w: truncated stream frame", ErrInvalid)
		}
		size := binary.BigEndian.Uint32(chunk)
		if size > maxFrame || int(size) > len(chunk)-4 {
			return nil, fmt.Errorf("%w: stream frame of %d bytes", ErrInvalid, size)
		}
		frames = append(frames, chunk[4:4+size])
		chunk = chunk[4+size:]
	}
	return frames, nil
}
