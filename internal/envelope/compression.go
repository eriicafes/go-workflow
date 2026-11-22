package envelope

import (
	"bytes"
	"compress/gzip"
	"io"

	"github.com/klauspost/compress/zstd"
)

// Codec is a compression codec.
type Codec string

// Compression codecs. CodecNone, the zero value, does not compress.
const (
	CodecNone Codec = ""
	CodecZstd Codec = "zstd"
	CodecGzip Codec = "gzip"
)

const (
	// compressMin is the smallest data worth compressing.
	compressMin = 1024
	// compressSavings is the share of its size compressed data must save to
	// be kept.
	compressSavings = 0.05
)

// Compress compresses prefixed data with codec: data of at least 1 KiB,
// kept only when that saves at least 5%. zstd is written at level 3, and
// falls back to gzip when no zstd encoder can be made.
func Compress(data []byte, codec Codec) []byte {
	if codec == CodecNone || len(data) < compressMin {
		return data
	}
	prefix, compressed := Prefix(""), []byte(nil)
	if codec == CodecZstd {
		if encoder := newZstdEncoder(); encoder != nil {
			prefix, compressed = PrefixZstd, encoder.EncodeAll(data, nil)
			_ = encoder.Close()
		}
	}
	if compressed == nil {
		var buffer bytes.Buffer
		writer, _ := gzip.NewWriterLevel(&buffer, gzip.DefaultCompression)
		_, _ = writer.Write(data)
		_ = writer.Close()
		prefix, compressed = PrefixGzip, buffer.Bytes()
	}
	if float64(prefixSize+len(compressed)) >= float64(len(data))*(1-compressSavings) {
		return data
	}
	return Encode(prefix, compressed)
}

// newZstdEncoder returns an encoder at zstd level 3, or nil if one cannot be
// made. It is made for the call, as unzstd's decoder is, since an encoder's
// channels cannot be shared across synctest bubbles.
func newZstdEncoder() *zstd.Encoder {
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault), zstd.WithEncoderConcurrency(1))
	if err != nil {
		return nil
	}
	return encoder
}

// unzstd decompresses zstd data. Its decoder is made for the call, so no
// goroutine outlives it.
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
