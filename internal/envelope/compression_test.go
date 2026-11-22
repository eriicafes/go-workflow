package envelope

import (
	"bytes"
	"crypto/rand"
	"errors"
	"testing"
)

func TestCompressKeepsOnlyWorthwhileSavings(t *testing.T) {
	large := Encode(PrefixDevalue, bytes.Repeat([]byte("a"), 2048))
	small := Encode(PrefixDevalue, []byte("[1]"))
	random := make([]byte, 2048)
	_, _ = rand.Read(random)
	random = Encode(PrefixDevalue, random)
	for _, test := range []struct {
		data  []byte
		codec Codec
		want  Prefix
	}{
		{large, CodecZstd, PrefixZstd},
		{large, CodecGzip, PrefixGzip},
		{large, CodecNone, PrefixDevalue},
		{small, CodecZstd, PrefixDevalue},
		{random, CodecZstd, PrefixDevalue},
	} {
		if got := Peek(Compress(test.data, test.codec)); got != test.want {
			t.Errorf("Compress(%d bytes, %q) prefix = %q, want %q", len(test.data), test.codec, got, test.want)
		}
	}
}

func TestDecompressionIsBounded(t *testing.T) {
	huge := Encode(PrefixDevalue, make([]byte, maxDecompressed))
	for _, codec := range []Codec{CodecZstd, CodecGzip} {
		compressed := Compress(huge, codec)
		if Peek(compressed) == PrefixDevalue {
			t.Fatalf("%s did not compress", codec)
		}
		if _, err := Unwrap(compressed, nil); !errors.Is(err, ErrInvalid) {
			t.Fatalf("%s: Unwrap of %d decompressed bytes = %v, want ErrInvalid", codec, len(huge), err)
		}
	}
}
