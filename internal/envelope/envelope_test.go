package envelope

import (
	"bytes"
	"compress/gzip"
	"errors"
	"testing"
)

func TestDecodeSplitsThePrefix(t *testing.T) {
	prefix, body, err := Decode([]byte("devl[1]"))
	if err != nil || prefix != PrefixDevalue || string(body) != "[1]" {
		t.Fatalf("Decode = %q %q %v", prefix, body, err)
	}
	for _, data := range []string{"dev", "DEVL[1]", "de-l[1]"} {
		if _, _, err := Decode([]byte(data)); !errors.Is(err, ErrInvalid) {
			t.Errorf("Decode(%q) err = %v, want ErrInvalid", data, err)
		}
		if got := Peek([]byte(data)); got != "" {
			t.Errorf("Peek(%q) = %q, want none", data, got)
		}
	}
}

func TestWrapRoundTripsThroughEveryLayer(t *testing.T) {
	keys, err := NewRunKeys(bytes.Repeat([]byte{7}, KeySize))
	if err != nil {
		t.Fatal(err)
	}
	text := []byte(`["` + string(bytes.Repeat([]byte("a"), 4096)) + `"]`)
	for _, test := range []struct {
		name    string
		options Options
		outer   Prefix
	}{
		{"plain", Options{}, PrefixDevalue},
		{"zstd", Options{Compression: CodecZstd}, PrefixZstd},
		{"gzip", Options{Compression: CodecGzip}, PrefixGzip},
		{"encrypted", Options{Keys: keys, Compression: CodecZstd}, PrefixEncrypted},
		{"sealed", Options{Keys: keys, SealTo: keys.PublicKey}, PrefixSealed},
	} {
		data, err := Wrap(text, test.options)
		if err != nil {
			t.Fatalf("%s: %v", test.name, err)
		}
		if got := Peek(data); got != test.outer {
			t.Fatalf("%s: outer prefix %q, want %q", test.name, got, test.outer)
		}
		got, err := Unwrap(data, keys)
		if err != nil || !bytes.Equal(got, text) {
			t.Fatalf("%s: Unwrap = %.20q err=%v", test.name, got, err)
		}
	}
}

func TestUnwrapNeedsKeysForCiphertext(t *testing.T) {
	keys, _ := NewRunKeys(bytes.Repeat([]byte{7}, KeySize))
	for _, options := range []Options{{Keys: keys}, {SealTo: keys.PublicKey}} {
		data, err := Wrap([]byte("[1]"), options)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := Unwrap(data, nil); !errors.Is(err, ErrInvalid) {
			t.Fatalf("Unwrap without keys err = %v, want ErrInvalid", err)
		}
	}
	if _, err := Unwrap([]byte("abcd[1]"), nil); !errors.Is(err, ErrInvalid) {
		t.Fatalf("unknown prefix err = %v", err)
	}
}

func TestFramesSplitAsAppended(t *testing.T) {
	chunk := AppendFrame(AppendFrame(nil, []byte("a")), []byte("bc"))
	frames, err := SplitFrames(chunk)
	if err != nil || len(frames) != 2 || string(frames[0]) != "a" || string(frames[1]) != "bc" {
		t.Fatalf("SplitFrames = %q err=%v", frames, err)
	}
	if _, err := SplitFrames(chunk[:len(chunk)-1]); !errors.Is(err, ErrInvalid) {
		t.Fatalf("truncated frame err = %v", err)
	}
}

func TestUnwrapBoundsNestedLayers(t *testing.T) {
	data := Encode(PrefixDevalue, []byte("[1]"))
	for range maxLayers - 1 {
		data = Encode(PrefixGzip, gzipped(t, data))
	}
	if _, err := Unwrap(data, nil); err != nil {
		t.Fatalf("Unwrap of %d layers = %v", maxLayers, err)
	}
	data = Encode(PrefixGzip, gzipped(t, data))
	if _, err := Unwrap(data, nil); !errors.Is(err, ErrInvalid) {
		t.Fatalf("Unwrap of %d layers = %v, want ErrInvalid", maxLayers+1, err)
	}
}

func gzipped(t *testing.T, data []byte) []byte {
	t.Helper()
	var buffer bytes.Buffer
	writer := gzip.NewWriter(&buffer)
	if _, err := writer.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}
