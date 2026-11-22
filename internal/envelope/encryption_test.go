package envelope

import (
	"bytes"
	"testing"
)

func TestRunKeysAreDerivedFromTheRunKey(t *testing.T) {
	key := bytes.Repeat([]byte{1}, KeySize)
	first, err := DeriveRunKey(key, "scope", "wrun_1")
	if err != nil {
		t.Fatal(err)
	}
	second, _ := DeriveRunKey(key, "scope", "wrun_2")
	if bytes.Equal(first, second) {
		t.Fatal("two runs derived the same key")
	}
	if _, err := DeriveRunKey(key[:16], "scope", "wrun_1"); err == nil {
		t.Fatal("derived from a short key")
	}
	keys, err := NewRunKeys(first)
	if err != nil {
		t.Fatal(err)
	}
	again, _ := NewRunKeys(first)
	if !bytes.Equal(keys.PublicKey, again.PublicKey) {
		t.Fatal("public key is not stable")
	}
	if got := DecodePublicKey(EncodePublicKey(keys.PublicKey)); !bytes.Equal(got, keys.PublicKey) {
		t.Fatalf("public key round trip = %x", got)
	}
	if DecodePublicKey("short") != nil {
		t.Fatal("decoded an invalid public key")
	}
}

func TestASealerOpensOnlyWithItsRun(t *testing.T) {
	keys, _ := NewRunKeys(bytes.Repeat([]byte{1}, KeySize))
	other, _ := NewRunKeys(bytes.Repeat([]byte{2}, KeySize))
	sealer, err := NewSealer(keys.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := sealer.Seal(Encode(PrefixDevalue, []byte("[1]")))
	if err != nil {
		t.Fatal(err)
	}
	if got, err := Unwrap(sealed, keys); err != nil || string(got) != "[1]" {
		t.Fatalf("Unwrap = %q err=%v", got, err)
	}
	if _, err := Unwrap(sealed, other); err == nil {
		t.Fatal("another run opened the sealed data")
	}
}
