package envelope

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"slices"
)

const (
	nonceSize = 12
	// KeySize is the size of a run's key and of its public key.
	KeySize = 32

	scalarInfo     = "workflow/encp/x25519/v1"
	contentKeyInfo = "workflow/encp/aes256gcm/v1"
)

// DeriveRunKey returns a run's 32-byte key derived from a 32-byte key, such
// as a deployment key: HKDF-SHA256 with a zero salt and "{scope}|{runID}" as
// info.
func DeriveRunKey(key []byte, scope, runID string) ([]byte, error) {
	if len(key) != KeySize {
		return nil, fmt.Errorf("workflow: encryption key is %d bytes, want %d", len(key), KeySize)
	}
	return hkdf.Key(sha256.New, key, make([]byte, 32), scope+"|"+runID, KeySize)
}

// RunKeys holds what a run's key opens and seals data with: the key itself
// and the X25519 key pair derived from it.
type RunKeys struct {
	key     []byte
	private *ecdh.PrivateKey
	// PublicKey is the run's X25519 public key, which writers outside the
	// run seal data to.
	PublicKey []byte
}

// NewRunKeys returns the keys of a run's 32-byte key.
func NewRunKeys(runKey []byte) (*RunKeys, error) {
	if len(runKey) != KeySize {
		return nil, fmt.Errorf("workflow: run key is %d bytes, want %d", len(runKey), KeySize)
	}
	scalar, err := hkdf.Key(sha256.New, runKey, make([]byte, 32), scalarInfo, KeySize)
	if err != nil {
		return nil, err
	}
	private, err := ecdh.X25519().NewPrivateKey(scalar)
	if err != nil {
		return nil, err
	}
	return &RunKeys{key: slices.Clone(runKey), private: private, PublicKey: private.PublicKey().Bytes()}, nil
}

// Encrypt returns data encrypted with the run's key, behind encr.
func (k *RunKeys) Encrypt(data []byte) ([]byte, error) {
	aead, err := newAESGCM(k.key)
	if err != nil {
		return nil, err
	}
	sealed, err := sealAEAD(aead, data)
	if err != nil {
		return nil, err
	}
	return Encode(PrefixEncrypted, sealed), nil
}

func (k *RunKeys) decrypt(body []byte) ([]byte, error) {
	aead, err := newAESGCM(k.key)
	if err != nil {
		return nil, err
	}
	return openAEAD(aead, body)
}

func (k *RunKeys) open(body []byte) ([]byte, error) {
	if len(body) < KeySize {
		return nil, errors.New("sealed payload is too short")
	}
	ephemeral, err := ecdh.X25519().NewPublicKey(body[:KeySize])
	if err != nil {
		return nil, err
	}
	aead, err := sealedContentKey(k.private, ephemeral, body[:KeySize], k.PublicKey)
	if err != nil {
		return nil, err
	}
	return openAEAD(aead, body[KeySize:])
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
	if err != nil || len(key) != KeySize {
		return nil
	}
	return key
}

// Sealer seals data to a run's public key. Its data shares one key
// exchange, each with a fresh nonce, so a stream writer seals all its frames
// with one exchange.
type Sealer struct {
	ephemeral []byte
	aead      cipher.AEAD
}

// NewSealer returns a sealer for the run whose public key is given.
func NewSealer(publicKey []byte) (*Sealer, error) {
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
	return &Sealer{ephemeral: public, aead: aead}, nil
}

// Seal returns data sealed behind encp.
func (s *Sealer) Seal(data []byte) ([]byte, error) {
	sealed, err := sealAEAD(s.aead, data)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, prefixSize+len(s.ephemeral)+len(sealed))
	out = append(append(append(out, PrefixSealed...), s.ephemeral...), sealed...)
	return out, nil
}

func sealedContentKey(private *ecdh.PrivateKey, public *ecdh.PublicKey, ephemeral, recipient []byte) (cipher.AEAD, error) {
	shared, err := private.ECDH(public)
	if err != nil {
		return nil, err
	}
	info := contentKeyInfo + string(ephemeral) + string(recipient)
	key, err := hkdf.Key(sha256.New, shared, make([]byte, 32), info, KeySize)
	if err != nil {
		return nil, err
	}
	return newAESGCM(key)
}

func newAESGCM(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

func sealAEAD(aead cipher.AEAD, data []byte) ([]byte, error) {
	nonce := make([]byte, nonceSize, nonceSize+len(data)+aead.Overhead())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	return aead.Seal(nonce, nonce, data, nil), nil
}

func openAEAD(aead cipher.AEAD, body []byte) ([]byte, error) {
	if len(body) < nonceSize+aead.Overhead() {
		return nil, errors.New("encrypted payload is too short")
	}
	return aead.Open(nil, body[:nonceSize], body[nonceSize:], nil)
}
