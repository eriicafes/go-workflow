package vercelworld

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	qstashJWTIssuer = "Upstash"
	qstashLeeway    = 30 * time.Second
)

// QStashConfig configures QStash to deliver work due more than seven days
// ahead, beyond the Vercel Queues delay limit. QStash holds such a delivery
// until it is due, then calls <Config.BaseURL>/callbacks/qstash, which
// HTTP(world) serves with a signature check, and that route publishes it to
// Vercel Queues. Config.BaseURL must be HTTPS.
type QStashConfig struct {
	// BaseURL is the QStash API root. Empty uses https://qstash.upstash.io.
	BaseURL string
	// Token authorizes publishing and cancelling QStash messages. It is
	// required.
	Token string
	// CurrentSigningKey and NextSigningKey verify QStash request signatures
	// across key rotation. Both are required.
	CurrentSigningKey string
	NextSigningKey    string
	// MaxDelay is the longest delay the QStash plan accepts. A longer timer
	// is delivered in steps of at most MaxDelay. Zero uses seven days.
	MaxDelay time.Duration
}

// defaultQStashMaxDelay is QStashConfig.MaxDelay when it is zero.
const defaultQStashMaxDelay = 7 * 24 * time.Hour

func (config QStashConfig) maxDelay() time.Duration {
	if config.MaxDelay > 0 {
		return config.MaxDelay
	}
	return defaultQStashMaxDelay
}

// publishQStash schedules messageID for delivery to the QStash route at due
// and returns QStash's message ID.
func (w *World) publishQStash(ctx context.Context, messageID string, due time.Time) (string, error) {
	body, err := json.Marshal(vqsMessage{ID: messageID})
	if err != nil {
		return "", err
	}
	config := w.config.QStash
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, config.BaseURL+"/v2/publish/"+w.qstashURL, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	request.Header.Set("Authorization", "Bearer "+config.Token)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Upstash-Not-Before", strconv.FormatInt((due.UnixMilli()+999)/1000, 10))
	request.Header.Set("Upstash-Deduplication-Id", messageID)
	response, err := w.http.Do(request)
	if err != nil {
		return "", fmt.Errorf("workflow/vercel: publish QStash timer: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return "", responseError("publish QStash timer", response)
	}
	var published struct {
		MessageID string `json:"messageId"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&published); err != nil {
		return "", fmt.Errorf("workflow/vercel: decode QStash timer: %w", err)
	}
	if published.MessageID == "" {
		return "", errors.New("workflow/vercel: QStash timer response has no message ID")
	}
	return published.MessageID, nil
}

// cancelQStash deletes a pending QStash message. An unknown message is
// already delivered or cancelled.
func (w *World) cancelQStash(ctx context.Context, qstashID string) error {
	config := w.config.QStash
	if config == nil {
		return errors.New("workflow/vercel: queue message has a QStash timer but QStash is not configured")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodDelete, config.BaseURL+"/v2/messages/"+url.PathEscape(qstashID), nil)
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+config.Token)
	response, err := w.http.Do(request)
	if err != nil {
		return fmt.Errorf("workflow/vercel: cancel QStash timer: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotFound || (response.StatusCode >= 200 && response.StatusCode < 300) {
		return nil
	}
	return responseError("cancel QStash timer", response)
}

// qstashDestination returns the public URL of the QStash route under
// baseURL, which QStash signs requests for.
func qstashDestination(baseURL string) (string, error) {
	if !strings.HasPrefix(baseURL, "https://") {
		return "", errors.New("workflow/vercel: QStash requires an HTTPS base URL")
	}
	return baseURL + qstashRouteSuffix, nil
}

// QStash verifies a QStash delivery and moves the named queue record
// onto Vercel Queues. A cancelled record is acknowledged without publishing.
// Config.QStash must be set. Mount it at POST <Config.BaseURL>/callbacks/qstash.
func QStash(world *World) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		defer request.Body.Close()
		body, err := io.ReadAll(http.MaxBytesReader(writer, request.Body, 1<<20))
		if err != nil {
			http.Error(writer, "invalid QStash message", http.StatusBadRequest)
			return
		}
		if err := verifyQStashSignature(request.Header.Get("Upstash-Signature"), body, world.qstashURL, world.config.QStash); err != nil {
			http.Error(writer, "invalid QStash signature", http.StatusUnauthorized)
			return
		}
		var message vqsMessage
		if err := json.Unmarshal(body, &message); err != nil || message.ID == "" {
			http.Error(writer, "invalid QStash message", http.StatusBadRequest)
			return
		}
		if err := world.releaseTimer(request.Context(), message.ID); err != nil {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
			return
		}
		writer.WriteHeader(http.StatusNoContent)
	})
}

func verifyQStashSignature(signature string, body []byte, destination string, config *QStashConfig) error {
	signature = strings.TrimSpace(signature)
	if signature == "" {
		return errors.New("workflow/vercel: missing QStash signature")
	}
	if verifyQStashToken(signature, body, destination, config.CurrentSigningKey) == nil {
		return nil
	}
	return verifyQStashToken(signature, body, destination, config.NextSigningKey)
}

func verifyQStashToken(signature string, body []byte, destination, signingKey string) error {
	parts := strings.Split(signature, ".")
	if len(parts) != 3 {
		return errors.New("workflow/vercel: malformed QStash signature")
	}
	var header struct {
		Algorithm string `json:"alg"`
	}
	if err := decodeQStashJWTPart(parts[0], &header); err != nil {
		return err
	}
	if header.Algorithm != "HS256" {
		return errors.New("workflow/vercel: unsupported QStash signature algorithm")
	}
	provided, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return fmt.Errorf("workflow/vercel: decode QStash signature: %w", err)
	}
	mac := hmac.New(sha256.New, []byte(signingKey))
	_, _ = mac.Write([]byte(parts[0] + "." + parts[1]))
	if !hmac.Equal(provided, mac.Sum(nil)) {
		return errors.New("workflow/vercel: invalid QStash signature")
	}
	var claims struct {
		Issuer    string `json:"iss"`
		Subject   string `json:"sub"`
		ExpiresAt int64  `json:"exp"`
		NotBefore int64  `json:"nbf"`
		Body      string `json:"body"`
	}
	if err := decodeQStashJWTPart(parts[1], &claims); err != nil {
		return err
	}
	now := time.Now()
	if claims.Issuer != qstashJWTIssuer || claims.Subject != destination ||
		!now.Add(-qstashLeeway).Before(time.Unix(claims.ExpiresAt, 0)) ||
		now.Add(qstashLeeway).Before(time.Unix(claims.NotBefore, 0)) {
		return errors.New("workflow/vercel: invalid QStash claims")
	}
	digest := sha256.Sum256(body)
	if strings.TrimRight(claims.Body, "=") != base64.RawURLEncoding.EncodeToString(digest[:]) {
		return errors.New("workflow/vercel: QStash body hash does not match")
	}
	return nil
}

func decodeQStashJWTPart(part string, value any) error {
	data, err := base64.RawURLEncoding.DecodeString(part)
	if err != nil {
		return fmt.Errorf("workflow/vercel: decode QStash token: %w", err)
	}
	if err = json.Unmarshal(data, value); err != nil {
		return fmt.Errorf("workflow/vercel: decode QStash token: %w", err)
	}
	return nil
}
