package vercelworld

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/workflowops"
)

const qstashJWTIssuer = "Upstash"

func qstashWebhookEndpoint(webhookEndpoint string) (string, error) {
	parsed, err := url.Parse(webhookEndpoint)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" {
		return "", errors.New("workflow/vercel: QStash webhook endpoint must be an HTTPS URL")
	}
	if parsed.Path != "" && parsed.Path != "/" {
		return "", errors.New("workflow/vercel: QStash webhook endpoint must not include a path")
	}
	parsed.Path = ""
	parsed.RawPath = ""
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return strings.TrimRight(parsed.String(), "/"), nil
}

func qstashHandler(world *World, runners workflowops.RunnerSet) http.Handler {
	engine := workflowops.NewEngine(world, runners)
	config := *world.config.QStash
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		defer request.Body.Close()
		body, err := io.ReadAll(http.MaxBytesReader(writer, request.Body, 1<<20))
		if err != nil {
			http.Error(writer, "invalid QStash message", http.StatusBadRequest)
			return
		}
		if err := verifyQStashSignature(request.Header.Get("Upstash-Signature"), body, config); err != nil {
			http.Error(writer, "invalid QStash signature", http.StatusUnauthorized)
			return
		}
		var task workflowops.Task
		if err := json.Unmarshal(body, &task); err != nil || task.RunID == "" {
			http.Error(writer, "invalid QStash message", http.StatusBadRequest)
			return
		}
		lease, err := world.ClaimTask(request.Context(), task)
		if err == nil && lease != nil {
			err = engine.Execute(request.Context(), lease)
		}
		if err != nil && !errors.Is(err, workflowops.ErrCancelled) {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
			return
		}
		writer.WriteHeader(http.StatusNoContent)
	})
}

func verifyQStashSignature(signature string, body []byte, config QStashConfig) error {
	signature = strings.TrimSpace(signature)
	if signature == "" {
		return errors.New("workflow/vercel: missing QStash signature")
	}
	webhookURL := config.WebhookEndpoint + workflow.APIPrefix + "/webhook/qstash"
	if verifyQStashToken(signature, body, webhookURL, config.CurrentSigningKey) == nil {
		return nil
	}
	return verifyQStashToken(signature, body, webhookURL, config.NextSigningKey)
}

func verifyQStashToken(signature string, body []byte, webhookURL, signingKey string) error {
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
	now := time.Now().Unix()
	if claims.Issuer != qstashJWTIssuer ||
		claims.Subject != webhookURL ||
		claims.ExpiresAt <= now ||
		claims.NotBefore > now {
		return errors.New("workflow/vercel: invalid QStash claims")
	}
	digest := sha256.Sum256(body)
	if claims.Body != base64.URLEncoding.EncodeToString(digest[:]) {
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
