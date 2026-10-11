package vercelworld

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow"
)

func TestVerifyQStashSignature(t *testing.T) {
	config := QStashConfig{
		WebhookEndpoint:   "https://worker.example.com",
		CurrentSigningKey: "current",
		NextSigningKey:    "next",
	}
	body := []byte(`{"runId":"run_1","sequence":2}`)
	webhookURL := config.WebhookEndpoint + workflow.APIPrefix + "/webhook/qstash"
	if err := verifyQStashSignature(qstashTestSignature(t, body, webhookURL, "current"), body, config); err != nil {
		t.Fatalf("verify current signature: %v", err)
	}
	if err := verifyQStashSignature(qstashTestSignature(t, body, webhookURL, "next"), body, config); err != nil {
		t.Fatalf("verify next signature: %v", err)
	}
	if err := verifyQStashSignature(qstashTestSignature(t, body, webhookURL, "current"), []byte("{}"), config); err == nil {
		t.Fatal("verify accepted a signature for a different body")
	}
}

func TestQStashWebhookEndpoint(t *testing.T) {
	webhookEndpoint, err := qstashWebhookEndpoint("https://worker.example.com/?region=iad1#timer")
	if err != nil {
		t.Fatal(err)
	}
	if webhookEndpoint != "https://worker.example.com" {
		t.Fatalf("webhook endpoint = %q, want https://worker.example.com", webhookEndpoint)
	}
	if _, err := qstashWebhookEndpoint("https://worker.example.com/workflow"); err == nil {
		t.Fatal("QStash webhook endpoint path was accepted")
	}
}

func TestHTTPMountsQStashHandler(t *testing.T) {
	world := &World{config: Config{QStash: &QStashConfig{
		WebhookEndpoint:   "https://worker.example.com",
		CurrentSigningKey: "current",
		NextSigningKey:    "next",
	}}}
	request := httptest.NewRequest(http.MethodPost, workflow.APIPrefix+"/webhook/qstash", nil)
	response := httptest.NewRecorder()
	HTTP(world, nil).ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusUnauthorized)
	}
}

func qstashTestSignature(t *testing.T, body []byte, webhookURL, key string) string {
	t.Helper()
	header, err := json.Marshal(map[string]string{"alg": "HS256", "typ": "JWT"})
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(body)
	now := time.Now()
	claims, err := json.Marshal(map[string]any{
		"iss":  "Upstash",
		"sub":  webhookURL,
		"exp":  now.Add(time.Minute).Unix(),
		"nbf":  now.Add(-time.Minute).Unix(),
		"body": base64.URLEncoding.EncodeToString(digest[:]),
	})
	if err != nil {
		t.Fatal(err)
	}
	signingInput := base64.RawURLEncoding.EncodeToString(header) + "." +
		base64.RawURLEncoding.EncodeToString(claims)
	mac := hmac.New(sha256.New, []byte(key))
	_, _ = mac.Write([]byte(signingInput))
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}
