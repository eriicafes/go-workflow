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
)

func TestVerifyQStashSignature(t *testing.T) {
	config := &QStashConfig{CurrentSigningKey: "current", NextSigningKey: "next"}
	destination := "https://worker.example.com/v1/workflow/qstash"
	body := []byte(`{"id":"01ID"}`)
	if err := verifyQStashSignature(qstashTestSignature(t, body, destination, "current"), body, destination, config); err != nil {
		t.Fatalf("verify current signature: %v", err)
	}
	if err := verifyQStashSignature(qstashTestSignature(t, body, destination, "next"), body, destination, config); err != nil {
		t.Fatalf("verify next signature: %v", err)
	}
	if err := verifyQStashSignature(qstashTestSignature(t, body, destination, "other"), body, destination, config); err == nil {
		t.Fatal("verify accepted an unknown key")
	}
	if err := verifyQStashSignature(qstashTestSignature(t, body, destination, "current"), []byte("{}"), destination, config); err == nil {
		t.Fatal("verify accepted a signature for a different body")
	}
	if err := verifyQStashSignature(qstashTestSignature(t, body, "https://other.example.com/v1/workflow/qstash", "current"), body, destination, config); err == nil {
		t.Fatal("verify accepted a signature for a different URL")
	}
	if err := verifyQStashSignature(qstashTestSignatureAt(t, body, destination, "current", time.Now().Add(-time.Hour)), body, destination, config); err == nil {
		t.Fatal("verify accepted an expired signature")
	}
	if err := verifyQStashSignature("", body, destination, config); err == nil {
		t.Fatal("verify accepted a missing signature")
	}
}

func TestQStashDestination(t *testing.T) {
	destination, err := qstashDestination("https://worker.example.com/?region=iad1#timer", "https://api.example.com/v1/workflow")
	if err != nil || destination != "https://worker.example.com/v1/workflow/qstash" {
		t.Fatalf("destination = %q %v", destination, err)
	}
	if _, err := qstashDestination("https://worker.example.com/workflow", "https://api.example.com/v1/workflow"); err == nil {
		t.Fatal("webhook endpoint path was accepted")
	}
	if _, err := qstashDestination("http://worker.example.com", "https://api.example.com/v1/workflow"); err == nil {
		t.Fatal("HTTP webhook endpoint was accepted")
	}
	destination, err = qstashDestination("", "https://api.example.com/v1/workflow")
	if err != nil || destination != "https://api.example.com/v1/workflow/qstash" {
		t.Fatalf("default destination = %q %v", destination, err)
	}
	if _, err := qstashDestination("", "http://localhost/v1/workflow"); err == nil {
		t.Fatal("HTTP base URL was accepted for QStash")
	}
}

func TestHTTPMountsQStashHandlerOnlyWhenConfigured(t *testing.T) {
	server := newRecorder(t, nil)
	world, _ := newQueueWorld(t, server, true)
	response := httptest.NewRecorder()
	HTTP(world).ServeHTTP(response, httptest.NewRequest(http.MethodPost, qstashRoutePath, nil))
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusUnauthorized)
	}
	world, _ = newQueueWorld(t, server, false)
	response = httptest.NewRecorder()
	HTTP(world).ServeHTTP(response, httptest.NewRequest(http.MethodPost, qstashRoutePath, nil))
	if response.Code != http.StatusNotFound {
		t.Fatalf("unconfigured status = %d, want %d", response.Code, http.StatusNotFound)
	}
}

func TestHTTPPrefixMountsQStashHandlerUnderPrefix(t *testing.T) {
	server := newRecorder(t, nil)
	world, _ := newQueueWorld(t, server, true)
	handler := HTTPPrefix(world, "/internal/workflow/")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/internal/workflow/qstash", nil))
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusUnauthorized)
	}
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, qstashRoutePath, nil))
	if response.Code != http.StatusNotFound {
		t.Fatalf("default route status = %d, want %d", response.Code, http.StatusNotFound)
	}
}

func qstashTestSignature(t *testing.T, body []byte, destination, key string) string {
	return qstashTestSignatureAt(t, body, destination, key, time.Now())
}

func qstashTestSignatureAt(t *testing.T, body []byte, destination, key string, now time.Time) string {
	t.Helper()
	header, err := json.Marshal(map[string]string{"alg": "HS256", "typ": "JWT"})
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(body)
	claims, err := json.Marshal(map[string]any{
		"iss":  "Upstash",
		"sub":  destination,
		"exp":  now.Add(time.Minute).Unix(),
		"nbf":  now.Add(-time.Minute).Unix(),
		"body": base64.URLEncoding.EncodeToString(digest[:]),
	})
	if err != nil {
		t.Fatal(err)
	}
	signingInput := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(claims)
	mac := hmac.New(sha256.New, []byte(key))
	_, _ = mac.Write([]byte(signingInput))
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}
