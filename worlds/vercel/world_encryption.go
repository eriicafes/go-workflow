package vercelworld

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

// Run keys are resolved as the Workflow SDK's Vercel World resolves them.
// Inside a Vercel Function, a run of this deployment derives its key from the
// deployment's key, VERCEL_DEPLOYMENT_KEY. A run of another deployment, or
// any run read from outside a function, asks Vercel's API for its key.

// runKeyURL is the Vercel API route that answers with a run's key.
const runKeyURL = "https://api.vercel.com/v1/workflow/run-key/"

// runKeyAttempts is how often a run-key request is sent while Vercel answers
// 429 or 5xx.
const runKeyAttempts = 4

var _ worlds.EncryptionKeys = (*World)(nil)

// EncryptionKeyForRun returns the key of a run's payloads, or nil when they
// are not encrypted, as the Workflow SDK's Vercel World does. With no project
// ID, from Config.ProjectID or VERCEL_PROJECT_ID, payloads are not encrypted.
func (w *World) EncryptionKeyForRun(ctx context.Context, runID worlds.RunID, deploymentID string) ([]byte, error) {
	projectID := w.config.ProjectID
	if projectID == "" {
		projectID = os.Getenv("VERCEL_PROJECT_ID")
	}
	if projectID == "" {
		return nil, nil
	}
	if os.Getenv("VERCEL") == "1" && (deploymentID == "" || deploymentID == os.Getenv("VERCEL_DEPLOYMENT_ID")) {
		encoded := os.Getenv("VERCEL_DEPLOYMENT_KEY")
		if encoded == "" {
			return nil, nil
		}
		key, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			return nil, fmt.Errorf("workflow/vercel: VERCEL_DEPLOYMENT_KEY: %w", err)
		}
		return worlds.DeriveRunKey(key, projectID, runID)
	}
	if deploymentID == "" {
		return nil, nil
	}
	return w.fetchRunKey(ctx, deploymentID, projectID, runID)
}

// fetchRunKey asks Vercel's API for a run's key, authorized by Config.Token,
// VERCEL_TOKEN, or the deployment's OIDC token.
func (w *World) fetchRunKey(ctx context.Context, deploymentID, projectID string, runID worlds.RunID) ([]byte, error) {
	token := w.config.Token
	if token == "" {
		token = os.Getenv("VERCEL_TOKEN")
	}
	if token == "" {
		var err error
		if token, err = w.config.OIDCToken(ctx); err != nil {
			return nil, fmt.Errorf("workflow/vercel: run key needs an OIDC token or VERCEL_TOKEN: %w", err)
		}
	}
	query := url.Values{"projectId": {projectID}, "runId": {string(runID)}}
	if w.config.TeamID != "" {
		query.Set("teamId", w.config.TeamID)
	}
	base := w.runKeyURL
	if base == "" {
		base = runKeyURL
	}
	target := base + url.PathEscape(deploymentID) + "?" + query.Encode()
	delay := 200 * time.Millisecond
	for attempt := 1; ; attempt++ {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
		if err != nil {
			return nil, err
		}
		request.Header.Set("Authorization", "Bearer "+token)
		response, err := w.http.Do(request)
		if err != nil {
			return nil, fmt.Errorf("workflow/vercel: fetch run key of %s: %w", runID, err)
		}
		body, _ := io.ReadAll(io.LimitReader(response.Body, 1<<16))
		_ = response.Body.Close()
		retry := response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500
		if retry && attempt < runKeyAttempts {
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
			}
			delay *= 2
			continue
		}
		if response.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("workflow/vercel: fetch run key of %s from deployment %s: %s: %s", runID, deploymentID, response.Status, body)
		}
		var answer struct {
			Key *string `json:"key"`
		}
		if err := json.Unmarshal(body, &answer); err != nil {
			return nil, fmt.Errorf("workflow/vercel: run key answer: %w", err)
		}
		if answer.Key == nil {
			return nil, nil
		}
		return base64.StdEncoding.DecodeString(*answer.Key)
	}
}
