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

	"github.com/eriicafes/go-workflow/worlds"
)

// Inside a Vercel Function, a run of this deployment derives its key from the
// deployment's key, VERCEL_DEPLOYMENT_KEY. A run of another deployment, or
// any run read from outside a function, asks Vercel's API for its key.

// runKeyURL is the Vercel API route that answers with a run's key.
const runKeyURL = "https://api.vercel.com/v1/workflow/run-key/"

var _ worlds.EncryptionKeys = (*World)(nil)

// EncryptionKeyForRun returns the key of a run's payloads, or nil when they
// are not encrypted. With no project ID, from Config.ProjectID or
// VERCEL_PROJECT_ID, payloads are not encrypted.
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
	// The request is sent again as a read of the workflow API is: after a
	// failed connection or a 500, 502, 503 or 504, up to maxReadRetries times.
	// A redirect is not followed, so the token stays with Vercel's API.
	for retry := 0; ; retry++ {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
		if err != nil {
			return nil, err
		}
		request.Header.Set("Authorization", "Bearer "+token)
		response, err := w.api.http.Do(request)
		var body []byte
		wait := min(readRetryBase<<min(retry, 16), readRetryMax)
		if err == nil {
			body, _ = io.ReadAll(io.LimitReader(response.Body, 1<<16))
			_ = response.Body.Close()
			switch response.StatusCode {
			case http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
				err = fmt.Errorf("%s: %s", response.Status, body)
				if after := retryAfter(response.Header.Get("Retry-After")); after > 0 {
					wait = min(after, readRetryMax)
				}
			}
		}
		if err != nil {
			if retry >= maxReadRetries || ctx.Err() != nil || sleep(ctx, wait) != nil {
				return nil, fmt.Errorf("workflow/vercel: fetch run key of %s from deployment %s: %w", runID, deploymentID, err)
			}
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
