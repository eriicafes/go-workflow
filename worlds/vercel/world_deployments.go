package vercelworld

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

const (
	// latestDeploymentTTL is how long a World reuses the latest deployment it
	// read. Chains of runs hand off far less often, so a stale answer delays a
	// chain's move to a new deployment by one handoff at most.
	latestDeploymentTTL = time.Hour

	// latestDeploymentTimeout bounds one request to the deployment route.
	latestDeploymentTimeout = 5 * time.Second
)

var _ worlds.LatestDeployments = (*World)(nil)

// LatestDeploymentID returns the deployment the production domain serves, by
// requesting the deployment route at Config.BaseURL, which Vercel routes to
// the current production deployment and HTTP or Deployment must serve there.
// It returns "" outside a production deployment, so preview and custom
// environment runs stay on the deployment that starts them, and when the
// request fails, which it logs. The result is reused for an hour.
func (w *World) LatestDeploymentID(ctx context.Context) (string, error) {
	if w.latestURL == "" {
		return "", nil
	}
	w.latest.Lock()
	defer w.latest.Unlock()
	if w.latestID != "" && time.Since(w.latestAt) < latestDeploymentTTL {
		return w.latestID, nil
	}
	id, err := w.fetchLatestDeployment(ctx)
	if err != nil {
		log.Printf("workflow/vercel: find the latest deployment at %s: %v", w.latestURL, err)
		return "", nil
	}
	w.latestID, w.latestAt = id, time.Now()
	return id, nil
}

func (w *World) fetchLatestDeployment(ctx context.Context) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, latestDeploymentTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, w.latestURL, nil)
	if err != nil {
		return "", err
	}
	response, err := w.http.Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 256))
	if err != nil {
		return "", err
	}
	id := strings.TrimSpace(string(body))
	if response.StatusCode != http.StatusOK || id == "" {
		return "", fmt.Errorf("unexpected response %s: %q", response.Status, id)
	}
	return id, nil
}
