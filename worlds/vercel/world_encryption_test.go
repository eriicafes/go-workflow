package vercelworld

import (
	"bytes"
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

func TestEncryptionKeysMatchWorldVercel(t *testing.T) {
	deploymentKey := bytes.Repeat([]byte{1}, 32)
	t.Setenv("VERCEL", "1")
	t.Setenv("VERCEL_DEPLOYMENT_ID", "dpl_this")
	t.Setenv("VERCEL_DEPLOYMENT_KEY", base64.StdEncoding.EncodeToString(deploymentKey))
	t.Setenv("VERCEL_PROJECT_ID", "prj_1")
	t.Setenv("VERCEL_TOKEN", "")
	all, _, _ := newWorlds(t, 1)
	world := all[0]
	ctx := context.Background()

	// A run of this deployment derives its key from the deployment key.
	want, err := worlds.DeriveRunKey(deploymentKey, "prj_1", "wrun_1")
	if err != nil {
		t.Fatal(err)
	}
	for _, deploymentID := range []string{"", "dpl_this"} {
		if key, err := world.EncryptionKeyForRun(ctx, "wrun_1", deploymentID); err != nil || !bytes.Equal(key, want) {
			t.Fatalf("local key for %q = %x err=%v", deploymentID, key, err)
		}
	}

	// A run of another deployment asks Vercel's API, authorized by the OIDC
	// token, retrying while it is unavailable.
	shorten(t, &readRetryBase, time.Millisecond)
	other := bytes.Repeat([]byte{9}, 32)
	calls := 0
	api := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		calls++
		if calls == 1 {
			writer.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		if request.URL.Path != "/dpl_other" || request.URL.Query().Get("projectId") != "prj_1" || request.URL.Query().Get("runId") == "" || request.Header.Get("Authorization") != "Bearer test-oidc" {
			http.Error(writer, "unexpected request "+request.URL.String(), http.StatusBadRequest)
			return
		}
		if request.URL.Query().Get("runId") == "wrun_none" {
			_, _ = writer.Write([]byte(`{"key":null}`))
			return
		}
		_, _ = writer.Write([]byte(`{"key":"` + base64.StdEncoding.EncodeToString(other) + `"}`))
	}))
	defer api.Close()
	world.runKeyURL = api.URL + "/"
	if key, err := world.EncryptionKeyForRun(ctx, "wrun_1", "dpl_other"); err != nil || !bytes.Equal(key, other) || calls != 2 {
		t.Fatalf("fetched key = %x err=%v calls=%d", key, err, calls)
	}

	// Outside a function, a key asked for without a deployment is the one of
	// the deployment the run is pinned to.
	t.Setenv("VERCEL", "")
	pinned := worlds.NewRunID()
	if _, err := world.EventsCreateRun(ctx, worlds.EventsCreateRunInput{RunID: pinned, WorkflowName: "flow", Input: []byte(`[1]`), DeploymentID: "dpl_other"}); err != nil {
		t.Fatal(err)
	}
	calls = 1
	if key, err := world.EncryptionKeyForRun(ctx, pinned, ""); err != nil || !bytes.Equal(key, other) || calls != 2 {
		t.Fatalf("key of the run's deployment = %x err=%v calls=%d", key, err, calls)
	}
	// A run the backend does not have yet is this World's deployment's, and
	// without a deployment of its own the World has no key for it.
	own := world.config.DeploymentID
	world.config.DeploymentID = ""
	if key, err := world.EncryptionKeyForRun(ctx, worlds.NewRunID(), ""); err != nil || key != nil {
		t.Fatalf("key of a missing run without a deployment = %x err=%v", key, err)
	}
	world.config.DeploymentID = "dpl_other"
	calls = 1
	if key, err := world.EncryptionKeyForRun(ctx, worlds.NewRunID(), ""); err != nil || !bytes.Equal(key, other) || calls != 2 {
		t.Fatalf("key of a missing run = %x err=%v calls=%d, want the World's deployment's", key, err, calls)
	}
	world.config.DeploymentID = own
	t.Setenv("VERCEL", "1")

	// A rate limit is Vercel's answer, and is not asked about again.
	limited := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		calls++
		writer.WriteHeader(http.StatusTooManyRequests)
	}))
	defer limited.Close()
	calls, world.runKeyURL = 0, limited.URL+"/"
	if _, err := world.EncryptionKeyForRun(ctx, "wrun_1", "dpl_other"); err == nil || calls != 1 {
		t.Fatalf("rate limited key = %v after %d calls, want its failure after one", err, calls)
	}

	// Without a project, payloads are not encrypted.
	t.Setenv("VERCEL_PROJECT_ID", "")
	if key, err := world.EncryptionKeyForRun(ctx, "wrun_1", ""); err != nil || key != nil {
		t.Fatalf("key without project = %x err=%v", key, err)
	}
}
