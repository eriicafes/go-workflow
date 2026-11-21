package vercelworld

import (
	"bytes"
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"testing"

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
	other := bytes.Repeat([]byte{9}, 32)
	calls := 0
	api := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		calls++
		if calls == 1 {
			writer.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		if request.URL.Path != "/dpl_other" || request.URL.Query().Get("projectId") != "prj_1" || request.URL.Query().Get("runId") != "wrun_1" || request.Header.Get("Authorization") != "Bearer test-oidc" {
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

	// Without a project, payloads are not encrypted.
	t.Setenv("VERCEL_PROJECT_ID", "")
	if key, err := world.EncryptionKeyForRun(ctx, "wrun_1", ""); err != nil || key != nil {
		t.Fatalf("key without project = %x err=%v", key, err)
	}
}
