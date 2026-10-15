package localworld

import (
	"errors"
	"io/fs"
	"os"
	"slices"
	"strings"
	"testing"
)

func TestWriteAndReadJSON(t *testing.T) {
	t.Parallel()
	root := openRoot(t)
	if err := writeJSON(root, "a/b/record.json", tokenRecord{HookID: "hook_a"}); err != nil {
		t.Fatal(err)
	}
	var record tokenRecord
	if err := readJSON(root, "a/b/record.json", &record); err != nil || record.HookID != "hook_a" {
		t.Fatalf("record = %#v err=%v", record, err)
	}
	if _, err := root.Stat("a/b/record.json.tmp"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("temporary file left behind: %v", err)
	}
	if err := writeJSON(root, "a/b/record.json", tokenRecord{HookID: "hook_b"}); err != nil {
		t.Fatal(err)
	}
	if err := readJSON(root, "a/b/record.json", &record); err != nil || record.HookID != "hook_b" {
		t.Fatalf("overwritten record = %#v err=%v", record, err)
	}
}

func TestReadJSONIsStrict(t *testing.T) {
	t.Parallel()
	root := openRoot(t)
	for name, content := range map[string]string{
		"malformed.json": `{"hookId":`,
		"unknown.json":   `{"hookId":"h","extra":1}`,
		"trailing.json":  `{"hookId":"h"} {}`,
	} {
		if err := root.WriteFile(name, []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
		var record tokenRecord
		if err := readJSON(root, name, &record); err == nil || !strings.HasPrefix(err.Error(), "workflow/local: decode") {
			t.Fatalf("%s: err = %v", name, err)
		}
	}
	var record tokenRecord
	if err := readJSON(root, "missing.json", &record); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("missing: err = %v", err)
	}
}

func TestListEntries(t *testing.T) {
	t.Parallel()
	root := openRoot(t)
	if names, err := listJSON(root, "missing"); err != nil || names != nil {
		t.Fatalf("missing dir = %v err=%v", names, err)
	}
	for _, name := range []string{"d/b.json", "d/a.json", "d/c.json.tmp", "d/sub/x.json"} {
		if err := writeJSON(root, name, 1); err != nil {
			t.Fatal(err)
		}
	}
	if err := root.WriteFile("d/c.json.tmp", nil, 0600); err != nil {
		t.Fatal(err)
	}
	names, err := listJSON(root, "d")
	if err != nil || !slices.Equal(names, []string{"a.json", "b.json"}) {
		t.Fatalf("listJSON = %v err=%v", names, err)
	}
	dirs, err := listDirs(root, "d")
	if err != nil || !slices.Equal(dirs, []string{"sub"}) {
		t.Fatalf("listDirs = %v err=%v", dirs, err)
	}
	if err := remove(root, "d/a.json"); err != nil {
		t.Fatal(err)
	}
	if err := remove(root, "d/a.json"); err != nil {
		t.Fatalf("remove missing: %v", err)
	}
	if ok, err := exists(root, "d/a.json"); ok || err != nil {
		t.Fatalf("exists after remove = %v err=%v", ok, err)
	}
}

func TestFileKey(t *testing.T) {
	t.Parallel()
	if got := fileKey("Ab/../c"); got != "41622f2e2e2f63" {
		t.Fatalf("fileKey = %q", got)
	}
	if fileKey("A") == fileKey("a") {
		t.Fatal("keys differ only by case")
	}
	long := strings.Repeat("x", 65)
	if got := fileKey(long); !strings.HasPrefix(got, "h") || len(got) != 65 || got == fileKey(long+"y") {
		t.Fatalf("long fileKey = %q", got)
	}
	if got := fileKey(strings.Repeat("x", 64)); len(got) != 128 {
		t.Fatalf("boundary fileKey length = %d", len(got))
	}
}

func TestValidateVersion(t *testing.T) {
	t.Parallel()
	root := openRoot(t)
	if err := root.Mkdir("locks", 0700); err != nil {
		t.Fatal(err)
	}
	if err := validateVersion(root); err != nil {
		t.Fatalf("empty directory: %v", err)
	}
	var version versionRecord
	if err := readJSON(root, versionName, &version); err != nil || version.Version != storeVersion {
		t.Fatalf("version = %#v err=%v", version, err)
	}
	if err := validateVersion(root); err != nil {
		t.Fatalf("current version: %v", err)
	}
	if err := writeJSON(root, versionName, versionRecord{Version: 1}); err != nil {
		t.Fatal(err)
	}
	if err := validateVersion(root); err == nil || !strings.Contains(err.Error(), "incompatible storage version 1") {
		t.Fatalf("old version: %v", err)
	}
	unversioned := openRoot(t)
	if err := unversioned.Mkdir("runs", 0700); err != nil {
		t.Fatal(err)
	}
	if err := validateVersion(unversioned); err == nil {
		t.Fatal("unversioned non-empty directory accepted")
	}
}

func openRoot(t *testing.T) *os.Root {
	t.Helper()
	root, err := os.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = root.Close() })
	return root
}
