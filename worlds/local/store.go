package localworld

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"sort"
)

// readJSON rejects malformed records and unknown fields. Callers never
// replace corrupt data.
func readJSON(root *os.Root, name string, target any) error {
	raw, err := root.ReadFile(name)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("workflow/local: decode %s: %w", name, err)
	}
	if decoder.More() {
		return fmt.Errorf("workflow/local: decode %s: trailing data", name)
	}
	return nil
}

// writeJSON writes a synced temporary sibling then atomically renames it.
func writeJSON(root *os.Root, name string, value any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	dir := path.Dir(name)
	if dir != "." {
		if err := root.MkdirAll(dir, 0700); err != nil {
			return err
		}
	}
	tmp := name + ".tmp"
	file, err := root.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(raw)
	if writeErr == nil {
		writeErr = file.Sync()
	}
	closeErr := file.Close()
	if writeErr != nil {
		_ = root.Remove(tmp)
		return writeErr
	}
	if closeErr != nil {
		_ = root.Remove(tmp)
		return closeErr
	}
	if err := root.Rename(tmp, name); err != nil {
		return err
	}
	return syncDir(root, dir)
}

func syncDir(root *os.Root, dir string) error {
	directory, err := root.Open(dir)
	if err != nil {
		return err
	}
	defer directory.Close()
	_ = directory.Sync() // Some filesystems do not support syncing directories.
	return nil
}

// listJSON lists direct JSON record names in deterministic order. A missing
// directory has no records.
func listJSON(root *os.Root, directory string) ([]string, error) {
	return listEntries(root, directory, false)
}

// listDirs lists direct subdirectory names in deterministic order.
func listDirs(root *os.Root, directory string) ([]string, error) {
	return listEntries(root, directory, true)
}

func listEntries(root *os.Root, directory string, dirs bool) ([]string, error) {
	file, err := root.Open(directory)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()
	entries, err := file.ReadDir(-1)
	if err != nil {
		return nil, err
	}
	result := make([]string, 0, len(entries))
	for _, entry := range entries {
		if dirs && entry.IsDir() || !dirs && !entry.IsDir() && path.Ext(entry.Name()) == ".json" {
			result = append(result, entry.Name())
		}
	}
	sort.Strings(result)
	return result, nil
}

func exists(root *os.Root, name string) (bool, error) {
	_, err := root.Stat(name)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	return err == nil, err
}

func remove(root *os.Root, name string) error {
	err := root.Remove(name)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}

// fileKey turns a user-controlled name into a portable, case-insensitive-safe
// file name. Long names are hashed, while records keep the original name.
func fileKey(value string) string {
	encoded := hex.EncodeToString([]byte(value))
	if len(encoded) <= 128 {
		return encoded
	}
	sum := sha256.Sum256([]byte(value))
	return "h" + hex.EncodeToString(sum[:])
}
