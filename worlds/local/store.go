package localworld

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"sort"
)

// readJSON rejects malformed records. Callers never replace corrupt data.
func readJSON(root *os.Root, name string, target any) error {
	raw, err := root.ReadFile(name)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(raw, target); err != nil {
		return fmt.Errorf("workflow/local: decode %s: %w", name, err)
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
	if directory, err := root.Open(dir); err == nil {
		defer directory.Close()
		_ = directory.Sync() // Some filesystems do not support syncing directories.
	}
	return nil
}

// listJSON lists direct JSON record names in deterministic order.
func listJSON(root *os.Root, directory string) ([]string, error) {
	file, err := root.Open(directory)
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
		if !entry.IsDir() && path.Ext(entry.Name()) == ".json" {
			result = append(result, entry.Name())
		}
	}
	sort.Strings(result)
	return result, nil
}

func remove(root *os.Root, name string) error {
	err := root.Remove(name)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}
