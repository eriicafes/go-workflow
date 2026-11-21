package localworld

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
)

// storeVersion is the version of the storage layout. A directory written
// with another version is refused.
const storeVersion = 1

const versionName = "version.json"

type versionRecord struct {
	Version int `json:"version"`
}

// validateVersion initializes an empty directory and rejects any directory
// written by another storage format.
func validateVersion(root *os.Root) error {
	var version versionRecord
	err := readJSON(root, versionName, &version)
	if errors.Is(err, fs.ErrNotExist) {
		entries, err := listEntries(root, ".", true)
		if err != nil {
			return err
		}
		files, err := listJSON(root, ".")
		if err != nil {
			return err
		}
		for _, name := range append(entries, files...) {
			if name != "locks" {
				return fmt.Errorf("workflow/local: storage has no %s but contains %q, use an empty directory instead", versionName, name)
			}
		}
		return writeJSON(root, versionName, versionRecord{Version: storeVersion})
	}
	if err != nil {
		return err
	}
	if version.Version != storeVersion {
		return fmt.Errorf("workflow/local: incompatible storage version %d (want %d), remove the directory to start over", version.Version, storeVersion)
	}
	return nil
}
