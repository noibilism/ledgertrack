//go:build windows

package legacyimport

import (
	"fmt"
	"os"
)

// lockFile uses exclusive creation on Windows. Delete the .lock file by hand
// if a previous process was killed.
func lockFile(path string) (func(), error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("state lock %s is held (delete it if no other import is running): %w", path, err)
	}
	return func() {
		f.Close()
		os.Remove(path)
	}, nil
}
