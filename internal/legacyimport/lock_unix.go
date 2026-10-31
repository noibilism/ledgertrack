//go:build !windows

package legacyimport

import (
	"fmt"
	"os"
	"syscall"
)

// lockFile takes an exclusive advisory lock so two import processes cannot
// share one state file. The lock disappears with the process, so a crash never
// leaves a stale lock behind.
func lockFile(path string) (func(), error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600) //nolint:gosec // path comes from the operator/CLI, not from request input
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("another ledgertrack-import process is using this state file (%s): %w", path, err)
	}
	return func() {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		_ = f.Close()
	}, nil
}
