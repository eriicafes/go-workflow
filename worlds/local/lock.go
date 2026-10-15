package localworld

import (
	"errors"
	"os"
	"syscall"
)

// ErrInUse reports that another World holds the storage directory.
var ErrInUse = errors.New("workflow/local: storage is already in use")

const lockName = "locks/world.lock"

// acquireLock takes a non-blocking exclusive flock on locks/world.lock. The
// lock is released by the kernel when the process exits.
func acquireLock(root *os.Root) (*os.File, error) {
	if err := root.MkdirAll("locks", 0700); err != nil {
		return nil, err
	}
	file, err := root.OpenFile(lockName, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = file.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, ErrInUse
		}
		return nil, err
	}
	return file, nil
}

func releaseLock(file *os.File) {
	if file != nil {
		_ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
		_ = file.Close()
	}
}
