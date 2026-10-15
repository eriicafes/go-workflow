package localworld

import (
	"errors"
	"testing"
)

func TestLockIsExclusive(t *testing.T) {
	t.Parallel()
	root := openRoot(t)
	first, err := acquireLock(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := acquireLock(root); !errors.Is(err, ErrInUse) {
		t.Fatalf("second acquire = %v, want ErrInUse", err)
	}
	releaseLock(first)
	second, err := acquireLock(root)
	if err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
	releaseLock(second)
	releaseLock(nil)
}
