package localworld

import (
	"testing"
)

// TestStreamChunksAllowNoRun checks a chunk row needs no run: the column is
// nullable, and the stream is still read by its name.
func TestStreamChunksAllowNoRun(t *testing.T) {
	t.Parallel()
	world := storageWorld(t, Config{})
	if required := scalar[int](t, world, `SELECT "notnull" FROM pragma_table_info('stream_chunks') WHERE name = 'run_id'`); required != 0 {
		t.Fatal("stream_chunks.run_id is NOT NULL")
	}
	if _, err := world.db.write.ExecContext(t.Context(), "INSERT INTO stream_chunks (name, idx, run_id, data, eof) VALUES ('orphan', 1, NULL, x'61', 0)"); err != nil {
		t.Fatal(err)
	}
	var count int
	var closed bool
	if err := world.read(t.Context(), func(s *store) error {
		var err error
		count, closed, err = s.streamState("orphan")
		return err
	}); err != nil || count != 1 || closed {
		t.Fatalf("stream without a run = %d chunks closed=%v err=%v", count, closed, err)
	}
}
