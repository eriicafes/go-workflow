package localworld

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// databaseFile is the World's database in its data directory, as
// world-local names it.
const databaseFile = "world.db"

// busyTimeout is how long a write waits for another connection, in this
// process or another, to release the database.
const busyTimeout = 30 * time.Second

// schemaVersion is PRAGMA user_version once the schema is created. A
// database with a higher version was written by a newer release and is
// refused.
const schemaVersion = 1

// schema is world-local's schema (db/schema.js), without the snapshots
// table, which only dynamic workflow code uses. A stream's chunks are keyed by
// their run as well, since a stream's name is unique only within its run.
const schema = `
CREATE TABLE runs (
  run_id TEXT PRIMARY KEY,
  tag TEXT,
  workflow_name TEXT NOT NULL,
  status TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  seq INTEGER NOT NULL DEFAULT 0,
  seq_in_band INTEGER NOT NULL DEFAULT 1,
  doc TEXT NOT NULL
);
CREATE INDEX runs_by_created ON runs (created_at, run_id);
CREATE INDEX runs_by_status ON runs (status, created_at);

CREATE TABLE events (
  run_id TEXT NOT NULL,
  seq INTEGER NOT NULL,
  event_type TEXT NOT NULL,
  correlation_id TEXT,
  doc BLOB NOT NULL,
  PRIMARY KEY (run_id, seq)
) WITHOUT ROWID;
CREATE INDEX events_by_correlation ON events (run_id, correlation_id, seq);

CREATE TABLE steps (
  run_id TEXT NOT NULL,
  step_id TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  doc TEXT NOT NULL,
  PRIMARY KEY (run_id, step_id)
) WITHOUT ROWID;

CREATE TABLE waits (
  run_id TEXT NOT NULL,
  wait_id TEXT NOT NULL,
  doc TEXT NOT NULL,
  PRIMARY KEY (run_id, wait_id)
) WITHOUT ROWID;

CREATE TABLE hooks (
  hook_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  token TEXT NOT NULL,
  tag TEXT,
  created_at INTEGER NOT NULL,
  doc TEXT NOT NULL
);
CREATE INDEX hooks_by_run ON hooks (run_id);
CREATE INDEX hooks_by_token ON hooks (token);
CREATE INDEX hooks_by_created ON hooks (created_at, hook_id);

CREATE TABLE hook_disposals (
  hook_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  force_claimed_by TEXT
);
CREATE INDEX hook_disposals_by_run ON hook_disposals (run_id);

CREATE TABLE hook_tokens (
  token TEXT PRIMARY KEY,
  hook_id TEXT NOT NULL,
  run_id TEXT NOT NULL,
  retention_until INTEGER
);
CREATE INDEX hook_tokens_by_run ON hook_tokens (run_id);

CREATE TABLE hook_resumes (
  run_id TEXT NOT NULL,
  resume_id TEXT NOT NULL,
  hook_id TEXT NOT NULL,
  seq INTEGER NOT NULL,
  payload_digest TEXT,
  PRIMARY KEY (run_id, resume_id)
) WITHOUT ROWID;

CREATE TABLE start_hook_rejections (
  run_id TEXT PRIMARY KEY,
  tag TEXT,
  token TEXT NOT NULL,
  conflicting_run_id TEXT NOT NULL
);

CREATE TABLE stream_chunks (
  run_id TEXT NOT NULL,
  name TEXT NOT NULL,
  idx INTEGER NOT NULL,
  tag TEXT,
  data BLOB,
  eof INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (run_id, name, idx)
) WITHOUT ROWID;
`

// database is the World's SQLite database: one connection for writes, which
// serializes this process's writers and holds BEGIN IMMEDIATE transactions,
// and a pool for reads, each a snapshot of the WAL.
type database struct {
	write *sql.DB
	read  *sql.DB
}

// openDatabase opens dir's database, configures it as world-local does, and
// creates its schema when it has none.
func openDatabase(ctx context.Context, dir string) (*database, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("workflow/local: create data directory: %w", err)
	}
	file := filepath.Join(dir, databaseFile)
	write, err := sql.Open("sqlite", dsn(file, "immediate"))
	if err != nil {
		return nil, err
	}
	write.SetMaxOpenConns(1)
	db := &database{write: write}
	if err := db.configure(ctx); err != nil {
		_ = write.Close()
		return nil, err
	}
	if db.read, err = sql.Open("sqlite", dsn(file, "deferred")); err != nil {
		_ = write.Close()
		return nil, err
	}
	db.read.SetMaxOpenConns(8)
	return db, nil
}

// dsn returns the connection string of file, with the pragmas every
// connection sets and the given transaction locking.
func dsn(file, lock string) string {
	query := url.Values{}
	for _, pragma := range []string{
		fmt.Sprintf("busy_timeout(%d)", busyTimeout.Milliseconds()),
		"journal_size_limit(67108864)",
		"synchronous(NORMAL)",
		"secure_delete(ON)",
		"foreign_keys(ON)",
	} {
		query.Add("_pragma", pragma)
	}
	query.Set("_txlock", lock)
	return "file:" + file + "?" + query.Encode()
}

// configure sets the database's persistent modes and creates its schema.
// Incremental vacuum is set before the schema exists, and WAL mode and the
// vacuum mode are set only when missing: setting either takes a lock.
func (db *database) configure(ctx context.Context) error {
	conn, err := db.write.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	var vacuum int
	if err := conn.QueryRowContext(ctx, "PRAGMA auto_vacuum").Scan(&vacuum); err != nil {
		return err
	}
	if vacuum != 2 {
		if _, err := conn.ExecContext(ctx, "PRAGMA auto_vacuum = INCREMENTAL"); err != nil {
			return err
		}
	}
	var mode string
	if err := conn.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&mode); err != nil {
		return err
	}
	if !strings.EqualFold(mode, "wal") {
		if err := whileBusy(ctx, func() error {
			_, err := conn.ExecContext(ctx, "PRAGMA journal_mode = WAL")
			return err
		}); err != nil {
			return err
		}
	}
	return migrate(ctx, conn)
}

// migrate creates the schema of an empty database under the write lock,
// re-reading the version there: another process may have created it.
func migrate(ctx context.Context, conn *sql.Conn) error {
	version := func() (int, error) {
		var value int
		err := conn.QueryRowContext(ctx, "PRAGMA user_version").Scan(&value)
		if err == nil && value > schemaVersion {
			err = fmt.Errorf("workflow/local: the database was written by a newer release (schema %d, this release knows %d); upgrade, or delete the data directory to start over", value, schemaVersion)
		}
		return value, err
	}
	if current, err := version(); err != nil || current == schemaVersion {
		return err
	}
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return err
	}
	current, err := version()
	if err == nil && current < schemaVersion {
		if _, err = conn.ExecContext(ctx, schema); err == nil {
			_, err = conn.ExecContext(ctx, fmt.Sprintf("PRAGMA user_version = %d", schemaVersion))
		}
	}
	if err != nil {
		_, _ = conn.ExecContext(ctx, "ROLLBACK")
		return err
	}
	_, err = conn.ExecContext(ctx, "COMMIT")
	return err
}

func (db *database) close() error {
	var err error
	if db.read != nil {
		err = db.read.Close()
	}
	return errors.Join(err, db.write.Close())
}

// update runs fn in a write transaction, which holds the database's write
// lock from its start, and commits it when fn returns nil.
func (db *database) update(ctx context.Context, fn func(*sql.Tx) error) error {
	return whileBusy(ctx, func() error {
		tx, err := db.write.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		if err := fn(tx); err != nil {
			_ = tx.Rollback()
			return err
		}
		return tx.Commit()
	})
}

// view runs fn in a read transaction, a snapshot of the database.
func (db *database) view(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := db.read.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	return fn(tx)
}

// whileBusy retries fn while the database is busy past its busy timeout's
// handler, such as a lock SQLite does not wait for, for up to busyTimeout.
func whileBusy(ctx context.Context, fn func() error) error {
	deadline := time.Now().Add(busyTimeout)
	for attempt := 0; ; attempt++ {
		err := fn()
		if !isBusy(err) || time.Now().After(deadline) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(min(50*time.Millisecond, time.Millisecond<<min(attempt, 6))):
		}
	}
}

func isBusy(err error) bool {
	var sqliteErr *sqlite.Error
	if errors.As(err, &sqliteErr) {
		code := sqliteErr.Code() & 0xff
		return code == sqlite3.SQLITE_BUSY || code == sqlite3.SQLITE_LOCKED
	}
	return false
}

// maintain checkpoints the WAL into the database and returns freed pages to
// the file system, after a purge deleted data.
func (db *database) maintain(ctx context.Context) {
	_, _ = db.write.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)")
	_, _ = db.write.ExecContext(ctx, "PRAGMA incremental_vacuum")
}

// sqlTx is a transaction of the database.
type sqlTx = sql.Tx

// clearTag deletes a tag's runs, everything they own, and its hooks and
// streams, or every row when tag is empty.
func clearTag(ctx context.Context, tx *sqlTx, tag string) error {
	exec := func(query string, args ...any) error {
		_, err := tx.ExecContext(ctx, query, args...)
		return err
	}
	if tag == "" {
		for _, table := range []string{"events", "steps", "waits", "hooks", "hook_disposals", "hook_tokens", "hook_resumes", "start_hook_rejections", "stream_chunks", "runs"} {
			if err := exec("DELETE FROM " + table); err != nil {
				return err
			}
		}
		return nil
	}
	const runs = "SELECT run_id FROM runs WHERE tag = ?"
	for _, table := range []string{"events", "steps", "waits", "hook_disposals", "hook_resumes"} {
		if err := exec("DELETE FROM "+table+" WHERE run_id IN ("+runs+")", tag); err != nil {
			return err
		}
	}
	for _, statement := range []string{
		"DELETE FROM hook_tokens WHERE run_id IN (" + runs + ") OR hook_id IN (SELECT hook_id FROM hooks WHERE tag = ?)",
		"DELETE FROM hooks WHERE tag = ?",
		"DELETE FROM start_hook_rejections WHERE tag = ?",
		"DELETE FROM stream_chunks WHERE tag = ?",
		"DELETE FROM runs WHERE tag = ?",
	} {
		args := []any{tag}
		if strings.Count(statement, "?") == 2 {
			args = append(args, tag)
		}
		if err := exec(statement, args...); err != nil {
			return err
		}
	}
	return nil
}

// hookRetentionLimit returns the cap on a hook's token retention:
// configured, WORKFLOW_LOCAL_HOOK_RETENTION_LIMIT_DAYS, or 30 days.
func hookRetentionLimit(configured time.Duration) (time.Duration, error) {
	if configured > 0 {
		return configured, nil
	}
	if raw := os.Getenv("WORKFLOW_LOCAL_HOOK_RETENTION_LIMIT_DAYS"); raw != "" {
		days, err := strconv.ParseFloat(raw, 64)
		if err != nil || days <= 0 {
			return 0, errors.New("workflow/local: WORKFLOW_LOCAL_HOOK_RETENTION_LIMIT_DAYS must be a positive number")
		}
		return time.Duration(days * float64(24*time.Hour)), nil
	}
	return 30 * 24 * time.Hour, nil
}
