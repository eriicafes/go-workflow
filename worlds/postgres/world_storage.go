package postgresworld

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const txAttempts = 5

var errInvalidCursor = errors.New("workflow/postgres: invalid cursor")

// inTx runs fn in a transaction and retries serialization failures and
// deadlocks, which Postgres may raise between concurrent row-locking writers.
func (w *World) inTx(ctx context.Context, fn func(pgx.Tx) error) error {
	for attempt := 1; ; attempt++ {
		err := pgx.BeginFunc(ctx, w.pool, fn)
		if err == nil || attempt == txAttempts || !retryable(err) {
			return err
		}
		timer := time.NewTimer(time.Duration(attempt*attempt) * 5 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// query accumulates WHERE conditions and positional arguments.
type query struct {
	conditions []string
	args       []any
}

func (q *query) arg(value any) string {
	q.args = append(q.args, value)
	return "$" + strconv.Itoa(len(q.args))
}

func (q *query) where(condition string) { q.conditions = append(q.conditions, condition) }

func (q *query) clause() string {
	if len(q.conditions) == 0 {
		return ""
	}
	return " WHERE " + strings.Join(q.conditions, " AND ")
}

// limit returns a LIMIT clause fetching one extra row to detect HasMore.
func (q *query) limit(limit int) string {
	if limit <= 0 {
		return ""
	}
	return " LIMIT " + q.arg(limit+1)
}

// keyset adds the condition continuing after cursor for rows ordered by
// (created_at, id), ascending or descending.
func (q *query) keyset(cursor string, createdAt, id string, descending bool) error {
	if cursor == "" {
		return nil
	}
	at, key, err := decodeCursor(cursor)
	if err != nil {
		return err
	}
	operator := ">"
	if descending {
		operator = "<"
	}
	q.where(fmt.Sprintf("(%s, %s) %s (%s, %s)", createdAt, id, operator, q.arg(at), q.arg(key)))
	return nil
}

func retryable(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && (pgErr.Code == "40001" || pgErr.Code == "40P01")
}

// now returns the current time at the microsecond precision Postgres stores.
func now() time.Time { return time.Now().UTC().Truncate(time.Microsecond) }

func utc(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	result := value.UTC()
	return &result
}

func order(createdAt, id string, descending bool) string {
	if descending {
		return " ORDER BY " + createdAt + " DESC, " + id + " DESC"
	}
	return " ORDER BY " + createdAt + ", " + id
}

func encodeCursor(at time.Time, id string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.FormatInt(at.UnixMicro(), 10) + ":" + id))
}

func decodeCursor(cursor string) (time.Time, string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return time.Time{}, "", errInvalidCursor
	}
	micros, id, ok := strings.Cut(string(raw), ":")
	value, err := strconv.ParseInt(micros, 10, 64)
	if !ok || err != nil {
		return time.Time{}, "", errInvalidCursor
	}
	return time.UnixMicro(value).UTC(), id, nil
}

// keysetPage trims the extra row fetched by query.limit and sets the cursor
// from the last returned row.
func keysetPage[T any](values []T, limit int, key func(T) (time.Time, string)) worlds.Page[T] {
	if limit <= 0 || len(values) <= limit {
		return worlds.Page[T]{Data: values}
	}
	values = values[:limit]
	at, id := key(values[limit-1])
	return worlds.Page[T]{Data: values, HasMore: true, Cursor: encodeCursor(at, id)}
}

// offsetPage pages an in-memory slice with a decimal offset cursor.
func offsetPage[T any](values []T, pagination worlds.Pagination) (worlds.Page[T], error) {
	start := 0
	if pagination.Cursor != "" {
		value, err := strconv.Atoi(pagination.Cursor)
		if err != nil || value < 0 {
			return worlds.Page[T]{}, errInvalidCursor
		}
		start = min(value, len(values))
	}
	end := len(values)
	if pagination.Limit > 0 {
		end = min(start+pagination.Limit, len(values))
	}
	result := worlds.Page[T]{Data: values[start:end], HasMore: end < len(values)}
	if result.HasMore {
		result.Cursor = strconv.Itoa(end)
	}
	return result, nil
}

func collect[T any](rows pgx.Rows, scan func(pgx.Row) (T, error)) ([]T, error) {
	defer rows.Close()
	values := []T{}
	for rows.Next() {
		value, err := scan(rows)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, rows.Err()
}
