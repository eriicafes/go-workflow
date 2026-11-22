package postgresworld

import (
	"context"
	"errors"
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
// The notifications fn queues with notify are sent once it commits.
func (w *World) inTx(ctx context.Context, fn func(pgx.Tx) error) error {
	for attempt := 1; ; attempt++ {
		var tx *notifyingTx
		err := pgx.BeginFunc(ctx, w.pool, func(inner pgx.Tx) error {
			tx = &notifyingTx{Tx: inner}
			return fn(tx)
		})
		if err == nil {
			w.announce(ctx, tx.pending)
		}
		if err == nil || attempt == txAttempts || !retryable(err) {
			return err
		}
		timer := time.NewTimer(time.Duration(attempt*attempt) * 5 * time.Millisecond)
		select {
		case <-ctx.Done():
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

// keyset adds the condition continuing after cursor, the last returned ID,
// for rows ordered by id, ascending or descending.
func (q *query) keyset(cursor string, id string, descending bool) {
	if cursor == "" {
		return
	}
	operator := ">"
	if descending {
		operator = "<"
	}
	q.where(id + " " + operator + " " + q.arg(cursor))
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

// order returns the ORDER BY of rows listed by id. Run, step, and hook IDs
// end in a ULID, so it is the order they were minted in.
func order(id string, descending bool) string {
	if descending {
		return " ORDER BY " + id + " DESC"
	}
	return " ORDER BY " + id
}

// keysetPage trims the extra row fetched by query.limit and sets the cursor
// to the last returned row's ID, on the last page too.
func keysetPage[T any](values []T, limit int, id func(T) string) worlds.Page[T] {
	page := worlds.Page[T]{Data: values}
	if limit > 0 && len(values) > limit {
		page.Data, page.HasMore = values[:limit], true
	}
	if len(page.Data) > 0 {
		page.Cursor = id(page.Data[len(page.Data)-1])
	}
	return page
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
	if end > start {
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
