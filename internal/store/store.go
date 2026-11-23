// Package store is the PostgreSQL persistence layer. PostgreSQL is both the
// source of truth and the durable work queue (claimed with FOR UPDATE SKIP LOCKED),
// so a notification is enqueued in the same transaction that creates it.
package store

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/keremsirtikizil/insider-notification-system/internal/domain"
)

var (
	// ErrNotFound is returned when the requested row does not exist.
	ErrNotFound = errors.New("not found")
	// ErrInvalidState is returned when a state transition is not allowed
	// (e.g. cancelling a notification that was already sent).
	ErrInvalidState = errors.New("invalid state transition")
	// ErrDuplicate is returned when a unique constraint is violated.
	ErrDuplicate = errors.New("already exists")
	// ErrLeaseLost is returned when a worker finishes an attempt for a row it no
	// longer owns (its lease expired and the row was recovered).
	ErrLeaseLost = errors.New("processing lease lost")
)

// IdempotencyConflictError is returned when an idempotency key is reused with a
// different payload.
type IdempotencyConflictError struct {
	Index int
	Key   string
}

func (e *IdempotencyConflictError) Error() string {
	return fmt.Sprintf("idempotency key %q was already used with a different payload", e.Key)
}

// Store implements persistence on top of a pgx connection pool.
type Store struct {
	pool *pgxpool.Pool
}

// New creates a Store.
func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// Ping checks database connectivity (readiness probe).
func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

var notificationColumnList = []string{
	"id", "batch_id", "idempotency_key", "correlation_id", "trace_parent", "channel",
	"recipient", "content", "priority", "status", "attempts", "template_id",
	"scheduled_at", "next_attempt_at", "provider_message_id", "last_error",
	"created_at", "updated_at", "sent_at", "failed_at", "cancelled_at",
}

var notificationColumns = strings.Join(notificationColumnList, ", ")

func prefixedColumns(prefix string) string {
	cols := make([]string, len(notificationColumnList))
	for i, c := range notificationColumnList {
		cols[i] = prefix + c
	}
	return strings.Join(cols, ", ")
}

func scanNotification(row pgx.Row) (*domain.Notification, error) {
	var (
		n        domain.Notification
		channel  string
		status   string
		priority int16
	)
	err := row.Scan(
		&n.ID, &n.BatchID, &n.IdempotencyKey, &n.CorrelationID, &n.TraceParent, &channel,
		&n.Recipient, &n.Content, &priority, &status, &n.Attempts, &n.TemplateID,
		&n.ScheduledAt, &n.NextAttemptAt, &n.ProviderMessageID, &n.LastError,
		&n.CreatedAt, &n.UpdatedAt, &n.SentAt, &n.FailedAt, &n.CancelledAt,
	)
	if err != nil {
		return nil, err
	}
	n.Channel = domain.Channel(channel)
	n.Status = domain.Status(status)
	n.Priority = domain.Priority(priority)
	return &n, nil
}

func collectNotifications(rows pgx.Rows) ([]*domain.Notification, error) {
	defer rows.Close()
	var out []*domain.Notification
	for rows.Next() {
		n, err := scanNotification(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

func notFoundIfNoRows(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	return err
}
