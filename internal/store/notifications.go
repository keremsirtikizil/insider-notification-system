package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/keremsirtikizil/insider-notification-system/internal/domain"
)

// CreateResult is the outcome of creating one notification. Created is false
// when an existing notification with the same idempotency key was returned.
type CreateResult struct {
	Notification *domain.Notification
	Created      bool
}

var insertNotificationSQL = `
INSERT INTO notifications (
    id, batch_id, idempotency_key, correlation_id, trace_parent, channel, recipient,
    content, priority, status, template_id, scheduled_at, next_attempt_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, COALESCE($12, now()))
ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
RETURNING ` + notificationColumns

// CreateNotifications inserts notifications (optionally as one batch) in a single
// transaction. Rows whose idempotency key already exists are not inserted; the
// existing row is returned instead, unless its payload differs, in which case
// the whole transaction is rolled back with an *IdempotencyConflictError.
func (s *Store) CreateNotifications(ctx context.Context, batch *domain.Batch, ns []*domain.Notification) ([]CreateResult, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if batch != nil {
		err := tx.QueryRow(ctx,
			`INSERT INTO batches (id, total) VALUES ($1, $2) RETURNING created_at`,
			batch.ID, len(ns)).Scan(&batch.CreatedAt)
		if err != nil {
			return nil, fmt.Errorf("insert batch: %w", err)
		}
	}

	// All inserts travel in one round trip (pipelined pgx batch).
	pb := &pgx.Batch{}
	for _, n := range ns {
		pb.Queue(insertNotificationSQL,
			n.ID, n.BatchID, n.IdempotencyKey, n.CorrelationID, n.TraceParent, string(n.Channel),
			n.Recipient, n.Content, int16(n.Priority), string(n.Status), n.TemplateID, n.ScheduledAt)
	}
	results := make([]CreateResult, len(ns))
	var duplicateKeys []string
	br := tx.SendBatch(ctx, pb)
	for i, n := range ns {
		created, err := scanNotification(br.QueryRow())
		switch {
		case errors.Is(err, pgx.ErrNoRows): // idempotency key conflict
			duplicateKeys = append(duplicateKeys, *n.IdempotencyKey)
		case err != nil:
			_ = br.Close()
			return nil, fmt.Errorf("insert notification %d: %w", i, err)
		default:
			results[i] = CreateResult{Notification: created, Created: true}
		}
	}
	if err := br.Close(); err != nil {
		return nil, err
	}

	if len(duplicateKeys) > 0 {
		rows, err := tx.Query(ctx,
			`SELECT `+notificationColumns+` FROM notifications WHERE idempotency_key = ANY($1)`, duplicateKeys)
		if err != nil {
			return nil, err
		}
		existing, err := collectNotifications(rows)
		if err != nil {
			return nil, err
		}
		byKey := make(map[string]*domain.Notification, len(existing))
		for _, e := range existing {
			byKey[*e.IdempotencyKey] = e
		}
		for i, n := range ns {
			if results[i].Notification != nil {
				continue
			}
			e, ok := byKey[*n.IdempotencyKey]
			if !ok {
				return nil, fmt.Errorf("idempotency key %q conflicted but no row was found", *n.IdempotencyKey)
			}
			if !e.SamePayload(n) {
				return nil, &IdempotencyConflictError{Index: i, Key: *n.IdempotencyKey}
			}
			results[i] = CreateResult{Notification: e, Created: false}
		}
		if batch != nil {
			// Duplicates stay owned by their original request; the batch only
			// counts the notifications it actually created.
			if _, err := tx.Exec(ctx, `UPDATE batches SET total = $2 WHERE id = $1`,
				batch.ID, len(ns)-len(duplicateKeys)); err != nil {
				return nil, err
			}
		}
	}
	if batch != nil {
		batch.Total = len(ns) - len(duplicateKeys)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return results, nil
}

// GetNotification returns one notification.
func (s *Store) GetNotification(ctx context.Context, id uuid.UUID) (*domain.Notification, error) {
	n, err := scanNotification(s.pool.QueryRow(ctx,
		`SELECT `+notificationColumns+` FROM notifications WHERE id = $1`, id))
	return n, notFoundIfNoRows(err)
}

// Cursor is a keyset pagination position: the last row of the previous page.
type Cursor struct {
	CreatedAt time.Time
	ID        uuid.UUID
}

// ListFilter selects notifications. Nil fields are not filtered on.
type ListFilter struct {
	Status      *domain.Status
	Channel     *domain.Channel
	Priority    *domain.Priority
	BatchID     *uuid.UUID
	CreatedFrom *time.Time // inclusive
	CreatedTo   *time.Time // exclusive
	After       *Cursor
	Limit       int
}

// ListNotifications returns one page, newest first, and the cursor of the next
// page (nil when this is the last page). Keyset pagination keeps every page
// O(limit) no matter how deep the client scrolls.
func (s *Store) ListNotifications(ctx context.Context, f ListFilter) ([]*domain.Notification, *Cursor, error) {
	var (
		where []string
		args  []any
	)
	add := func(cond string, v any) {
		args = append(args, v)
		where = append(where, strings.ReplaceAll(cond, "?", fmt.Sprintf("$%d", len(args))))
	}
	if f.Status != nil {
		add("status = ?", string(*f.Status))
	}
	if f.Channel != nil {
		add("channel = ?", string(*f.Channel))
	}
	if f.Priority != nil {
		add("priority = ?", int16(*f.Priority))
	}
	if f.BatchID != nil {
		add("batch_id = ?", *f.BatchID)
	}
	if f.CreatedFrom != nil {
		add("created_at >= ?", *f.CreatedFrom)
	}
	if f.CreatedTo != nil {
		add("created_at < ?", *f.CreatedTo)
	}
	if f.After != nil {
		args = append(args, f.After.CreatedAt, f.After.ID)
		where = append(where, fmt.Sprintf("(created_at, id) < ($%d, $%d)", len(args)-1, len(args)))
	}
	query := `SELECT ` + notificationColumns + ` FROM notifications`
	if len(where) > 0 {
		query += ` WHERE ` + strings.Join(where, " AND ")
	}
	args = append(args, f.Limit+1)
	query += fmt.Sprintf(` ORDER BY created_at DESC, id DESC LIMIT $%d`, len(args))

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, nil, err
	}
	items, err := collectNotifications(rows)
	if err != nil {
		return nil, nil, err
	}
	if len(items) <= f.Limit {
		return items, nil, nil
	}
	items = items[:f.Limit]
	last := items[len(items)-1]
	return items, &Cursor{CreatedAt: last.CreatedAt, ID: last.ID}, nil
}

// CancelNotification cancels a notification that has not been picked up yet.
// The conditional UPDATE is the race guard against a worker claiming it.
func (s *Store) CancelNotification(ctx context.Context, id uuid.UUID) (*domain.Notification, error) {
	n, err := scanNotification(s.pool.QueryRow(ctx, `
		UPDATE notifications
		SET status = 'cancelled', cancelled_at = now(), updated_at = now()
		WHERE id = $1 AND status IN ('pending', 'scheduled', 'retrying')
		RETURNING `+notificationColumns, id))
	if errors.Is(err, pgx.ErrNoRows) {
		current, getErr := s.GetNotification(ctx, id)
		if getErr != nil {
			return nil, getErr
		}
		return current, fmt.Errorf("%w: notification is %s", ErrInvalidState, current.Status)
	}
	return n, err
}

// RetryNotification puts a failed notification back into the queue with a fresh
// attempt budget (manual dead-letter replay).
func (s *Store) RetryNotification(ctx context.Context, id uuid.UUID) (*domain.Notification, error) {
	n, err := scanNotification(s.pool.QueryRow(ctx, `
		UPDATE notifications
		SET status = 'pending', attempts = 0, next_attempt_at = now(), failed_at = NULL, updated_at = now()
		WHERE id = $1 AND status = 'failed'
		RETURNING `+notificationColumns, id))
	if errors.Is(err, pgx.ErrNoRows) {
		current, getErr := s.GetNotification(ctx, id)
		if getErr != nil {
			return nil, getErr
		}
		return current, fmt.Errorf("%w: only failed notifications can be retried, notification is %s",
			ErrInvalidState, current.Status)
	}
	return n, err
}

// GetBatch returns a batch with the number of notifications in each status.
func (s *Store) GetBatch(ctx context.Context, id uuid.UUID) (*domain.Batch, error) {
	b := &domain.Batch{ID: id, StatusCounts: map[domain.Status]int{}}
	err := s.pool.QueryRow(ctx, `SELECT total, created_at FROM batches WHERE id = $1`, id).
		Scan(&b.Total, &b.CreatedAt)
	if err != nil {
		return nil, notFoundIfNoRows(err)
	}
	rows, err := s.pool.Query(ctx,
		`SELECT status, count(*) FROM notifications WHERE batch_id = $1 GROUP BY status`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			status string
			count  int
		)
		if err := rows.Scan(&status, &count); err != nil {
			return nil, err
		}
		b.StatusCounts[domain.Status(status)] = count
	}
	return b, rows.Err()
}

// CancelBatch cancels every notification of the batch that is still queued and
// returns the cancelled rows.
func (s *Store) CancelBatch(ctx context.Context, id uuid.UUID) ([]*domain.Notification, error) {
	var exists bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM batches WHERE id = $1)`, id).Scan(&exists); err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrNotFound
	}
	rows, err := s.pool.Query(ctx, `
		UPDATE notifications
		SET status = 'cancelled', cancelled_at = now(), updated_at = now()
		WHERE batch_id = $1 AND status IN ('pending', 'scheduled', 'retrying')
		RETURNING `+notificationColumns, id)
	if err != nil {
		return nil, err
	}
	return collectNotifications(rows)
}

// ListAttempts returns the delivery history of a notification, oldest first.
func (s *Store) ListAttempts(ctx context.Context, id uuid.UUID) ([]domain.DeliveryAttempt, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, notification_id, attempt, outcome, status_code, error, duration_ms, created_at
		FROM delivery_attempts WHERE notification_id = $1 ORDER BY id`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	attempts := []domain.DeliveryAttempt{}
	for rows.Next() {
		var (
			a          domain.DeliveryAttempt
			outcome    string
			durationMS int64
		)
		if err := rows.Scan(&a.ID, &a.NotificationID, &a.Attempt, &outcome, &a.StatusCode,
			&a.Error, &durationMS, &a.CreatedAt); err != nil {
			return nil, err
		}
		a.Outcome = domain.AttemptOutcome(outcome)
		a.Duration = time.Duration(durationMS) * time.Millisecond
		attempts = append(attempts, a)
	}
	return attempts, rows.Err()
}
