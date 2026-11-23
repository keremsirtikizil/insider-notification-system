package store

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/keremsirtikizil/insider-notification-system/internal/domain"
)

// ClaimDue atomically takes up to limit due notifications of one channel, highest
// priority first, and leases them to the caller until now()+lease.
//
// FOR UPDATE SKIP LOCKED lets any number of worker processes poll the same
// queue without blocking each other or receiving the same row twice. The attempt
// counter is incremented at claim time, so a message that crashes its worker
// still consumes its attempt budget (poison-pill protection).
func (s *Store) ClaimDue(ctx context.Context, channel domain.Channel, limit int, lease time.Duration) ([]*domain.Notification, error) {
	rows, err := s.pool.Query(ctx, `
		WITH due AS (
			SELECT id FROM notifications
			WHERE channel = $1
			  AND status IN ('pending', 'scheduled', 'retrying')
			  AND next_attempt_at <= now()
			ORDER BY priority, next_attempt_at
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
		UPDATE notifications n
		SET status = 'processing',
		    attempts = n.attempts + 1,
		    locked_until = now() + make_interval(secs => $3),
		    updated_at = now()
		FROM due
		WHERE n.id = due.id
		RETURNING `+prefixedColumns("n."),
		string(channel), limit, lease.Seconds())
	if err != nil {
		return nil, err
	}
	items, err := collectNotifications(rows)
	if err != nil {
		return nil, err
	}
	// UPDATE ... RETURNING does not preserve the CTE order.
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].Priority != items[j].Priority {
			return items[i].Priority < items[j].Priority
		}
		return items[i].NextAttemptAt.Before(items[j].NextAttemptAt)
	})
	return items, nil
}

// FinishAttempt records the outcome of a provider call: it moves the notification
// to sent / retrying / failed and appends the audit row, in one statement.
// It only succeeds while the caller still holds the processing lease.
func (s *Store) FinishAttempt(ctx context.Context, id uuid.UUID, r domain.AttemptResult) (*domain.Notification, error) {
	n, err := scanNotification(s.pool.QueryRow(ctx, `
		WITH upd AS (
			UPDATE notifications
			SET status = $2,
			    provider_message_id = COALESCE($3, provider_message_id),
			    last_error = $4,
			    next_attempt_at = COALESCE($5, next_attempt_at),
			    sent_at = CASE WHEN $2 = 'sent' THEN now() ELSE sent_at END,
			    failed_at = CASE WHEN $2 = 'failed' THEN now() ELSE failed_at END,
			    locked_until = NULL,
			    updated_at = now()
			WHERE id = $1 AND status = 'processing'
			RETURNING *
		), audit AS (
			INSERT INTO delivery_attempts (notification_id, channel, attempt, outcome, status_code, error, duration_ms)
			SELECT id, channel, attempts, $6::text, $7::int, $4::text, $8::int FROM upd
		)
		SELECT `+notificationColumns+` FROM upd`,
		id, string(r.Status), r.ProviderMessageID, r.Error, r.NextAttemptAt,
		string(r.Outcome), r.StatusCode, r.Duration.Milliseconds()))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrLeaseLost
	}
	return n, err
}

// Release hands a claimed notification back to the queue without consuming an
// attempt (used when the circuit breaker is open or the worker shuts down).
func (s *Store) Release(ctx context.Context, id uuid.UUID, nextAttemptAt time.Time) (*domain.Notification, error) {
	n, err := scanNotification(s.pool.QueryRow(ctx, `
		UPDATE notifications
		SET status = CASE WHEN attempts > 1 THEN 'retrying' ELSE 'pending' END,
		    attempts = GREATEST(attempts - 1, 0),
		    next_attempt_at = $2,
		    locked_until = NULL,
		    updated_at = now()
		WHERE id = $1 AND status = 'processing'
		RETURNING `+notificationColumns, id, nextAttemptAt))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrLeaseLost
	}
	return n, err
}

// RecoverExpiredLeases returns rows whose worker died mid-attempt to the queue
// (or fails them when their attempt budget is spent).
func (s *Store) RecoverExpiredLeases(ctx context.Context, maxAttempts int) ([]*domain.Notification, error) {
	rows, err := s.pool.Query(ctx, `
		UPDATE notifications
		SET status = CASE WHEN attempts >= $1 THEN 'failed' ELSE 'retrying' END,
		    failed_at = CASE WHEN attempts >= $1 THEN now() ELSE failed_at END,
		    last_error = 'worker lease expired before the attempt completed',
		    next_attempt_at = now(),
		    locked_until = NULL,
		    updated_at = now()
		WHERE id IN (
			SELECT id FROM notifications
			WHERE status = 'processing' AND locked_until < now()
			ORDER BY locked_until
			LIMIT 1000
			FOR UPDATE SKIP LOCKED
		)
		RETURNING `+notificationColumns, maxAttempts)
	if err != nil {
		return nil, err
	}
	return collectNotifications(rows)
}
