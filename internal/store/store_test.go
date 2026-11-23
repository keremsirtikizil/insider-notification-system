package store_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keremsirtikizil/insider-notification-system/internal/domain"
	"github.com/keremsirtikizil/insider-notification-system/internal/store"
	"github.com/keremsirtikizil/insider-notification-system/internal/testutil"
)

func newStore(t *testing.T) *store.Store {
	t.Helper()
	t.Parallel()
	return store.New(testutil.NewDatabase(t))
}

func newNotification(ch domain.Channel, p domain.Priority) *domain.Notification {
	recipient := "+905551234567"
	if ch == domain.ChannelEmail {
		recipient = "jane@example.com"
	}
	return &domain.Notification{
		ID: uuid.New(), Channel: ch, Priority: p, Status: domain.StatusPending,
		Recipient: recipient, Content: "hello " + p.String(), CorrelationID: "corr-1",
	}
}

func create(t *testing.T, s *store.Store, ns ...*domain.Notification) []*domain.Notification {
	t.Helper()
	res, err := s.CreateNotifications(context.Background(), nil, ns)
	require.NoError(t, err)
	out := make([]*domain.Notification, len(res))
	for i, r := range res {
		out[i] = r.Notification
	}
	return out
}

func ptr[T any](v T) *T { return &v }

func TestCreateAndGetNotification(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	in := newNotification(domain.ChannelSMS, domain.PriorityHigh)
	in.IdempotencyKey = ptr("key-1")
	created := create(t, s, in)[0]

	got, err := s.GetNotification(ctx, in.ID)
	require.NoError(t, err)
	assert.Equal(t, created, got)
	assert.Equal(t, domain.StatusPending, got.Status)
	assert.Equal(t, domain.PriorityHigh, got.Priority)
	assert.Equal(t, "key-1", *got.IdempotencyKey)
	assert.Equal(t, "corr-1", got.CorrelationID)
	assert.WithinDuration(t, time.Now(), got.NextAttemptAt, 5*time.Second)

	_, err = s.GetNotification(ctx, uuid.New())
	assert.ErrorIs(t, err, store.ErrNotFound)
}

func TestIdempotencyKey(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	first := newNotification(domain.ChannelSMS, domain.PriorityNormal)
	first.IdempotencyKey = ptr("order-42")
	create(t, s, first)

	again := newNotification(domain.ChannelSMS, domain.PriorityNormal)
	again.IdempotencyKey = ptr("order-42")
	res, err := s.CreateNotifications(ctx, nil, []*domain.Notification{again})
	require.NoError(t, err)
	assert.False(t, res[0].Created)
	assert.Equal(t, first.ID, res[0].Notification.ID, "the original notification is returned")

	// Same key, different payload, inside a batch: whole batch rolls back.
	other := newNotification(domain.ChannelSMS, domain.PriorityNormal)
	other.Content = "something else"
	other.IdempotencyKey = ptr("order-42")
	fresh := newNotification(domain.ChannelSMS, domain.PriorityNormal)
	batch := &domain.Batch{ID: uuid.New()}
	_, err = s.CreateNotifications(ctx, batch, []*domain.Notification{fresh, other})
	var conflict *store.IdempotencyConflictError
	require.ErrorAs(t, err, &conflict)
	assert.Equal(t, 1, conflict.Index)
	_, err = s.GetNotification(ctx, fresh.ID)
	assert.ErrorIs(t, err, store.ErrNotFound, "nothing from the failed batch is persisted")
	_, err = s.GetBatch(ctx, batch.ID)
	assert.ErrorIs(t, err, store.ErrNotFound)
}

func TestBatchCreateStatusAndCancel(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	batch := &domain.Batch{ID: uuid.New()}
	var ns []*domain.Notification
	for range 1000 {
		n := newNotification(domain.ChannelPush, domain.PriorityLow)
		n.Recipient = "device-token-0123456789"
		n.BatchID = &batch.ID
		ns = append(ns, n)
	}
	res, err := s.CreateNotifications(ctx, batch, ns)
	require.NoError(t, err)
	require.Len(t, res, 1000)
	assert.Equal(t, 1000, batch.Total)

	// One of them is already being processed and cannot be cancelled.
	claimed, err := s.ClaimDue(ctx, domain.ChannelPush, 1, time.Minute)
	require.NoError(t, err)
	require.Len(t, claimed, 1)

	b, err := s.GetBatch(ctx, batch.ID)
	require.NoError(t, err)
	assert.Equal(t, 1000, b.Total)
	assert.Equal(t, 999, b.StatusCounts[domain.StatusPending])
	assert.Equal(t, 1, b.StatusCounts[domain.StatusProcessing])

	cancelled, err := s.CancelBatch(ctx, batch.ID)
	require.NoError(t, err)
	assert.Len(t, cancelled, 999)
	b, _ = s.GetBatch(ctx, batch.ID)
	assert.Equal(t, 999, b.StatusCounts[domain.StatusCancelled])

	_, err = s.CancelBatch(ctx, uuid.New())
	assert.ErrorIs(t, err, store.ErrNotFound)
}

func TestClaimDueOrdersByPriority(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	low := newNotification(domain.ChannelSMS, domain.PriorityLow)
	create(t, s, low) // oldest, but lowest priority
	high := newNotification(domain.ChannelSMS, domain.PriorityHigh)
	normal := newNotification(domain.ChannelSMS, domain.PriorityNormal)
	email := newNotification(domain.ChannelEmail, domain.PriorityHigh)
	create(t, s, normal, high, email)

	got, err := s.ClaimDue(ctx, domain.ChannelSMS, 2, time.Minute)
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, []uuid.UUID{high.ID, normal.ID}, []uuid.UUID{got[0].ID, got[1].ID})
	for _, n := range got {
		assert.Equal(t, domain.StatusProcessing, n.Status)
		assert.Equal(t, 1, n.Attempts)
	}

	got, err = s.ClaimDue(ctx, domain.ChannelSMS, 10, time.Minute)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, low.ID, got[0].ID)

	got, err = s.ClaimDue(ctx, domain.ChannelSMS, 10, time.Minute)
	require.NoError(t, err)
	assert.Empty(t, got, "claimed rows are not handed out twice")
}

func TestConcurrentClaimersNeverShareRows(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	const total = 300
	var ns []*domain.Notification
	for range total {
		ns = append(ns, newNotification(domain.ChannelSMS, domain.PriorityNormal))
	}
	create(t, s, ns...)

	var (
		mu   sync.Mutex
		seen = map[uuid.UUID]int{}
		wg   sync.WaitGroup
	)
	for range 8 {
		wg.Go(func() {
			for {
				got, err := s.ClaimDue(ctx, domain.ChannelSMS, 7, time.Minute)
				if !assert.NoError(t, err) || len(got) == 0 {
					return
				}
				mu.Lock()
				for _, n := range got {
					seen[n.ID]++
				}
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	assert.Len(t, seen, total)
	for id, count := range seen {
		assert.Equal(t, 1, count, "notification %s claimed %d times", id, count)
	}
}

func TestScheduledNotificationsWaitUntilDue(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	later := newNotification(domain.ChannelSMS, domain.PriorityHigh)
	later.Status, later.ScheduledAt = domain.StatusScheduled, ptr(time.Now().Add(time.Hour))
	soon := newNotification(domain.ChannelSMS, domain.PriorityLow)
	soon.Status, soon.ScheduledAt = domain.StatusScheduled, ptr(time.Now().Add(300*time.Millisecond))
	create(t, s, later, soon)

	got, err := s.ClaimDue(ctx, domain.ChannelSMS, 10, time.Minute)
	require.NoError(t, err)
	assert.Empty(t, got)

	require.Eventually(t, func() bool {
		got, err = s.ClaimDue(ctx, domain.ChannelSMS, 10, time.Minute)
		return err == nil && len(got) == 1
	}, 3*time.Second, 50*time.Millisecond)
	assert.Equal(t, soon.ID, got[0].ID, "high priority does not jump its schedule")
}

func TestFinishAttemptLifecycle(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	n := create(t, s, newNotification(domain.ChannelEmail, domain.PriorityNormal))[0]

	// Attempt 1: retryable failure.
	_, err := s.ClaimDue(ctx, domain.ChannelEmail, 1, time.Minute)
	require.NoError(t, err)
	retryAt := time.Now().Add(250 * time.Millisecond)
	updated, err := s.FinishAttempt(ctx, n.ID, domain.AttemptResult{
		Status: domain.StatusRetrying, Error: ptr("provider returned 503"), NextAttemptAt: &retryAt,
		Outcome: domain.OutcomeRetryableError, StatusCode: ptr(503), Duration: 120 * time.Millisecond,
	})
	require.NoError(t, err)
	assert.Equal(t, domain.StatusRetrying, updated.Status)
	assert.Equal(t, "provider returned 503", *updated.LastError)

	_, err = s.FinishAttempt(ctx, n.ID, domain.AttemptResult{Status: domain.StatusSent, Outcome: domain.OutcomeSuccess})
	assert.ErrorIs(t, err, store.ErrLeaseLost, "only the lease holder may record an outcome")

	got, err := s.ClaimDue(ctx, domain.ChannelEmail, 1, time.Minute)
	require.NoError(t, err)
	assert.Empty(t, got, "backoff not elapsed yet")

	// Attempt 2: success.
	require.Eventually(t, func() bool {
		got, err = s.ClaimDue(ctx, domain.ChannelEmail, 1, time.Minute)
		return err == nil && len(got) == 1
	}, 3*time.Second, 50*time.Millisecond)
	assert.Equal(t, 2, got[0].Attempts)
	updated, err = s.FinishAttempt(ctx, n.ID, domain.AttemptResult{
		Status: domain.StatusSent, ProviderMessageID: ptr("prov-1"), Outcome: domain.OutcomeSuccess,
		StatusCode: ptr(202), Duration: 80 * time.Millisecond,
	})
	require.NoError(t, err)
	assert.Equal(t, domain.StatusSent, updated.Status)
	assert.Equal(t, "prov-1", *updated.ProviderMessageID)
	assert.NotNil(t, updated.SentAt)
	assert.Nil(t, updated.LastError)

	attempts, err := s.ListAttempts(ctx, n.ID)
	require.NoError(t, err)
	require.Len(t, attempts, 2)
	assert.Equal(t, 1, attempts[0].Attempt)
	assert.Equal(t, domain.OutcomeRetryableError, attempts[0].Outcome)
	assert.Equal(t, 503, *attempts[0].StatusCode)
	assert.Equal(t, 120*time.Millisecond, attempts[0].Duration)
	assert.Equal(t, 2, attempts[1].Attempt)
	assert.Equal(t, domain.OutcomeSuccess, attempts[1].Outcome)

	_, err = s.CancelNotification(ctx, n.ID)
	assert.ErrorIs(t, err, store.ErrInvalidState, "sent notifications cannot be cancelled")
}

func TestCancelOnlyWhileQueued(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	queued := create(t, s, newNotification(domain.ChannelSMS, domain.PriorityNormal))[0]
	cancelled, err := s.CancelNotification(ctx, queued.ID)
	require.NoError(t, err)
	assert.Equal(t, domain.StatusCancelled, cancelled.Status)
	assert.NotNil(t, cancelled.CancelledAt)

	got, err := s.ClaimDue(ctx, domain.ChannelSMS, 10, time.Minute)
	require.NoError(t, err)
	assert.Empty(t, got, "cancelled notifications are never dispatched")

	inFlight := create(t, s, newNotification(domain.ChannelSMS, domain.PriorityNormal))[0]
	_, err = s.ClaimDue(ctx, domain.ChannelSMS, 10, time.Minute)
	require.NoError(t, err)
	current, err := s.CancelNotification(ctx, inFlight.ID)
	assert.ErrorIs(t, err, store.ErrInvalidState)
	assert.Equal(t, domain.StatusProcessing, current.Status)

	_, err = s.CancelNotification(ctx, uuid.New())
	assert.ErrorIs(t, err, store.ErrNotFound)
}

func TestReleaseDoesNotConsumeAnAttempt(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	n := create(t, s, newNotification(domain.ChannelSMS, domain.PriorityNormal))[0]
	_, err := s.ClaimDue(ctx, domain.ChannelSMS, 1, time.Minute)
	require.NoError(t, err)
	released, err := s.Release(ctx, n.ID, time.Now())
	require.NoError(t, err)
	assert.Equal(t, domain.StatusPending, released.Status)
	assert.Equal(t, 0, released.Attempts)
}

func TestRecoverExpiredLeases(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	a := create(t, s, newNotification(domain.ChannelSMS, domain.PriorityNormal))[0]
	_, err := s.ClaimDue(ctx, domain.ChannelSMS, 1, time.Millisecond)
	require.NoError(t, err)
	b := create(t, s, newNotification(domain.ChannelSMS, domain.PriorityNormal))[0]
	_, err = s.ClaimDue(ctx, domain.ChannelSMS, 1, time.Hour) // healthy lease
	require.NoError(t, err)
	time.Sleep(50 * time.Millisecond)

	recovered, err := s.RecoverExpiredLeases(ctx, 5)
	require.NoError(t, err)
	require.Len(t, recovered, 1)
	assert.Equal(t, a.ID, recovered[0].ID)
	assert.Equal(t, domain.StatusRetrying, recovered[0].Status)

	got, err := s.GetNotification(ctx, b.ID)
	require.NoError(t, err)
	assert.Equal(t, domain.StatusProcessing, got.Status)

	// With the attempt budget spent, an expired lease means failure.
	_, err = s.ClaimDue(ctx, domain.ChannelSMS, 1, time.Millisecond)
	require.NoError(t, err)
	time.Sleep(50 * time.Millisecond)
	recovered, err = s.RecoverExpiredLeases(ctx, 2)
	require.NoError(t, err)
	require.Len(t, recovered, 1)
	assert.Equal(t, domain.StatusFailed, recovered[0].Status)
	assert.NotNil(t, recovered[0].FailedAt)

	retried, err := s.RetryNotification(ctx, a.ID)
	require.NoError(t, err)
	assert.Equal(t, domain.StatusPending, retried.Status)
	assert.Equal(t, 0, retried.Attempts)
	_, err = s.RetryNotification(ctx, a.ID)
	assert.ErrorIs(t, err, store.ErrInvalidState)
}

func TestListNotificationsFiltersAndKeysetPagination(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	var smsIDs []uuid.UUID
	for i := range 7 {
		n := newNotification(domain.ChannelSMS, domain.PriorityNormal)
		n.Content = fmt.Sprintf("sms %d", i)
		create(t, s, n)
		smsIDs = append(smsIDs, n.ID)
	}
	create(t, s, newNotification(domain.ChannelEmail, domain.PriorityHigh))

	var paged []uuid.UUID
	f := store.ListFilter{Channel: ptr(domain.ChannelSMS), Limit: 3}
	for {
		page, next, err := s.ListNotifications(ctx, f)
		require.NoError(t, err)
		for _, n := range page {
			paged = append(paged, n.ID)
		}
		if next == nil {
			break
		}
		f.After = next
	}
	require.Len(t, paged, 7)
	for i := range paged { // newest first
		assert.Equal(t, smsIDs[len(smsIDs)-1-i], paged[i])
	}

	page, _, err := s.ListNotifications(ctx, store.ListFilter{Priority: ptr(domain.PriorityHigh), Limit: 10})
	require.NoError(t, err)
	require.Len(t, page, 1)
	assert.Equal(t, domain.ChannelEmail, page[0].Channel)

	future := time.Now().Add(time.Hour)
	page, _, err = s.ListNotifications(ctx, store.ListFilter{CreatedFrom: &future, Limit: 10})
	require.NoError(t, err)
	assert.Empty(t, page)

	page, _, err = s.ListNotifications(ctx, store.ListFilter{Status: ptr(domain.StatusSent), Limit: 10})
	require.NoError(t, err)
	assert.Empty(t, page)
}

func TestTemplates(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	tpl := &domain.Template{ID: uuid.New(), Name: "welcome", Channel: domain.ChannelEmail,
		Content: "Hi {{name}}", Variables: []string{"name"}}
	created, err := s.CreateTemplate(ctx, tpl)
	require.NoError(t, err)
	assert.Equal(t, []string{"name"}, created.Variables)

	_, err = s.CreateTemplate(ctx, &domain.Template{ID: uuid.New(), Name: "welcome", Channel: domain.ChannelSMS, Content: "x"})
	assert.ErrorIs(t, err, store.ErrDuplicate)

	got, err := s.GetTemplate(ctx, tpl.ID)
	require.NoError(t, err)
	assert.Equal(t, created, got)
	all, err := s.ListTemplates(ctx)
	require.NoError(t, err)
	assert.Len(t, all, 1)
	_, err = s.GetTemplate(ctx, uuid.New())
	assert.ErrorIs(t, err, store.ErrNotFound)
}

func TestQueueDepthAndStats(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	create(t, s,
		newNotification(domain.ChannelSMS, domain.PriorityHigh),
		newNotification(domain.ChannelSMS, domain.PriorityHigh),
		newNotification(domain.ChannelEmail, domain.PriorityLow))
	sent := create(t, s, newNotification(domain.ChannelPush, domain.PriorityNormal))[0]
	_, err := s.ClaimDue(ctx, domain.ChannelPush, 1, time.Minute)
	require.NoError(t, err)
	_, err = s.FinishAttempt(ctx, sent.ID, domain.AttemptResult{
		Status: domain.StatusSent, Outcome: domain.OutcomeSuccess, Duration: 40 * time.Millisecond})
	require.NoError(t, err)

	depth, err := s.QueueDepth(ctx)
	require.NoError(t, err)
	counts := map[string]int64{}
	for _, r := range depth {
		counts[fmt.Sprintf("%s/%s/%s", r.Channel, r.Status, r.Priority)] += r.Count
	}
	assert.Equal(t, map[string]int64{"sms/pending/high": 2, "email/pending/low": 1}, counts)

	stats, err := s.Stats(ctx, time.Minute)
	require.NoError(t, err)
	push := stats[domain.ChannelPush]
	require.NotNil(t, push)
	assert.EqualValues(t, 1, push.Sent)
	assert.EqualValues(t, 0, push.Failed)
	assert.EqualValues(t, 1, push.Attempts)
	assert.InDelta(t, 40, push.ProviderLatencyAvgMS, 0.001)
	assert.GreaterOrEqual(t, push.LatencyP95MS, 0.0)
}

func TestErrorsAreDistinguishable(t *testing.T) {
	assert.False(t, errors.Is(store.ErrNotFound, store.ErrInvalidState))
}
