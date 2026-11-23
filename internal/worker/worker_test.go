package worker

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keremsirtikizil/insider-notification-system/internal/domain"
	"github.com/keremsirtikizil/insider-notification-system/internal/events"
	"github.com/keremsirtikizil/insider-notification-system/internal/observability"
	"github.com/keremsirtikizil/insider-notification-system/internal/provider"
	"github.com/keremsirtikizil/insider-notification-system/internal/ratelimit"
	"github.com/keremsirtikizil/insider-notification-system/internal/store"
)

// memStore is an in-memory implementation of the worker's Store with the same
// state machine as the SQL queries.
type memStore struct {
	mu       sync.Mutex
	rows     map[uuid.UUID]*domain.Notification
	attempts map[uuid.UUID][]domain.AttemptResult
}

func newMemStore(ns ...*domain.Notification) *memStore {
	s := &memStore{rows: map[uuid.UUID]*domain.Notification{}, attempts: map[uuid.UUID][]domain.AttemptResult{}}
	for _, n := range ns {
		s.rows[n.ID] = n
	}
	return s
}

func (s *memStore) ClaimDue(_ context.Context, ch domain.Channel, limit int, _ time.Duration) ([]*domain.Notification, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var due []*domain.Notification
	for _, n := range s.rows {
		if n.Channel == ch && n.Status.Cancellable() && !n.NextAttemptAt.After(time.Now()) {
			due = append(due, n)
		}
	}
	sort.Slice(due, func(i, j int) bool {
		if due[i].Priority != due[j].Priority {
			return due[i].Priority < due[j].Priority
		}
		return due[i].NextAttemptAt.Before(due[j].NextAttemptAt)
	})
	if len(due) > limit {
		due = due[:limit]
	}
	out := make([]*domain.Notification, len(due))
	for i, n := range due {
		n.Status = domain.StatusProcessing
		n.Attempts++
		cp := *n
		out[i] = &cp
	}
	return out, nil
}

func (s *memStore) FinishAttempt(_ context.Context, id uuid.UUID, r domain.AttemptResult) (*domain.Notification, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.rows[id]
	if n == nil || n.Status != domain.StatusProcessing {
		return nil, store.ErrLeaseLost
	}
	n.Status, n.LastError, n.UpdatedAt = r.Status, r.Error, time.Now()
	if r.NextAttemptAt != nil {
		n.NextAttemptAt = *r.NextAttemptAt
	}
	if r.ProviderMessageID != nil {
		n.ProviderMessageID = r.ProviderMessageID
	}
	s.attempts[id] = append(s.attempts[id], r)
	cp := *n
	return &cp, nil
}

func (s *memStore) Release(_ context.Context, id uuid.UUID, at time.Time) (*domain.Notification, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.rows[id]
	if n == nil || n.Status != domain.StatusProcessing {
		return nil, store.ErrLeaseLost
	}
	n.Attempts--
	n.Status, n.NextAttemptAt = domain.StatusPending, at
	cp := *n
	return &cp, nil
}

func (s *memStore) RecoverExpiredLeases(context.Context, int) ([]*domain.Notification, error) {
	return nil, nil
}

func (s *memStore) QueueDepth(context.Context) ([]store.QueueDepthRow, error) { return nil, nil }

func (s *memStore) get(id uuid.UUID) domain.Notification {
	s.mu.Lock()
	defer s.mu.Unlock()
	return *s.rows[id]
}

func (s *memStore) allFinal() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, n := range s.rows {
		if n.Status != domain.StatusSent && n.Status != domain.StatusFailed {
			return false
		}
	}
	return true
}

// fakeSender records calls and answers with a scripted function.
type fakeSender struct {
	mu    sync.Mutex
	calls []sentCall
	reply func(call int, m provider.Message) (provider.Result, error)
}

type sentCall struct {
	at  time.Time
	msg provider.Message
}

func (f *fakeSender) Send(_ context.Context, m provider.Message) (provider.Result, error) {
	f.mu.Lock()
	f.calls = append(f.calls, sentCall{at: time.Now(), msg: m})
	n := len(f.calls)
	f.mu.Unlock()
	if f.reply == nil {
		return provider.Result{MessageID: "m-" + m.IdempotencyKey, StatusCode: http.StatusAccepted}, nil
	}
	return f.reply(n, m)
}

func (f *fakeSender) snapshot() []sentCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]sentCall(nil), f.calls...)
}

func notification(ch domain.Channel, p domain.Priority, content string) *domain.Notification {
	return &domain.Notification{
		ID: uuid.New(), Channel: ch, Priority: p, Status: domain.StatusPending,
		Recipient: "+905551234567", Content: content, NextAttemptAt: time.Now().Add(-time.Second),
		CreatedAt: time.Now(),
	}
}

func testConfig() Config {
	return Config{
		Concurrency: 4, BatchSize: 50, PollInterval: 5 * time.Millisecond, Lease: time.Minute,
		MaxAttempts: 3, SendTimeout: 2 * time.Second,
		Retry:            RetryPolicy{Base: 5 * time.Millisecond, Max: 20 * time.Millisecond},
		BreakerThreshold: 100, BreakerCooldown: time.Minute,
		ReaperInterval: time.Hour, GaugeInterval: time.Hour,
	}
}

func startWorker(t *testing.T, cfg Config, st Store, s Sender, lim ratelimit.Limiter) *Worker {
	t.Helper()
	w := New(cfg, st, s, lim, events.Nop{}, observability.NewMetrics(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { w.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	return w
}

func TestWorkerServesHigherPriorityFirst(t *testing.T) {
	low := notification(domain.ChannelSMS, domain.PriorityLow, "low")
	normal := notification(domain.ChannelSMS, domain.PriorityNormal, "normal")
	high := notification(domain.ChannelSMS, domain.PriorityHigh, "high")
	low.NextAttemptAt = time.Now().Add(-time.Hour) // oldest, still last
	st := newMemStore(low, normal, high)
	sender := &fakeSender{}
	cfg := testConfig()
	cfg.Concurrency = 1
	startWorker(t, cfg, st, sender, ratelimit.NewLocal(1000))

	require.Eventually(t, st.allFinal, 2*time.Second, 5*time.Millisecond)
	calls := sender.snapshot()
	require.Len(t, calls, 3)
	assert.Equal(t, []string{"high", "normal", "low"},
		[]string{calls[0].msg.Content, calls[1].msg.Content, calls[2].msg.Content})
	assert.Equal(t, domain.StatusSent, st.get(high.ID).Status)
	assert.Equal(t, "m-"+high.ID.String(), *st.get(high.ID).ProviderMessageID)
}

func TestWorkerRetriesRetryableErrorsThenSucceeds(t *testing.T) {
	n := notification(domain.ChannelEmail, domain.PriorityNormal, "hello")
	st := newMemStore(n)
	sender := &fakeSender{reply: func(call int, _ provider.Message) (provider.Result, error) {
		if call == 1 {
			return provider.Result{}, &provider.Error{StatusCode: 503, Retryable: true, Message: "busy"}
		}
		return provider.Result{MessageID: "ok", StatusCode: 202}, nil
	}}
	startWorker(t, testConfig(), st, sender, ratelimit.NewLocal(1000))

	require.Eventually(t, st.allFinal, 2*time.Second, 5*time.Millisecond)
	got := st.get(n.ID)
	assert.Equal(t, domain.StatusSent, got.Status)
	assert.Equal(t, 2, got.Attempts)
	require.Len(t, st.attempts[n.ID], 2)
	assert.Equal(t, domain.OutcomeRetryableError, st.attempts[n.ID][0].Outcome)
	assert.Equal(t, domain.StatusRetrying, st.attempts[n.ID][0].Status)
	assert.Equal(t, domain.OutcomeSuccess, st.attempts[n.ID][1].Outcome)
}

func TestWorkerDoesNotRetryPermanentErrors(t *testing.T) {
	n := notification(domain.ChannelPush, domain.PriorityNormal, "hello")
	st := newMemStore(n)
	sender := &fakeSender{reply: func(int, provider.Message) (provider.Result, error) {
		return provider.Result{}, &provider.Error{StatusCode: 400, Message: "bad token"}
	}}
	startWorker(t, testConfig(), st, sender, ratelimit.NewLocal(1000))

	require.Eventually(t, st.allFinal, 2*time.Second, 5*time.Millisecond)
	got := st.get(n.ID)
	assert.Equal(t, domain.StatusFailed, got.Status)
	assert.Equal(t, 1, got.Attempts)
	assert.Contains(t, *got.LastError, "bad token")
	assert.Len(t, sender.snapshot(), 1)
}

func TestWorkerGivesUpAfterMaxAttempts(t *testing.T) {
	n := notification(domain.ChannelSMS, domain.PriorityNormal, "hello")
	st := newMemStore(n)
	sender := &fakeSender{reply: func(int, provider.Message) (provider.Result, error) {
		return provider.Result{}, &provider.Error{Retryable: true, Message: "connection refused"}
	}}
	cfg := testConfig()
	cfg.MaxAttempts = 3
	startWorker(t, cfg, st, sender, ratelimit.NewLocal(1000))

	require.Eventually(t, st.allFinal, 2*time.Second, 5*time.Millisecond)
	got := st.get(n.ID)
	assert.Equal(t, domain.StatusFailed, got.Status)
	assert.Equal(t, 3, got.Attempts)
	assert.Contains(t, *got.LastError, "gave up after 3 attempts")
	assert.Len(t, sender.snapshot(), 3)
}

func TestWorkerRespectsRateLimitPerChannel(t *testing.T) {
	const perSecond, total = 40, 60
	var ns []*domain.Notification
	for range total {
		ns = append(ns, notification(domain.ChannelSMS, domain.PriorityNormal, "x"))
	}
	for range 10 { // another channel has its own budget
		ns = append(ns, notification(domain.ChannelEmail, domain.PriorityNormal, "y"))
	}
	st := newMemStore(ns...)
	sender := &fakeSender{}
	cfg := testConfig()
	cfg.Concurrency = 16
	startWorker(t, cfg, st, sender, ratelimit.NewLocal(perSecond))

	require.Eventually(t, st.allFinal, 5*time.Second, 10*time.Millisecond)
	var smsTimes []time.Time
	for _, c := range sender.snapshot() {
		if c.msg.Channel == "sms" {
			smsTimes = append(smsTimes, c.at)
		}
	}
	require.Len(t, smsTimes, total)
	sort.Slice(smsTimes, func(i, j int) bool { return smsTimes[i].Before(smsTimes[j]) })
	// No sliding one-second window may contain more than the limit
	// (+1 tolerates timer jitter at the window edges).
	for i := range smsTimes {
		count := 0
		for j := i; j < len(smsTimes) && smsTimes[j].Sub(smsTimes[i]) < time.Second; j++ {
			count++
		}
		assert.LessOrEqual(t, count, perSecond+1, "window starting at send %d", i)
	}
}

func TestWorkerCircuitBreakerStopsCallingFailingProvider(t *testing.T) {
	var ns []*domain.Notification
	for range 20 {
		ns = append(ns, notification(domain.ChannelSMS, domain.PriorityNormal, "x"))
	}
	st := newMemStore(ns...)
	sender := &fakeSender{reply: func(int, provider.Message) (provider.Result, error) {
		return provider.Result{}, &provider.Error{StatusCode: 502, Retryable: true, Message: "bad gateway"}
	}}
	cfg := testConfig()
	cfg.Concurrency = 1
	cfg.BreakerThreshold = 3
	cfg.MaxAttempts = 100
	w := startWorker(t, cfg, st, sender, ratelimit.NewLocal(1000))

	require.Eventually(t, func() bool { return w.Breakers()[domain.ChannelSMS].State() == BreakerOpen },
		2*time.Second, 5*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	assert.LessOrEqual(t, len(sender.snapshot()), 4, "no calls while the circuit is open")
	assert.Equal(t, BreakerClosed, w.Breakers()[domain.ChannelEmail].State(), "breakers are per channel")
}
