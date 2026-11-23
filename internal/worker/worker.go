// Package worker is the processing engine: it pulls due notifications from the
// PostgreSQL queue, paces them through the per-channel rate limiter, calls the
// provider and records the outcome (sent / retrying / failed).
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/keremsirtikizil/insider-notification-system/internal/domain"
	"github.com/keremsirtikizil/insider-notification-system/internal/events"
	"github.com/keremsirtikizil/insider-notification-system/internal/observability"
	"github.com/keremsirtikizil/insider-notification-system/internal/provider"
	"github.com/keremsirtikizil/insider-notification-system/internal/ratelimit"
	"github.com/keremsirtikizil/insider-notification-system/internal/store"
)

// Store is the subset of persistence the worker needs.
type Store interface {
	ClaimDue(ctx context.Context, channel domain.Channel, limit int, lease time.Duration) ([]*domain.Notification, error)
	FinishAttempt(ctx context.Context, id uuid.UUID, r domain.AttemptResult) (*domain.Notification, error)
	Release(ctx context.Context, id uuid.UUID, nextAttemptAt time.Time) (*domain.Notification, error)
	RecoverExpiredLeases(ctx context.Context, maxAttempts int) ([]*domain.Notification, error)
	QueueDepth(ctx context.Context) ([]store.QueueDepthRow, error)
}

// Sender delivers one message to the provider.
type Sender interface {
	Send(ctx context.Context, m provider.Message) (provider.Result, error)
}

// Config tunes the processing engine.
type Config struct {
	Concurrency      int           // provider calls in flight per channel
	BatchSize        int           // rows claimed per queue poll
	PollInterval     time.Duration // sleep when the queue is empty
	Lease            time.Duration // how long a claimed row is reserved
	MaxAttempts      int           // attempts before a notification fails for good
	SendTimeout      time.Duration // upper bound of one provider call incl. bookkeeping
	Retry            RetryPolicy
	BreakerThreshold int
	BreakerCooldown  time.Duration
	ReaperInterval   time.Duration
	GaugeInterval    time.Duration
}

// Worker runs one dispatcher per channel plus housekeeping loops.
type Worker struct {
	cfg       Config
	store     Store
	sender    Sender
	limiter   ratelimit.Limiter
	publisher events.Publisher
	metrics   *observability.Metrics
	logger    *slog.Logger
	tracer    trace.Tracer
	breakers  map[domain.Channel]*Breaker
}

// New creates a worker.
func New(cfg Config, st Store, sender Sender, limiter ratelimit.Limiter, pub events.Publisher,
	metrics *observability.Metrics, logger *slog.Logger) *Worker {
	breakers := make(map[domain.Channel]*Breaker, len(domain.Channels))
	for _, ch := range domain.Channels {
		breakers[ch] = NewBreaker(cfg.BreakerThreshold, cfg.BreakerCooldown)
	}
	return &Worker{
		cfg: cfg, store: st, sender: sender, limiter: limiter, publisher: pub,
		metrics: metrics, logger: logger, breakers: breakers,
		tracer: otel.Tracer("github.com/keremsirtikizil/insider-notification-system/worker"),
	}
}

// Run blocks until ctx is cancelled, then drains in-flight deliveries and
// returns claimed-but-unsent rows to the queue.
func (w *Worker) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, ch := range domain.Channels {
		wg.Go(func() { w.runChannel(ctx, ch) })
	}
	wg.Go(func() { w.every(ctx, w.cfg.ReaperInterval, w.recoverLeases) })
	wg.Go(func() { w.every(ctx, w.cfg.GaugeInterval, w.refreshGauges) })
	w.logger.Info("worker started", "channels", domain.Channels, "concurrency_per_channel", w.cfg.Concurrency)
	wg.Wait()
	w.logger.Info("worker stopped")
}

// runChannel is the dispatcher of one channel:
//
//	claim batch (priority order) ─▶ wait for rate-limit token ─▶ hand to sender pool
//
// Tokens are taken by this single goroutine, so the limiter sees one caller
// per channel per process; the sender pool only provides I/O concurrency.
func (w *Worker) runChannel(ctx context.Context, ch domain.Channel) {
	jobs := make(chan *domain.Notification)
	var senders sync.WaitGroup
	for range w.cfg.Concurrency {
		senders.Go(func() {
			for n := range jobs {
				w.deliver(n)
			}
		})
	}
	defer func() {
		close(jobs)
		senders.Wait() // in-flight provider calls finish even during shutdown
	}()

	breaker := w.breakers[ch]
	for ctx.Err() == nil {
		if wait := breaker.Wait(); wait > 0 {
			sleep(ctx, min(wait, time.Second))
			continue
		}
		batch, err := w.store.ClaimDue(ctx, ch, w.cfg.BatchSize, w.cfg.Lease)
		if err != nil {
			if ctx.Err() == nil {
				w.logger.Error("claim failed", "channel", ch, "error", err)
				sleep(ctx, time.Second)
			}
			continue
		}
		if len(batch) == 0 {
			sleep(ctx, w.cfg.PollInterval)
			continue
		}
		w.publish(batch...)

		for i, n := range batch {
			start := time.Now()
			if err := w.limiter.Wait(ctx, string(ch)); err != nil {
				w.releaseAll(batch[i:], time.Now())
				if ctx.Err() == nil {
					w.logger.Error("rate limiter unavailable, pausing channel", "channel", ch, "error", err)
					sleep(ctx, time.Second)
				}
				break
			}
			w.metrics.RateLimitWait.WithLabelValues(string(ch)).Observe(time.Since(start).Seconds())
			select {
			case jobs <- n:
			case <-ctx.Done():
				// Shutdown while every sender is busy: unsent rows go back to the queue.
				// (If ctx ends after a hand-off, the next limiter.Wait releases the rest.)
				w.releaseAll(batch[i:], time.Now())
				return
			}
		}
	}
}

// deliver performs one attempt. It deliberately does not use the worker's
// shutdown context: once a message is on its way, the outcome must be recorded.
func (w *Worker) deliver(n *domain.Notification) {
	ctx, cancel := context.WithTimeout(context.Background(), w.cfg.SendTimeout)
	defer cancel()
	ctx = observability.WithCorrelationID(ctx, n.CorrelationID)
	ctx = observability.ExtractTraceParent(ctx, n.TraceParent)
	ctx, span := w.tracer.Start(ctx, "notification.deliver", trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("notification.id", n.ID.String()),
			attribute.String("notification.channel", string(n.Channel)),
			attribute.String("notification.priority", n.Priority.String()),
			attribute.Int("notification.attempt", n.Attempts),
		))
	defer span.End()
	log := w.logger.With("notification_id", n.ID, "channel", n.Channel, "attempt", n.Attempts)

	breaker := w.breakers[n.Channel]
	if ok, wait := breaker.Allow(); !ok {
		w.release(ctx, n, time.Now().Add(wait))
		span.SetAttributes(attribute.String("notification.result", "released"))
		return
	}

	channelLabel := string(n.Channel)
	w.metrics.InFlight.WithLabelValues(channelLabel).Inc()
	start := time.Now()
	res, sendErr := w.sender.Send(ctx, provider.Message{
		To: n.Recipient, Channel: string(n.Channel), Content: n.Content,
		IdempotencyKey: n.ID.String(), CorrelationID: n.CorrelationID,
	})
	elapsed := time.Since(start)
	w.metrics.InFlight.WithLabelValues(channelLabel).Dec()
	w.metrics.DeliveryDuration.WithLabelValues(channelLabel).Observe(elapsed.Seconds())

	result := w.classify(n, res, sendErr, elapsed)
	if result.Outcome == domain.OutcomeRetryableError {
		breaker.Failure()
	} else {
		breaker.Success()
	}

	updated, err := w.store.FinishAttempt(ctx, n.ID, result)
	if err != nil {
		// ErrLeaseLost: the reaper already re-queued it; the next attempt will
		// carry the same Idempotency-Key to the provider.
		log.ErrorContext(ctx, "record delivery outcome failed", "error", err, "result", result.Status)
		span.RecordError(err)
		span.SetStatus(codes.Error, "record outcome failed")
		return
	}
	w.metrics.Deliveries.WithLabelValues(channelLabel, string(updated.Status)).Inc()
	w.publish(updated)

	attrs := []any{"result", updated.Status, "duration_ms", elapsed.Milliseconds()}
	span.SetAttributes(attribute.String("notification.result", string(updated.Status)))
	switch updated.Status {
	case domain.StatusSent:
		latencyFrom := updated.CreatedAt
		if updated.ScheduledAt != nil {
			latencyFrom = *updated.ScheduledAt
		}
		w.metrics.EndToEndLatency.WithLabelValues(channelLabel).Observe(time.Since(latencyFrom).Seconds())
		log.InfoContext(ctx, "notification sent", append(attrs, "provider_message_id", res.MessageID)...)
	case domain.StatusRetrying:
		span.SetStatus(codes.Error, *result.Error)
		log.WarnContext(ctx, "delivery failed, will retry",
			append(attrs, "error", *result.Error, "next_attempt_at", result.NextAttemptAt)...)
	default:
		span.SetStatus(codes.Error, *result.Error)
		log.ErrorContext(ctx, "delivery failed permanently", append(attrs, "error", *result.Error)...)
	}
}

// classify turns a provider response into the next state of the notification.
func (w *Worker) classify(n *domain.Notification, res provider.Result, sendErr error, elapsed time.Duration) domain.AttemptResult {
	r := domain.AttemptResult{Duration: elapsed}
	if sendErr == nil {
		r.Status, r.Outcome = domain.StatusSent, domain.OutcomeSuccess
		code := res.StatusCode
		r.StatusCode = &code
		if res.MessageID != "" {
			id := res.MessageID
			r.ProviderMessageID = &id
		}
		return r
	}

	msg := sendErr.Error()
	r.Error = &msg
	var perr *provider.Error
	if errors.As(sendErr, &perr) && perr.StatusCode != 0 {
		code := perr.StatusCode
		r.StatusCode = &code
	}
	if !provider.IsRetryable(sendErr) {
		r.Status, r.Outcome = domain.StatusFailed, domain.OutcomePermanentError
		return r
	}
	r.Outcome = domain.OutcomeRetryableError
	if n.Attempts >= w.cfg.MaxAttempts {
		r.Status = domain.StatusFailed
		final := fmt.Sprintf("gave up after %d attempts: %s", n.Attempts, msg)
		r.Error = &final
		return r
	}
	var retryAfter time.Duration
	if perr != nil {
		retryAfter = perr.RetryAfter
	}
	next := time.Now().Add(w.cfg.Retry.Backoff(n.Attempts, retryAfter))
	r.Status, r.NextAttemptAt = domain.StatusRetrying, &next
	return r
}

func (w *Worker) release(ctx context.Context, n *domain.Notification, at time.Time) {
	updated, err := w.store.Release(ctx, n.ID, at)
	if err != nil {
		w.logger.ErrorContext(ctx, "release notification failed", "notification_id", n.ID, "error", err)
		return
	}
	w.metrics.Deliveries.WithLabelValues(string(n.Channel), "released").Inc()
	w.publish(updated)
}

func (w *Worker) releaseAll(ns []*domain.Notification, at time.Time) {
	if len(ns) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, n := range ns {
		w.release(ctx, n, at)
	}
}

func (w *Worker) recoverLeases(ctx context.Context) {
	recovered, err := w.store.RecoverExpiredLeases(ctx, w.cfg.MaxAttempts)
	if err != nil {
		if ctx.Err() == nil {
			w.logger.Error("recover expired leases failed", "error", err)
		}
		return
	}
	if len(recovered) > 0 {
		w.logger.Warn("recovered notifications with expired leases", "count", len(recovered))
		w.publish(recovered...)
	}
}

func (w *Worker) refreshGauges(ctx context.Context) {
	for ch, b := range w.breakers {
		w.metrics.CircuitBreakerOpen.WithLabelValues(string(ch)).Set(float64(b.State()))
	}
	rows, err := w.store.QueueDepth(ctx)
	if err != nil {
		if ctx.Err() == nil {
			w.logger.Warn("queue depth query failed", "error", err)
		}
		return
	}
	depth := map[[2]string]float64{}
	for _, ch := range domain.Channels {
		for _, st := range []domain.Status{domain.StatusPending, domain.StatusScheduled, domain.StatusRetrying, domain.StatusProcessing} {
			depth[[2]string{string(ch), string(st)}] = 0
		}
	}
	for _, r := range rows {
		depth[[2]string{string(r.Channel), string(r.Status)}] += float64(r.Count)
	}
	for k, v := range depth {
		w.metrics.QueueDepth.WithLabelValues(k[0], k[1]).Set(v)
	}
}

func (w *Worker) publish(ns ...*domain.Notification) {
	evs := make([]events.Event, len(ns))
	for i, n := range ns {
		evs[i] = events.FromNotification(n)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	w.publisher.Publish(ctx, evs...)
}

func (w *Worker) every(ctx context.Context, interval time.Duration, fn func(context.Context)) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		fn(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// Breakers exposes the circuit breakers (health endpoint, tests).
func (w *Worker) Breakers() map[domain.Channel]*Breaker { return w.breakers }

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
