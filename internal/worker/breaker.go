package worker

import (
	"sync"
	"time"
)

// BreakerState is the state of a circuit breaker.
type BreakerState int

const (
	BreakerClosed   BreakerState = 0
	BreakerHalfOpen BreakerState = 1
	BreakerOpen     BreakerState = 2
)

func (s BreakerState) String() string {
	switch s {
	case BreakerClosed:
		return "closed"
	case BreakerHalfOpen:
		return "half_open"
	default:
		return "open"
	}
}

// Breaker is a per-channel circuit breaker in front of the provider.
//
// After `threshold` consecutive retryable failures the circuit opens: the
// dispatcher stops claiming work for that channel for `cooldown`, so an outage
// does not burn the attempt budget of every queued message. Then a single probe
// is let through (half-open); success closes the circuit, failure re-opens it.
// Permanent (4xx) errors count as success: the provider is healthy, the
// request was just invalid.
type Breaker struct {
	mu        sync.Mutex
	threshold int
	cooldown  time.Duration
	now       func() time.Time

	state    BreakerState
	failures int
	openedAt time.Time
	probing  bool
}

// NewBreaker creates a closed breaker.
func NewBreaker(threshold int, cooldown time.Duration) *Breaker {
	return &Breaker{threshold: threshold, cooldown: cooldown, now: time.Now}
}

// Allow reports whether a call may proceed. When it may not, it also returns
// how long the caller should wait before asking again.
func (b *Breaker) Allow() (bool, time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case BreakerClosed:
		return true, 0
	case BreakerOpen:
		if wait := b.cooldown - b.now().Sub(b.openedAt); wait > 0 {
			return false, wait
		}
		b.state, b.probing = BreakerHalfOpen, true
		return true, 0
	default: // half-open: exactly one probe in flight
		if b.probing {
			return false, time.Second
		}
		b.probing = true
		return true, 0
	}
}

// Wait returns how long the circuit stays open (0 when calls may be attempted).
func (b *Breaker) Wait() time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state != BreakerOpen {
		return 0
	}
	return max(b.cooldown-b.now().Sub(b.openedAt), 0)
}

// Success records a call that reached a healthy provider.
func (b *Breaker) Success() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == BreakerOpen {
		return // late result of a call started before the circuit opened
	}
	b.state, b.failures, b.probing = BreakerClosed, 0, false
}

// Failure records a retryable provider failure.
func (b *Breaker) Failure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case BreakerHalfOpen:
		b.trip()
	case BreakerClosed:
		b.failures++
		if b.failures >= b.threshold {
			b.trip()
		}
	}
}

func (b *Breaker) trip() {
	b.state, b.openedAt, b.probing, b.failures = BreakerOpen, b.now(), false, 0
}

// State returns the current state.
func (b *Breaker) State() BreakerState {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}
