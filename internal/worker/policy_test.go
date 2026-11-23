package worker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBackoffGrowsExponentiallyWithJitterAndCap(t *testing.T) {
	p := RetryPolicy{Base: time.Second, Max: 30 * time.Second}
	for attempt, ceiling := range map[int]time.Duration{
		1: time.Second, 2: 2 * time.Second, 3: 4 * time.Second, 4: 8 * time.Second, 6: 30 * time.Second, 50: 30 * time.Second,
	} {
		for range 100 {
			d := p.Backoff(attempt, 0)
			assert.GreaterOrEqual(t, d, ceiling/2, "attempt %d", attempt)
			assert.LessOrEqual(t, d, ceiling, "attempt %d", attempt)
		}
	}
}

func TestBackoffDeterministicBounds(t *testing.T) {
	low := RetryPolicy{Base: time.Second, Max: time.Minute, Random: func() float64 { return 0 }}
	high := RetryPolicy{Base: time.Second, Max: time.Minute, Random: func() float64 { return 0.999999 }}
	assert.Equal(t, 2*time.Second, low.Backoff(3, 0))
	assert.InDelta(t, float64(4*time.Second), float64(high.Backoff(3, 0)), float64(time.Millisecond))
}

func TestBackoffHonoursRetryAfterUpToMax(t *testing.T) {
	p := RetryPolicy{Base: time.Second, Max: time.Minute, Random: func() float64 { return 0 }}
	assert.Equal(t, 20*time.Second, p.Backoff(1, 20*time.Second))
	assert.Equal(t, time.Minute, p.Backoff(1, time.Hour))
	assert.Equal(t, 500*time.Millisecond, p.Backoff(1, 100*time.Millisecond), "shorter hint keeps the backoff")
}

func TestBreakerLifecycle(t *testing.T) {
	now := time.Unix(0, 0)
	b := NewBreaker(3, 10*time.Second)
	b.now = func() time.Time { return now }

	ok, _ := b.Allow()
	assert.True(t, ok)
	b.Failure()
	b.Failure()
	b.Success() // resets the consecutive count
	b.Failure()
	b.Failure()
	assert.Equal(t, BreakerClosed, b.State())
	b.Failure()
	assert.Equal(t, BreakerOpen, b.State())

	ok, wait := b.Allow()
	assert.False(t, ok)
	assert.Equal(t, 10*time.Second, wait)
	assert.Equal(t, 10*time.Second, b.Wait())

	now = now.Add(10 * time.Second)
	ok, _ = b.Allow() // the single half-open probe
	assert.True(t, ok)
	assert.Equal(t, BreakerHalfOpen, b.State())
	ok, _ = b.Allow()
	assert.False(t, ok, "only one probe while half-open")

	b.Failure() // probe failed
	assert.Equal(t, BreakerOpen, b.State())

	now = now.Add(10 * time.Second)
	ok, _ = b.Allow()
	assert.True(t, ok)
	b.Success() // probe succeeded
	assert.Equal(t, BreakerClosed, b.State())
	assert.Zero(t, b.Wait())
}
