package worker

import (
	"math/rand/v2"
	"time"
)

// RetryPolicy computes how long to wait before the next attempt.
//
// Exponential backoff with "equal jitter": the n-th retry waits a random
// duration in [d/2, d] where d = base * 2^(n-1), capped at max. Jitter spreads
// out the retries of a burst that failed together (e.g. a provider outage
// during a flash sale), so they do not hit the provider again in lockstep.
// A Retry-After hint from the provider is honoured when it is longer.
type RetryPolicy struct {
	Base   time.Duration
	Max    time.Duration
	Random func() float64 // [0,1); nil uses math/rand
}

// Backoff returns the delay before retrying after the given (1-based) attempt.
func (p RetryPolicy) Backoff(attempt int, retryAfter time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := p.Max
	if shift := attempt - 1; shift < 32 {
		if exp := p.Base << shift; exp > 0 && exp < p.Max {
			d = exp
		}
	}
	random := p.Random
	if random == nil {
		random = rand.Float64
	}
	delay := d/2 + time.Duration(random()*float64(d/2))
	if retryAfter > delay {
		delay = min(retryAfter, p.Max)
	}
	return delay
}
