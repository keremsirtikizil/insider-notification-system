// Package ratelimit paces deliveries per channel.
//
// The Redis implementation is shared by every worker process, so the
// "100 messages per second per channel" limit holds globally no matter how
// many worker replicas run. It uses GCRA (generic cell rate algorithm) with a
// burst of 1: messages are spaced evenly (10 ms apart at 100/s), which
// guarantees that no 1-second window ever contains more than the limit.
package ratelimit

import (
	"context"
	"sync"
	"time"

	"github.com/go-redis/redis_rate/v10"
	"github.com/redis/go-redis/v9"
	"golang.org/x/time/rate"
)

// Limiter blocks until the caller may send one message for key.
type Limiter interface {
	Wait(ctx context.Context, key string) error
}

// Redis is a distributed GCRA limiter.
type Redis struct {
	limiter *redis_rate.Limiter
	limit   redis_rate.Limit
	prefix  string
}

// NewRedis creates a limiter allowing perSecond events per key.
func NewRedis(rdb *redis.Client, prefix string, perSecond int) *Redis {
	return &Redis{
		limiter: redis_rate.NewLimiter(rdb),
		limit:   redis_rate.Limit{Rate: perSecond, Burst: 1, Period: time.Second},
		prefix:  prefix,
	}
}

// Wait blocks until a token is available or ctx is done. Redis errors are
// returned to the caller, which must not send without a token (fail closed).
func (l *Redis) Wait(ctx context.Context, key string) error {
	for {
		res, err := l.limiter.Allow(ctx, l.prefix+key, l.limit)
		if err != nil {
			return err
		}
		if res.Allowed > 0 {
			return nil
		}
		wait := res.RetryAfter
		if wait <= 0 {
			wait = time.Millisecond
		}
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
	}
}

// Local is an in-process limiter (single worker deployments and tests).
type Local struct {
	mu        sync.Mutex
	perKey    map[string]*rate.Limiter
	perSecond int
}

// NewLocal creates an in-process limiter allowing perSecond events per key.
func NewLocal(perSecond int) *Local {
	return &Local{perKey: map[string]*rate.Limiter{}, perSecond: perSecond}
}

// Wait blocks until a token is available or ctx is done.
func (l *Local) Wait(ctx context.Context, key string) error {
	l.mu.Lock()
	lim, ok := l.perKey[key]
	if !ok {
		lim = rate.NewLimiter(rate.Limit(l.perSecond), 1)
		l.perKey[key] = lim
	}
	l.mu.Unlock()
	return lim.Wait(ctx)
}
