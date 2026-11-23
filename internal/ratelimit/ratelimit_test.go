package ratelimit_test

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keremsirtikizil/insider-notification-system/internal/ratelimit"
	"github.com/keremsirtikizil/insider-notification-system/internal/testutil"
)

// maxInWindow returns the largest number of events inside any window.
func maxInWindow(ts []time.Time, window time.Duration) int {
	sort.Slice(ts, func(i, j int) bool { return ts[i].Before(ts[j]) })
	best := 0
	for i := range ts {
		n := 0
		for j := i; j < len(ts) && ts[j].Sub(ts[i]) < window; j++ {
			n++
		}
		best = max(best, n)
	}
	return best
}

// hammer lets several "worker processes" compete for tokens for a while and
// returns the time of every granted token.
func hammer(t *testing.T, limiters []ratelimit.Limiter, key string, d time.Duration) []time.Time {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	var (
		mu    sync.Mutex
		times []time.Time
		wg    sync.WaitGroup
	)
	for _, l := range limiters {
		wg.Go(func() {
			for l.Wait(ctx, key) == nil {
				mu.Lock()
				times = append(times, time.Now())
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	return times
}

func TestLocalLimiter(t *testing.T) {
	t.Parallel()
	l := ratelimit.NewLocal(50)
	times := hammer(t, []ratelimit.Limiter{l, l, l}, "sms", 1500*time.Millisecond)
	assert.LessOrEqual(t, maxInWindow(times, time.Second), 51)
	assert.GreaterOrEqual(t, len(times), 60, "the limiter must not starve callers")
}

func TestRedisLimiterIsSharedAcrossInstances(t *testing.T) {
	t.Parallel()
	rdb, prefix := testutil.NewRedis(t)
	// Three limiter instances = three worker processes sharing one budget.
	limiters := []ratelimit.Limiter{
		ratelimit.NewRedis(rdb, prefix, 50),
		ratelimit.NewRedis(rdb, prefix, 50),
		ratelimit.NewRedis(rdb, prefix, 50),
	}
	times := hammer(t, limiters, "sms", 1500*time.Millisecond)
	assert.LessOrEqual(t, maxInWindow(times, time.Second), 51, "global limit holds across instances")
	assert.GreaterOrEqual(t, len(times), 60)

	// Another key has an independent budget.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	require.NoError(t, limiters[0].Wait(ctx, "email"))
}

func TestRedisLimiterHonoursContext(t *testing.T) {
	t.Parallel()
	rdb, prefix := testutil.NewRedis(t)
	l := ratelimit.NewRedis(rdb, prefix, 1)
	require.NoError(t, l.Wait(context.Background(), "push"))
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	assert.ErrorIs(t, l.Wait(ctx, "push"), context.DeadlineExceeded)
}
