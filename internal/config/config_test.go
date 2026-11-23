package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaults(t *testing.T) {
	c, err := Load()
	require.NoError(t, err)
	assert.Equal(t, 100, c.RateLimitPerSecond, "assessment requirement: 100 msg/s per channel")
	assert.Equal(t, 1000, c.MaxBatchSize)
	assert.Equal(t, 5, c.MaxAttempts)
	assert.Equal(t, "redis", c.RateLimiter)
}

func TestOverrides(t *testing.T) {
	t.Setenv("RATE_LIMIT_PER_SECOND", "250")
	t.Setenv("PROVIDER_TIMEOUT", "3s")
	t.Setenv("PROVIDER_URL", "https://webhook.site/abc")
	c, err := Load()
	require.NoError(t, err)
	assert.Equal(t, 250, c.RateLimitPerSecond)
	assert.Equal(t, 3*time.Second, c.ProviderTimeout)
	assert.Equal(t, "https://webhook.site/abc", c.ProviderURL)
}

func TestInvalidValuesAreReported(t *testing.T) {
	t.Setenv("WORKER_CONCURRENCY", "many")
	t.Setenv("RETRY_BASE_DELAY", "-1s")
	t.Setenv("RATE_LIMITER", "memcached")
	_, err := Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "WORKER_CONCURRENCY")
	assert.Contains(t, err.Error(), "RETRY_BASE_DELAY")
	assert.Contains(t, err.Error(), "RATE_LIMITER")
}

func TestLeaseMustOutliveProviderTimeout(t *testing.T) {
	t.Setenv("WORKER_LEASE", "5s")
	t.Setenv("PROVIDER_TIMEOUT", "10s")
	_, err := Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "WORKER_LEASE")
}
