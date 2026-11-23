// Package config loads settings from environment variables (12-factor).
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds every setting of the API and worker processes.
type Config struct {
	LogLevel    string
	HTTPAddr    string // API listen address
	MetricsAddr string // worker /metrics + /healthz listen address
	DatabaseURL string
	RedisURL    string
	DBMaxConns  int

	ProviderURL     string
	ProviderTimeout time.Duration

	RateLimitPerSecond int    // per channel, shared by all workers through Redis
	RateLimiter        string // "redis" (default) or "local"

	WorkerConcurrency  int
	WorkerBatchSize    int
	WorkerPollInterval time.Duration
	WorkerLease        time.Duration
	MaxAttempts        int
	RetryBaseDelay     time.Duration
	RetryMaxDelay      time.Duration
	BreakerThreshold   int
	BreakerCooldown    time.Duration
	ShutdownTimeout    time.Duration

	MaxBatchSize int
}

// Load reads the configuration from the environment and validates it.
func Load() (Config, error) {
	var errs []error
	c := Config{
		LogLevel:    str("LOG_LEVEL", "info"),
		HTTPAddr:    str("HTTP_ADDR", ":8080"),
		MetricsAddr: str("METRICS_ADDR", ":9091"),
		DatabaseURL: str("DATABASE_URL", "postgres://notifier:notifier@localhost:5432/notifier?sslmode=disable"),
		RedisURL:    str("REDIS_URL", "redis://localhost:6379/0"),
		DBMaxConns:  num("DB_MAX_CONNS", 20, &errs),

		ProviderURL:     str("PROVIDER_URL", "http://localhost:8090/send"),
		ProviderTimeout: dur("PROVIDER_TIMEOUT", 10*time.Second, &errs),

		RateLimitPerSecond: num("RATE_LIMIT_PER_SECOND", 100, &errs),
		RateLimiter:        str("RATE_LIMITER", "redis"),

		WorkerConcurrency:  num("WORKER_CONCURRENCY", 20, &errs),
		WorkerBatchSize:    num("WORKER_BATCH_SIZE", 50, &errs),
		WorkerPollInterval: dur("WORKER_POLL_INTERVAL", 250*time.Millisecond, &errs),
		WorkerLease:        dur("WORKER_LEASE", 60*time.Second, &errs),
		MaxAttempts:        num("MAX_ATTEMPTS", 5, &errs),
		RetryBaseDelay:     dur("RETRY_BASE_DELAY", 2*time.Second, &errs),
		RetryMaxDelay:      dur("RETRY_MAX_DELAY", 5*time.Minute, &errs),
		BreakerThreshold:   num("BREAKER_FAILURE_THRESHOLD", 10, &errs),
		BreakerCooldown:    dur("BREAKER_COOLDOWN", 30*time.Second, &errs),
		ShutdownTimeout:    dur("SHUTDOWN_TIMEOUT", 20*time.Second, &errs),

		MaxBatchSize: num("MAX_BATCH_SIZE", 1000, &errs),
	}
	errs = append(errs, c.validate()...)
	return c, errors.Join(errs...)
}

func (c Config) validate() []error {
	var errs []error
	positive := map[string]int{
		"DB_MAX_CONNS": c.DBMaxConns, "RATE_LIMIT_PER_SECOND": c.RateLimitPerSecond,
		"WORKER_CONCURRENCY": c.WorkerConcurrency, "WORKER_BATCH_SIZE": c.WorkerBatchSize,
		"MAX_ATTEMPTS": c.MaxAttempts, "BREAKER_FAILURE_THRESHOLD": c.BreakerThreshold,
		"MAX_BATCH_SIZE": c.MaxBatchSize,
	}
	for name, v := range positive {
		if v <= 0 {
			errs = append(errs, fmt.Errorf("%s must be > 0", name))
		}
	}
	if c.RateLimiter != "redis" && c.RateLimiter != "local" {
		errs = append(errs, fmt.Errorf("RATE_LIMITER must be redis or local"))
	}
	if c.RateLimitPerSecond > 0 {
		// A claimed row may wait for its rate-limit token (pacing a whole batch)
		// and then for a full provider call. A shorter lease would let the reaper
		// re-queue a message that is still being sent: a duplicate delivery.
		pacing := time.Duration(c.WorkerBatchSize) * time.Second / time.Duration(c.RateLimitPerSecond)
		if minLease := c.ProviderTimeout + 5*time.Second + pacing; c.WorkerLease <= minLease {
			errs = append(errs, fmt.Errorf(
				"WORKER_LEASE (%s) must exceed PROVIDER_TIMEOUT + 5s + WORKER_BATCH_SIZE/RATE_LIMIT_PER_SECOND (%s)",
				c.WorkerLease, minLease))
		}
	}
	if c.RetryMaxDelay < c.RetryBaseDelay {
		errs = append(errs, fmt.Errorf("RETRY_MAX_DELAY must be >= RETRY_BASE_DELAY"))
	}
	return errs
}

func str(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func num(key string, def int, errs *[]error) int {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s: %q is not an integer", key, v))
		return def
	}
	return n
}

func dur(key string, def time.Duration, errs *[]error) time.Duration {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		*errs = append(*errs, fmt.Errorf("%s: %q is not a positive duration (e.g. 500ms, 10s)", key, v))
		return def
	}
	return d
}
