// Package app wires configuration, infrastructure clients and components into
// the runnable API and worker processes.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/keremsirtikizil/insider-notification-system/internal/config"
	"github.com/keremsirtikizil/insider-notification-system/internal/events"
	"github.com/keremsirtikizil/insider-notification-system/internal/httpapi"
	"github.com/keremsirtikizil/insider-notification-system/internal/observability"
	"github.com/keremsirtikizil/insider-notification-system/internal/provider"
	"github.com/keremsirtikizil/insider-notification-system/internal/ratelimit"
	"github.com/keremsirtikizil/insider-notification-system/internal/store"
	"github.com/keremsirtikizil/insider-notification-system/internal/worker"
)

const (
	// EventsChannel is the Redis Pub/Sub channel of status events.
	EventsChannel = "notifications:events"
	// RateLimitPrefix namespaces the per-channel GCRA keys in Redis.
	RateLimitPrefix = "ratelimit:"
)

// ConnectPostgres opens a pool and waits (up to 30 s) for the database.
func ConnectPostgres(ctx context.Context, url string, maxConns int) (*pgxpool.Pool, error) {
	pcfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parse DATABASE_URL: %w", err)
	}
	pcfg.MaxConns = int32(min(maxConns, 10_000)) //nolint:gosec // clamped above, validated > 0
	pool, err := pgxpool.NewWithConfig(ctx, pcfg)
	if err != nil {
		return nil, err
	}
	if err := waitFor(ctx, "postgres", pool.Ping); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

// ConnectRedis opens a client and waits (up to 30 s) for the server.
func ConnectRedis(ctx context.Context, url string) (*redis.Client, error) {
	opts, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("parse REDIS_URL: %w", err)
	}
	rdb := redis.NewClient(opts)
	if err := waitFor(ctx, "redis", func(ctx context.Context) error { return rdb.Ping(ctx).Err() }); err != nil {
		_ = rdb.Close()
		return nil, err
	}
	return rdb, nil
}

func waitFor(ctx context.Context, name string, ping func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for {
		pctx, pcancel := context.WithTimeout(ctx, 2*time.Second)
		err := ping(pctx)
		pcancel()
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s not reachable: %w", name, err)
		case <-time.After(time.Second):
		}
	}
}

// RunAPI serves the HTTP API until ctx is cancelled.
func RunAPI(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	pool, err := ConnectPostgres(ctx, cfg.DatabaseURL, cfg.DBMaxConns)
	if err != nil {
		return err
	}
	defer pool.Close()
	rdb, err := ConnectRedis(ctx, cfg.RedisURL)
	if err != nil {
		return err
	}
	defer rdb.Close()

	st := store.New(pool)
	bus := events.NewRedisBus(rdb, EventsChannel, logger)
	metrics := observability.NewMetrics()
	hub := httpapi.NewHub(logger, metrics)
	stream, err := bus.Subscribe(ctx)
	if err != nil {
		return fmt.Errorf("subscribe to status events: %w", err)
	}
	go hub.Run(ctx, stream)

	srv := httpapi.New(httpapi.Options{
		Store: st, Publisher: bus, Hub: hub, Metrics: metrics, Logger: logger,
		MaxBatchSize: cfg.MaxBatchSize,
		HealthChecks: []httpapi.HealthCheck{
			{Name: "postgres", Check: st.Ping},
			{Name: "redis", Check: func(ctx context.Context) error { return rdb.Ping(ctx).Err() }},
		},
	})
	httpServer := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	logger.Info("api listening", "addr", cfg.HTTPAddr)
	return serveUntilDone(ctx, httpServer, cfg.ShutdownTimeout)
}

// WorkerConfig maps the process configuration onto the engine settings.
func WorkerConfig(cfg config.Config) worker.Config {
	return worker.Config{
		Concurrency:      cfg.WorkerConcurrency,
		BatchSize:        cfg.WorkerBatchSize,
		PollInterval:     cfg.WorkerPollInterval,
		Lease:            cfg.WorkerLease,
		MaxAttempts:      cfg.MaxAttempts,
		SendTimeout:      cfg.ProviderTimeout + 5*time.Second,
		Retry:            worker.RetryPolicy{Base: cfg.RetryBaseDelay, Max: cfg.RetryMaxDelay},
		BreakerThreshold: cfg.BreakerThreshold,
		BreakerCooldown:  cfg.BreakerCooldown,
		ReaperInterval:   15 * time.Second,
		GaugeInterval:    5 * time.Second,
	}
}

// RunWorker runs the processing engine until ctx is cancelled, then drains.
func RunWorker(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	pool, err := ConnectPostgres(ctx, cfg.DatabaseURL, cfg.DBMaxConns)
	if err != nil {
		return err
	}
	defer pool.Close()
	rdb, err := ConnectRedis(ctx, cfg.RedisURL)
	if err != nil {
		return err
	}
	defer rdb.Close()

	st := store.New(pool)
	metrics := observability.NewMetrics()
	var limiter ratelimit.Limiter = ratelimit.NewRedis(rdb, RateLimitPrefix, cfg.RateLimitPerSecond)
	if cfg.RateLimiter == "local" {
		limiter = ratelimit.NewLocal(cfg.RateLimitPerSecond)
	}
	client := provider.New(cfg.ProviderURL, cfg.ProviderTimeout, otelhttp.NewTransport(http.DefaultTransport))
	w := worker.New(WorkerConfig(cfg), st, client, limiter, events.NewRedisBus(rdb, EventsChannel, logger), metrics, logger)

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", metrics.Handler())
	mux.HandleFunc("GET /healthz", func(rw http.ResponseWriter, r *http.Request) {
		breakers := map[string]string{}
		for ch, b := range w.Breakers() {
			breakers[string(ch)] = b.State().String()
		}
		rw.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(rw, `{"status":"ok","circuit_breakers":{"sms":%q,"email":%q,"push":%q}}`+"\n",
			breakers["sms"], breakers["email"], breakers["push"])
	})
	metricsServer := &http.Server{Addr: cfg.MetricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("worker metrics server failed", "error", err)
		}
	}()
	logger.Info("worker metrics listening", "addr", cfg.MetricsAddr, "provider_url", cfg.ProviderURL,
		"rate_limit_per_second", cfg.RateLimitPerSecond, "rate_limiter", cfg.RateLimiter)

	w.Run(ctx) // returns once in-flight deliveries are recorded

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return metricsServer.Shutdown(shutdownCtx)
}

func serveUntilDone(ctx context.Context, srv *http.Server, timeout time.Duration) error {
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()
	select {
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}
