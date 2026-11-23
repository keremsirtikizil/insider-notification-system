// Command notifier is the single binary of the notification system.
//
//	notifier api                 serve the REST/WebSocket API
//	notifier worker              run the processing engine
//	notifier migrate [up|down N|version]
//	notifier healthcheck [URL]   exit 0 if URL answers 200 (Docker HEALTHCHECK)
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/keremsirtikizil/insider-notification-system/internal/app"
	"github.com/keremsirtikizil/insider-notification-system/internal/config"
	"github.com/keremsirtikizil/insider-notification-system/internal/observability"
	"github.com/keremsirtikizil/insider-notification-system/migrations"
)

const usage = `usage: notifier <command>

commands:
  api                    serve the HTTP API
  worker                 run the delivery workers
  migrate [up|down N|version]
  healthcheck [URL]      probe a health endpoint (default http://127.0.0.1:8080/healthz)
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	if os.Args[1] == "healthcheck" {
		os.Exit(healthcheck(os.Args[2:]))
	}
	if err := run(os.Args[1], os.Args[2:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(cmd string, args []string) error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}
	logger := observability.NewLogger(os.Stdout, cfg.LogLevel, "notification-"+cmd)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	switch cmd {
	case "api", "worker":
		shutdownTracing, err := observability.SetupTracing(ctx, "notification-"+cmd)
		if err != nil {
			return fmt.Errorf("setup tracing: %w", err)
		}
		defer func() {
			sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = shutdownTracing(sctx)
		}()
		if cmd == "api" {
			return app.RunAPI(ctx, cfg, logger)
		}
		return app.RunWorker(ctx, cfg, logger)
	case "migrate":
		return migrate(cfg, args, logger.Info)
	default:
		fmt.Fprint(os.Stderr, usage)
		return fmt.Errorf("unknown command %q", cmd)
	}
}

func migrate(cfg config.Config, args []string, info func(string, ...any)) error {
	sub := "up"
	if len(args) > 0 {
		sub = args[0]
	}
	switch sub {
	case "up":
		if err := migrations.Up(cfg.DatabaseURL); err != nil {
			return err
		}
	case "down":
		steps := 1
		if len(args) > 1 {
			n, err := strconv.Atoi(args[1])
			if err != nil || n < 1 {
				return fmt.Errorf("down expects a positive number of steps")
			}
			steps = n
		}
		if err := migrations.Down(cfg.DatabaseURL, steps); err != nil {
			return err
		}
	case "version":
	default:
		return fmt.Errorf("unknown migrate command %q", sub)
	}
	version, dirty, err := migrations.Version(cfg.DatabaseURL)
	if err != nil {
		return err
	}
	info("schema version", "version", version, "dirty", dirty)
	return nil
}

func healthcheck(args []string) int {
	url := "http://127.0.0.1:8080/healthz"
	if len(args) > 0 {
		url = args[0]
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody) //nolint:gosec // the probe URL is supplied by the operator
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	resp, err := http.DefaultClient.Do(req) //nolint:gosec // same operator-supplied URL
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintln(os.Stderr, "unhealthy:", resp.Status)
		return 1
	}
	return 0
}
