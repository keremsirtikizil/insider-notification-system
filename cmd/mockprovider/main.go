// Command mockprovider imitates the webhook.site endpoint configured in the
// assessment: POST {to, channel, content} -> 202 {messageId, status, timestamp}.
//
// It can inject latency and failures so retries, the circuit breaker and the
// rate limiter can be observed locally without an internet connection.
//
//	ADDR=:8090  LATENCY_MIN=20ms  LATENCY_MAX=150ms
//	FAILURE_RATE=0.1   share of requests answered with 500
//	THROTTLE_RATE=0.0  share of requests answered with 429 + Retry-After: 1
package main

import (
	"encoding/json"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

type message struct {
	To      string `json:"to"`
	Channel string `json:"channel"`
	Content string `json:"content"`
}

type stats struct {
	Received  atomic.Int64
	Accepted  atomic.Int64
	Failed    atomic.Int64
	Throttled atomic.Int64
	Invalid   atomic.Int64
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "mock-provider")
	addr := env("ADDR", ":8090")
	failureRate := envFloat("FAILURE_RATE", 0)
	throttleRate := envFloat("THROTTLE_RATE", 0)
	latencyMin := envDuration("LATENCY_MIN", 20*time.Millisecond)
	latencyMax := envDuration("LATENCY_MAX", 150*time.Millisecond)

	var st stats
	mux := http.NewServeMux()
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]int64{
			"received": st.Received.Load(), "accepted": st.Accepted.Load(), "failed": st.Failed.Load(),
			"throttled": st.Throttled.Load(), "invalid": st.Invalid.Load(),
		})
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("POST /", func(w http.ResponseWriter, r *http.Request) {
		st.Received.Add(1)
		var m message
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&m); err != nil || m.To == "" || m.Channel == "" {
			st.Invalid.Add(1)
			http.Error(w, `{"error":"invalid payload"}`, http.StatusBadRequest)
			return
		}
		delay := latencyMin
		if latencyMax > latencyMin {
			delay += time.Duration(rand.Int64N(int64(latencyMax - latencyMin)))
		}
		time.Sleep(delay)

		w.Header().Set("Content-Type", "application/json")
		switch roll := rand.Float64(); {
		case roll < throttleRate:
			st.Throttled.Add(1)
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":"rate limited"}`))
			return
		case roll < throttleRate+failureRate:
			st.Failed.Add(1)
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":"simulated provider failure"}`))
			return
		}
		st.Accepted.Add(1)
		id := uuid.NewString()
		logger.Info("accepted", "message_id", id, "channel", m.Channel,
			"correlation_id", r.Header.Get("X-Correlation-ID"), "idempotency_key", r.Header.Get("Idempotency-Key"))
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"messageId": id, "status": "accepted", "timestamp": time.Now().UTC().Format(time.RFC3339),
		})
	})

	logger.Info("mock provider listening", "addr", addr, "failure_rate", failureRate, "throttle_rate", throttleRate)
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	if err := srv.ListenAndServe(); err != nil {
		logger.Error("server stopped", "error", err)
		os.Exit(1)
	}
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envFloat(key string, def float64) float64 {
	if f, err := strconv.ParseFloat(os.Getenv(key), 64); err == nil {
		return f
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if d, err := time.ParseDuration(os.Getenv(key)); err == nil {
		return d
	}
	return def
}
