package httpapi

import (
	"context"
	"net/http"
	"time"

	"github.com/keremsirtikizil/insider-notification-system/internal/domain"
)

type latencySummary struct {
	AvgMS float64 `json:"avg"`
	P50MS float64 `json:"p50"`
	P95MS float64 `json:"p95"`
	P99MS float64 `json:"p99"`
}

type providerSummary struct {
	Attempts     int64   `json:"attempts"`
	Errors       int64   `json:"errors"`
	ErrorRate    float64 `json:"error_rate"`
	LatencyAvgMS float64 `json:"latency_avg_ms"`
	LatencyP95MS float64 `json:"latency_p95_ms"`
}

type channelSummary struct {
	Queue         map[string]int64 `json:"queue"`
	QueuedByPrio  map[string]int64 `json:"queued_by_priority"`
	Sent          int64            `json:"sent"`
	Failed        int64            `json:"failed"`
	SuccessRate   float64          `json:"success_rate"`
	FailureRate   float64          `json:"failure_rate"`
	ThroughputRPS float64          `json:"throughput_per_second"`
	LatencyMS     latencySummary   `json:"latency_ms"`
	Provider      providerSummary  `json:"provider"`
}

type metricsSummaryResponse struct {
	GeneratedAt   time.Time                  `json:"generated_at"`
	WindowSeconds int                        `json:"window_seconds"`
	QueueDepth    int64                      `json:"queue_depth"`
	Channels      map[string]*channelSummary `json:"channels"`
}

// GET /v1/metrics?window=5m
//
// Computed from the database, so it reflects every API and worker instance,
// not just the process that serves the request. Prometheus metrics per
// process are on /metrics.
func (s *Server) metricsSummary(w http.ResponseWriter, r *http.Request) {
	window := 5 * time.Minute
	if v := r.URL.Query().Get("window"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d < 10*time.Second || d > 24*time.Hour {
			writeError(w, r, http.StatusBadRequest, "invalid_query",
				"window must be a duration between 10s and 24h, e.g. 5m", nil)
			return
		}
		window = d
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	depth, err := s.opts.Store.QueueDepth(ctx)
	if err != nil {
		s.writeStoreError(w, r, err)
		return
	}
	stats, err := s.opts.Store.Stats(ctx, window)
	if err != nil {
		s.writeStoreError(w, r, err)
		return
	}

	resp := metricsSummaryResponse{
		GeneratedAt: s.opts.Now().UTC(), WindowSeconds: int(window.Seconds()),
		Channels: map[string]*channelSummary{},
	}
	for _, ch := range domain.Channels {
		cs := &channelSummary{
			Queue:        map[string]int64{},
			QueuedByPrio: map[string]int64{"high": 0, "normal": 0, "low": 0},
		}
		for _, st := range []domain.Status{domain.StatusPending, domain.StatusScheduled, domain.StatusRetrying, domain.StatusProcessing} {
			cs.Queue[string(st)] = 0
		}
		resp.Channels[string(ch)] = cs
	}
	for _, row := range depth {
		cs := resp.Channels[string(row.Channel)]
		if cs == nil {
			continue
		}
		cs.Queue[string(row.Status)] += row.Count
		if row.Status != domain.StatusProcessing {
			cs.QueuedByPrio[row.Priority.String()] += row.Count
		}
		resp.QueueDepth += row.Count
	}
	for ch, st := range stats {
		cs := resp.Channels[string(ch)]
		if cs == nil {
			continue
		}
		cs.Sent, cs.Failed = st.Sent, st.Failed
		if total := st.Sent + st.Failed; total > 0 {
			cs.SuccessRate = round(float64(st.Sent) / float64(total))
			cs.FailureRate = round(float64(st.Failed) / float64(total))
		}
		cs.ThroughputRPS = round(float64(st.Sent) / window.Seconds())
		cs.LatencyMS = latencySummary{AvgMS: round(st.LatencyAvgMS), P50MS: round(st.LatencyP50MS),
			P95MS: round(st.LatencyP95MS), P99MS: round(st.LatencyP99MS)}
		cs.Provider = providerSummary{Attempts: st.Attempts, Errors: st.AttemptErrors,
			LatencyAvgMS: round(st.ProviderLatencyAvgMS), LatencyP95MS: round(st.ProviderLatencyP95MS)}
		if st.Attempts > 0 {
			cs.Provider.ErrorRate = round(float64(st.AttemptErrors) / float64(st.Attempts))
		}
	}
	writeJSON(w, http.StatusOK, resp)
}

func round(f float64) float64 {
	return float64(int64(f*1000+0.5)) / 1000
}

// GET /healthz — liveness: the process is up and serving.
func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// GET /readyz — readiness: dependencies (PostgreSQL, Redis) are reachable.
func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	checks := map[string]string{}
	status, code := "ok", http.StatusOK
	for _, hc := range s.opts.HealthChecks {
		if err := hc.Check(ctx); err != nil {
			checks[hc.Name] = "error: " + err.Error()
			status, code = "unavailable", http.StatusServiceUnavailable
			continue
		}
		checks[hc.Name] = "ok"
	}
	writeJSON(w, code, map[string]any{"status": status, "checks": checks})
}
