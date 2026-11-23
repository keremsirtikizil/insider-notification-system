package observability

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds every Prometheus collector of the service. API and worker
// processes register the same set; each only moves the ones it owns.
type Metrics struct {
	Registry *prometheus.Registry

	HTTPRequests         *prometheus.CounterVec
	HTTPDuration         *prometheus.HistogramVec
	NotificationsCreated *prometheus.CounterVec
	Deliveries           *prometheus.CounterVec
	DeliveryDuration     *prometheus.HistogramVec
	EndToEndLatency      *prometheus.HistogramVec
	RateLimitWait        *prometheus.HistogramVec
	QueueDepth           *prometheus.GaugeVec
	CircuitBreakerOpen   *prometheus.GaugeVec
	InFlight             *prometheus.GaugeVec
	WebSocketClients     prometheus.Gauge
}

// NewMetrics creates and registers the collectors on a fresh registry.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	m := &Metrics{
		Registry: reg,
		HTTPRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_requests_total", Help: "HTTP requests by route and status code.",
		}, []string{"method", "route", "code"}),
		HTTPDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "http_request_duration_seconds", Help: "HTTP request latency.",
			Buckets: prometheus.DefBuckets,
		}, []string{"method", "route"}),
		NotificationsCreated: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notifications_created_total", Help: "Notifications accepted by the API.",
		}, []string{"channel", "priority"}),
		Deliveries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notification_deliveries_total",
			Help: "Delivery attempts by result: sent, retrying, failed, released.",
		}, []string{"channel", "result"}),
		DeliveryDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "notification_provider_request_duration_seconds", Help: "Latency of provider calls.",
			Buckets: []float64{.01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}, []string{"channel"}),
		EndToEndLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "notification_end_to_end_latency_seconds",
			Help:    "Time from creation (or schedule time) until the provider accepted the message.",
			Buckets: []float64{.05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 300, 900},
		}, []string{"channel"}),
		RateLimitWait: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "notification_rate_limit_wait_seconds", Help: "Time spent waiting for a rate-limit token.",
			Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1},
		}, []string{"channel"}),
		QueueDepth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "notification_queue_depth", Help: "Unfinished notifications by channel and status.",
		}, []string{"channel", "status"}),
		CircuitBreakerOpen: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "notification_circuit_breaker_state",
			Help: "Provider circuit breaker per channel: 0 closed, 1 half-open, 2 open.",
		}, []string{"channel"}),
		InFlight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "notification_deliveries_in_flight", Help: "Provider calls currently in progress.",
		}, []string{"channel"}),
		WebSocketClients: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "websocket_clients", Help: "Connected WebSocket subscribers.",
		}),
	}
	reg.MustRegister(m.HTTPRequests, m.HTTPDuration, m.NotificationsCreated, m.Deliveries,
		m.DeliveryDuration, m.EndToEndLatency, m.RateLimitWait, m.QueueDepth,
		m.CircuitBreakerOpen, m.InFlight, m.WebSocketClients)
	return m
}

// Handler exposes the registry in the Prometheus text format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{Registry: m.Registry})
}
