// Package httpapi is the REST + WebSocket interface of the notification system.
package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/keremsirtikizil/insider-notification-system/internal/domain"
	"github.com/keremsirtikizil/insider-notification-system/internal/events"
	"github.com/keremsirtikizil/insider-notification-system/internal/observability"
	"github.com/keremsirtikizil/insider-notification-system/internal/store"
)

// Store is the persistence the API depends on.
type Store interface {
	CreateNotifications(ctx context.Context, batch *domain.Batch, ns []*domain.Notification) ([]store.CreateResult, error)
	GetNotification(ctx context.Context, id uuid.UUID) (*domain.Notification, error)
	ListNotifications(ctx context.Context, f store.ListFilter) ([]*domain.Notification, *store.Cursor, error)
	CancelNotification(ctx context.Context, id uuid.UUID) (*domain.Notification, error)
	RetryNotification(ctx context.Context, id uuid.UUID) (*domain.Notification, error)
	ListAttempts(ctx context.Context, id uuid.UUID) ([]domain.DeliveryAttempt, error)
	GetBatch(ctx context.Context, id uuid.UUID) (*domain.Batch, error)
	CancelBatch(ctx context.Context, id uuid.UUID) ([]*domain.Notification, error)
	CreateTemplate(ctx context.Context, t *domain.Template) (*domain.Template, error)
	GetTemplate(ctx context.Context, id uuid.UUID) (*domain.Template, error)
	ListTemplates(ctx context.Context) ([]*domain.Template, error)
	QueueDepth(ctx context.Context) ([]store.QueueDepthRow, error)
	Stats(ctx context.Context, window time.Duration) (map[domain.Channel]*store.ChannelStats, error)
}

// HealthCheck is one named readiness dependency.
type HealthCheck struct {
	Name  string
	Check func(context.Context) error
}

// Options configures a Server.
type Options struct {
	Store            Store
	Publisher        events.Publisher
	Hub              *Hub
	Metrics          *observability.Metrics
	Logger           *slog.Logger
	HealthChecks     []HealthCheck
	MaxBatchSize     int           // default 1000
	MaxScheduleAhead time.Duration // default 30 days
	Now              func() time.Time
}

// Server holds the HTTP handlers.
type Server struct {
	opts Options
	mux  *http.ServeMux
}

// New creates the server and registers its routes.
func New(opts Options) *Server {
	if opts.MaxBatchSize == 0 {
		opts.MaxBatchSize = 1000
	}
	if opts.MaxScheduleAhead == 0 {
		opts.MaxScheduleAhead = 30 * 24 * time.Hour
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Publisher == nil {
		opts.Publisher = events.Nop{}
	}
	if opts.Hub == nil {
		opts.Hub = NewHub(opts.Logger, opts.Metrics)
	}
	s := &Server{opts: opts, mux: http.NewServeMux()}
	s.routes()
	return s
}

func (s *Server) routes() {
	m := s.mux
	m.HandleFunc("POST /v1/notifications", s.createNotification)
	m.HandleFunc("POST /v1/notifications/batch", s.createBatch)
	m.HandleFunc("GET /v1/notifications", s.listNotifications)
	m.HandleFunc("GET /v1/notifications/{id}", s.getNotification)
	m.HandleFunc("GET /v1/notifications/{id}/attempts", s.listAttempts)
	m.HandleFunc("POST /v1/notifications/{id}/cancel", s.cancelNotification)
	m.HandleFunc("POST /v1/notifications/{id}/retry", s.retryNotification)

	m.HandleFunc("GET /v1/batches/{id}", s.getBatch)
	m.HandleFunc("POST /v1/batches/{id}/cancel", s.cancelBatch)

	m.HandleFunc("POST /v1/templates", s.createTemplate)
	m.HandleFunc("GET /v1/templates", s.listTemplates)
	m.HandleFunc("GET /v1/templates/{id}", s.getTemplate)

	m.HandleFunc("GET /v1/metrics", s.metricsSummary)
	m.HandleFunc("GET /v1/ws", s.opts.Hub.ServeWS)

	m.Handle("GET /metrics", s.opts.Metrics.Handler())
	m.HandleFunc("GET /healthz", s.healthz)
	m.HandleFunc("GET /readyz", s.readyz)
	m.HandleFunc("GET /docs", serveDocs)
	m.HandleFunc("GET /openapi.yaml", serveSpec)
	m.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/docs", http.StatusFound)
	})
}

// Handler returns the routes wrapped in the middleware chain:
// tracing -> correlation ID -> panic recovery -> access log + metrics -> router.
func (s *Server) Handler() http.Handler {
	var h http.Handler = s.mux
	h = s.observe(h)
	h = s.recoverPanics(h)
	h = correlate(h)
	return otelhttp.NewHandler(h, "http.server",
		otelhttp.WithFilter(func(r *http.Request) bool { return !isProbe(r.URL.Path) }))
}

func isProbe(path string) bool {
	return path == "/metrics" || path == "/healthz" || path == "/readyz"
}
