package httpapi

import (
	"bufio"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"regexp"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	"github.com/keremsirtikizil/insider-notification-system/internal/observability"
)

var correlationIDRe = regexp.MustCompile(`^[A-Za-z0-9._:\-]{1,128}$`)

// correlate takes the caller's X-Correlation-ID (or X-Request-ID), or creates
// one, stores it in the request context and echoes it in the response.
func correlate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(observability.CorrelationHeader)
		if id == "" {
			id = r.Header.Get("X-Request-ID")
		}
		if !correlationIDRe.MatchString(id) {
			id = uuid.NewString()
		}
		w.Header().Set(observability.CorrelationHeader, id)
		next.ServeHTTP(w, r.WithContext(observability.WithCorrelationID(r.Context(), id)))
	})
}

func (s *Server) recoverPanics(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				if err, ok := rec.(error); ok && errors.Is(err, http.ErrAbortHandler) {
					panic(rec)
				}
				s.opts.Logger.ErrorContext(r.Context(), "panic in handler",
					"panic", fmt.Sprint(rec), "stack", string(debug.Stack()))
				writeError(w, r, http.StatusInternalServerError, "internal_error", "internal server error", nil)
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// observe writes the access log and HTTP metrics. It must wrap the mux
// directly: the mux records the matched pattern on the same *http.Request,
// which gives a low-cardinality route label.
func (s *Server) observe(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)

		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}
		elapsed := time.Since(start)
		trace.SpanFromContext(r.Context()).SetName(route)
		s.opts.Metrics.HTTPRequests.WithLabelValues(r.Method, route, strconv.Itoa(rec.status)).Inc()
		s.opts.Metrics.HTTPDuration.WithLabelValues(r.Method, route).Observe(elapsed.Seconds())

		level := slog.LevelInfo
		switch {
		case isProbe(r.URL.Path):
			level = slog.LevelDebug
		case rec.status >= 500:
			level = slog.LevelError
		}
		s.opts.Logger.Log(r.Context(), level, "http request",
			"method", r.Method, "route", route, "path", r.URL.Path, "status", rec.status,
			"bytes", rec.bytes, "duration_ms", elapsed.Milliseconds(), "remote", r.RemoteAddr)
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status      int
	bytes       int
	wroteHeader bool
}

func (r *statusRecorder) WriteHeader(code int) {
	if !r.wroteHeader {
		r.status, r.wroteHeader = code, true
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	r.wroteHeader = true
	n, err := r.ResponseWriter.Write(b)
	r.bytes += n
	return n, err
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (r *statusRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

// Hijack is required for the WebSocket upgrade.
func (r *statusRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hj, ok := r.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("response writer does not support hijacking")
	}
	r.status, r.wroteHeader = http.StatusSwitchingProtocols, true
	return hj.Hijack()
}

// Flush supports streaming responses.
func (r *statusRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}
