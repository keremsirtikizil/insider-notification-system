package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
)

func TestLoggerAddsCorrelationAndTraceIDs(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(&buf, "debug", "notification-api").With("component", "test")

	traceID, _ := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	spanID, _ := trace.SpanIDFromHex("00f067aa0ba902b7")
	ctx := trace.ContextWithSpanContext(context.Background(),
		trace.NewSpanContext(trace.SpanContextConfig{TraceID: traceID, SpanID: spanID, TraceFlags: trace.FlagsSampled}))
	ctx = WithCorrelationID(ctx, "req-1")
	logger.InfoContext(ctx, "hello", "n", 1)

	var line map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &line))
	assert.Equal(t, "hello", line["msg"])
	assert.Equal(t, "notification-api", line["service"])
	assert.Equal(t, "test", line["component"])
	assert.Equal(t, "req-1", line["correlation_id"])
	assert.Equal(t, "4bf92f3577b34da6a3ce929d0e0e4736", line["trace_id"])
	assert.Equal(t, "00f067aa0ba902b7", line["span_id"])

	buf.Reset()
	logger.Info("no context")
	assert.NotContains(t, buf.String(), "correlation_id")
}

func TestParseLevel(t *testing.T) {
	assert.Equal(t, slog.LevelDebug, ParseLevel("DEBUG"))
	assert.Equal(t, slog.LevelWarn, ParseLevel("warning"))
	assert.Equal(t, slog.LevelError, ParseLevel("error"))
	assert.Equal(t, slog.LevelInfo, ParseLevel("nonsense"))
}

func TestTraceParentRoundTrip(t *testing.T) {
	shutdown, err := SetupTracing(context.Background(), "test") // no exporter configured: propagation only
	require.NoError(t, err)
	defer func() { _ = shutdown(context.Background()) }()

	traceID, _ := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	spanID, _ := trace.SpanIDFromHex("00f067aa0ba902b7")
	ctx := trace.ContextWithSpanContext(context.Background(),
		trace.NewSpanContext(trace.SpanContextConfig{TraceID: traceID, SpanID: spanID, TraceFlags: trace.FlagsSampled}))

	tp := InjectTraceParent(ctx)
	assert.Equal(t, "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", tp)
	restored := trace.SpanContextFromContext(ExtractTraceParent(context.Background(), tp))
	assert.Equal(t, traceID, restored.TraceID())
	assert.True(t, restored.IsRemote())
	assert.Equal(t, context.Background(), ExtractTraceParent(context.Background(), ""))
}
