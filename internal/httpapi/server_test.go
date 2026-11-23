package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keremsirtikizil/insider-notification-system/internal/domain"
	"github.com/keremsirtikizil/insider-notification-system/internal/events"
	"github.com/keremsirtikizil/insider-notification-system/internal/observability"
	"github.com/keremsirtikizil/insider-notification-system/internal/store"
)

var testNow = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

type recordingPublisher struct{ events []events.Event }

func (p *recordingPublisher) Publish(_ context.Context, evs ...events.Event) {
	p.events = append(p.events, evs...)
}

type testAPI struct {
	store *fakeStore
	pub   *recordingPublisher
	hub   *Hub
	h     http.Handler
}

func newTestAPI(t *testing.T, checks ...HealthCheck) *testAPI {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	metrics := observability.NewMetrics()
	api := &testAPI{store: newFakeStore(), pub: &recordingPublisher{}, hub: NewHub(logger, metrics)}
	api.h = New(Options{
		Store: api.store, Publisher: api.pub, Hub: api.hub, Metrics: metrics, Logger: logger,
		HealthChecks: checks, MaxBatchSize: 5, Now: func() time.Time { return testNow },
	}).Handler()
	return api
}

func (a *testAPI) do(t *testing.T, method, path string, body any, headers ...string) *httptest.ResponseRecorder {
	t.Helper()
	var r io.Reader
	switch b := body.(type) {
	case nil:
	case string:
		r = strings.NewReader(b)
	default:
		raw, err := json.Marshal(b)
		require.NoError(t, err)
		r = bytes.NewReader(raw)
	}
	req := httptest.NewRequest(method, path, r)
	req.Header.Set("Content-Type", "application/json")
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	rec := httptest.NewRecorder()
	a.h.ServeHTTP(rec, req)
	return rec
}

func decode[T any](t *testing.T, rec *httptest.ResponseRecorder) T {
	t.Helper()
	var v T
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &v), rec.Body.String())
	return v
}

func sms(content string) map[string]any {
	return map[string]any{"recipient": "+905551234567", "channel": "sms", "content": content}
}

func TestCreateNotification(t *testing.T) {
	api := newTestAPI(t)
	body := sms("Flash sale starts now")
	body["priority"] = "high"
	rec := api.do(t, http.MethodPost, "/v1/notifications", body, "X-Correlation-ID", "req-42")

	require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())
	n := decode[notificationResponse](t, rec)
	assert.Equal(t, "pending", n.Status)
	assert.Equal(t, "high", n.Priority)
	assert.Equal(t, "req-42", n.CorrelationID)
	assert.Equal(t, "req-42", rec.Header().Get("X-Correlation-ID"))
	assert.Equal(t, "/v1/notifications/"+n.ID.String(), rec.Header().Get("Location"))
	require.Len(t, api.pub.events, 1)
	assert.Equal(t, n.ID, api.pub.events[0].NotificationID)
}

func TestCreateNotificationGeneratesCorrelationID(t *testing.T) {
	api := newTestAPI(t)
	rec := api.do(t, http.MethodPost, "/v1/notifications", sms("hi"))
	require.Equal(t, http.StatusAccepted, rec.Code)
	_, err := uuid.Parse(rec.Header().Get("X-Correlation-ID"))
	assert.NoError(t, err)
}

func TestCreateNotificationValidation(t *testing.T) {
	api := newTestAPI(t)
	cases := []struct {
		name  string
		body  map[string]any
		field string
	}{
		{"bad phone", map[string]any{"recipient": "555", "channel": "sms", "content": "x"}, "recipient"},
		{"bad email", map[string]any{"recipient": "nope", "channel": "email", "content": "x"}, "recipient"},
		{"unknown channel", map[string]any{"recipient": "x", "channel": "fax", "content": "x"}, "channel"},
		{"empty content", sms(""), "content"},
		{"sms too long", sms(strings.Repeat("a", domain.MaxSMSLength+1)), "content"},
		{"bad priority", map[string]any{"recipient": "+905551234567", "channel": "sms", "content": "x", "priority": "urgent"}, "priority"},
		{"past schedule", map[string]any{"recipient": "+905551234567", "channel": "sms", "content": "x",
			"scheduled_at": testNow.Add(-time.Minute)}, "scheduled_at"},
		{"schedule too far", map[string]any{"recipient": "+905551234567", "channel": "sms", "content": "x",
			"scheduled_at": testNow.Add(60 * 24 * time.Hour)}, "scheduled_at"},
		{"variables without template", map[string]any{"recipient": "+905551234567", "channel": "sms", "content": "x",
			"variables": map[string]string{"a": "b"}}, "variables"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := api.do(t, http.MethodPost, "/v1/notifications", c.body)
			require.Equal(t, http.StatusUnprocessableEntity, rec.Code, rec.Body.String())
			body := decode[errorBody](t, rec)
			assert.Equal(t, "validation_failed", body.Error.Code)
			require.NotEmpty(t, body.Error.Details)
			assert.Equal(t, c.field, body.Error.Details[0].Field)
		})
	}
	assert.Empty(t, api.store.rows, "nothing is stored for invalid requests")
}

func TestCreateNotificationRejectsMalformedJSON(t *testing.T) {
	api := newTestAPI(t)
	for _, body := range []string{"", "{", `{"recipient":"+905551234567","chanel":"sms"}`, `{} {}`} {
		rec := api.do(t, http.MethodPost, "/v1/notifications", body)
		assert.Equal(t, http.StatusBadRequest, rec.Code, body)
		assert.Equal(t, "invalid_json", decode[errorBody](t, rec).Error.Code, body)
	}
}

func TestScheduledNotification(t *testing.T) {
	api := newTestAPI(t)
	at := testNow.Add(2 * time.Hour)
	body := sms("reminder")
	body["scheduled_at"] = at
	rec := api.do(t, http.MethodPost, "/v1/notifications", body)
	require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())
	n := decode[notificationResponse](t, rec)
	assert.Equal(t, "scheduled", n.Status)
	require.NotNil(t, n.ScheduledAt)
	assert.True(t, at.Equal(*n.ScheduledAt))
	assert.True(t, at.Equal(*n.NextAttemptAt))
}

func TestIdempotentCreate(t *testing.T) {
	api := newTestAPI(t)
	first := api.do(t, http.MethodPost, "/v1/notifications", sms("hi"), "Idempotency-Key", "order-1")
	require.Equal(t, http.StatusAccepted, first.Code)

	replay := api.do(t, http.MethodPost, "/v1/notifications", sms("hi"), "Idempotency-Key", "order-1")
	require.Equal(t, http.StatusOK, replay.Code)
	assert.Equal(t, "true", replay.Header().Get("Idempotent-Replayed"))
	assert.Equal(t, decode[notificationResponse](t, first).ID, decode[notificationResponse](t, replay).ID)
	assert.Len(t, api.store.rows, 1)
	assert.Len(t, api.pub.events, 1, "a replay publishes nothing")

	conflict := api.do(t, http.MethodPost, "/v1/notifications", sms("different"), "Idempotency-Key", "order-1")
	require.Equal(t, http.StatusConflict, conflict.Code)
	assert.Equal(t, "idempotency_conflict", decode[errorBody](t, conflict).Error.Code)
}

func TestCreateBatch(t *testing.T) {
	api := newTestAPI(t)
	items := []map[string]any{sms("a"), sms("b"), {"recipient": "jane@example.com", "channel": "email", "content": "c"}}
	items[0]["idempotency_key"] = "k-0"
	rec := api.do(t, http.MethodPost, "/v1/notifications/batch", map[string]any{"notifications": items})
	require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())
	resp := decode[batchCreateResponse](t, rec)
	assert.Equal(t, 3, resp.Accepted)
	assert.Zero(t, resp.Duplicates)
	require.Len(t, resp.Notifications, 3)
	for i, item := range resp.Notifications {
		assert.Equal(t, i, item.Index)
		assert.Equal(t, resp.BatchID, *item.BatchID)
	}

	// Batch status.
	rec = api.do(t, http.MethodGet, "/v1/batches/"+resp.BatchID.String(), nil)
	require.Equal(t, http.StatusOK, rec.Code)
	b := decode[batchResponse](t, rec)
	assert.Equal(t, 3, b.Total)
	assert.Equal(t, 3, b.StatusCounts["pending"])
	assert.Zero(t, b.StatusCounts["sent"])
	assert.False(t, b.Completed)

	// Replaying the batch marks the keyed item as a duplicate.
	rec = api.do(t, http.MethodPost, "/v1/notifications/batch", map[string]any{"notifications": items[:1]})
	require.Equal(t, http.StatusAccepted, rec.Code)
	replay := decode[batchCreateResponse](t, rec)
	assert.Equal(t, 1, replay.Duplicates)
	assert.True(t, replay.Notifications[0].Duplicate)
	assert.Equal(t, resp.Notifications[0].ID, replay.Notifications[0].ID)

	// Cancel the batch.
	rec = api.do(t, http.MethodPost, "/v1/batches/"+resp.BatchID.String()+"/cancel", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.EqualValues(t, 3, decode[map[string]any](t, rec)["cancelled"])
}

func TestCreateBatchIsAllOrNothing(t *testing.T) {
	api := newTestAPI(t)
	items := []map[string]any{sms("ok"), {"recipient": "bad", "channel": "sms", "content": "x"}}
	rec := api.do(t, http.MethodPost, "/v1/notifications/batch", map[string]any{"notifications": items})
	require.Equal(t, http.StatusUnprocessableEntity, rec.Code)
	assert.Equal(t, "notifications[1].recipient", decode[errorBody](t, rec).Error.Details[0].Field)
	assert.Empty(t, api.store.rows)
}

func TestCreateBatchLimits(t *testing.T) {
	api := newTestAPI(t) // MaxBatchSize: 5
	rec := api.do(t, http.MethodPost, "/v1/notifications/batch", map[string]any{"notifications": []any{}})
	assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)

	var many []map[string]any
	for range 6 {
		many = append(many, sms("x"))
	}
	rec = api.do(t, http.MethodPost, "/v1/notifications/batch", map[string]any{"notifications": many})
	require.Equal(t, http.StatusUnprocessableEntity, rec.Code)
	assert.Contains(t, decode[errorBody](t, rec).Error.Details[0].Message, "at most 5")

	dup := []map[string]any{sms("a"), sms("b")}
	dup[0]["idempotency_key"], dup[1]["idempotency_key"] = "same", "same"
	rec = api.do(t, http.MethodPost, "/v1/notifications/batch", map[string]any{"notifications": dup})
	require.Equal(t, http.StatusUnprocessableEntity, rec.Code)
	assert.Equal(t, "notifications[1].idempotency_key", decode[errorBody](t, rec).Error.Details[0].Field)
}

func TestTemplates(t *testing.T) {
	api := newTestAPI(t)
	rec := api.do(t, http.MethodPost, "/v1/templates", map[string]any{
		"name": "order.shipped", "channel": "sms", "content": "Hi {{name}}, order {{order_id}} shipped.",
	})
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	tpl := decode[templateResponse](t, rec)
	assert.Equal(t, []string{"name", "order_id"}, tpl.Variables)

	dup := api.do(t, http.MethodPost, "/v1/templates", map[string]any{"name": "order.shipped", "channel": "sms", "content": "x"})
	assert.Equal(t, http.StatusConflict, dup.Code)

	rec = api.do(t, http.MethodPost, "/v1/notifications", map[string]any{
		"recipient": "+905551234567", "channel": "sms", "template_id": tpl.ID,
		"variables": map[string]string{"name": "Ayşe", "order_id": "A-1"},
	})
	require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())
	n := decode[notificationResponse](t, rec)
	assert.Equal(t, "Hi Ayşe, order A-1 shipped.", n.Content)
	assert.Equal(t, tpl.ID, *n.TemplateID)

	for name, body := range map[string]map[string]any{
		"variables": {"recipient": "+905551234567", "channel": "sms", "template_id": tpl.ID,
			"variables": map[string]string{"name": "x"}},
		"template_id": {"recipient": "jane@example.com", "channel": "email", "template_id": tpl.ID,
			"variables": map[string]string{"name": "x", "order_id": "y"}},
		"content": {"recipient": "+905551234567", "channel": "sms", "template_id": tpl.ID, "content": "both",
			"variables": map[string]string{"name": "x", "order_id": "y"}},
	} {
		rec := api.do(t, http.MethodPost, "/v1/notifications", body)
		require.Equal(t, http.StatusUnprocessableEntity, rec.Code, name)
		assert.Equal(t, name, decode[errorBody](t, rec).Error.Details[0].Field)
	}

	rec = api.do(t, http.MethodPost, "/v1/notifications", map[string]any{
		"recipient": "+905551234567", "channel": "sms", "template_id": uuid.New(),
	})
	assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)

	rec = api.do(t, http.MethodGet, "/v1/templates", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Len(t, decode[map[string][]templateResponse](t, rec)["data"], 1)
}

func TestGetAndCancelNotification(t *testing.T) {
	api := newTestAPI(t)
	n := decode[notificationResponse](t, api.do(t, http.MethodPost, "/v1/notifications", sms("x")))

	rec := api.do(t, http.MethodGet, "/v1/notifications/"+n.ID.String(), nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, n.ID, decode[notificationResponse](t, rec).ID)

	assert.Equal(t, http.StatusNotFound, api.do(t, http.MethodGet, "/v1/notifications/"+uuid.NewString(), nil).Code)
	assert.Equal(t, http.StatusBadRequest, api.do(t, http.MethodGet, "/v1/notifications/not-a-uuid", nil).Code)

	rec = api.do(t, http.MethodPost, "/v1/notifications/"+n.ID.String()+"/cancel", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "cancelled", decode[notificationResponse](t, rec).Status)

	rec = api.do(t, http.MethodPost, "/v1/notifications/"+n.ID.String()+"/cancel", nil)
	require.Equal(t, http.StatusConflict, rec.Code)
	assert.Equal(t, "invalid_state", decode[errorBody](t, rec).Error.Code)

	rec = api.do(t, http.MethodPost, "/v1/notifications/"+n.ID.String()+"/retry", nil)
	assert.Equal(t, http.StatusConflict, rec.Code, "only failed notifications can be retried")

	rec = api.do(t, http.MethodGet, "/v1/notifications/"+n.ID.String()+"/attempts", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestListNotificationsFiltersAndPaginates(t *testing.T) {
	api := newTestAPI(t)
	for i := range 5 {
		api.do(t, http.MethodPost, "/v1/notifications", sms(fmt.Sprintf("sms %d", i)))
	}
	api.do(t, http.MethodPost, "/v1/notifications", map[string]any{"recipient": "a@b.co", "channel": "email", "content": "e"})

	var seen []string
	cursor := ""
	for page := 0; ; page++ {
		path := "/v1/notifications?channel=sms&status=pending&limit=2"
		if cursor != "" {
			path += "&cursor=" + cursor
		}
		rec := api.do(t, http.MethodGet, path, nil)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		resp := decode[listResponse](t, rec)
		for _, n := range resp.Data {
			assert.Equal(t, "sms", n.Channel)
			seen = append(seen, n.Content)
		}
		if resp.Pagination.NextCursor == nil {
			break
		}
		cursor = *resp.Pagination.NextCursor
		require.Less(t, page, 5)
	}
	assert.Equal(t, []string{"sms 4", "sms 3", "sms 2", "sms 1", "sms 0"}, seen, "newest first, no gaps or repeats")

	for _, q := range []string{"status=bogus", "channel=fax", "limit=0", "limit=500", "cursor=bm9wZQ",
		"batch_id=x", "created_from=yesterday", "created_from=2026-01-02T00:00:00Z&created_to=2026-01-01T00:00:00Z"} {
		rec := api.do(t, http.MethodGet, "/v1/notifications?"+q, nil)
		assert.Equal(t, http.StatusBadRequest, rec.Code, q)
	}
}

func TestCursorRoundTrip(t *testing.T) {
	c := encodeCursor(nil)
	assert.Nil(t, c)
	in := struct {
		at time.Time
		id uuid.UUID
	}{time.Date(2026, 3, 4, 5, 6, 7, 891011000, time.UTC), uuid.New()}
	enc := encodeCursor(&store.Cursor{CreatedAt: in.at, ID: in.id})
	out, err := decodeCursor(*enc)
	require.NoError(t, err)
	assert.True(t, in.at.Equal(out.CreatedAt))
	assert.Equal(t, in.id, out.ID)
}

func TestMetricsSummary(t *testing.T) {
	api := newTestAPI(t)
	api.do(t, http.MethodPost, "/v1/notifications", sms("x"))
	rec := api.do(t, http.MethodGet, "/v1/metrics?window=1m", nil)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	m := decode[metricsSummaryResponse](t, rec)
	assert.Equal(t, 60, m.WindowSeconds)
	assert.EqualValues(t, 1, m.QueueDepth)
	assert.EqualValues(t, 1, m.Channels["sms"].Queue["pending"])
	assert.EqualValues(t, 1, m.Channels["sms"].QueuedByPrio["normal"])
	assert.InDelta(t, 0.9, m.Channels["sms"].SuccessRate, 0.0001)
	assert.InDelta(t, 1.5, m.Channels["sms"].ThroughputRPS, 0.0001)
	assert.Contains(t, m.Channels, "push")

	assert.Equal(t, http.StatusBadRequest, api.do(t, http.MethodGet, "/v1/metrics?window=1s", nil).Code)

	prom := api.do(t, http.MethodGet, "/metrics", nil)
	require.Equal(t, http.StatusOK, prom.Code)
	assert.Contains(t, prom.Body.String(), `notifications_created_total{channel="sms",priority="normal"} 1`)
	assert.Contains(t, prom.Body.String(), `http_requests_total{code="202",method="POST",route="POST /v1/notifications"} 1`)
}

func TestHealthEndpoints(t *testing.T) {
	api := newTestAPI(t,
		HealthCheck{Name: "postgres", Check: func(context.Context) error { return nil }},
		HealthCheck{Name: "redis", Check: func(context.Context) error { return errors.New("connection refused") }},
	)
	assert.Equal(t, http.StatusOK, api.do(t, http.MethodGet, "/healthz", nil).Code)
	rec := api.do(t, http.MethodGet, "/readyz", nil)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	body := decode[map[string]any](t, rec)
	assert.Equal(t, "unavailable", body["status"])
	assert.Equal(t, "ok", body["checks"].(map[string]any)["postgres"])
}

func TestStoreErrorsBecome500WithoutLeakingDetails(t *testing.T) {
	api := newTestAPI(t)
	api.store.err = errors.New("pq: connection reset by peer")
	rec := api.do(t, http.MethodPost, "/v1/notifications", sms("x"))
	require.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.NotContains(t, rec.Body.String(), "connection reset")
	assert.NotEmpty(t, decode[errorBody](t, rec).CorrelationID)
}

func TestDocsAndSpecAreServed(t *testing.T) {
	api := newTestAPI(t)
	assert.Contains(t, api.do(t, http.MethodGet, "/docs", nil).Body.String(), "swagger-ui")
	spec := api.do(t, http.MethodGet, "/openapi.yaml", nil)
	assert.Equal(t, http.StatusOK, spec.Code)
	assert.Contains(t, spec.Body.String(), "openapi:")
}

func TestWebSocketReceivesFilteredStatusEvents(t *testing.T) {
	api := newTestAPI(t)
	srv := httptest.NewServer(api.h)
	defer srv.Close()

	target := uuid.New()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http")+"/v1/ws?notification_id="+target.String(), nil)
	require.NoError(t, err)
	defer conn.CloseNow()

	var hello map[string]any
	require.NoError(t, wsjson.Read(ctx, conn, &hello))
	assert.Equal(t, "subscribed", hello["type"])

	api.hub.Broadcast(events.Event{Type: events.TypeStatusChanged, NotificationID: uuid.New(), Status: "sent"})
	api.hub.Broadcast(events.Event{Type: events.TypeStatusChanged, NotificationID: target, Status: "sent", Channel: "sms"})

	var got events.Event
	require.NoError(t, wsjson.Read(ctx, conn, &got))
	assert.Equal(t, target, got.NotificationID, "events for other notifications are filtered out")
	assert.Equal(t, "sent", got.Status)

	rec := api.do(t, http.MethodGet, "/v1/ws?batch_id=nope", nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
