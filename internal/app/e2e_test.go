package app_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keremsirtikizil/insider-notification-system/internal/events"
	"github.com/keremsirtikizil/insider-notification-system/internal/httpapi"
	"github.com/keremsirtikizil/insider-notification-system/internal/observability"
	"github.com/keremsirtikizil/insider-notification-system/internal/provider"
	"github.com/keremsirtikizil/insider-notification-system/internal/ratelimit"
	"github.com/keremsirtikizil/insider-notification-system/internal/store"
	"github.com/keremsirtikizil/insider-notification-system/internal/testutil"
	"github.com/keremsirtikizil/insider-notification-system/internal/worker"
)

// fakeProvider imitates webhook.site; recipients listed in flaky fail once.
type fakeProvider struct {
	mu     sync.Mutex
	calls  []providerCall
	flaky  map[string]bool
	failed map[string]bool
}

type providerCall struct {
	at            time.Time
	msg           provider.Message
	correlationID string
	idempotency   string
}

func (p *fakeProvider) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var m provider.Message
	_ = json.NewDecoder(r.Body).Decode(&m)
	p.mu.Lock()
	p.calls = append(p.calls, providerCall{at: time.Now(), msg: m,
		correlationID: r.Header.Get("X-Correlation-ID"), idempotency: r.Header.Get("Idempotency-Key")})
	fail := p.flaky[m.To] && !p.failed[m.To]
	if fail {
		p.failed[m.To] = true
	}
	p.mu.Unlock()
	if fail {
		http.Error(w, `{"error":"temporarily unavailable"}`, http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"messageId": uuid.NewString(), "status": "accepted", "timestamp": time.Now().UTC().Format(time.RFC3339),
	})
}

func (p *fakeProvider) snapshot() []providerCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]providerCall(nil), p.calls...)
}

func postJSON(t *testing.T, url string, body any, headers ...string) (*http.Response, []byte) {
	t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(raw))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	return resp, out
}

func getJSON(t *testing.T, url string, dst any) {
	t.Helper()
	resp, err := http.Get(url)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, json.NewDecoder(resp.Body).Decode(dst))
}

// TestEndToEnd runs the real API, worker, PostgreSQL queue, Redis rate limiter
// and Redis Pub/Sub against a fake provider.
func TestEndToEnd(t *testing.T) {
	pool := testutil.NewDatabase(t)
	rdb, prefix := testutil.NewRedis(t)
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	st := store.New(pool)
	bus := events.NewRedisBus(rdb, prefix+"events", logger)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// API with WebSocket hub.
	apiMetrics := observability.NewMetrics()
	hub := httpapi.NewHub(logger, apiMetrics)
	stream, err := bus.Subscribe(ctx)
	require.NoError(t, err)
	go hub.Run(ctx, stream)
	api := httptest.NewServer(httpapi.New(httpapi.Options{
		Store: st, Publisher: bus, Hub: hub, Metrics: apiMetrics, Logger: logger,
	}).Handler())
	defer api.Close()

	// Worker against the fake provider, 40 msg/s per channel.
	const perSecond = 40
	prov := &fakeProvider{flaky: map[string]bool{"flaky@example.com": true}, failed: map[string]bool{}}
	provSrv := httptest.NewServer(prov)
	defer provSrv.Close()
	w := worker.New(worker.Config{
		Concurrency: 8, BatchSize: 20, PollInterval: 20 * time.Millisecond, Lease: time.Minute,
		MaxAttempts: 3, SendTimeout: 5 * time.Second,
		Retry:            worker.RetryPolicy{Base: 100 * time.Millisecond, Max: 200 * time.Millisecond},
		BreakerThreshold: 50, BreakerCooldown: time.Second, ReaperInterval: time.Second, GaugeInterval: time.Second,
	}, st, provider.New(provSrv.URL, 2*time.Second, nil), ratelimit.NewRedis(rdb, prefix, perSecond), bus,
		observability.NewMetrics(), logger)
	workerDone := make(chan struct{})
	go func() { w.Run(ctx); close(workerDone) }()
	defer func() { cancel(); <-workerDone }()

	// A template for the email.
	resp, body := postJSON(t, api.URL+"/v1/templates", map[string]any{
		"name": "welcome", "channel": "email", "content": "Welcome {{name}}!",
	})
	require.Equal(t, http.StatusCreated, resp.StatusCode, string(body))
	var tpl struct{ ID uuid.UUID }
	require.NoError(t, json.Unmarshal(body, &tpl))

	// Batch: 60 SMS + 1 templated email whose first delivery fails.
	items := make([]map[string]any, 0, 61)
	for i := range 60 {
		items = append(items, map[string]any{
			"recipient": fmt.Sprintf("+9055500%05d", i), "channel": "sms", "content": fmt.Sprintf("sale %d", i),
			"priority": []string{"high", "normal", "low"}[i%3],
		})
	}
	items = append(items, map[string]any{
		"recipient": "flaky@example.com", "channel": "email", "template_id": tpl.ID,
		"variables": map[string]string{"name": "Ayşe"},
	})

	// Subscribe to the batch's events before creating it... we need the ID
	// first, so subscribe to all events of the email channel instead.
	wsCtx, wsCancel := context.WithTimeout(ctx, 15*time.Second)
	defer wsCancel()
	conn, _, err := websocket.Dial(wsCtx, "ws"+strings.TrimPrefix(api.URL, "http")+"/v1/ws?channel=email", nil)
	require.NoError(t, err)
	defer conn.CloseNow()
	var hello map[string]any
	require.NoError(t, wsjson.Read(wsCtx, conn, &hello))

	resp, body = postJSON(t, api.URL+"/v1/notifications/batch", map[string]any{"notifications": items},
		"X-Correlation-ID", "flash-sale-1")
	require.Equal(t, http.StatusAccepted, resp.StatusCode, string(body))
	var created struct {
		BatchID       uuid.UUID `json:"batch_id"`
		Accepted      int       `json:"accepted"`
		Notifications []struct {
			ID uuid.UUID `json:"id"`
		} `json:"notifications"`
	}
	require.NoError(t, json.Unmarshal(body, &created))
	require.Equal(t, 61, created.Accepted)
	emailID := created.Notifications[60].ID

	// Wait until the whole batch is delivered.
	var batch struct {
		Total        int            `json:"total"`
		Completed    bool           `json:"completed"`
		StatusCounts map[string]int `json:"status_counts"`
	}
	require.Eventually(t, func() bool {
		getJSON(t, api.URL+"/v1/batches/"+created.BatchID.String(), &batch)
		return batch.Completed
	}, 15*time.Second, 100*time.Millisecond)
	assert.Equal(t, 61, batch.StatusCounts["sent"], "%v", batch.StatusCounts)

	calls := prov.snapshot()
	assert.Len(t, calls, 62, "61 messages + 1 retry")
	var smsTimes []time.Time
	for _, c := range calls {
		assert.Equal(t, "flash-sale-1", c.correlationID, "correlation ID reaches the provider")
		assert.NotEmpty(t, c.idempotency)
		if c.msg.Channel == "sms" {
			smsTimes = append(smsTimes, c.at)
		}
	}
	sort.Slice(smsTimes, func(i, j int) bool { return smsTimes[i].Before(smsTimes[j]) })
	for i := range smsTimes {
		n := 0
		for j := i; j < len(smsTimes) && smsTimes[j].Sub(smsTimes[i]) < time.Second; j++ {
			n++
		}
		assert.LessOrEqual(t, n, perSecond+1, "SMS rate limit exceeded")
	}

	// The flaky email: retried once, content rendered from the template.
	var email struct {
		Status   string `json:"status"`
		Attempts int    `json:"attempts"`
		Content  string `json:"content"`
	}
	getJSON(t, api.URL+"/v1/notifications/"+emailID.String(), &email)
	assert.Equal(t, "sent", email.Status)
	assert.Equal(t, 2, email.Attempts)
	assert.Equal(t, "Welcome Ayşe!", email.Content)
	var attempts struct {
		Data []struct {
			Outcome    string `json:"outcome"`
			StatusCode *int   `json:"status_code"`
		} `json:"data"`
	}
	getJSON(t, api.URL+"/v1/notifications/"+emailID.String()+"/attempts", &attempts)
	require.Len(t, attempts.Data, 2)
	assert.Equal(t, "retryable_error", attempts.Data[0].Outcome)
	assert.Equal(t, 503, *attempts.Data[0].StatusCode)
	assert.Equal(t, "success", attempts.Data[1].Outcome)

	// The WebSocket subscriber saw the email go through its lifecycle.
	var statuses []string
	for !containsStr(statuses, "sent") {
		var e events.Event
		require.NoError(t, wsjson.Read(wsCtx, conn, &e))
		assert.Equal(t, "email", e.Channel)
		statuses = append(statuses, e.Status)
	}
	assert.Equal(t, []string{"pending", "processing", "retrying", "processing", "sent"}, statuses)

	// Metrics reflect the deliveries.
	var metrics struct {
		Channels map[string]struct {
			Sent     int64 `json:"sent"`
			Provider struct {
				Attempts int64 `json:"attempts"`
				Errors   int64 `json:"errors"`
			} `json:"provider"`
		} `json:"channels"`
	}
	getJSON(t, api.URL+"/v1/metrics", &metrics)
	assert.EqualValues(t, 60, metrics.Channels["sms"].Sent)
	assert.EqualValues(t, 1, metrics.Channels["email"].Sent)
	assert.EqualValues(t, 2, metrics.Channels["email"].Provider.Attempts)
	assert.EqualValues(t, 1, metrics.Channels["email"].Provider.Errors)
}

func containsStr(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}
