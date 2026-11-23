package provider

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSendAccepted(t *testing.T) {
	var got Message
	var headers http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		headers = r.Header.Clone()
		require.NoError(t, json.NewDecoder(r.Body).Decode(&got))
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"messageId":"m-1","status":"accepted","timestamp":"2026-01-02T03:04:05Z"}`))
	}))
	defer srv.Close()

	res, err := New(srv.URL, time.Second, nil).Send(context.Background(), Message{
		To: "+905551234567", Channel: "sms", Content: "hi", IdempotencyKey: "n-1", CorrelationID: "c-1",
	})
	require.NoError(t, err)
	assert.Equal(t, "m-1", res.MessageID)
	assert.Equal(t, "accepted", res.Status)
	assert.Equal(t, http.StatusAccepted, res.StatusCode)
	assert.Equal(t, Message{To: "+905551234567", Channel: "sms", Content: "hi"}, got)
	assert.Equal(t, "n-1", headers.Get("Idempotency-Key"))
	assert.Equal(t, "c-1", headers.Get("X-Correlation-ID"))
}

func TestSendAcceptsNonJSONSuccessBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("This URL has no default content configured."))
	}))
	defer srv.Close()
	res, err := New(srv.URL, time.Second, nil).Send(context.Background(), Message{To: "x", Channel: "sms"})
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, res.StatusCode)
	assert.Empty(t, res.MessageID)
}

func TestSendErrorClassification(t *testing.T) {
	cases := []struct {
		status     int
		retryAfter string
		retryable  bool
		wantAfter  time.Duration
	}{
		{http.StatusInternalServerError, "", true, 0},
		{http.StatusServiceUnavailable, "7", true, 7 * time.Second},
		{http.StatusTooManyRequests, "2", true, 2 * time.Second},
		{http.StatusRequestTimeout, "", true, 0},
		{http.StatusBadRequest, "", false, 0},
		{http.StatusUnauthorized, "", false, 0},
		{http.StatusUnprocessableEntity, "", false, 0},
	}
	for _, c := range cases {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			if c.retryAfter != "" {
				w.Header().Set("Retry-After", c.retryAfter)
			}
			w.WriteHeader(c.status)
			_, _ = w.Write([]byte(`{"error":"nope"}`))
		}))
		_, err := New(srv.URL, time.Second, nil).Send(context.Background(), Message{To: "x", Channel: "sms"})
		srv.Close()

		var perr *Error
		require.True(t, errors.As(err, &perr), "status %d", c.status)
		assert.Equal(t, c.status, perr.StatusCode)
		assert.Equal(t, c.retryable, perr.Retryable, "status %d", c.status)
		assert.Equal(t, c.retryable, IsRetryable(err))
		assert.Equal(t, c.wantAfter, perr.RetryAfter, "status %d", c.status)
		assert.Contains(t, perr.Error(), "nope")
	}
}

func TestSendNetworkErrorsAreRetryable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		time.Sleep(200 * time.Millisecond)
	}))
	defer srv.Close()

	_, err := New(srv.URL, 50*time.Millisecond, nil).Send(context.Background(), Message{To: "x"})
	assert.True(t, IsRetryable(err), "timeout must be retryable: %v", err)

	_, err = New("http://127.0.0.1:1", time.Second, nil).Send(context.Background(), Message{To: "x"})
	assert.True(t, IsRetryable(err), "connection refused must be retryable: %v", err)
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	assert.Equal(t, 30*time.Second, parseRetryAfter("30", now))
	assert.Equal(t, time.Minute, parseRetryAfter(now.Add(time.Minute).Format(http.TimeFormat), now))
	assert.Zero(t, parseRetryAfter("", now))
	assert.Zero(t, parseRetryAfter("soon", now))
	assert.Zero(t, parseRetryAfter("-5", now))
}
