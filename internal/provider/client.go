// Package provider is the HTTP client of the external notification provider
// (webhook.site in the assessment, or the bundled mock provider).
package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Message is the payload the provider expects.
type Message struct {
	To      string `json:"to"`
	Channel string `json:"channel"`
	Content string `json:"content"`

	// Sent as headers, not in the body.
	IdempotencyKey string `json:"-"`
	CorrelationID  string `json:"-"`
}

// Result is the provider's acceptance receipt.
type Result struct {
	MessageID  string    `json:"messageId"`
	Status     string    `json:"status"`
	Timestamp  time.Time `json:"timestamp"`
	StatusCode int       `json:"-"`
}

// Error is a failed provider call, classified for the retry policy.
type Error struct {
	StatusCode int           // 0 when no HTTP response was received
	Retryable  bool          // safe and useful to try again later
	RetryAfter time.Duration // provider hint (Retry-After header), if any
	Message    string
}

func (e *Error) Error() string {
	if e.StatusCode == 0 {
		return "provider unreachable: " + e.Message
	}
	return fmt.Sprintf("provider returned %d: %s", e.StatusCode, e.Message)
}

// Client posts messages to the provider URL.
type Client struct {
	url  string
	http *http.Client
}

// New creates a client. transport may be nil (http.DefaultTransport); the API
// wraps it with OpenTelemetry instrumentation.
func New(url string, timeout time.Duration, transport http.RoundTripper) *Client {
	if transport == nil {
		transport = http.DefaultTransport
	}
	return &Client{url: url, http: &http.Client{Timeout: timeout, Transport: transport}}
}

const maxResponseBody = 64 << 10

// Send delivers one message. A nil error means the provider accepted it (2xx).
//
// Error classification:
//   - network error / timeout          -> retryable (provider may be down)
//   - 408, 425, 429, 5xx               -> retryable (honours Retry-After)
//   - any other 4xx                    -> permanent (the request itself is wrong)
func (c *Client) Send(ctx context.Context, m Message) (Result, error) {
	body, err := json.Marshal(m)
	if err != nil {
		return Result{}, &Error{Message: err.Error()}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		return Result{}, &Error{Message: err.Error()}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if m.IdempotencyKey != "" {
		// Lets a real provider de-duplicate if we ever retry an accepted message.
		req.Header.Set("Idempotency-Key", m.IdempotencyKey)
	}
	if m.CorrelationID != "" {
		req.Header.Set("X-Correlation-ID", m.CorrelationID)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return Result{}, &Error{Retryable: true, Message: err.Error()}
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, maxResponseBody))

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		var res Result
		// The receipt is informative; an unparsable body does not undo the acceptance.
		_ = json.Unmarshal(raw, &res)
		res.StatusCode = resp.StatusCode
		return res, nil
	}

	perr := &Error{StatusCode: resp.StatusCode, Message: summarize(raw, resp.Status)}
	switch {
	case resp.StatusCode == http.StatusRequestTimeout,
		resp.StatusCode == http.StatusTooEarly,
		resp.StatusCode == http.StatusTooManyRequests,
		resp.StatusCode >= 500:
		perr.Retryable = true
		perr.RetryAfter = parseRetryAfter(resp.Header.Get("Retry-After"), time.Now())
	}
	return Result{}, perr
}

// IsRetryable reports whether err is a provider error worth retrying.
func IsRetryable(err error) bool {
	var perr *Error
	return errors.As(err, &perr) && perr.Retryable
}

func summarize(body []byte, fallback string) string {
	s := strings.TrimSpace(string(body))
	if s == "" {
		return fallback
	}
	if len(s) > 200 {
		s = s[:200] + "..."
	}
	return s
}

// parseRetryAfter supports both forms of the header: delta-seconds and HTTP-date.
func parseRetryAfter(v string, now time.Time) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil && secs > 0 {
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil && t.After(now) {
		return t.Sub(now)
	}
	return 0
}
