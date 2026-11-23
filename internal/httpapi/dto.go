package httpapi

import (
	"encoding/base64"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/keremsirtikizil/insider-notification-system/internal/domain"
	"github.com/keremsirtikizil/insider-notification-system/internal/store"
)

type notificationRequest struct {
	Recipient      string            `json:"recipient"`
	Channel        string            `json:"channel"`
	Content        string            `json:"content,omitempty"`
	Priority       string            `json:"priority,omitempty"`
	TemplateID     *uuid.UUID        `json:"template_id,omitempty"`
	Variables      map[string]string `json:"variables,omitempty"`
	ScheduledAt    *time.Time        `json:"scheduled_at,omitempty"`
	IdempotencyKey string            `json:"idempotency_key,omitempty"`
}

type batchRequest struct {
	Notifications []notificationRequest `json:"notifications"`
}

type notificationResponse struct {
	ID                uuid.UUID  `json:"id"`
	BatchID           *uuid.UUID `json:"batch_id"`
	IdempotencyKey    *string    `json:"idempotency_key"`
	CorrelationID     string     `json:"correlation_id"`
	Channel           string     `json:"channel"`
	Recipient         string     `json:"recipient"`
	Content           string     `json:"content"`
	Priority          string     `json:"priority"`
	Status            string     `json:"status"`
	Attempts          int        `json:"attempts"`
	TemplateID        *uuid.UUID `json:"template_id"`
	ScheduledAt       *time.Time `json:"scheduled_at"`
	NextAttemptAt     *time.Time `json:"next_attempt_at"`
	ProviderMessageID *string    `json:"provider_message_id"`
	LastError         *string    `json:"last_error"`
	CreatedAt         time.Time  `json:"created_at"`
	UpdatedAt         time.Time  `json:"updated_at"`
	SentAt            *time.Time `json:"sent_at"`
	FailedAt          *time.Time `json:"failed_at"`
	CancelledAt       *time.Time `json:"cancelled_at"`
}

func toNotificationResponse(n *domain.Notification) notificationResponse {
	resp := notificationResponse{
		ID: n.ID, BatchID: n.BatchID, IdempotencyKey: n.IdempotencyKey, CorrelationID: n.CorrelationID,
		Channel: string(n.Channel), Recipient: n.Recipient, Content: n.Content,
		Priority: n.Priority.String(), Status: string(n.Status), Attempts: n.Attempts,
		TemplateID: n.TemplateID, ScheduledAt: n.ScheduledAt, ProviderMessageID: n.ProviderMessageID,
		LastError: n.LastError, CreatedAt: n.CreatedAt, UpdatedAt: n.UpdatedAt,
		SentAt: n.SentAt, FailedAt: n.FailedAt, CancelledAt: n.CancelledAt,
	}
	if n.Status.Cancellable() {
		next := n.NextAttemptAt
		resp.NextAttemptAt = &next
	}
	return resp
}

func toNotificationResponses(ns []*domain.Notification) []notificationResponse {
	out := make([]notificationResponse, len(ns))
	for i, n := range ns {
		out[i] = toNotificationResponse(n)
	}
	return out
}

type batchItemResponse struct {
	Index          int        `json:"index"`
	ID             uuid.UUID  `json:"id"`
	Status         string     `json:"status"`
	IdempotencyKey *string    `json:"idempotency_key"`
	BatchID        *uuid.UUID `json:"batch_id"`
	Duplicate      bool       `json:"duplicate"`
}

type batchCreateResponse struct {
	BatchID       uuid.UUID           `json:"batch_id"`
	Accepted      int                 `json:"accepted"`
	Duplicates    int                 `json:"duplicates"`
	Notifications []batchItemResponse `json:"notifications"`
}

type batchResponse struct {
	ID           uuid.UUID      `json:"id"`
	Total        int            `json:"total"`
	StatusCounts map[string]int `json:"status_counts"`
	Completed    bool           `json:"completed"`
	CreatedAt    time.Time      `json:"created_at"`
}

func toBatchResponse(b *domain.Batch) batchResponse {
	counts := make(map[string]int, len(domain.Statuses))
	completed := true
	for _, st := range domain.Statuses {
		counts[string(st)] = b.StatusCounts[st]
		if b.StatusCounts[st] > 0 && (st.Cancellable() || st == domain.StatusProcessing) {
			completed = false
		}
	}
	return batchResponse{ID: b.ID, Total: b.Total, StatusCounts: counts, Completed: completed, CreatedAt: b.CreatedAt}
}

type listResponse struct {
	Data       []notificationResponse `json:"data"`
	Pagination pagination             `json:"pagination"`
}

type pagination struct {
	Limit      int     `json:"limit"`
	NextCursor *string `json:"next_cursor"`
}

type attemptResponse struct {
	Attempt    int       `json:"attempt"`
	Outcome    string    `json:"outcome"`
	StatusCode *int      `json:"status_code"`
	Error      *string   `json:"error"`
	DurationMS int64     `json:"duration_ms"`
	CreatedAt  time.Time `json:"created_at"`
}

type templateRequest struct {
	Name    string `json:"name"`
	Channel string `json:"channel"`
	Content string `json:"content"`
}

type templateResponse struct {
	ID        uuid.UUID `json:"id"`
	Name      string    `json:"name"`
	Channel   string    `json:"channel"`
	Content   string    `json:"content"`
	Variables []string  `json:"variables"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func toTemplateResponse(t *domain.Template) templateResponse {
	return templateResponse{ID: t.ID, Name: t.Name, Channel: string(t.Channel), Content: t.Content,
		Variables: t.Variables, CreatedAt: t.CreatedAt, UpdatedAt: t.UpdatedAt}
}

// encodeCursor makes an opaque pagination token from the last row of a page.
// Postgres timestamps have microsecond precision, so UnixMicro is lossless.
func encodeCursor(c *store.Cursor) *string {
	if c == nil {
		return nil
	}
	s := base64.RawURLEncoding.EncodeToString(
		[]byte(strconv.FormatInt(c.CreatedAt.UnixMicro(), 10) + "_" + c.ID.String()))
	return &s
}

func decodeCursor(s string) (*store.Cursor, error) {
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return nil, errors.New("malformed cursor")
	}
	ts, id, ok := strings.Cut(string(raw), "_")
	if !ok {
		return nil, errors.New("malformed cursor")
	}
	micros, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return nil, errors.New("malformed cursor")
	}
	uid, err := uuid.Parse(id)
	if err != nil {
		return nil, errors.New("malformed cursor")
	}
	return &store.Cursor{CreatedAt: time.UnixMicro(micros).UTC(), ID: uid}, nil
}
