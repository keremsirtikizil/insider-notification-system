package httpapi

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"

	"github.com/keremsirtikizil/insider-notification-system/internal/domain"
	"github.com/keremsirtikizil/insider-notification-system/internal/events"
	"github.com/keremsirtikizil/insider-notification-system/internal/observability"
	"github.com/keremsirtikizil/insider-notification-system/internal/store"
)

// templateCache resolves templates once per request (a batch of 1000 items
// usually references one or two templates).
type templateCache struct {
	store Store
	byID  map[uuid.UUID]*domain.Template
}

func (c *templateCache) get(ctx context.Context, id uuid.UUID) (*domain.Template, error) {
	if t, ok := c.byID[id]; ok {
		return t, nil
	}
	t, err := c.store.GetTemplate(ctx, id)
	if err != nil {
		return nil, err
	}
	c.byID[id] = t
	return t, nil
}

// buildNotification validates one request item and turns it into a domain
// notification. Field problems are added to ve; the returned error is only for
// infrastructure failures.
func (s *Server) buildNotification(ctx context.Context, req notificationRequest, prefix string,
	templates *templateCache, ve *domain.ValidationError) (*domain.Notification, error) {
	n := &domain.Notification{
		ID:            uuid.New(),
		Recipient:     strings.TrimSpace(req.Recipient),
		Content:       req.Content,
		Status:        domain.StatusPending,
		CorrelationID: observability.CorrelationID(ctx),
		TraceParent:   observability.InjectTraceParent(ctx),
	}

	channel, err := domain.ParseChannel(req.Channel)
	if err != nil {
		ve.Add(prefix+"channel", "%s", err.Error())
	}
	n.Channel = channel

	priority, err := domain.ParsePriority(req.Priority)
	if err != nil {
		ve.Add(prefix+"priority", "%s", err.Error())
	}
	n.Priority = priority

	if req.TemplateID != nil {
		switch t, err := templates.get(ctx, *req.TemplateID); {
		case errors.Is(err, store.ErrNotFound):
			ve.Add(prefix+"template_id", "template not found")
		case err != nil:
			return nil, err
		case req.Content != "":
			ve.Add(prefix+"content", "must be omitted when template_id is set")
		case channel != "" && t.Channel != channel:
			ve.Add(prefix+"template_id", "template is for channel %s, not %s", t.Channel, channel)
		default:
			rendered, err := domain.RenderTemplate(t.Content, req.Variables)
			if err != nil {
				ve.Add(prefix+"variables", "%s", err.Error())
				break
			}
			n.Content, n.TemplateID = rendered, &t.ID
		}
	} else if len(req.Variables) > 0 {
		ve.Add(prefix+"variables", "only allowed together with template_id")
	}

	if channel != "" {
		if err := domain.ValidateRecipient(channel, n.Recipient); err != nil {
			ve.Add(prefix+"recipient", "%s", err.Error())
		}
		if req.TemplateID == nil || n.TemplateID != nil {
			if err := domain.ValidateContent(channel, n.Content); err != nil {
				ve.Add(prefix+"content", "%s", err.Error())
			}
		}
	}

	if req.ScheduledAt != nil {
		now := s.opts.Now()
		switch at := req.ScheduledAt.UTC(); {
		case !at.After(now):
			ve.Add(prefix+"scheduled_at", "must be in the future")
		case at.After(now.Add(s.opts.MaxScheduleAhead)):
			ve.Add(prefix+"scheduled_at", "must be at most %s ahead", s.opts.MaxScheduleAhead)
		default:
			n.ScheduledAt, n.Status = &at, domain.StatusScheduled
		}
	}

	if key := req.IdempotencyKey; key != "" {
		if len(key) > domain.MaxIdempotencyKeyLength || strings.IndexFunc(key, unicode.IsControl) >= 0 {
			ve.Add(prefix+"idempotency_key", "must be at most %d printable characters", domain.MaxIdempotencyKeyLength)
		}
		n.IdempotencyKey = &key
	}
	return n, nil
}

// POST /v1/notifications
func (s *Server) createNotification(w http.ResponseWriter, r *http.Request) {
	var req notificationRequest
	if !decodeJSON(w, r, &req, maxSingleBody) {
		return
	}
	if req.IdempotencyKey == "" {
		req.IdempotencyKey = r.Header.Get("Idempotency-Key")
	}
	ve := &domain.ValidationError{}
	n, err := s.buildNotification(r.Context(), req, "", &templateCache{store: s.opts.Store, byID: map[uuid.UUID]*domain.Template{}}, ve)
	if err != nil {
		s.writeStoreError(w, r, err)
		return
	}
	if ve.Err() != nil {
		writeValidation(w, r, ve)
		return
	}
	results, err := s.opts.Store.CreateNotifications(r.Context(), nil, []*domain.Notification{n})
	if err != nil {
		s.writeStoreError(w, r, err)
		return
	}
	res := results[0]
	if !res.Created {
		w.Header().Set("Idempotent-Replayed", "true")
		writeJSON(w, http.StatusOK, toNotificationResponse(res.Notification))
		return
	}
	s.accepted(r.Context(), res.Notification)
	w.Header().Set("Location", "/v1/notifications/"+res.Notification.ID.String())
	writeJSON(w, http.StatusAccepted, toNotificationResponse(res.Notification))
}

// POST /v1/notifications/batch
func (s *Server) createBatch(w http.ResponseWriter, r *http.Request) {
	var req batchRequest
	if !decodeJSON(w, r, &req, maxBatchBody) {
		return
	}
	ve := &domain.ValidationError{}
	switch count := len(req.Notifications); {
	case count == 0:
		ve.Add("notifications", "must contain at least one notification")
	case count > s.opts.MaxBatchSize:
		ve.Add("notifications", "must contain at most %d notifications, got %d", s.opts.MaxBatchSize, count)
	}
	if ve.Err() != nil {
		writeValidation(w, r, ve)
		return
	}

	batch := &domain.Batch{ID: uuid.New()}
	templates := &templateCache{store: s.opts.Store, byID: map[uuid.UUID]*domain.Template{}}
	seenKeys := map[string]int{}
	ns := make([]*domain.Notification, len(req.Notifications))
	for i, item := range req.Notifications {
		prefix := fmt.Sprintf("notifications[%d].", i)
		n, err := s.buildNotification(r.Context(), item, prefix, templates, ve)
		if err != nil {
			s.writeStoreError(w, r, err)
			return
		}
		if item.IdempotencyKey != "" {
			if first, dup := seenKeys[item.IdempotencyKey]; dup {
				ve.Add(prefix+"idempotency_key", "duplicates notifications[%d].idempotency_key", first)
			}
			seenKeys[item.IdempotencyKey] = i
		}
		n.BatchID = &batch.ID
		ns[i] = n
	}
	// All-or-nothing: one invalid item rejects the batch, so clients never have
	// to reconcile a partially accepted request.
	if ve.Err() != nil {
		writeValidation(w, r, ve)
		return
	}

	results, err := s.opts.Store.CreateNotifications(r.Context(), batch, ns)
	if err != nil {
		var idem *store.IdempotencyConflictError
		if errors.As(err, &idem) {
			writeError(w, r, http.StatusConflict, "idempotency_conflict", idem.Error(), []domain.FieldError{{
				Field: fmt.Sprintf("notifications[%d].idempotency_key", idem.Index), Message: idem.Error(),
			}})
			return
		}
		s.writeStoreError(w, r, err)
		return
	}
	resp := batchCreateResponse{BatchID: batch.ID, Notifications: make([]batchItemResponse, len(results))}
	var created []*domain.Notification
	for i, res := range results {
		n := res.Notification
		resp.Notifications[i] = batchItemResponse{Index: i, ID: n.ID, Status: string(n.Status),
			IdempotencyKey: n.IdempotencyKey, BatchID: n.BatchID, Duplicate: !res.Created}
		if res.Created {
			resp.Accepted++
			created = append(created, n)
		} else {
			resp.Duplicates++
		}
	}
	s.accepted(r.Context(), created...)
	w.Header().Set("Location", "/v1/batches/"+batch.ID.String())
	writeJSON(w, http.StatusAccepted, resp)
}

func (s *Server) accepted(ctx context.Context, ns ...*domain.Notification) {
	evs := make([]events.Event, 0, len(ns))
	for _, n := range ns {
		s.opts.Metrics.NotificationsCreated.WithLabelValues(string(n.Channel), n.Priority.String()).Inc()
		evs = append(evs, events.FromNotification(n))
	}
	s.opts.Publisher.Publish(ctx, evs...)
}

// GET /v1/notifications/{id}
func (s *Server) getNotification(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	n, err := s.opts.Store.GetNotification(r.Context(), id)
	if err != nil {
		s.writeStoreError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toNotificationResponse(n))
}

// GET /v1/notifications/{id}/attempts
func (s *Server) listAttempts(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	if _, err := s.opts.Store.GetNotification(r.Context(), id); err != nil {
		s.writeStoreError(w, r, err)
		return
	}
	attempts, err := s.opts.Store.ListAttempts(r.Context(), id)
	if err != nil {
		s.writeStoreError(w, r, err)
		return
	}
	out := make([]attemptResponse, len(attempts))
	for i, a := range attempts {
		out[i] = attemptResponse{Attempt: a.Attempt, Outcome: string(a.Outcome), StatusCode: a.StatusCode,
			Error: a.Error, DurationMS: a.Duration.Milliseconds(), CreatedAt: a.CreatedAt}
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": out})
}

// POST /v1/notifications/{id}/cancel
func (s *Server) cancelNotification(w http.ResponseWriter, r *http.Request) {
	s.transition(w, r, s.opts.Store.CancelNotification)
}

// POST /v1/notifications/{id}/retry
func (s *Server) retryNotification(w http.ResponseWriter, r *http.Request) {
	s.transition(w, r, s.opts.Store.RetryNotification)
}

func (s *Server) transition(w http.ResponseWriter, r *http.Request,
	apply func(context.Context, uuid.UUID) (*domain.Notification, error)) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	n, err := apply(r.Context(), id)
	if err != nil {
		s.writeStoreError(w, r, err)
		return
	}
	s.opts.Publisher.Publish(r.Context(), events.FromNotification(n))
	writeJSON(w, http.StatusOK, toNotificationResponse(n))
}

const (
	defaultPageSize = 50
	maxPageSize     = 200
)

// GET /v1/notifications
func (s *Server) listNotifications(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	ve := &domain.ValidationError{}
	f := store.ListFilter{Limit: defaultPageSize}

	if v := q.Get("status"); v != "" {
		if st, err := domain.ParseStatus(v); err != nil {
			ve.Add("status", "%s", err.Error())
		} else {
			f.Status = &st
		}
	}
	if v := q.Get("channel"); v != "" {
		if ch, err := domain.ParseChannel(v); err != nil {
			ve.Add("channel", "%s", err.Error())
		} else {
			f.Channel = &ch
		}
	}
	if v := q.Get("priority"); v != "" {
		if p, err := domain.ParsePriority(v); err != nil {
			ve.Add("priority", "%s", err.Error())
		} else {
			f.Priority = &p
		}
	}
	if v := q.Get("batch_id"); v != "" {
		if id, err := uuid.Parse(v); err != nil {
			ve.Add("batch_id", "must be a UUID")
		} else {
			f.BatchID = &id
		}
	}
	f.CreatedFrom = parseTimeParam(q.Get("created_from"), "created_from", ve)
	f.CreatedTo = parseTimeParam(q.Get("created_to"), "created_to", ve)
	if f.CreatedFrom != nil && f.CreatedTo != nil && !f.CreatedFrom.Before(*f.CreatedTo) {
		ve.Add("created_to", "must be after created_from")
	}
	if v := q.Get("limit"); v != "" {
		if l, err := strconv.Atoi(v); err != nil || l < 1 || l > maxPageSize {
			ve.Add("limit", "must be an integer between 1 and %d", maxPageSize)
		} else {
			f.Limit = l
		}
	}
	if v := q.Get("cursor"); v != "" {
		if c, err := decodeCursor(v); err != nil {
			ve.Add("cursor", "%s", err.Error())
		} else {
			f.After = c
		}
	}
	if ve.Err() != nil {
		writeError(w, r, http.StatusBadRequest, "invalid_query", "invalid query parameters", ve.Fields)
		return
	}

	items, next, err := s.opts.Store.ListNotifications(r.Context(), f)
	if err != nil {
		s.writeStoreError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, listResponse{
		Data:       toNotificationResponses(items),
		Pagination: pagination{Limit: f.Limit, NextCursor: encodeCursor(next)},
	})
}

func parseTimeParam(v, field string, ve *domain.ValidationError) *time.Time {
	if v == "" {
		return nil
	}
	t, err := time.Parse(time.RFC3339, v)
	if err != nil {
		ve.Add(field, "must be an RFC 3339 timestamp such as 2026-01-02T15:04:05Z")
		return nil
	}
	return &t
}
