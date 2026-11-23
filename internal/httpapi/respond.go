package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/google/uuid"

	"github.com/keremsirtikizil/insider-notification-system/internal/domain"
	"github.com/keremsirtikizil/insider-notification-system/internal/observability"
	"github.com/keremsirtikizil/insider-notification-system/internal/store"
)

const (
	maxSingleBody = 1 << 20  // 1 MiB
	maxBatchBody  = 16 << 20 // 16 MiB
)

type errorBody struct {
	Error         errorDetail `json:"error"`
	CorrelationID string      `json:"correlation_id,omitempty"`
}

type errorDetail struct {
	Code    string              `json:"code"`
	Message string              `json:"message"`
	Details []domain.FieldError `json:"details,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, r *http.Request, status int, code, message string, details []domain.FieldError) {
	writeJSON(w, status, errorBody{
		Error:         errorDetail{Code: code, Message: message, Details: details},
		CorrelationID: observability.CorrelationID(r.Context()),
	})
}

func writeValidation(w http.ResponseWriter, r *http.Request, ve *domain.ValidationError) {
	writeError(w, r, http.StatusUnprocessableEntity, "validation_failed",
		"the request contains invalid fields", ve.Fields)
}

// decodeJSON strictly decodes a JSON body: unknown fields and trailing data are
// rejected so client typos surface immediately instead of being ignored.
func decodeJSON(w http.ResponseWriter, r *http.Request, dst any, maxBytes int64) bool {
	if ct := r.Header.Get("Content-Type"); ct != "" && !strings.HasPrefix(ct, "application/json") {
		writeError(w, r, http.StatusUnsupportedMediaType, "unsupported_media_type",
			"Content-Type must be application/json", nil)
		return false
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBytes))
	dec.DisallowUnknownFields()
	err := dec.Decode(dst)
	if err == nil && dec.More() {
		err = errors.New("body must contain a single JSON object")
	}
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeError(w, r, http.StatusRequestEntityTooLarge, "body_too_large",
				fmt.Sprintf("request body must not exceed %d bytes", maxBytes), nil)
			return false
		}
		if errors.Is(err, io.EOF) {
			err = errors.New("request body is empty")
		}
		writeError(w, r, http.StatusBadRequest, "invalid_json", err.Error(), nil)
		return false
	}
	return true
}

// writeStoreError maps persistence errors to HTTP responses.
func (s *Server) writeStoreError(w http.ResponseWriter, r *http.Request, err error) {
	var idem *store.IdempotencyConflictError
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeError(w, r, http.StatusNotFound, "not_found", "resource not found", nil)
	case errors.Is(err, store.ErrInvalidState):
		writeError(w, r, http.StatusConflict, "invalid_state", err.Error(), nil)
	case errors.Is(err, store.ErrDuplicate):
		writeError(w, r, http.StatusConflict, "duplicate", err.Error(), nil)
	case errors.As(err, &idem):
		writeError(w, r, http.StatusConflict, "idempotency_conflict", idem.Error(), nil)
	default:
		s.opts.Logger.ErrorContext(r.Context(), "request failed", "error", err)
		writeError(w, r, http.StatusInternalServerError, "internal_error", "internal server error", nil)
	}
}

func pathID(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "invalid_id", "id must be a UUID", nil)
		return uuid.Nil, false
	}
	return id, true
}
