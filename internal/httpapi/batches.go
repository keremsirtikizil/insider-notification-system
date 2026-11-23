package httpapi

import (
	"net/http"

	"github.com/keremsirtikizil/insider-notification-system/internal/events"
)

// GET /v1/batches/{id}
func (s *Server) getBatch(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	b, err := s.opts.Store.GetBatch(r.Context(), id)
	if err != nil {
		s.writeStoreError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toBatchResponse(b))
}

// POST /v1/batches/{id}/cancel
func (s *Server) cancelBatch(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	cancelled, err := s.opts.Store.CancelBatch(r.Context(), id)
	if err != nil {
		s.writeStoreError(w, r, err)
		return
	}
	evs := make([]events.Event, len(cancelled))
	for i, n := range cancelled {
		evs[i] = events.FromNotification(n)
	}
	s.opts.Publisher.Publish(r.Context(), evs...)
	writeJSON(w, http.StatusOK, map[string]any{"batch_id": id, "cancelled": len(cancelled)})
}
