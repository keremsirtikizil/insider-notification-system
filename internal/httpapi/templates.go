package httpapi

import (
	"net/http"

	"github.com/google/uuid"

	"github.com/keremsirtikizil/insider-notification-system/internal/domain"
)

// POST /v1/templates
func (s *Server) createTemplate(w http.ResponseWriter, r *http.Request) {
	var req templateRequest
	if !decodeJSON(w, r, &req, maxSingleBody) {
		return
	}
	ve := &domain.ValidationError{}
	if err := domain.ValidateTemplateName(req.Name); err != nil {
		ve.Add("name", "%s", err.Error())
	}
	channel, err := domain.ParseChannel(req.Channel)
	if err != nil {
		ve.Add("channel", "%s", err.Error())
	} else if err := domain.ValidateContent(channel, req.Content); err != nil {
		// Rendered output is validated again per notification; this catches
		// templates that are too long before any variable is substituted.
		ve.Add("content", "%s", err.Error())
	}
	if ve.Err() != nil {
		writeValidation(w, r, ve)
		return
	}
	t, err := s.opts.Store.CreateTemplate(r.Context(), &domain.Template{
		ID: uuid.New(), Name: req.Name, Channel: channel, Content: req.Content,
		Variables: domain.TemplateVariables(req.Content),
	})
	if err != nil {
		s.writeStoreError(w, r, err)
		return
	}
	w.Header().Set("Location", "/v1/templates/"+t.ID.String())
	writeJSON(w, http.StatusCreated, toTemplateResponse(t))
}

// GET /v1/templates
func (s *Server) listTemplates(w http.ResponseWriter, r *http.Request) {
	ts, err := s.opts.Store.ListTemplates(r.Context())
	if err != nil {
		s.writeStoreError(w, r, err)
		return
	}
	out := make([]templateResponse, len(ts))
	for i, t := range ts {
		out[i] = toTemplateResponse(t)
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": out})
}

// GET /v1/templates/{id}
func (s *Server) getTemplate(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	t, err := s.opts.Store.GetTemplate(r.Context(), id)
	if err != nil {
		s.writeStoreError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toTemplateResponse(t))
}
