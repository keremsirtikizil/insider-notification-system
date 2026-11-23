package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	legacyrouter "github.com/getkin/kin-openapi/routers/legacy"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	apispec "github.com/keremsirtikizil/insider-notification-system/api"
	"github.com/keremsirtikizil/insider-notification-system/internal/domain"
)

// TestContractMatchesOpenAPISpec sends requests through the real handlers and
// validates both the request and the response against api/openapi.yaml, so the
// documentation cannot drift from the implementation.
func TestContractMatchesOpenAPISpec(t *testing.T) {
	ctx := context.Background()
	doc, err := openapi3.NewLoader().LoadFromData(apispec.OpenAPI)
	require.NoError(t, err)
	require.NoError(t, doc.Validate(ctx))
	router, err := legacyrouter.NewRouter(doc)
	require.NoError(t, err)
	api := newTestAPI(t)

	call := func(method, path string, body any, want int) []byte {
		t.Helper()
		var raw []byte
		if body != nil {
			raw, err = json.Marshal(body)
			require.NoError(t, err)
		}
		newReq := func() *http.Request {
			r := httptest.NewRequest(method, "http://localhost:8080"+path, bytes.NewReader(raw))
			if body != nil {
				r.Header.Set("Content-Type", "application/json")
			}
			return r
		}
		rec := httptest.NewRecorder()
		api.h.ServeHTTP(rec, newReq())
		require.Equal(t, want, rec.Code, "%s %s: %s", method, path, rec.Body.String())

		req := newReq()
		route, params, err := router.FindRoute(req)
		require.NoError(t, err, "%s %s is not in the spec", method, path)
		reqInput := &openapi3filter.RequestValidationInput{Request: req, PathParams: params, Route: route}
		if want < 400 {
			require.NoError(t, openapi3filter.ValidateRequest(ctx, reqInput), "request %s %s", method, path)
		}
		respInput := &openapi3filter.ResponseValidationInput{
			RequestValidationInput: reqInput, Status: rec.Code, Header: rec.Header(),
			Options: &openapi3filter.Options{IncludeResponseStatus: true},
		}
		respInput.SetBodyBytes(rec.Body.Bytes())
		require.NoError(t, openapi3filter.ValidateResponse(ctx, respInput), "response %s %s", method, path)
		return rec.Body.Bytes()
	}

	tpl := decodeBytes[templateResponse](t, call(http.MethodPost, "/v1/templates",
		map[string]any{"name": "promo", "channel": "sms", "content": "Hi {{name}}"}, http.StatusCreated))
	call(http.MethodGet, "/v1/templates", nil, http.StatusOK)
	call(http.MethodGet, "/v1/templates/"+tpl.ID.String(), nil, http.StatusOK)

	n := decodeBytes[notificationResponse](t, call(http.MethodPost, "/v1/notifications", map[string]any{
		"recipient": "+905551234567", "channel": "sms", "template_id": tpl.ID,
		"variables": map[string]string{"name": "Ali"}, "priority": "high", "idempotency_key": "k1",
	}, http.StatusAccepted))
	call(http.MethodPost, "/v1/notifications", map[string]any{
		"recipient": "+905551234567", "channel": "sms", "template_id": tpl.ID,
		"variables": map[string]string{"name": "Ali"}, "priority": "high", "idempotency_key": "k1",
	}, http.StatusOK)
	call(http.MethodPost, "/v1/notifications", map[string]any{
		"recipient": "jane@example.com", "channel": "email", "content": "hello",
		"scheduled_at": testNow.Add(time.Hour).Format(time.RFC3339),
	}, http.StatusAccepted)
	call(http.MethodPost, "/v1/notifications", map[string]any{"recipient": "bad", "channel": "sms", "content": "x"},
		http.StatusUnprocessableEntity)

	batch := decodeBytes[batchCreateResponse](t, call(http.MethodPost, "/v1/notifications/batch", map[string]any{
		"notifications": []map[string]any{
			{"recipient": "+905551234567", "channel": "sms", "content": "a"},
			{"recipient": "device-token-0123456789", "channel": "push", "content": "b", "priority": "low"},
		},
	}, http.StatusAccepted))

	call(http.MethodGet, "/v1/notifications/"+n.ID.String(), nil, http.StatusOK)
	call(http.MethodGet, "/v1/notifications/"+n.ID.String()+"/attempts", nil, http.StatusOK)
	call(http.MethodGet, "/v1/notifications/"+uuid.NewString(), nil, http.StatusNotFound)
	call(http.MethodGet, "/v1/notifications?channel=sms&limit=1", nil, http.StatusOK)
	call(http.MethodGet, "/v1/batches/"+batch.BatchID.String(), nil, http.StatusOK)
	call(http.MethodPost, "/v1/notifications/"+n.ID.String()+"/cancel", nil, http.StatusOK)
	call(http.MethodPost, "/v1/notifications/"+n.ID.String()+"/cancel", nil, http.StatusConflict)

	api.store.rows[n.ID].Status = domain.StatusFailed
	call(http.MethodPost, "/v1/notifications/"+n.ID.String()+"/retry", nil, http.StatusOK)
	call(http.MethodPost, "/v1/batches/"+batch.BatchID.String()+"/cancel", nil, http.StatusOK)

	call(http.MethodGet, "/v1/metrics", nil, http.StatusOK)
	call(http.MethodGet, "/healthz", nil, http.StatusOK)
	call(http.MethodGet, "/readyz", nil, http.StatusOK)
}

func decodeBytes[T any](t *testing.T, b []byte) T {
	t.Helper()
	var v T
	require.NoError(t, json.Unmarshal(b, &v), string(b))
	return v
}
