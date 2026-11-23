package api_test

import (
	"context"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keremsirtikizil/insider-notification-system/api"
)

func TestOpenAPISpecIsValid(t *testing.T) {
	doc, err := openapi3.NewLoader().LoadFromData(api.OpenAPI)
	require.NoError(t, err)
	require.NoError(t, doc.Validate(context.Background()))
}

// TestSpecDocumentsEveryRoute keeps the spec in sync with the router in
// internal/httpapi/server.go.
func TestSpecDocumentsEveryRoute(t *testing.T) {
	doc, err := openapi3.NewLoader().LoadFromData(api.OpenAPI)
	require.NoError(t, err)
	routes := map[string][]string{
		"/v1/notifications":               {"POST", "GET"},
		"/v1/notifications/batch":         {"POST"},
		"/v1/notifications/{id}":          {"GET"},
		"/v1/notifications/{id}/attempts": {"GET"},
		"/v1/notifications/{id}/cancel":   {"POST"},
		"/v1/notifications/{id}/retry":    {"POST"},
		"/v1/batches/{id}":                {"GET"},
		"/v1/batches/{id}/cancel":         {"POST"},
		"/v1/templates":                   {"POST", "GET"},
		"/v1/templates/{id}":              {"GET"},
		"/v1/metrics":                     {"GET"},
		"/v1/ws":                          {"GET"},
		"/metrics":                        {"GET"},
		"/healthz":                        {"GET"},
		"/readyz":                         {"GET"},
	}
	for path, methods := range routes {
		item := doc.Paths.Find(path)
		require.NotNil(t, item, path)
		for _, m := range methods {
			assert.NotNil(t, item.GetOperation(m), "%s %s", m, path)
		}
	}
	assert.Equal(t, len(routes), doc.Paths.Len(), "spec documents a route the router does not serve")
}
