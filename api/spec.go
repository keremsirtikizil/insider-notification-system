// Package api embeds the OpenAPI specification so the service can serve it
// (and Swagger UI) without files on disk.
package api

import _ "embed"

// OpenAPI is the OpenAPI 3 specification of the HTTP API.
//
//go:embed openapi.yaml
var OpenAPI []byte
