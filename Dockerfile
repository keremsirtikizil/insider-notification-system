# syntax=docker/dockerfile:1

# ---- build: static binaries -------------------------------------------------
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/notifier ./cmd/notifier && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/mockprovider ./cmd/mockprovider

# ---- test: full suite incl. integration tests (used by `make test`) ---------
FROM golang:1.26 AS test
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
CMD ["sh", "-c", "go test -race -count=1 -coverpkg=./internal/...,./migrations/... -coverprofile=/tmp/coverage.out ./... && go tool cover -func=/tmp/coverage.out | tail -1"]

# ---- runtime: distroless, non-root -------------------------------------------
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/notifier /out/mockprovider /usr/local/bin/
USER nonroot:nonroot
EXPOSE 8080 9091 8090
ENTRYPOINT ["/usr/local/bin/notifier"]
CMD ["api"]
