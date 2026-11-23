SHELL := /bin/bash
TEST_ENV := TEST_DATABASE_URL=postgres://notifier:notifier@localhost:55432/notifier?sslmode=disable \
            TEST_REDIS_URL=redis://localhost:56379/0

.PHONY: help up down logs test test-unit test-local lint fmt build smoke

help: ## Show this help
	@grep -E '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-12s %s\n", $$1, $$2}'

up: ## Build and start the whole stack (API, worker, Postgres, Redis, mock provider, Jaeger, Prometheus)
	docker compose up --build -d --wait
	@echo "API docs: http://localhost:8080/docs   Jaeger: http://localhost:16686   Prometheus: http://localhost:9090"

down: ## Stop the stack and delete its data
	docker compose down -v

logs: ## Follow API and worker logs
	docker compose logs -f api worker

test: ## Run every test (unit + PostgreSQL/Redis integration + e2e) in Docker; no local Go needed
	@docker compose -f docker-compose.test.yml run --rm --build tests; status=$$?; \
	  docker compose -f docker-compose.test.yml down -v >/dev/null 2>&1; exit $$status

test-unit: ## Run unit tests only (integration tests skip without TEST_* env vars)
	go test -race -count=1 ./...

test-local: ## Run every test with local Go against throwaway Postgres/Redis containers
	docker compose -f docker-compose.test.yml up -d --wait postgres redis
	@$(TEST_ENV) go test -race -count=1 -coverpkg=./internal/...,./migrations/... -coverprofile=coverage.out ./...; status=$$?; \
	  docker compose -f docker-compose.test.yml down -v >/dev/null 2>&1; \
	  go tool cover -func=coverage.out | tail -1; exit $$status

lint: ## Run golangci-lint
	golangci-lint run ./...

fmt: ## Format code
	golangci-lint fmt ./...

build: ## Build binaries into ./bin
	CGO_ENABLED=0 go build -o bin/notifier ./cmd/notifier
	CGO_ENABLED=0 go build -o bin/mockprovider ./cmd/mockprovider

smoke: ## Send a few notifications to a running stack and wait for delivery
	./scripts/smoke.sh
