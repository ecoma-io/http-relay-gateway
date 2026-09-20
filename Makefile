.PHONY: build test bench e2e

build:
	gofmt -w . && go vet ./... && go build -ldflags "-X main.version=0.1.0-dev" -o bin/http-relay-gateway ./cmd/http-relay-gateway

test:
	go test -race ./...

bench:
	go test -bench=. -run='^$$' ./internal/pool/ ./internal/readiness/ ./internal/gateway/ ./cmd/http-relay-gateway/

e2e:
	go test ./e2e/
