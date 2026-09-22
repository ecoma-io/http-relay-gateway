# Multi-stage build. Runtime stage is `scratch`: exactly one static binary
# plus the CA bundle used to verify HTTPS relay and platform-API URLs. No
# shell — the Docker HEALTHCHECK works because `http-relay-gateway
# healthcheck` is a binary subcommand of the entrypoint itself.
FROM golang:1.26-alpine AS build
WORKDIR /src
# Dependencies resolve before source lands: only go.mod / go.sum bust this
# layer, so a source edit re-downloads nothing — that layer cache is what
# survives on an ephemeral CI builder. The cache mounts below are
# builder-local state and never leave it: `cache-to` exports layer metadata,
# not mount contents, and CI runners start a fresh builder every job, so the
# mounts pay off only where the builder persists (local compose builds, a
# long-lived daemon) — there an unchanged go.mod/go.sum plus warm mounts
# rebuilds a source edit in seconds instead of recompiling every package.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
ARG VERSION=0.1.0-dev
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -buildvcs=false \
    -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/http-relay-gateway ./cmd/http-relay-gateway

FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/http-relay-gateway /app/http-relay-gateway
# scratch has no WORKDIR; pin the desired-state file path to the image layout
# so a bare `docker run` works (compose sets CONFIG_FILE explicitly anyway).
# The file is bind-mounted in compose; the gateway needs a writable directory
# only if the operator wants it to start empty and pick the file up later.
ENV CONFIG_FILE=/app/config.yaml
USER 65532:65532
EXPOSE 20130
ENTRYPOINT ["/app/http-relay-gateway"]
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 CMD ["/app/http-relay-gateway", "healthcheck"]
