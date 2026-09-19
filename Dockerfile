# Multi-stage build. Runtime stage is `scratch`: exactly one static binary
# plus the CA bundle used to verify HTTPS relay URLs. No shell — the Docker
# HEALTHCHECK works because `http-relay-gateway healthcheck` is a binary
# subcommand of the entrypoint itself.
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY . .
ARG VERSION=0.1.0-dev
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -buildvcs=false \
    -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/http-relay-gateway ./cmd/http-relay-gateway

FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/http-relay-gateway /app/http-relay-gateway
# scratch has no WORKDIR; pin the database path to the image layout so a
# bare `docker run` works (compose sets DATA_FILE explicitly anyway).
ENV DATA_FILE=/app/data/gateway.db
USER 65532:65532
EXPOSE 20130 20131
ENTRYPOINT ["/app/http-relay-gateway"]
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 CMD ["/app/http-relay-gateway", "healthcheck"]
