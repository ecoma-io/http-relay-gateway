# Multi-stage build. Runtime stage is `scratch`: exactly one static binary
# plus the CA bundle used to verify HTTPS relay URLs. No shell — the Docker
# HEALTHCHECK works because `http-relay-gateway healthcheck` is a binary
# subcommand of the entrypoint itself.

# Stage 1: the management SPA. The pnpm version comes from the root
# packageManager field via corepack — no unpinned install in the chain.
FROM node:24-alpine AS web
WORKDIR /src
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
COPY web/package.json web/package.json
# --ignore-scripts skips the root prepare hook (lefthook; pointless in an
# image, and there is no .git to install hooks into). esbuild needs no
# install script: its platform binary ships as a package.
RUN corepack enable && pnpm install --frozen-lockfile --ignore-scripts
COPY web/ web/
RUN pnpm --filter web build

# Stage 2: the Go binary with the real bundle embedded (never the committed
# stub — that placeholder only exists so host-side `go build` needs no node).
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY . .
COPY --from=web /src/web/dist /src/internal/web/dist
ARG VERSION=0.1.0-dev
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -buildvcs=false \
    -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/http-relay-gateway ./cmd/http-relay-gateway
# The runtime image must carry /app/data owned by the runtime uid: when a
# NAMED volume is first mounted, Docker copies the image directory's content
# AND ownership into the volume — but only if the directory exists in the
# image. Without it the volume is created root-owned and the nonroot process
# cannot create its database (SQLITE_CANTOPEN, crash loop at boot).
RUN install -d -o 65532 -g 65532 /out/data

FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/http-relay-gateway /app/http-relay-gateway
COPY --from=build --chown=65532:65532 /out/data /app/data
# scratch has no WORKDIR; pin the database path to the image layout so a
# bare `docker run` works (compose sets DATA_FILE explicitly anyway).
ENV DATA_FILE=/app/data/gateway.db
USER 65532:65532
EXPOSE 20130 20131
ENTRYPOINT ["/app/http-relay-gateway"]
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 CMD ["/app/http-relay-gateway", "healthcheck"]
