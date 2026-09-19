# http-relay-gateway

General-purpose Go HTTP relay manager: one internal-network endpoint that
forwards relay-spec requests (`X-Relay-Target` / `X-Relay-Path`) through a
health-aware, round-robin pool of edge relays (Vercel / Cloudflare Workers /
Deno Deploy apps) — any application that wants to hide its egress IP points
at the gateway with two headers. **No authentication — the process is an
internal-network sidecar and must never be exposed beyond its compose
network.** The authoritative behavior contract is
[`README.md`](README.md).

## Build and test

```bash
gofmt -w . && go vet ./... && go test -race ./...
go build -ldflags "-X main.version=0.1.0-dev" -o bin/http-relay-gateway ./cmd/http-relay-gateway
```

Go ≥ 1.26 (`go.mod` owns the floor). SQLite is `modernc.org/sqlite` (pure
Go, no CGO — the image is `scratch`); the store runs a single connection,
so a query on the pool while a transaction is open deadlocks — transaction
work takes the `*sql.Tx`. Style rules: `math/rand/v2` (never `math/rand`),
`for range n` loops, and no weak `any` parsing — an API number must decode
as a whole int or be rejected.

## Configure and run

Everything dynamic lives in the SQLite database and is managed through the
admin plane (`internal/admin`, REST at `/api/v1` on `ADMIN_ADDR`); first
access performs a one-time setup (password + confirm), after which every
management call requires a logged-in session (bcrypt + HS256 cookie, login
rate-limited). The data plane stays unauthenticated. Relay URLs may carry
private hostnames; never log them or commit them.

```bash
LISTEN_ADDR=0.0.0.0:20130 \
ADMIN_ADDR=127.0.0.1:20131 \
./bin/http-relay-gateway
# first visit: http://127.0.0.1:20131/ — setup, then manage relays
```

Environment variables are bootstrap-only and require restart:

| Env                   |           Default | Meaning                                            |
| --------------------- | ----------------: | -------------------------------------------------- |
| `LISTEN_ADDR`         |           `:8080` | Relay endpoint; also serves `/healthz`, `/stats`   |
| `ADMIN_ADDR`          | `127.0.0.1:20131` | Admin plane listener (REST + UI); keep it loopback |
| `DATA_FILE`           | `data/gateway.db` | SQLite database — the source of truth              |
| `SHUTDOWN_GRACE`      |             `20s` | Whole-process drain budget for shutdown            |
| `ADMIN_COOKIE_SECURE` |           `false` | `Secure` cookie attribute, for HTTPS termination   |

Runtime state lives in the SQLite database (`internal/store`): relays,
settings, providers, platform accounts, deployments, admin credentials.
Every accepted admin mutation notifies a coalesced change channel; the
applier loop in `cmd/http-relay-gateway` rebuilds one immutable pool
generation from the database and swaps it atomically — a rejected mutation
leaves the last-known-good pool serving. Do not add a config-file path,
a reload signal, or any second source of truth: the database is the only
one.

## Behavior notes

Read [`README.md`](README.md) before changing the wire contract.

- `X-Relay-Provider` (or the `/{provider}` path prefix; header wins) pins a
  provider; empty / `none` / `auto` / `all` round-robins every provider.
  Round-robin cursors are per selector key; with all relays healthy the
  served sequence is exactly the config order — deterministic, so tests pin
  exact sequences. An unknown pin is a `404`.
- Failover replays the buffered body on the next relay only while the
  failure is a transport error (before any response byte). Once the relay
  answered, the response streams through untouched — never retried.
- Body limits are per provider (a `providers` row's `maxBody`, platform
  numbers: vercel ~4.5MB, cloudflare ~100MB). The gateway buffers up to the
  largest limit and _skips_ to a provider that accepts the body; `413` only
  when no accepting provider remains. Requests above the streaming
  threshold (`stream_threshold_bytes`) are relayed live instead — one
  attempt, no failover, no gateway-side `413`.
- Health is passive: `failure-threshold` consecutive failures → `cooldown`
  → half-open recovery. When every candidate is down, Pick still returns
  one (best effort beats a 503).
- End-to-end headers forward verbatim (provider `Authorization` included).
  Stripped: `X-Relay-Provider`, hop-by-hop headers, and any client-supplied
  `X-Relay-Token`. Per-relay header policies may strip or set further
  headers. **Never added: `X-Forwarded-For` or anything that identifies the
  caller** — that is the whole point (see SECURITY.md).
- `/stats` exposes relay names, providers, health, counters, max-body —
  never relay URLs or provider credentials. Error text goes through
  `internal/sanitize` (userinfo redaction, ANSI stripping, 512-byte bound).
- Shutdown drains in-flight requests (streaming included) against one
  whole-process `SHUTDOWN_GRACE` budget, then process exit closes whatever
  is left. Keep the surrounding orchestrator's kill timer above the budget
  (`stop_grace_period: 30s` in compose vs the 20s default).

## Admin

```bash
curl http://127.0.0.1:20130/healthz # body "ok\n"
curl http://127.0.0.1:20130/stats   # JSON pool snapshot
curl http://127.0.0.1:20131/api/v1/status # setupRequired + version
LISTEN_ADDR=127.0.0.1:20130 ./bin/http-relay-gateway healthcheck
./bin/http-relay-gateway version
```

The admin plane (`internal/admin`) is authenticated after a one-time setup:
`POST /api/v1/setup` (once, then `409`), `POST /api/v1/login` (bcrypt,
HS256 `HttpOnly` cookie, 12h), `POST /api/v1/logout`. Login failures are
rate-limited 5 per 15 minutes per source address plus a global cap
(`429` + `Retry-After`). Management resources: `relays`, `providers`,
`settings`. Relay tokens are write-only through the API — responses carry a
last-4 suffix at most, and neither tokens nor passwords ever reach logs.

## Docker

```bash
docker build -t relay-gateway:dev --build-arg VERSION=0.1.0-dev .
docker run --rm relay-gateway:dev version
docker compose up -d --build
curl http://127.0.0.1:20130/stats
```

The image is `scratch`: one static binary plus the CA bundle, running as
uid 65532 — the database lives in a named volume so the uid owns its files
without host chowning. `ENV DATA_FILE=/app/data/gateway.db` is set in-image
because scratch has no WORKDIR. Compose publishes host **20130** for the
relay endpoint and **127.0.0.1:20131** for the admin plane, keeps
`stop_grace_period` above `SHUTDOWN_GRACE`, and uses the binary
`healthcheck` subcommand (no shell in the image).

## Layout

- `cmd/http-relay-gateway` — lifecycle, signals, the apply loop that turns
  database changes into atomic pool swaps, `version` / `healthcheck`
- `internal/config` — bootstrap environment only (`LISTEN_ADDR`, `ADMIN_ADDR`, `DATA_FILE`, `SHUTDOWN_GRACE`, `ADMIN_COOKIE_SECURE`)
- `internal/store` — SQLite persistence: embedded migrations, relays/settings/providers/accounts/deployments rows, coalesced change channel, credential-isolating `Tokens` reads
- `internal/admin` — admin plane: setup-once + login auth (bcrypt, JWT cookie, limiter), the `/api/v1` REST surface, SPA hosting
- `internal/gateway` — HTTP data plane: pinning, bounded failover, streaming pass-through, `/healthz` + `/stats`
- `internal/pool` — per-selector round-robin cursors, passive health, header policies, stats snapshots
- `internal/logging`, `internal/sanitize` — zerolog setup and redaction helpers shared by all log/error paths
- `e2e` — black-box tests and sims driving the real binary as a subprocess
  with in-process fake edge relays; `go test ./e2e/` (skip with `-short`)
