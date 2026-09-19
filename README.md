# http-relay-gateway

A general-purpose Golang **HTTP relay manager**: one internal-network
endpoint that load-balances, health-checks, and fails over across a pool of
HTTP relays — so any application that wants to hide its egress IP can point
at the gateway instead of managing a relay list itself.

```
client ──(two headers)──▶ relay-gateway ──(verbatim forward)──▶ edge relay ──▶ target API
                          round-robin + failover
                          per-provider body limits
                          passive health + database-backed pool
```

## Why

Anything a server calls over HTTP carries its origin IP: third-party APIs
see it, rate-limit it, and block it. Edge platforms (Vercel, Cloudflare
Workers, Deno Deploy) give you disposable deployments that forward requests
on the caller's behalf — cheap relays that stand between your server and
the target. But a bare list of relay URLs puts rotation, failover, health
tracking, and per-platform body limits on every consumer.

This gateway centralizes all of it: consumers point at **one** internal
address, and the gateway owns the relay list, the rotation, the failover,
and the body limits. Everything is internal-network only — **there is no
authentication**; the process is a sidecar and must never be exposed beyond
its compose network.

Any HTTP client works: a curl one-liner, an app-side fetch wrapper, an AI
gateway, a scraper.

## Wire contract

Two headers carry the routing intent; everything else is pass-through. The
caller sends the request to the gateway root (optionally `/{provider}`)
with the real origin in `X-Relay-Target` and the real path+query in
`X-Relay-Path`:

```http
POST /vercel HTTP/1.1
X-Relay-Target: https://api.example.com
X-Relay-Path: /v1/chat?beta=true
X-Relay-Provider: vercel
Authorization: Bearer $TARGET_TOKEN

{...}
```

- The gateway picks the next healthy relay and forwards method, body, and
  all end-to-end headers **verbatim** — the target's auth flows through
  untouched.
- `X-Relay-Provider` and hop-by-hop headers are stripped; `X-Forwarded-For`
  is never added (hiding the client IP is the whole point).
- `X-Relay-Provider` pins a provider (`vercel`, `cloudflare`, `deno`, …);
  empty, `none`, `auto`, or `all` round-robins across **every** provider.
  The header beats the `/{provider}` path prefix. Unknown pins get `404`.
- Responses stream straight through with a flush per write, so SSE chunks
  reach the client immediately.
- Failover happens only while the failure is still a transport error (before
  any response byte). Once the relay answered, the response is never retried.
- `GET /healthz` → `ok`; `GET /stats` → JSON pool snapshot (version, per-relay
  health, requests, failures, max-body).

### Body limits

Edge platforms cap request bodies themselves (Vercel ~4.5 MB, Cloudflare
~100 MB). The gateway buffers a body up to the largest configured provider
limit, then enforces the picked relay's per-provider limit by _skipping_ to
a provider that accepts the body. `413` is returned only when the pin leaves
no accepting provider or the body exceeds every configured limit.

## Configure

Everything dynamic — relays, providers, runtime settings — lives in the
SQLite database and is managed through the **admin plane** (`ADMIN_ADDR`,
REST at `/api/v1` plus a built-in UI). The data plane itself stays
unauthenticated; the admin plane is not.

- **First run**: an empty database puts the admin API in setup mode —
  `POST /api/v1/setup` with a password (min 8 chars, sent twice) creates the
  one admin account and issues a session cookie. Setup is once-ever; every
  later access logs in first (`POST /api/v1/login`). Passwords are stored as
  bcrypt hashes; sessions are signed HS256 JWTs in an `HttpOnly` cookie
  valid 12h.
- **Logins are rate-limited**: 5 failed attempts per 15 minutes per source
  address, plus a global cap, then `429` + `Retry-After` — including for the
  correct password, by design.
- **Relays**: `POST /api/v1/relays` with `name`, `provider`, `url`. A relay
  serves from its URL immediately; platform deployments attach later and
  then supply the relay's authentication token. Provider labels are free-form
  (`vercel`, `cloudflare`, `deno`, or your own); body limits attach to the
  label via `PUT /api/v1/providers`.
- **Settings**: `GET`/`PATCH /api/v1/settings` — log level, retry/cooldown
  knobs, streaming threshold, transport timeouts. Unknown keys are rejected.

Bind `ADMIN_ADDR` to loopback (the default `127.0.0.1:20131`) and front it
with an authenticating proxy if it must be reachable remotely. Never expose
the admin plane bare.

### Live reconfiguration

Every accepted admin mutation writes to the database, which signals a
coalesced change; the process rebuilds one immutable pool generation from
the database and swaps it atomically — in-flight requests finish on their
original generation, and pool health counters restart with the new pool
(`configuration reloaded` in the log). A rejected mutation (validation,
conflicts) changes nothing: the last-known-good pool keeps serving. No
config file, no reload signal, no restart.

### Environment (bootstrap-only, restart to change)

| Env                   |           Default | Meaning                                           |
| --------------------- | ----------------: | ------------------------------------------------- |
| `LISTEN_ADDR`         |           `:8080` | Relay endpoint (also serves `/healthz`, `/stats`) |
| `ADMIN_ADDR`          | `127.0.0.1:20131` | Admin plane listener (REST API + UI)              |
| `DATA_FILE`           | `data/gateway.db` | SQLite database — the source of truth             |
| `SHUTDOWN_GRACE`      |             `20s` | Whole-process drain budget for graceful shutdown  |
| `ADMIN_COOKIE_SECURE` |           `false` | Set `true` when the admin plane terminates HTTPS  |

### State

Relays, providers, settings, platform accounts and deployments all live in
the SQLite database at `DATA_FILE` — there is no other state. Delete the
file and the gateway boots empty (setup mode, data plane returns `503`
until a relay exists). Admin credentials are database rows too: the bcrypt
password hash and the JWT signing secret survive restarts, so sessions stay
valid across one. The database enables WAL journaling: put `DATA_FILE` on a
filesystem that supports it, and back the file up — it is the whole system.

## Run

```bash
gofmt -w . && go vet ./... && go test -race ./...
go build -ldflags "-X main.version=0.1.0-dev" -o bin/http-relay-gateway ./cmd/http-relay-gateway

LISTEN_ADDR=0.0.0.0:20130 ADMIN_ADDR=127.0.0.1:20131 ./bin/http-relay-gateway
# then: open http://127.0.0.1:20131/ — first visit offers the setup form
```

Subcommands: `http-relay-gateway version` prints the build version;
`http-relay-gateway healthcheck` probes `LISTEN_ADDR` and asserts the
`/healthz` body — this is what the Docker HEALTHCHECK runs, since the
scratch image has no shell.

## Pointing a client at it

Any HTTP client speaks the contract with two headers:

```bash
curl -x '' http://relay-gateway:20130/vercel \
  -H 'X-Relay-Target: https://api.example.com' \
  -H 'X-Relay-Path: /v1/chat' \
  -H "Authorization: Bearer $TARGET_TOKEN" \
  -d '{...}'
```

The real path travels in `X-Relay-Path`, so the `/{provider}` prefix costs
nothing. Anything with a configurable relay/base URL points at
`http://relay-gateway:20130` for round-robin across every provider, or
`http://relay-gateway:20130/vercel` pinned to one. Both sides should join
the same compose network.

## Docker

```bash
docker build -t relay-gateway:dev --build-arg VERSION=0.1.0-dev .
docker run --rm relay-gateway:dev version
docker compose up -d --build
curl http://127.0.0.1:20130/stats
```

`compose.yaml` publishes the relay endpoint on host **20130** and the admin
plane on host loopback **127.0.0.1:20131**, keeps the database in a named
volume, defaults to bounded `json-file` logs, keeps `stop_grace_period`
(30s) above `SHUTDOWN_GRACE` (default 20s), and uses the binary
`healthcheck` subcommand (no shell in the scratch image). On the volume:
uid 65532 owns the database files — a bind mount needs a writable,
chowned directory, which is why the named volume is the default.

## Layout

- `cmd/http-relay-gateway` — lifecycle, signals, the apply loop that turns
  database changes into pool swaps, `version` / `healthcheck`
- `internal/config` — bootstrap environment only (`LISTEN_ADDR`,
  `ADMIN_ADDR`, `DATA_FILE`, `SHUTDOWN_GRACE`, `ADMIN_COOKIE_SECURE`)
- `internal/store` — SQLite persistence: embedded migrations, relays,
  providers, settings, admin credentials, coalesced change channel
- `internal/admin` — admin plane: cookie-session auth (bcrypt + JWT), login
  rate limiting, setup-once flow, the `/api/v1` REST surface
- `internal/gateway` — HTTP data plane: provider selection, bounded failover, streaming pass-through, `/healthz` + `/stats`
- `internal/pool` — per-selector round-robin cursors, passive health (threshold → cooldown → half-open), stats snapshots
- `internal/logging`, `internal/sanitize` — zerolog setup and redaction helpers shared by all log/error paths
- `e2e` — black-box tests driving the real binary as a subprocess with fake
  edge-relay servers; `go test ./e2e/` (skip with `-short`)
