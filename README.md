# http-relay-gateway

A general-purpose Golang **HTTP relay manager**: one internal-network
endpoint that load-balances, health-checks, and fails over across a pool of
HTTP relays — so any application that wants to hide its egress IP can point
at the gateway instead of managing a relay list itself.

```
client ──(two headers)──▶ relay-gateway ──(verbatim forward)──▶ edge relay ──▶ target API
                          round-robin + failover
                          per-provider body limits
                          readiness gate + passive health
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
the body limits, and admission. Everything is internal-network only —
**there is no authentication**; the process is a sidecar and must never be
exposed beyond its compose network.

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

- The gateway picks the next **ready** relay and forwards method, body, and
  all end-to-end headers **verbatim** — the target's auth flows through
  untouched.
- `X-Relay-Provider` and hop-by-hop headers are stripped; `X-Forwarded-For`
  is never added (hiding the client IP is the whole point).
- `X-Relay-Provider` pins a provider (`vercel`, `cloudflare`, `deno`, …);
  empty, `none`, `auto`, or `all` round-robins across **every** provider.
  The header beats the `/{provider}` path prefix. Unknown pins get `404` —
  except while no relay is ready at all: an empty ready set answers `503`
  for any pin, because configured-but-unverified and never-configured are
  indistinguishable there and `503` is the retryable answer.
- Responses stream straight through with a flush per write, so SSE chunks
  reach the client immediately.
- Failover happens only while the failure is still a transport error (before
  any response byte). Once the relay answered, the response is never retried.
- `GET /healthz` → `ok` (process liveness); `GET /readyz` → `200` once at
  least one relay has passed its readiness gate, `503` before that;
  `GET /stats` → JSON pool snapshot (version, per-relay health, requests,
  failures, max-body, readiness + lifecycle).

### Readiness gate

A relay serves traffic only after it _proves_ it can: its deployment answers
on its origin, carries the expected worker version, and completes a forwarded
request through its own relay URL end to end (relay key included). Every
relay's lifecycle is tracked in memory and surfaced on `/stats`:

| State        | Meaning                                                                 |
| ------------ | ----------------------------------------------------------------------- |
| `configured` | Row exists; first verification pending                                  |
| `discovered` | Legacy relay (no platform account); probing                             |
| `deploying`  | A deploy/redeploy is in flight (single-flight per relay)                |
| `verifying`  | Version + forward probe in progress                                     |
| `ready`      | Gate passed; admitted to the pool; round-robins traffic                 |
| `unready`    | Was serving, then failed verification `DemoteAfter` times consecutively |
| `failed`     | Never served; verification keeps failing (backoff-gated retries)        |
| `removing`   | Deleted; held 30s before the registry purges it                         |

Verification runs on its own cadence and after every reconcile pass.
Failures are exponential-backoff gated and never crash the process; while
_nothing_ is ready the retry delay is capped harder so a total outage heals
quickly, and a relay that recovers re-enters the pool on the next pass — no
restart, no manual intervention. Transient probe failures demote (`unready`)
and never trigger a redeploy; a version mismatch or a wrong relay key _does_
queue a redeploy, so a stale worker replaces itself with fresh configuration.
Legacy relays are readiness-probed only — never version-probed, never
redeployed.

Two layers of truth. The database `active` flag records deployment currency:
it stays `Active` through auth/probe failures so an operator can see what is
deployed and why it is not serving. The in-memory readiness gate decides
admission. A relay that fails its forward probe stays `Active` in the
database but is not `ready`, and appears in `/stats` lifecycle with its
failure reason instead of in the serving pool.

Zero-ready behavior: `/readyz` answers `503`, `/healthz` still answers `ok`
(the process is alive — no relay is), the data plane answers `503` for every
request including pinned ones, and `/stats` reports readiness `false` with an
empty relay list. Round-robin requests alternate only over the _ready_ set,
in configuration order.

### Body limits

Edge platforms cap request bodies themselves (Vercel ~4.5 MB, Cloudflare
~100 MB). The gateway buffers a body up to the largest configured provider
limit, then enforces the picked relay's per-provider limit by _skipping_ to
a provider that accepts the body. `413` is returned only when the pin leaves
no accepting provider or the body exceeds every configured limit.

### Proxy inbound

The same endpoint also speaks forward proxy: an HTTP client that routes
through the gateway (`--proxy`, `HTTP_PROXY`, any library's proxy setting)
hits the same pool. Routing intent is read from the absolute-form request
target itself — the gateway derives `X-Relay-Target` (scheme + host) and
`X-Relay-Path` (path + query) from the URL — so client-supplied values of
those two headers are **overwritten**, never trusted. The provider pin is
header-only in proxy mode (`X-Relay-Provider`); the `/{provider}` prefix is a
relay-spec feature and has no meaning here — the target's path belongs to the
target. `CONNECT` tunneling is not supported (edge relays carry HTTP, not raw
TCP) and is answered `501`; an absolute-form `https://` target works as an
ordinary request because the relay itself fetches the target:

```bash
curl --proxy http://relay-gateway:20130 http://api.example.com/v1/chat \
  -H 'Authorization: Bearer $TARGET_TOKEN'
```

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
  label via `PUT /api/v1/providers`. Creating a relay with `accountId` births
  it as **managed**: the gateway deploys its embedded worker to that platform
  account and serves the deployment URL with a deploy-time token. A relay
  created without one is **legacy** — it serves its own URL tokenless, and
  `POST /api/v1/relays/{id}/adopt` (with an `accountId`) migrates it to a
  managed deployment.
- **Platform accounts**: `POST /api/v1/accounts` stores a platform API token
  (verified against the platform before it is accepted, shown last-4 only
  ever after). `POST /api/v1/relays/{id}/redeploy` redeploys a managed
  relay's worker with a fresh token; `DELETE /api/v1/relays/{id}
?deleteRemote=true` also deletes the deployed worker from the platform.
  A failed redeploy never pulls a serving relay out of rotation — the old
  deployment keeps serving with the failure recorded on the row.
- **Fleet**: `GET /api/v1/fleet/version` reports the embedded worker version
  and per-platform deployment counts; `POST /api/v1/fleet/check` probes every
  deployment and records drift; `POST /api/v1/fleet/reconcile` also redeploys
  deployments whose reported version differs from the gateway's — how the
  fleet upgrades (or downgrades) with the gateway. Startup probes run
  automatically; an unreachable relay is never redeployed on a hunch.
- **Platform pauses**: when a platform answers a probe but the relay worker
  does not — the suspension page free tiers serve after quota exhaustion
  (Vercel: HTTP 402, `x-vercel-error: DEPLOYMENT_DISABLED`) — the deployment
  is marked `paused` and the relay leaves the pool: no deploy could lift a
  platform suspension, and no client is ever served the platform's page.
  This verdict is relay-side by construction: the probe only ever contacts
  the relay's own `/__relay/version` endpoint, so it can never mistake an
  upstream error for a pause. An always-on revival scan re-probes paused
  deployments roughly every 10 minutes, independent of the reconcile
  interval; when the worker answers again the relay rejoins automatically
  (or queues a catch-up redeploy if the fleet version moved on while it was
  dark). While paused, the relay's row shows the platform's marker.
- **Settings**: `GET`/`PATCH /api/v1/settings` — log level, retry/cooldown
  knobs, streaming threshold, transport timeouts, reconcile interval. Unknown
  keys are rejected.

Bind `ADMIN_ADDR` to loopback (the default `127.0.0.1:20131`) and front it
with an authenticating proxy if it must be reachable remotely. Never expose
the admin plane bare.

### Bringing an existing relay list

`POST /api/v1/relays/import` takes `{items: [{name, provider, url, active?}]}`
(500 per batch) and lands what it can: each row is validated and inserted on
its own, and the reply reports `{imported, rejected}` so a typo never fails a
whole migration. Imported relays serve their own URLs immediately and are
**unmanaged** — no worker, no token. To move one behind a deployed
worker, create a platform account and `POST /api/v1/relays/{id}/adopt` with
the `accountId`: the embedded worker deploys, the URL is verified, and only a
verified success flips the relay to managed. The UI carries the same flow —
_Import_ on the Relays page, _Adopt_ on any unmanaged row.

### Live reconfiguration

Every accepted admin mutation writes to the database, which signals a
coalesced change; the process rebuilds one immutable pool generation from
the database and swaps it atomically — in-flight requests finish on their
original generation, and pool health counters restart with the new pool
(`configuration reloaded` in the log). A rejected mutation (validation,
conflicts) changes nothing: the last-known-good pool keeps serving. No
config file, no reload signal, no restart.

### Environment (bootstrap-only, restart to change)

| Env                          |           Default | Meaning                                                      |
| ---------------------------- | ----------------: | ------------------------------------------------------------ |
| `LISTEN_ADDR`                |           `:8080` | Relay endpoint (also serves `/healthz`, `/readyz`, `/stats`) |
| `ADMIN_ADDR`                 | `127.0.0.1:20131` | Admin plane listener (REST API + UI)                         |
| `DATA_FILE`                  | `data/gateway.db` | SQLite database — the source of truth                        |
| `SHUTDOWN_GRACE`             |             `20s` | Whole-process drain budget for graceful shutdown             |
| `ADMIN_COOKIE_SECURE`        |           `false` | Set `true` when the admin plane terminates HTTPS             |
| `RELAY_VERIFY_INTERVAL`      |             `60s` | Readiness verification scan cadence                          |
| `RELAY_REVIVE_SCAN_INTERVAL` |             `10m` | Paused-deployment revival scan cadence                       |
| `RELAY_VERIFY_BACKOFF_BASE`  |              `5s` | First verification-failure retry delay                       |
| `RELAY_VERIFY_BACKOFF_MAX`   |              `5m` | Exponential retry ceiling                                    |
| `RELAY_VERIFY_RECOVER_MAX`   |             `15s` | Retry ceiling while no relay is ready                        |
| `RELAY_VERIFY_DEMOTE_AFTER`  |               `3` | Consecutive failures before a serving relay demotes          |

### State

Relays, providers, settings, platform accounts and deployments all live in
the SQLite database at `DATA_FILE` — the database is the only durable state.
Readiness is in-memory: the registry mirrors the database relay set on every
rebuild, re-announcing rows as configured-but-unverified, so after a restart
every relay re-proves itself before serving. That is the correct posture for
a sidecar whose pool must always be verified. Delete the file and the gateway
boots empty (setup mode, `/readyz` answers `503` until a relay verifies).
Admin credentials are database rows too: the bcrypt password hash and the JWT
signing secret survive restarts, so sessions stay valid across one. The
database enables WAL journaling: put `DATA_FILE` on a filesystem that supports
it, and back the file up — it is the whole system.

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
- `internal/readiness` — in-memory relay lifecycle registry: states,
  single-flight deploys, backoff-gated verification, pool admission
- `internal/reconcile` — fleet worker: verification passes, redeploys,
  adoptions, the paused-deployment revival scan
- `internal/gateway` — HTTP data plane: provider selection, bounded failover, streaming pass-through, `/healthz` + `/readyz` + `/stats`
- `internal/pool` — per-selector round-robin cursors, passive health (threshold → cooldown → half-open), stats snapshots
- `internal/logging`, `internal/sanitize` — zerolog setup and redaction helpers shared by all log/error paths
- `e2e` — black-box tests driving the real binary as a subprocess with fake
  edge-relay servers; `go test ./e2e/` (skip with `-short`)
