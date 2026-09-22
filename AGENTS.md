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

Benchmarks cover the hot paths — `go test -bench=. -run=^$ ./internal/pool/
./internal/readiness/ ./internal/gateway/ ./cmd/http-relay-gateway/`: pool
admission counting, registry `IsReady` / `Snapshot`, the data-plane forward
path, and `buildState`.

Go ≥ 1.26 (`go.mod` owns the floor). The image is `scratch` — pure Go end
to end; the dependency floor is zerolog plus the YAML parser. Style rules:
`math/rand/v2` (never `math/rand`), `for range n` loops, and no weak `any`
parsing — an API number must decode as a whole int or be rejected.

## Configure and run

The desired-state YAML file is the only source of intent: relays and
runtime settings live there, the poller re-reads it every second, and every
accepted change reconciles the remote fleet to match. There is no
database, no admin plane, no UI, no reload signal — do not add a second
source of truth. Relay URLs may carry private hostnames; never log them or
commit them. Provider credentials and the relay key ride the environment
(`${VAR}` references in the file) or secret files, never the file in clear.

```bash
LISTEN_ADDR=0.0.0.0:20130 \
RELAY_AUTH_TOKEN_FILE=./relay.key \
./bin/http-relay-gateway
```

Environment variables are bootstrap-only and require restart:

| Env                     |       Default | Meaning                                                                                  |
| ----------------------- | ------------: | ---------------------------------------------------------------------------------------- |
| `LISTEN_ADDR`           |       `:8080` | Relay endpoint; also serves `/healthz`, `/readyz`, `/stats`                              |
| `CONFIG_FILE`           | `config.yaml` | Desired-state file the poller watches                                                    |
| `RELAY_AUTH_TOKEN`      |             — | The relay key; exactly one of token / token file is required — boot is fatal without one |
| `RELAY_AUTH_TOKEN_FILE` |             — | File whose trimmed content is the relay key (re-read per pass)                           |
| `SHUTDOWN_GRACE`        |         `20s` | Whole-process drain budget for shutdown                                                  |

Desired-state file: a `settings:` block (every key optional, malformed
values are load errors) and a `relays:` list of `{name, provider,
token|token_file, team? (vercel only), account? (cloudflare only)}`.
Identity is `(provider, name)`; the project slug and serving URL derive
from it — never configured. Unknown keys, duplicate names, slug collisions
on one provider, and unset `${VAR}` references all reject the load;
an invalid or missing file leaves the last-known-good fleet serving (an
empty fleet at boot, `/readyz` `503`, self-heals when the file appears).

| Key                       | Default | Meaning                                                          |
| ------------------------- | ------- | ---------------------------------------------------------------- |
| `log_level`               | `info`  | `debug` \| `info` \| `warn` \| `error`                           |
| `max_retries`             | `2`     | Failover attempts beyond the first (0–16)                        |
| `failure_threshold`       | `3`     | Consecutive passive failures before cooldown                     |
| `cooldown`                | `30s`   | Passive-health cooldown after the failure threshold              |
| `stream_threshold_bytes`  | `0`     | Bodies above this stream through; `0` = always buffer            |
| `dial_timeout`            | `5s`    | Outbound dial timeout (relay legs and probes)                    |
| `response_header_timeout` | `0`     | Response-header timeout; `0` = off                               |
| `verify_interval`         | `60s`   | Readiness re-verification cadence                                |
| `revive_scan_interval`    | `10m`   | Paused-relay revival probe cadence                               |
| `verify_backoff_base`     | `5s`    | First verification-failure retry delay                           |
| `verify_backoff_max`      | `5m`    | Exponential retry ceiling                                        |
| `verify_recover_max`      | `15s`   | Retry ceiling while no relay is ready                            |
| `verify_demote_after`     | `3`     | Consecutive verification failures before a serving relay demotes |

## Behavior notes

Read [`README.md`](README.md) before changing the wire contract.

- `X-Relay-Provider` (or the `/{provider}` path prefix; header wins) pins a
  provider; empty / `none` / `auto` / `all` round-robins every provider.
  Round-robin cursors are per selector key; with all relays healthy the
  served sequence is exactly pool order — sorted by `(provider, name)` —
  deterministic, so tests pin exact sequences. An unknown pin is a `404` —
  except while the ready set is empty (nothing verified yet), where any
  explicit pin is a retryable `503`.
- Only **ready** relays serve. Admission is the verified readiness gate
  (`internal/readiness` + `internal/reconcile`): deployment discovered from
  the provider + expected `deploy.RelayVersion` + relay key accepted + a
  forwarded probe through the relay's own URL. The registry is in-memory
  and re-verifies from scratch after a restart — nothing is persisted, by
  design. Probe failures demote and back off (never redeploy); version
  mismatch, a missing worker, or a wrong relay key queue a redeploy;
  a platform suspension page (Vercel `402` / `DEPLOYMENT_DISABLED`) pauses
  the relay on its own revival cadence — no deploy is ever fired at a
  suspension.
- Replacements run Strategy A, in this order: demote (`replacing`) → pool
  swap (the settle barrier) → drain in-flight requests (`AwaitIdle`,
  bounded) → deploy → verify. The order is forced by the platforms — a
  project has one production URL and deploy switches what answers on it.
  A replacement that fails verification stays demoted: a failing
  verification must not keep serving. A single-relay fleet has a brief
  zero-ready window during its own replacement; accepted by design.
- Removal: delete the entry from the file → admission revoked, generation
  (incarnation) bumped, `removing` → settle + drain → remote delete (a
  `404` is success) → purge. Stale completions (old generation) are
  discarded; a re-added identity cannot deploy until the stale delete has
  finished deleting the old project. After a restart there is never a
  pending delete — config vanished mid-run leaves the remote behind with a
  warning, deliberately.
- Forward-proxy inbound: an absolute-form request target (or `CONNECT`)
  routes through the proxy branch, which derives `X-Relay-Target` /
  `X-Relay-Path` from the URL and overwrites client-supplied values
  (no smuggling); the pin is header-only; `CONNECT` is a documented `501`
  (edge relays carry no raw TCP tunnels). Control endpoints answer
  origin-form only — a proxy-form `/healthz` relays, it does not shadow.
- Failover replays the buffered body on the next relay only while the
  failure is a transport error (before any response byte). Once the relay
  answered, the response streams through untouched — never retried.
- Body limits are per provider (vercel ~4.5MB, cloudflare ~100MB, deno
  ~100MB). The gateway buffers up to the largest limit and _skips_ to a
  provider that accepts the body; `413` only when no accepting provider
  remains. Requests above `stream_threshold_bytes` are relayed live
  instead — one attempt, no failover, no gateway-side `413`.
- Health is passive beneath the readiness gate: `failure_threshold`
  consecutive transport failures → `cooldown` → half-open recovery; a
  passive failure skips a relay, it never changes membership. When every
  candidate is down, Pick still returns one (best effort beats a 503).
- End-to-end headers forward verbatim (provider `Authorization` included).
  Stripped: `X-Relay-Provider`, hop-by-hop headers, and any
  client-supplied `X-Relay-Token` — the only value that header may carry
  on the relay leg is the verified snapshot's relay key, injected by the
  gateway. **Never added: `X-Forwarded-For` or anything that identifies
  the caller** — that is the whole point (see SECURITY.md).
- `/stats` exposes relay names, providers, health, counters, max-body, and
  the lifecycle states — never relay URLs or provider credentials. Error
  text goes through `internal/sanitize` (userinfo redaction, ANSI
  stripping, 512-byte bound).
- Shutdown drains in-flight requests (streaming included) against one
  whole-process `SHUTDOWN_GRACE` budget — the reconciler stops first —
  then process exit closes whatever is left. Keep the surrounding
  orchestrator's kill timer above the budget (`stop_grace_period: 30s` in
  compose vs the 20s default).

## Operations

```bash
curl http://127.0.0.1:20130/healthz # body "ok\n" — liveness, always answers
curl http://127.0.0.1:20130/readyz  # 200 once a relay is verified, else 503
curl http://127.0.0.1:20130/stats   # JSON pool + lifecycle snapshot
LISTEN_ADDR=127.0.0.1:20130 ./bin/http-relay-gateway healthcheck
LISTEN_ADDR=127.0.0.1:20130 ./bin/http-relay-gateway readinesscheck
./bin/http-relay-gateway version
```

## Docker

```bash
docker build -t relay-gateway:dev --build-arg VERSION=0.1.0-dev .
docker run --rm relay-gateway:dev version
docker compose up -d --build
curl http://127.0.0.1:20130/stats
```

The image is `scratch`: one static binary plus the CA bundle, running as
uid 65532. `ENV CONFIG_FILE=/app/config.yaml` is set in-image because
scratch has no WORKDIR. Compose publishes host **20130** only, bind-mounts
`./config.yaml` read-only, requires `RELAY_AUTH_TOKEN`, keeps
`stop_grace_period` above `SHUTDOWN_GRACE`, and uses the binary
`healthcheck` subcommand (no shell in the image).

## Layout

- `cmd/http-relay-gateway` — lifecycle, signals, the config poller, the
  apply loop that turns registry notifications into atomic pool swaps, and
  `version` / `healthcheck` / `readinesscheck`
- `internal/config` — the desired-state file (strict YAML, `${VAR}`
  interpolation, validation) and the bootstrap environment
- `internal/deploy` — platform deployers (vercel/cloudflare/deno):
  discovery, deploy, delete, scope pins, probes; `deploy/workers` holds the
  embedded relay workers shipped to every platform
- `internal/readiness` — the in-memory admission gate: lifecycle states,
  incarnations, single-flight, backoff, the verified serving snapshot
- `internal/reconcile` — the desired-state worker: sync, deletes, probe
  classification, redeploys, Strategy A replacements, paused-relay revival
- `internal/gateway` — HTTP data plane: pinning, bounded failover,
  streaming pass-through, in-flight drain, `/healthz` + `/readyz` + `/stats`
- `internal/pool` — the serving set: per-selector round-robin cursors,
  passive health, stats snapshots
- `internal/relayversion` — the release-managed worker version artifact
- `internal/logging`, `internal/sanitize` — zerolog setup and redaction
  helpers shared by all log/error paths
- `e2e` — black-box tests and sims driving the real binary as a subprocess
  with in-process fake edge relays and fake platform APIs; `go test ./e2e/`
  (skip with `-short`)
