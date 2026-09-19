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

Go ≥ 1.26 (`go.mod` owns the floor). Viper is used for runtime YAML loading
and validation (`UnmarshalExact` — unknown keys are rejected); hot reload is
a self-contained 1s content-hash poller (`internal/config.Poller`). Style
rules: `math/rand/v2` (never `math/rand`), `for range n` loops, and no weak
`any` parsing — a config number must decode as a whole int or be rejected.

## Configure and run

Copy [`config.example.yaml`](config.example.yaml) to Git-ignored
`config.yaml`, then replace the placeholder relays with real edge
deployments. Relay URLs may carry private hostnames; never log, commit, or
bake the file into an image.

```bash
CONFIG_FILE=config.yaml \
LISTEN_ADDR=0.0.0.0:20130 \
./bin/http-relay-gateway
```

Environment variables are bootstrap-only and require restart:

| Env              |       Default | Meaning                                          |
| ---------------- | ------------: | ------------------------------------------------ |
| `CONFIG_FILE`    | `config.yaml` | Runtime YAML path                                |
| `LISTEN_ADDR`    |       `:8080` | Relay endpoint; also serves `/healthz`, `/stats` |
| `SHUTDOWN_GRACE` |         `20s` | Whole-process drain budget for shutdown          |

Runtime settings live only in `config.yaml`: `log-level`, `max-retries`,
`failure-threshold`, `cooldown`, `providers`, and `relays`. The process
polls the file each second and reloads when its content hash changes, so
in-place edits and atomic replacements both reload under any mount style. A
failed parse/validation leaves the last-known-good configuration serving.
Do not add a manual reload fallback (for example SIGHUP), and do not
reintroduce event-based watching: neither can fix the one blind spot, a
rename-over a single-file bind mount (the mount pins the old inode) — see
README "Hot reload".

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
- Body limits are per provider (`providers.<name>.max-body`, platform
  numbers: vercel ~4.5MB, cloudflare ~100MB). The gateway buffers up to the
  largest limit and _skips_ to a provider that accepts the body; `413` only
  when no accepting provider remains.
- Health is passive: `failure-threshold` consecutive failures → `cooldown`
  → half-open recovery. When every candidate is down, Pick still returns
  one (best effort beats a 503).
- End-to-end headers forward verbatim (provider `Authorization` included).
  Stripped: `X-Relay-Provider` and hop-by-hop headers. **Never added:
  `X-Forwarded-For` or anything that identifies the caller** — that is the
  whole point (see SECURITY.md).
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
LISTEN_ADDR=127.0.0.1:20130 ./bin/http-relay-gateway healthcheck
./bin/http-relay-gateway version
```

## Docker

```bash
docker build -t relay-gateway:dev --build-arg VERSION=0.1.0-dev .
docker run --rm relay-gateway:dev version
# Copy config.example.yaml to config.yaml and add real relays first.
docker compose up -d --build
curl http://127.0.0.1:20130/stats
```

The image is `scratch`: one static binary plus the CA bundle, running as
uid 65532 — a bind-mounted `config.yaml` must be readable by that uid
(`chmod 644`). `ENV CONFIG_FILE=/app/config.yaml` is set in-image because
scratch has no WORKDIR. Compose publishes host **20130**, bind-mounts the
config read-only, keeps `stop_grace_period` above `SHUTDOWN_GRACE`, and
uses the binary `healthcheck` subcommand (no shell in the image).

## Layout

- `cmd/http-relay-gateway` — lifecycle, signals, poller loop, `version` / `healthcheck`
- `internal/config` — bootstrap environment, Viper YAML validation, body-size parsing, content-hash change poller
- `internal/gateway` — HTTP data plane: pinning, bounded failover, streaming pass-through, `/healthz` + `/stats`
- `internal/pool` — per-selector round-robin cursors, passive health, stats snapshots
- `internal/logging`, `internal/sanitize` — zerolog setup and redaction helpers shared by all log/error paths
- `e2e` — black-box tests and sims driving the real binary as a subprocess
  with in-process fake edge relays; `go test ./e2e/` (skip with `-short`)
