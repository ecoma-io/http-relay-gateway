# http-relay-gateway

A general-purpose Golang **HTTP relay manager**: one internal-network
endpoint that load-balances, health-checks, and fails over across a pool of
HTTP relays — so any application that wants to hide its egress IP can point
at the gateway instead of managing a relay list itself.

```
client ──(two headers)──▶ relay-gateway ──(verbatim forward)──▶ edge relay ──▶ target API
                          round-robin + failover
                          per-provider body limits
                          passive health + hot reload
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

Copy `config.example.yaml` to `config.yaml` and replace the example relays.
The file may contain private URLs; it is Git- and Docker-context-ignored.

```yaml
log-level: info
max-retries: 2
failure-threshold: 3
cooldown: 30s
providers:
  vercel:
    max-body: 4.5mb
  cloudflare:
    max-body: 100mb
relays:
  - name: vercel-1
    provider: vercel
    url: https://my-relay.vercel.app
```

A "provider" is any label you choose for grouping relays (`vercel`,
`cloudflare`, `deno`, …, or names of your own); body limits attach to the
label. Runtime settings live only in `config.yaml`: `log-level`,
`max-retries`, `failure-threshold`, `cooldown`, `providers`, and `relays`.
Strict decoding rejects unknown keys; a failed reload keeps the
last-known-good configuration serving, and first boot refuses to start.

### Hot reload

The process polls the file each second and reloads when its content hash
changes, so in-place edits and atomic replacements both reload under any
mount style. Reloads swap one immutable config+pool generation atomically:
in-flight requests finish on their original generation, and pool health
counters restart with the new pool.

Caveat on a **single-file bind mount** (`./config.yaml:/app/config.yaml:ro`):
a rename-over-the-mount update splices in a new inode that the mount never
follows, so the poller keeps hashing the old content. Edit in place
(`nano`, `echo >>`, `sed -i`) or `docker compose restart` after an atomic
replace. Do not add a SIGHUP fallback or an event-based watcher: neither can
fix that blind spot, while the content-hash poll already covers everything
else.

### Environment (bootstrap-only, restart to change)

| Env              |       Default | Meaning                                           |
| ---------------- | ------------: | ------------------------------------------------- |
| `CONFIG_FILE`    | `config.yaml` | Runtime YAML path                                 |
| `LISTEN_ADDR`    |       `:8080` | Relay endpoint (also serves `/healthz`, `/stats`) |
| `SHUTDOWN_GRACE` |         `20s` | Whole-process drain budget for graceful shutdown  |

## Run

```bash
gofmt -w . && go vet ./... && go test -race ./...
go build -ldflags "-X main.version=0.1.0-dev" -o bin/http-relay-gateway ./cmd/http-relay-gateway

CONFIG_FILE=config.yaml LISTEN_ADDR=0.0.0.0:20130 ./bin/http-relay-gateway
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
# Copy config.example.yaml to config.yaml and add real relays first.
docker compose up -d --build
curl http://127.0.0.1:20130/stats
```

`compose.yaml` publishes host **20130**, bind-mounts `config.yaml`
read-only, defaults to bounded `json-file` logs, keeps `stop_grace_period`
(30s) above `SHUTDOWN_GRACE` (default 20s), and uses the binary
`healthcheck` subcommand (no shell in the scratch image).

## Layout

- `cmd/http-relay-gateway` — lifecycle, signals, poller loop, `version` / `healthcheck`
- `internal/config` — bootstrap environment, Viper YAML validation, body-size parsing, content-hash change poller
- `internal/gateway` — HTTP data plane: provider selection, bounded failover, streaming pass-through, `/healthz` + `/stats`
- `internal/pool` — per-selector round-robin cursors, passive health (threshold → cooldown → half-open), stats snapshots
- `internal/logging`, `internal/sanitize` — zerolog setup and redaction helpers shared by all log/error paths
- `e2e` — black-box tests driving the real binary as a subprocess with fake
  edge-relay servers; `go test ./e2e/` (skip with `-short`)
