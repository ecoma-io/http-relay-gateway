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
address, and the gateway owns the relay fleet — it deploys the relay workers
onto your platform accounts, verifies them end to end before admitting them,
rotates, fails over, and enforces the body limits. Everything is
internal-network only — **the data plane has no authentication**; the
process is a sidecar and must never be exposed beyond its compose network.

Any HTTP client works: a curl one-liner, an app-side fetch wrapper, an AI
gateway, a scraper.

## The shape in one paragraph

You declare **desired state** in one YAML file: which relays should exist
(identity = provider + name) and which provider credential manages each.
The gateway reconciles reality to that file: it discovers (or deploys) each
relay's worker on the platform, verifies it end to end, and admits only
verified relays to the serving pool. The file is re-read every second — edit
it and the fleet follows; delete an entry and the gateway drains the relay
and deletes the remote deployment. Nothing else holds state: no database, no
admin UI, no API to manage. The file plus the environment it references
_is_ the whole system; the process can be killed at any moment and rebuilds
everything from scratch on boot.

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
  untouched. The relay key the gateway itself presents on the relay leg is
  injected by the gateway and cannot be forged by a caller (below).
- `X-Relay-Provider`, any client-supplied `X-Relay-Token`, and hop-by-hop
  headers are stripped; `X-Forwarded-For` is never added — hiding the
  client IP is the whole point.
- `X-Relay-Provider` pins a provider (`vercel`, `cloudflare`, `deno`);
  empty, `none`, `auto`, or `all` round-robins across **every** provider.
  The header beats the `/{provider}` path prefix. An unknown pin answers
  `404` — except while no relay is ready at all, where any pin answers the
  retryable `503`, because configured-but-unverified and never-configured
  are indistinguishable there.
- Round-robin is deterministic: one cursor per selector key (pinned
  `vercel` traffic never skews the all-providers rotation), and with all
  relays healthy the served sequence is exactly pool order — sorted by
  `(provider, name)` — so tests can pin exact sequences.
- Responses stream straight through with a flush per write, so SSE chunks
  reach the client immediately. The one exception lives in the relay worker,
  not here: a `text/event-stream` response that carries **no**
  `content-encoding` gets an SSE comment line (`: relay-ping`) after 15 s of
  upstream silence — the worker's `SSE_PING_MS`, overridable through the
  worker env `RELAY_SSE_PING_MS` (a hook the conformance suite uses to time
  the case; no deployer sets it). A stream whose origin is thinking, not
  writing, is byte-silent, and an internal LB read timeout, a Cloudflare
  zone proxy read timeout or a platform lifecycle rule would reap it. The
  comment is SSE grammar, not data — no parser surfaces it as an event — it
  stops with the stream, an encoded body is never touched (the gateway never
  decodes relay traffic), and every other response is relayed byte for byte.
- The worker can also open an SSE caller's response before the origin answers
  — and that, like the heartbeat, is where the silence would kill the stream:
  a caller whose `Accept` asks for `text/event-stream` and whose origin stays
  silent gets its response opened at 20 s (the worker's
  `SSE_OPEN_BEFORE_UPSTREAM_MS`, overridable through the worker env
  `RELAY_SSE_OPEN_BEFORE_UPSTREAM_MS`, same hook rule). The opened response
  is a deliberate `200` with `content-type: text/event-stream` and an SSE
  comment line (`: relay-open`) first; the origin's body feeds through as it
  settles, heartbeating in the meantime. Inside the grace nothing changes: a
  fast answer — real status, `204` included, or a `502` on a refused
  connection — relays exactly as it does today. Past the grace the response
  is already open, so an origin failure ends the stream instead of being
  reported as a status — and a non-SSE caller is never opened early at all.
  The gateway forwards the response like any relayed byte.
- Failover happens only while the failure is still a transport error
  (before any response byte). Once the relay answered, the response is
  never retried. A body that dies mid-stream is recorded against the relay
  (a passive failure and a `midstreamFailures` counter, accumulating toward
  the cooldown) — but the truncated answer simply ends on the client; a
  client that hangs up is never evidence about the relay at all, neither
  success nor failure.

### The relay key

The gateway authenticates to **its own** relay workers with one global
relay key (`RELAY_AUTH_TOKEN` or `RELAY_AUTH_TOKEN_FILE`; the process
refuses to boot without exactly one). It is injected into every worker at
deploy time; a worker answers `404` to any request that does not carry it
on `X-Relay-Token`. The key is therefore not a caller credential — callers
stay unauthenticated — it is what keeps a stranger who discovers a relay
URL from using your deployment as their proxy. Rotating it means changing
the environment and restarting; every relay then redeploys (the old key is
rejected, which classifies as drift a deploy fixes).

### Endpoints

| Endpoint             | Meaning                                                                                |
| -------------------- | -------------------------------------------------------------------------------------- |
| `/healthz`           | `200` + `ok` — process liveness; answers even with zero ready relays                   |
| `/readyz`            | `200` once at least one relay has passed the readiness gate, else `503`                |
| `/stats`             | JSON snapshot: versions, per-relay health + counters, lifecycle — never URLs or tokens |
| `/{provider}` or `/` | the relay spec (above); `/{provider}` pins, `/` round-robins all                       |

`/stats` example:

```json
{
  "version": "0.1.0-dev",
  "relayVersion": "0.1.0",
  "relays": [
    {
      "name": "relay-a",
      "provider": "vercel",
      "healthy": true,
      "maxBody": 4500000,
      "requests": 12,
      "failures": 0
    }
  ],
  "readiness": { "ready": true, "readyRelays": 1 },
  "lifecycle": [{ "name": "relay-a", "provider": "vercel", "state": "ready", "generation": 1 }]
}
```

A relay row also carries `lastError` — the sanitized label of the most
recent passive transport failure (never a URL) — omitted entirely while
the relay has never failed, and `midstreamFailures` — how many responses
broke after the headers had gone through — omitted while it is zero.

## Readiness gate

A relay serves traffic only after it _proves_ it can. The proof, in order:

1. the deployment exists and answers on its origin (discovered from the
   provider, never configured);
2. it answers `/__relay/version` with the worker generation this binary
   deploys (`relayVersion` on `/stats`);
3. it accepts the relay key; and
4. a relay-spec request forwarded **through the relay's own URL** round
   trips end to end.

Every probe rides the same no-proxy transport the data plane uses — a probe
that could succeed via `HTTP_PROXY` while production fails direct would
make readiness lie. The self-origin probe is honest precisely because the
relay's upstream is controlled (the worker this binary ships); documented
limitation: it proves the relay forwards, not that any particular caller
target is reachable.

Lifecycle is tracked in memory and rendered on `/stats`:

| State         | Meaning                                                      |
| ------------- | ------------------------------------------------------------ |
| `configured`  | Desired; discovery pending                                   |
| `discovering` | Resolving the deployment from the provider                   |
| `discovered`  | Deployment located; verification pending                     |
| `deploying`   | A deploy/redeploy is in flight (single-flight per relay)     |
| `ready`       | Gate passed; admitted to the pool; round-robins traffic      |
| `unready`     | Was serving; verification failed `verify_demote_after` times |
| `failed`      | Never verified; retries under backoff                        |
| `paused`      | The platform answers instead of the worker (suspension)      |
| `removing`    | Left the desired config; remote delete in flight             |

Why a relay is not ready (`reason` on `/stats`):

| Reason              | Meaning                                                      |
| ------------------- | ------------------------------------------------------------ |
| `unreachable`       | No HTTP answer at all (cold start, network blip)             |
| `version_failed`    | Answers a different worker version                           |
| `auth_failed`       | Rejected the relay key                                       |
| `probe_failed`      | The forwarding round trip did not complete                   |
| `missing`           | No deployment exists for this identity (deploy queued)       |
| `deploy_failed`     | The platform deploy errored                                  |
| `credential_failed` | Provider credential rejected, ambiguous scope, or unreadable |
| `paused`            | The platform suspended the deployment                        |
| `replacing`         | Admission revoked on purpose: a replacement is in flight     |
| `delete_failed`     | The remote delete errored; retried under backoff             |

Verification runs on `verify_interval` and after every configuration
change. Failures are exponential-backoff gated (`verify_backoff_base`
doubling to `verify_backoff_max`) and never crash the process; while
_nothing_ is ready the retry delay is capped harder (`verify_recover_max`)
so a total outage heals quickly. A relay that recovers re-enters the pool
on the next pass — no restart, no manual intervention.

Zero-ready behavior: `/readyz` answers `503`, `/healthz` still answers
`ok` (the process is alive — no relay is), the data plane answers `503`
for every request including pinned ones, and `/stats` reports
`readiness.ready: false` with an empty relay list.

### What each probe answer does

| Probe observation                                               | Verdict        | Action                                           |
| --------------------------------------------------------------- | -------------- | ------------------------------------------------ |
| No HTTP answer                                                  | unreachable    | retry under backoff — **never redeploy**         |
| Platform suspension page (Vercel `402` / `DEPLOYMENT_DISABLED`) | paused         | out of the pool; re-probe on the revival cadence |
| Answers, but is not our worker (no version)                     | missing worker | redeploy                                         |
| Worker answers with a stale version                             | drift          | redeploy                                         |
| Worker rejects the relay key                                    | key drift      | redeploy with the current key                    |
| Worker ok, but the round trip fails on its side                 | probe failure  | retry under backoff                              |

The split is deliberate: transport failures and broken round trips may be
transient, so they demote and back off — a redeploy is never fired on a
hunch. Version drift, a missing worker, and a rejected key are exact, known
fixes, so they queue a replacement. A platform suspension cannot be lifted
by any deploy, so the relay pauses instead: no client is ever served the
platform's page, and its own pause gate (`revive_scan_interval`) re-probes
it on the ordinary pass until the worker answers again — the relay rejoins
automatically, or queues a catch-up redeploy first if the fleet version
moved on while it was dark.

### Two layers of health

The readiness gate above is the **verified** layer: slow, control-plane
work that decides membership. Beneath it, the pool keeps a **passive**
layer: `failure_threshold` consecutive transport failures put a relay on
`cooldown` and it is skipped until the cooldown expires (half-open
recovery); interleaved completed responses reset the streak, and one also
lifts a cooldown — a cooled relay picked best-effort recovers only when a
body actually completes. Passive failures skip a
relay — they never change membership; the verified layer owns that. When
every candidate is down, the pool still returns one (best effort beats a
`503` when the whole fleet is having a bad minute).

Passive health belongs to the **endpoint**, not to the serving generation.
The counters, the failure streak, the cooldown and the per-selector
round-robin position are keyed by relay identity (provider, name) plus the
exact endpoint they were earned against (scope pin fingerprint, URL,
relay-key fingerprint), so a rebuild — a registry notification for an
unrelated relay, an accepted reload, a replacement rollout — carries them
instead of resetting them, and rotation continues where it stopped rather
than restarting at the first relay. Runtime state resets only when the
identity leaves the desired configuration (see [Removal](#removal)), when
the endpoint itself changes (a moved scope pin, a different URL, a rotated
relay key — a different worker behind the same name), or when the process
restarts: nothing is persisted.

Every attempt is classified into exactly one outcome, and the classification
is what the counters and the logs record:

- **`upstream_pre_response`** — the relay leg failed before any response
  byte: a passive failure, and a buffered body replays on the next relay.
- **`relayed`** — the relay answered: a counted attempt at header time
  (never-retry is structural: the client already holds part of the answer).
  The passive-health success waits for the body to complete — a success
  that resets the streak is a completed response, never the header receipt
  the same attempt may go on to invalidate.
- **`upstream_midstream`** — the body died after the headers went through:
  a passive failure and `midstreamFailures` on the relay, never a replay —
  the truncated answer simply ends on the client, because an error status
  can no longer replace a response already in flight. The failure
  accumulates: with no completed response in between, consecutive
  mid-stream failures stack toward the threshold and trip the cooldown like
  any transport streak.
- **`client_aborted`** — the caller went away (the request context died:
  `http.Server.Shutdown` never cancels request contexts, so shutdown does
  not masquerade as this). The relay's leg is torn down _by_ the client's
  departure, so it counts as neither success nor failure and is never
  retried.

Shutdown drains in-flight attempts under this classification unchanged;
log lines written while the process is draining carry a `draining` field —
a label, nothing more.

Each attempt binds to exactly one serving generation, loaded once at the
pick: pool, client, retry budget, body limit and passive-health recording
all come from that single load, so a settings change or a replacement that
swaps the pool mid-request can neither split an attempt across generations
nor lose its health update to the generation it left. The one legitimate
exception is body acquisition, which freezes at entry — the bytes read
there cannot be re-read onto a different attempt. Streaming requests
always run exactly one attempt; buffered requests run `max_retries + 1`
against the generation they picked from.

### Replacements (Strategy A)

When a serving relay must be redeployed — version drift, key rotation, a
missing worker — the rollout runs in this exact order:

1. **Demote**: the relay's admission is revoked (`replacing`), so the next
   pool rebuild excludes it.
2. **Settle**: the gateway waits for that pool swap to be applied.
3. **Drain**: it waits for the old incarnation's in-flight requests to
   finish (bounded by a per-attempt quiesce window; a request that outlives
   it defers the replacement to the next pass).
4. **Deploy**: only then is the new worker pushed, because the platform
   switches what answers on the project's one production URL mid-deploy —
   an old admission left standing would serve whatever lands on that URL
   next, verified or not.
5. **Verify**: the new worker must pass the full gate before admission
   returns. A replacement that fails verification stays out — a failing
   verification may never keep serving.

A single-relay fleet therefore has a brief zero-ready window during its own
replacement (`503`, retryable). That is accepted by design: the alternative
is serving unverified content from the URL a deploy just switched.

### Removal

Delete an entry from the desired-state file and the relay's admission is
revoked immediately; the gateway waits for the pool swap and the drain of
in-flight requests, then deletes the remote deployment from the platform
(a `404` counts as success — the desired end state is "the remote does not
exist"), then forgets the relay. A failed delete retries under backoff
(`delete_failed`). Re-adding the entry mid-delete is safe: every relay has
an incarnation counter (its `generation` on `/stats`) that keeps stale
delete completions from touching a newer incarnation — the re-added
identity (or, if the delete finished first, its fresh incarnation) cannot
deploy until the old project has actually been deleted.
Provider credentials for deletes are remembered in memory; if the process
restarts while an entry is absent from the file, the remote is left behind
with a warning — the gateway never guesses a deletion.

### Body limits

Edge platforms cap request bodies themselves (Vercel ~4.5 MB, Cloudflare
~100 MB, Deno Deploy matched at 100 MB). The gateway buffers a body up to
the largest provider limit, then enforces the picked relay's per-provider
limit by _skipping_ to a provider that accepts the body. `413` is returned
only when the pin leaves no accepting provider or the body exceeds every
provider limit. Above `stream_threshold_bytes` a body is relayed live
instead — one attempt, no failover (the body is consumed), and no
gateway-side `413`.

### Proxy inbound

The same endpoint also speaks forward proxy: an HTTP client that routes
through the gateway (`--proxy`, `HTTP_PROXY`, any library's proxy setting)
hits the same pool. Routing intent is read from the absolute-form request
target itself — the gateway derives `X-Relay-Target` (scheme + host) and
`X-Relay-Path` (path + query) from the URL — so client-supplied values of
those two headers are **overwritten**, never trusted (no smuggling). The
provider pin is header-only in proxy mode (`X-Relay-Provider`); the
`/{provider}` prefix is a relay-spec feature and has no meaning here — the
target's path belongs to the target. `CONNECT` tunneling is not supported
(edge relays carry HTTP, not raw TCP) and is answered `501`; an
absolute-form `https://` target works as an ordinary request because the
relay itself fetches the target:

```bash
curl --proxy http://relay-gateway:20130 http://api.example.com/v1/chat \
  -H 'Authorization: Bearer $TARGET_TOKEN'
```

Control endpoints answer origin-form only — a proxy-form `/healthz` targets
some other host and relays, it does not shadow.

## Desired state

One YAML file (default `./config.yaml`, `CONFIG_FILE` to move it) is the
only source of intent:

```yaml
settings:
  log_level: info # debug | info | warn | error
  max_retries: 2 # failover attempts beyond the first
  stream_threshold_bytes: 0 # bodies above this stream; 0 = always buffer

relays:
  - name: relay-a
    provider: vercel
    token: ${VERCEL_TOKEN} # ${VAR} resolved from the environment at load
    team: my-team # optional vercel scope pin
  - name: relay-b
    provider: cloudflare
    token_file: /run/secrets/cf-token
    account: "023e105f4ecef8ad9ca31a8372d0c353" # optional cloudflare account pin
  - name: relay-c
    provider: deno
    token: ${DENO_TOKEN}
    organization: ${DENO_ORG_ID} # required deno organization pin (UUID; ${VAR} resolved like token)
```

Start from the committed reference layout: `cp config.example.yaml
config.yaml` (the copy is gitignored; every key ships commented out with
its default).

A relay is exactly four things: its **name** (identity, ≤ 128 chars), its
**provider** (`vercel` | `cloudflare` | `deno` — hard-coded), the provider
credential that manages its deployment, and an optional scope pin.
Everything else is derived: the platform project slug comes from the name
(lowercase letters, digits, dashes; ≤ 40 chars; two names that would slug
to the same project on one provider are rejected at load), and the serving
URL is discovered from the provider — never configured. Unknown YAML keys
are rejected (a typo must be loud); duplicate names are rejected.

Credentials: `token` and `token_file` are mutually exclusive; use at least
one. A `token` may embed `${VAR}` references — an unset or empty variable
rejects the load, so a credential never silently resolves to nothing. A
`token_file` names a file whose trimmed content is the credential; both
token files and the relay key file are **re-read every reconcile pass**, so
rotating a secret is dropping a new file in place — no restart, no config
touch. Write the new content atomically (to a temp file, then `rename` over
the old one): the poller reads every second and a partially written secret
reads as a corrupt credential and demotes the relay until the write
completes. Scope pins: `team` (vercel only) names the team scope when the token
can reach more than one; `account` (cloudflare only) pins the account id —
a cloudflare token that sees exactly one account resolves it automatically,
anything more ambiguous requires the explicit pin; `organization` (deno only)
pins the Deploy organization id, required because that API addresses projects
by organization and offers no route to resolve one from the token. Changing a
scope pin under an unchanged relay name is a new identity, not an update:
the relay's readiness generation bumps, admission is revoked first, and the
replacement runs the same settle → drain barrier as any rollout, so nothing
deploys into the new scope while the old scope's worker still carries a
request. The new scope then gets a fresh deployment; the deployment the old
scope hosted is **orphaned** — no later pass can reach or delete it, so the
log warns (scopes named by fingerprint, never by pin value) and the old
project must be removed manually. Generations are monotonic only during one
registry entry's lifetime and start at 1 for a fresh entry. A successful
remote delete purges its entry, so re-adding the same relay starts again at 1
even in the same process; they are not persisted, so a restart also recreates
every relay at 1.

The poller re-reads the file every second and compares content hashes — a
design, not a gap: no inotify, and unlike a watcher it cannot miss events
on bind mounts. A valid parse whose content differs becomes the desired
state; byte-identical content is not a change — no reconciler wake, no pool
rebuild; an invalid file is logged and ignored, so the last-known-good
fleet keeps serving. The file
being absent at boot is fine: the gateway starts with an empty fleet
(`/readyz` answers `503`) and reconciles the moment the file appears. A
file that disappears or turns invalid mid-run is a rejected reload, not a
fleet change — the last-known-good fleet keeps serving and the log warns
until a valid file returns. Removal happens by deleting a relay entry from
a valid file (see [Removal](#removal)) — back the file up; it plus the
referenced environment is the whole system.

A rebuild is a membership event, not a health event: the relays a reload
keeps take their passive health, counters and rotation position with them
(see [Two layers of health](#two-layers-of-health)) — adding an unrelated
relay, or a replacement rollout on one member, never resets the others. A
removed relay's runtime state is dropped, so re-adding its name starts
clean.

### Settings

Every key is optional; absent keys take the default. Malformed values are
load errors, never silent defaults. Every key applies on the next accepted
reload — no restart: the verification knobs reach the registry at once
(already-armed retry gates are recomputed from the failed attempt), the
rest land with the serving-generation rebuild. The timeout pair
`dial_timeout` / `response_header_timeout` is the one change that moves
the data plane onto a different outbound client (and retires the old one's
pooled connections); every other rebuild reuses the shared client, so its
keep-alive connections survive.

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

## Environment (bootstrap-only, restart to change)

| Env                     |       Default | Meaning                                                                       |
| ----------------------- | ------------: | ----------------------------------------------------------------------------- |
| `LISTEN_ADDR`           |       `:8080` | Relay endpoint; also serves `/healthz`, `/readyz`, `/stats`                   |
| `CONFIG_FILE`           | `config.yaml` | Desired-state file the poller watches                                         |
| `RELAY_AUTH_TOKEN`      |             — | The relay key workers authenticate with; exactly one of these two is required |
| `RELAY_AUTH_TOKEN_FILE` |             — | File whose trimmed content is the relay key                                   |
| `SHUTDOWN_GRACE`        |         `20s` | Whole-process drain budget for graceful shutdown                              |

## Run

```bash
gofmt -w . && go vet ./... && go test -race ./...
go build -ldflags "-X main.version=0.1.0-dev" -o bin/http-relay-gateway ./cmd/http-relay-gateway

LISTEN_ADDR=0.0.0.0:20130 RELAY_AUTH_TOKEN_FILE=./relay.key ./bin/http-relay-gateway
```

Subcommands: `http-relay-gateway version` prints the build version;
`http-relay-gateway healthcheck` probes `LISTEN_ADDR` and asserts the
`/healthz` body — this is what the Docker HEALTHCHECK runs, since the
scratch image has no shell; `http-relay-gateway readinesscheck` asserts
`/readyz` answers `200` — for orchestrators that gate traffic on a
verified fleet rather than a live process.

## Pointing a client at it

Any HTTP client speaks the contract with two headers:

```bash
curl http://relay-gateway:20130/vercel \
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

## Security posture

- The data plane authenticates **no one** — it is an internal-network
  sidecar, and its security boundary is network placement. Never publish it
  beyond the compose network.
- The relay key guards the workers, not the gateway: a stranger who finds a
  relay URL still cannot use it without the key, and the key is injected at
  deploy time and presented only on the relay leg — a caller-supplied
  `X-Relay-Token` is stripped.
- `X-Forwarded-For` is never added and the pin header never reaches the
  relay: the edge platform sees the gateway's egress, never yours.
- Relay URLs and provider tokens never appear in logs, `/stats`, or error
  text (error text is sanitized and bounded); secrets live in the
  environment or secret files, referenced from the desired-state file —
  never committed.
- Provider credentials travel to the platform management APIs only; relay
  legs and probes ride a no-proxy transport.
- Known limitation: the readiness probes are **not** authenticated against
  the platform — a man-in-the-middle on the relay leg can answer the probes
  itself and pass verification without ever being a real worker. The threat
  this design accepts is network-level: the gateway runs on a trusted
  internal network and reaches the platforms over TLS; if that path is
  compromised, probe fidelity is compromised with it.
- Anything security-shaped goes through [SECURITY.md](SECURITY.md) — a
  private advisory, never a public issue.

## Docker

```bash
docker build -t relay-gateway:dev --build-arg VERSION=0.1.0-dev .
docker run --rm relay-gateway:dev version
docker compose up -d --build
curl http://127.0.0.1:20130/stats
```

The image is `scratch`: one static binary plus the CA bundle, running as
uid 65532. `ENV CONFIG_FILE=/app/config.yaml` is set in-image because
scratch has no WORKDIR. `compose.yaml` publishes host **20130** only,
bind-mounts `./config.yaml` read-only into the container, requires
`RELAY_AUTH_TOKEN` from the environment, keeps `stop_grace_period` (30s)
above `SHUTDOWN_GRACE` (default 20s), bounds `json-file` logs, and uses the
binary `healthcheck` subcommand (no shell in the image).

Provider credentials reach the container through **`relay.env`** — an
operator-created, never-committed file next to `compose.yaml` that compose
injects (`env_file`) into the container environment, so the `${VAR}`
references inside `config.yaml` resolve there rather than against the
host shell:

```bash
cat > relay.env <<'EOF'
VERCEL_TOKEN=...
CLOUDFLARE_API_TOKEN=...
DENO_DEPLOY_TOKEN=...
# optional scope pins, when the config references them:
# VERCEL_TEAM_ID=...
# CLOUDFLARE_ACCOUNT_ID=...
# DENO_ORG_ID=...
EOF
chmod 600 relay.env
```

The file is optional — compose treats it as such — because a fleet whose
relays all use `token_file:` secrets needs none of these variables. (The
optional form, `env_file` with `required: false`, needs Docker Compose
v2.24 or newer; on an older compose use `env_file: relay.env` and create
the file — empty is fine.)

## Layout

- `cmd/http-relay-gateway` — lifecycle, signals, the config poller, the
  apply loop that turns registry notifications into atomic pool swaps, and
  `version` / `healthcheck` / `readinesscheck`
- `internal/config` — the desired-state file (strict YAML, `${VAR}`
  interpolation, validation) and the bootstrap environment
- `internal/deploy` — the platform deployers (vercel/cloudflare/deno):
  discovery, deploy, delete, scope pins, probes; `deploy/workers` holds the
  embedded relay workers shipped to every platform
- `internal/readiness` — the in-memory admission gate: lifecycle states,
  incarnations, single-flight, backoff, the verified serving snapshot
- `internal/reconcile` — the desired-state worker: sync, deletes,
  probe classification, redeploys, Strategy A replacements, paused-relay
  revival
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
