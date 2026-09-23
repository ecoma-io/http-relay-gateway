// The relay worker shared by every platform. The deployer ships exactly ONE
// file per platform: this core concatenated with the platform entry, so no
// import/export may appear here.
//
// Behavior contract (the gateway and the reconciler depend on every line):
//   - GET /__relay/healthz and GET /__relay/version are unauthenticated and
//     answered no-store: the reconciler probes them, and a platform cache
//     that pinned an old version answer would break drift detection.
//   - Every other request must carry X-Relay-Token matching the deployed
//     RELAY_AUTH_TOKEN. A missing or wrong token is a 404 — never a 403 —
//     so a scanned relay is indistinguishable from an empty worker.
//   - X-Relay-Target (absolute http/https URL) is required; X-Relay-Path
//     (default "/") replaces the target's path and query. Everything else
//     end-to-end forwards verbatim — relay-spec, host and hop-by-hop headers
//     excepted. X-Forwarded-For is never added.
//   - Bodies stream through untouched, and so does every response — with one
//     deliberate exception: a text/event-stream response that carries no
//     content-encoding is piped through a heartbeat that emits the SSE
//     comment line ": relay-ping\n\n" whenever the upstream has been silent
//     for SSE_PING_MS. It exists because a long stream whose origin is
//     thinking, not writing, is byte-silent: an internal load balancer's
//     read timeout, a Cloudflare zone's proxy read timeout or a platform
//     lifecycle rule would reap the connection and truncate a stream the
//     origin is deliberately holding open. Comment lines are part of the SSE
//     grammar and surfaced by no parser as data, so the stream stays
//     byte-compatible for the caller. An encoded body is never touched (the
//     gateway never decodes relay traffic), a response without a body is
//     never wrapped, the heartbeat stops with the stream, and an upstream
//     read error errors the relayed body instead of ending it cleanly.
//     An upstream transport failure is a 502.

// Hop-by-hop headers are connection-scoped and never forwarded. The lookup
// must be a Set: a plain object would answer truthy for prototype-named
// inbound headers ("constructor", "toString", "__proto__") and silently
// drop them as if they were hop-by-hop.
const HOP_BY_HOP = new Set([
  "connection",
  "keep-alive",
  "proxy-authenticate",
  "proxy-authorization",
  "te",
  "trailer",
  "transfer-encoding",
  "upgrade",
]);

function relayConfig(env) {
  const version =
    env && typeof env.RELAY_VERSION === "string" && env.RELAY_VERSION !== ""
      ? env.RELAY_VERSION
      : "unknown";
  const token = env && typeof env.RELAY_AUTH_TOKEN === "string" ? env.RELAY_AUTH_TOKEN : "";
  return { version, token, ssePingMs: ssePingInterval(env) };
}

// The one deliberate exception to byte-for-byte pass-through, and the
// silence it measures: an SSE comment line every SSE_PING_MS of upstream
// silence. Only a whole positive integer in env.RELAY_SSE_PING_MS overrides
// the default — an unset, malformed or non-positive value is the default,
// never a disabled or zero-delay heartbeat.
//
// The comment is bytes, never a string: a response body is a byte stream,
// and a text chunk enqueued next to the upstream's byte chunks would be
// rejected by whoever consumes the body (undici included).
const SSE_PING = new TextEncoder().encode(": relay-ping\n\n");
const SSE_PING_MS = 15000;

function ssePingInterval(env) {
  const raw = env ? env.RELAY_SSE_PING_MS : undefined;
  if (typeof raw === "string" && /^[0-9]+$/.test(raw)) {
    const ms = Number(raw);
    if (ms > 0) return ms;
  }
  return SSE_PING_MS;
}

// sseHeartbeat is the TransformStream an SSE body is piped through. Its one
// job is the silence: the timer is armed at start (an origin may think for a
// while before its first token), re-armed after every upstream chunk, and
// cleared in flush (the upstream ended) and in cancel (the pipe aborted, or
// the consumer left) — so no comment can follow the upstream's last byte and
// no heartbeat can outlive its origin.
//
// Backpressure stays the pipe's: the transform pulls the upstream only as
// the consumer takes bytes. An upstream read error aborts the writable side,
// and pipeTo errors the readable side with that error rather than closing
// it — the relayed body must fail, never end cleanly, so the gateway keeps
// classifying a broken upstream as a mid-stream failure.
function sseHeartbeat(intervalMs) {
  let timer = null;

  const stop = () => {
    if (timer !== null) {
      clearTimeout(timer);
      timer = null;
    }
  };
  const beat = (controller) => {
    timer = setTimeout(() => {
      timer = null;
      // A callback already queued cannot be unqueued by clearTimeout, so an
      // abort can still land here after the stream closed or errored —
      // desiredSize is null then, and enqueuing would throw. Nothing re-arms
      // either: the heartbeat stops with the stream.
      if (controller.desiredSize === null) return;
      controller.enqueue(SSE_PING);
      beat(controller);
    }, intervalMs);
  };

  return new TransformStream({
    start(controller) {
      beat(controller);
    },
    transform(chunk, controller) {
      stop();
      controller.enqueue(chunk);
      beat(controller);
    },
    flush() {
      stop();
    },
    cancel() {
      stop();
    },
  });
}

// withSSEHeartbeat returns resp untouched unless it is a plain
// text/event-stream response: the content type must be text/event-stream AND
// no content-encoding header may ride the response (an encoded body must
// never be touched — the gateway deliberately never decodes relay traffic),
// and there must be a body to stream. Status, statusText and every header
// are preserved; content-length is dropped, because the body is no longer
// the same length.
function withSSEHeartbeat(resp, intervalMs) {
  if (resp.body === null) return resp;
  const type = resp.headers.get("content-type");
  if (type === null || type.split(";")[0].trim().toLowerCase() !== "text/event-stream") {
    return resp;
  }
  if (resp.headers.get("content-encoding") !== null) return resp;
  const headers = new Headers(resp.headers);
  headers.delete("content-length");
  return new Response(resp.body.pipeThrough(sseHeartbeat(intervalMs)), {
    status: resp.status,
    statusText: resp.statusText,
    headers,
  });
}

// Length-independent comparison: the loop always runs over the longer input
// and the length difference is folded into the accumulator, so timing leaks
// nothing beyond the longest token either side could hold.
function tokensMatch(expected, provided) {
  if (typeof provided !== "string") {
    return false;
  }
  const length = Math.max(expected.length, provided.length);
  let diff = expected.length ^ provided.length;
  for (let i = 0; i < length; i++) {
    diff |= (expected.charCodeAt(i) | 0) ^ (provided.charCodeAt(i) | 0);
  }
  return diff === 0;
}

function jsonReply(status, body) {
  return new Response(JSON.stringify(body) + "\n", {
    status,
    headers: { "content-type": "application/json", "cache-control": "no-store" },
  });
}

async function relayFetch(request, env) {
  const cfg = relayConfig(env);
  const url = new URL(request.url);

  if (request.method === "GET" && url.pathname === "/__relay/healthz") {
    return jsonReply(200, { status: "ok" });
  }
  if (request.method === "GET" && url.pathname === "/__relay/version") {
    return jsonReply(200, { version: cfg.version });
  }

  // 404, not 403: an unauthenticated scanner must not learn a relay lives
  // behind this URL.
  if (cfg.token === "" || !tokensMatch(cfg.token, request.headers.get("x-relay-token"))) {
    return jsonReply(404, { error: "not found" });
  }

  const target = request.headers.get("x-relay-target");
  if (target === null || target === "") {
    return jsonReply(400, { error: "x-relay-target header is required" });
  }
  let targetURL;
  try {
    targetURL = new URL(target);
  } catch {
    return jsonReply(400, { error: "x-relay-target must be an absolute URL" });
  }
  if (targetURL.protocol !== "http:" && targetURL.protocol !== "https:") {
    return jsonReply(400, { error: "x-relay-target must be an http(s) URL" });
  }

  const relayPath = request.headers.get("x-relay-path") || "/";
  let upstream;
  try {
    upstream = new URL(relayPath, targetURL);
  } catch {
    return jsonReply(400, { error: "x-relay-path must be a relative path" });
  }
  // The path may replace the target's path, never its origin: a relay must
  // not become an open proxy to wherever the caller likes.
  if (upstream.origin !== targetURL.origin) {
    return jsonReply(400, { error: "x-relay-path must stay on the target origin" });
  }

  const headers = new Headers();
  for (const [name, value] of request.headers) {
    const lower = name.toLowerCase();
    if (HOP_BY_HOP.has(lower)) continue;
    if (lower === "host" || lower === "content-length") continue;
    if (lower === "x-relay-target" || lower === "x-relay-path") continue;
    if (lower === "x-relay-provider" || lower === "x-relay-token") continue;
    // append, never set: iteration yields each name once with its values
    // joined (set-cookie iterates per value) — set would collapse a
    // per-value-iterated name to its last value, append keeps every value.
    headers.append(name, value);
  }

  const init = { method: request.method, headers, redirect: "manual" };
  if (request.method !== "GET" && request.method !== "HEAD") {
    init.body = request.body;
    // Required by undici (and harmless elsewhere) when the body is a stream.
    init.duplex = "half";
  }

  let upstreamResponse;
  try {
    upstreamResponse = await fetch(upstream, init);
  } catch {
    return jsonReply(502, { error: "upstream fetch failed" });
  }
  return withSSEHeartbeat(upstreamResponse, cfg.ssePingMs);
}
