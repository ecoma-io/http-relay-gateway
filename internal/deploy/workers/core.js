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
//   - Bodies and responses stream through untouched; an upstream transport
//     failure is a 502.

const HOP_BY_HOP = {
  connection: true,
  "keep-alive": true,
  "proxy-authenticate": true,
  "proxy-authorization": true,
  te: true,
  trailer: true,
  "transfer-encoding": true,
  upgrade: true,
};

function relayConfig(env) {
  const version =
    env && typeof env.RELAY_VERSION === "string" && env.RELAY_VERSION !== ""
      ? env.RELAY_VERSION
      : "unknown";
  const token = env && typeof env.RELAY_AUTH_TOKEN === "string" ? env.RELAY_AUTH_TOKEN : "";
  return { version, token };
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
    if (HOP_BY_HOP[lower]) continue;
    if (lower === "host" || lower === "content-length") continue;
    if (lower === "x-relay-target" || lower === "x-relay-path") continue;
    if (lower === "x-relay-provider" || lower === "x-relay-token") continue;
    headers.set(name, value);
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
  return upstreamResponse;
}
