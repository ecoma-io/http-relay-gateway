// Worker conformance suite. For every platform entry it builds the exact
// concatenation the deployer ships (core.js + entry), imports it under plain
// Node, and drives the handle(request, env) surface every entry exports.
// Run: node --test internal/deploy/workers/
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync, mkdtempSync, writeFileSync } from "node:fs";
import { createServer } from "node:http";
import { join, dirname } from "node:path";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const core = readFileSync(join(here, "core.js"), "utf8");

const PLATFORMS = ["vercel", "cloudflare", "deno"];

const env = { RELAY_VERSION: "v-conformance", RELAY_AUTH_TOKEN: "conform-token-0123456789abcdef" };

async function loadEntry(platform) {
  const source = core + "\n" + readFileSync(join(here, `entry.${platform}.js`), "utf8");
  const dir = mkdtempSync(join(tmpdir(), `relay-conformance-${platform}-`));
  const file = join(dir, "worker.mjs");
  writeFileSync(file, source);
  return import(file);
}

function relayRequest(path, init) {
  return new Request(`https://relay.example${path}`, init);
}

// Local upstream the worker fetches into; it echoes what it saw. resHeaders
// optionally ride on its answer — an array value stays one header line per
// value, which is what the set-cookie pass-through case asserts against.
async function withUpstream(run, resHeaders) {
  let seen = null;
  const upstream = createServer((req, res) => {
    const chunks = [];
    req.on("data", (c) => chunks.push(c));
    req.on("end", () => {
      seen = {
        method: req.method,
        url: req.url,
        headers: req.headers,
        body: Buffer.concat(chunks).toString("utf8"),
      };
      res.writeHead(203, {
        "x-upstream": "yes",
        "content-type": "application/json",
        ...resHeaders,
      });
      res.end(JSON.stringify({ served: true }));
    });
  });
  await new Promise((resolve) => upstream.listen(0, "127.0.0.1", resolve));
  try {
    return await run(`http://127.0.0.1:${upstream.address().port}`, () => seen);
  } finally {
    await new Promise((resolve) => upstream.close(resolve));
  }
}

// Streaming upstream for the SSE cases: it answers 200 with the given
// headers and writes every string the frame generator yields, flushing per
// write. An await inside the generator leaves the origin silent while the
// relay is already serving — which is exactly the window the heartbeat
// cases observe. (withUpstream above buffers its request and answers in one
// write; this one stays open.)
async function withStreamUpstream(resHeaders, frame, run) {
  const upstream = createServer((req, res) => {
    req.resume();
    res.writeHead(200, { ...resHeaders });
    (async () => {
      for await (const chunk of frame()) {
        res.write(chunk);
        if (typeof res.flush === "function") res.flush();
      }
      res.end();
    })().catch(() => {});
  });
  await new Promise((resolve) => upstream.listen(0, "127.0.0.1", resolve));
  try {
    return await run(`http://127.0.0.1:${upstream.address().port}`);
  } finally {
    upstream.closeAllConnections?.();
    await new Promise((resolve) => upstream.close(resolve));
  }
}

// readUntil pulls from a relayed body until want(text) holds and returns
// everything read up to that point. A bounded wait turns a relay that
// buffered, swallowed or stalled a write into a reported observation instead
// of a hung suite.
async function readUntil(reader, want, what, timeoutMs = 3000) {
  const decoder = new TextDecoder();
  const deadline = Date.now() + timeoutMs;
  let text = "";
  for (;;) {
    const left = deadline - Date.now();
    if (left <= 0) throw new Error(`timed out waiting for ${what}; saw ${JSON.stringify(text)}`);
    let timer;
    const expired = new Promise((_, reject) => {
      timer = setTimeout(
        () => reject(new Error(`timed out waiting for ${what}; saw ${JSON.stringify(text)}`)),
        left,
      );
      timer.unref?.();
    });
    let result;
    try {
      result = await Promise.race([reader.read(), expired]);
    } finally {
      clearTimeout(timer);
    }
    if (result.done) return { text, done: true };
    text += decoder.decode(result.value, { stream: true });
    if (want(text)) return { text, done: false };
  }
}

// The comment line the worker emits, and the fast interval its cases run on:
// the timing cases hold the origin silent for a few intervals and read what
// crossed the relay meanwhile, so the interval stays small enough to keep
// the suite quick while leaving the default (15000ms) untouched.
const SSE_PING = ": relay-ping\n\n";
const SSE_PING_MS = 25;
const sseEnv = { ...env, RELAY_SSE_PING_MS: String(SSE_PING_MS) };

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// sseRequest targets the streaming upstream; marker selects one origin gate.
function sseRequest(upstreamBase) {
  return relayRequest("/x", {
    headers: {
      "x-relay-token": env.RELAY_AUTH_TOKEN,
      "x-relay-target": upstreamBase,
      "x-relay-path": "/sse",
    },
  });
}

// The single comment byte the early-open path emits, and the short grace it
// runs on in the suite: the cases need to observe "origin silent past the
// grace" in milliseconds, not in the default's 20 seconds.
const SSE_OPEN = ": relay-open\n\n";
const SSE_OPEN_MS = 25;

// The same SSE call with an explicit accept for text/event-stream, so the
// request itself qualifies for the early-open gate.
function sseAcceptRequest(upstreamBase) {
  return relayRequest("/x", {
    headers: {
      "x-relay-token": env.RELAY_AUTH_TOKEN,
      "x-relay-target": upstreamBase,
      "x-relay-path": "/sse",
      accept: "text/event-stream",
    },
  });
}

// The early-open environment: heartbeat interval AND grace are both shortened
// — the two are independent, and the cases exercise both.
const earlyOpenEnv = {
  ...env,
  RELAY_SSE_PING_MS: String(SSE_PING_MS),
  RELAY_SSE_OPEN_BEFORE_UPSTREAM_MS: String(SSE_OPEN_MS),
};

// stripPings removes every heartbeat comment from a relayed SSE body, which
// must leave exactly the bytes the upstream wrote.
const stripPings = (text) => text.split(SSE_PING).join("");

for (const platform of PLATFORMS) {
  const mod = await loadEntry(platform);

  test(`${platform}: healthz is unauthenticated and no-store`, async () => {
    const res = await mod.handle(relayRequest("/__relay/healthz"), env);
    assert.equal(res.status, 200);
    assert.equal(res.headers.get("cache-control"), "no-store");
    assert.deepEqual(await res.json(), { status: "ok" });
  });

  test(`${platform}: version answers the deployed RELAY_VERSION`, async () => {
    const res = await mod.handle(relayRequest("/__relay/version"), env);
    assert.equal(res.status, 200);
    assert.equal(res.headers.get("cache-control"), "no-store");
    assert.deepEqual(await res.json(), { version: "v-conformance" });
  });

  test(`${platform}: missing or wrong token is a 404, not a 403`, async () => {
    for (const init of [
      { method: "GET" },
      { method: "GET", headers: { "x-relay-token": "wrong" } },
      { method: "POST", headers: { "x-relay-token": "wrong" } },
    ]) {
      const res = await mod.handle(relayRequest("/v1/messages", init), env);
      assert.equal(res.status, 404, `token case ${JSON.stringify(init)}`);
      assert.deepEqual(await res.json(), { error: "not found" });
    }
  });

  test(`${platform}: empty token env locks the relay entirely`, async () => {
    const res = await mod.handle(relayRequest("/x", { headers: { "x-relay-token": "anything" } }), {
      RELAY_VERSION: "v-conformance",
      RELAY_AUTH_TOKEN: "",
    });
    assert.equal(res.status, 404);
  });

  test(`${platform}: target is required, absolute, and http(s)`, async () => {
    const authed = { headers: { "x-relay-token": env.RELAY_AUTH_TOKEN } };
    for (const [target, want] of [
      [null, "required"],
      ["not a url", "absolute"],
      ["ftp://files.example.com/x", "http(s)"],
    ]) {
      const headers = { ...authed.headers };
      if (target !== null) headers["x-relay-target"] = target;
      const res = await mod.handle(relayRequest("/x", { headers }), env);
      assert.equal(res.status, 400, `target ${String(target)}`);
      assert.match(await res.text(), /error/);
      void want;
    }
  });

  test(`${platform}: path cannot escape the target origin`, async () => {
    const res = await mod.handle(
      relayRequest("/x", {
        headers: {
          "x-relay-token": env.RELAY_AUTH_TOKEN,
          "x-relay-target": "https://api.example.com",
          "x-relay-path": "https://evil.example.com/steal",
        },
      }),
      env,
    );
    assert.equal(res.status, 400);
  });

  test(`${platform}: forwards method, path, body and headers verbatim`, async () => {
    await withUpstream(async (upstreamBase, seen) => {
      const res = await mod.handle(
        relayRequest("/ignored-by-the-relay", {
          method: "POST",
          headers: {
            "x-relay-token": env.RELAY_AUTH_TOKEN,
            "x-relay-target": upstreamBase,
            "x-relay-path": "/v1/messages?beta=true",
            "x-relay-provider": "vercel",
            authorization: "Bearer upstream-secret",
            connection: "keep-alive",
            "content-type": "application/json",
            "x-custom-trace": "keep-me",
          },
          body: JSON.stringify({ question: 42 }),
        }),
        env,
      );
      assert.equal(res.status, 203);
      assert.equal(res.headers.get("x-upstream"), "yes");
      assert.deepEqual(await res.json(), { served: true });

      const got = seen();
      assert.equal(got.method, "POST");
      assert.equal(got.url, "/v1/messages?beta=true");
      assert.equal(got.headers.authorization, "Bearer upstream-secret");
      assert.equal(got.headers["x-custom-trace"], "keep-me");
      assert.equal(got.headers["content-type"], "application/json");
      assert.equal(got.headers["x-relay-token"], undefined);
      assert.equal(got.headers["x-relay-target"], undefined);
      assert.equal(got.headers["x-relay-path"], undefined);
      assert.equal(got.headers["x-relay-provider"], undefined);
      // Node's undici adds its own connection: keep-alive on the outbound
      // hop, so the forwarded-inbound strip of hop-by-hop headers is not
      // observable here — the runtimes on the real platforms manage that
      // header themselves.
      assert.equal(JSON.parse(got.body).question, 42);
    });
  });

  test(`${platform}: multi-value request headers forward with every value`, async () => {
    await withUpstream(async (upstreamBase, seen) => {
      const res = await mod.handle(
        relayRequest("/x", {
          headers: [
            ["x-relay-token", env.RELAY_AUTH_TOKEN],
            ["x-relay-target", upstreamBase],
            ["x-relay-path", "/multi"],
            ["cookie", "a=1"],
            ["cookie", "b=2"],
            ["set-cookie", "x=1"],
            ["set-cookie", "y=2"],
            ["x-multi", "one"],
            ["x-multi", "two"],
          ],
        }),
        env,
      );
      assert.equal(res.status, 203);
      const got = seen();
      // Headers iteration hands the worker each name once with its values
      // joined (set-cookie iterates per value); every value must reach the
      // origin — the copy appends and never re-sets, so nothing collapses
      // to the last value. The exact join separator stays the runtime's.
      const both = (name, v1, v2) => {
        const raw = got.headers[name];
        const joined = Array.isArray(raw) ? raw.join("\n") : String(raw);
        assert.ok(
          joined.includes(v1) && joined.includes(v2),
          `${name} = ${JSON.stringify(raw)}, want both ${v1} and ${v2}`,
        );
      };
      both("cookie", "a=1", "b=2");
      both("set-cookie", "x=1", "y=2");
      both("x-multi", "one", "two");
    });
  });

  test(`${platform}: prototype-named headers forward, not mistaken for hop-by-hop`, async () => {
    await withUpstream(async (upstreamBase, seen) => {
      const res = await mod.handle(
        relayRequest("/x", {
          headers: {
            "x-relay-token": env.RELAY_AUTH_TOKEN,
            "x-relay-target": upstreamBase,
            constructor: "built-by",
            ToString: "rendered",
            hasOwnProperty: "owned",
          },
        }),
        env,
      );
      assert.equal(res.status, 203);
      const got = seen();
      // Node's incoming headers are a null-prototype object, so these reads
      // see the forwarded header or nothing — never an inherited value.
      assert.equal(got.headers.constructor, "built-by");
      assert.equal(got.headers.tostring, "rendered");
      assert.equal(got.headers.hasownproperty, "owned");
    });
  });

  test(`${platform}: response set-cookie values pass through per value`, async () => {
    await withUpstream(
      async (upstreamBase) => {
        const res = await mod.handle(
          relayRequest("/x", {
            headers: { "x-relay-token": env.RELAY_AUTH_TOKEN, "x-relay-target": upstreamBase },
          }),
          env,
        );
        assert.equal(res.status, 203);
        // The worker hands the upstream response through untouched: the
        // values must stay separate, never joined into one cookie line.
        assert.deepEqual(res.headers.getSetCookie(), ["a=1", "b=2"]);
      },
      { "set-cookie": ["a=1", "b=2"] },
    );
  });

  test(`${platform}: default path is / and query comes from x-relay-path only`, async () => {
    await withUpstream(async (upstreamBase, seen) => {
      const res = await mod.handle(
        relayRequest("/x", {
          headers: { "x-relay-token": env.RELAY_AUTH_TOKEN, "x-relay-target": upstreamBase },
        }),
        env,
      );
      assert.equal(res.status, 203);
      assert.equal(seen().url, "/");
    });
  });

  test(`${platform}: upstream transport failure is a 502`, async () => {
    const dead = createServer();
    await new Promise((resolve) => dead.listen(0, "127.0.0.1", resolve));
    const deadPort = dead.address().port;
    await new Promise((resolve) => dead.close(resolve));
    const res = await mod.handle(
      relayRequest("/x", {
        method: "POST",
        headers: {
          "x-relay-token": env.RELAY_AUTH_TOKEN,
          "x-relay-target": `http://127.0.0.1:${deadPort}`,
        },
        body: "payload",
      }),
      env,
    );
    assert.equal(res.status, 502);
    assert.deepEqual(await res.json(), { error: "upstream fetch failed" });
  });

  test(`${platform}: a silent SSE upstream is kept alive by heartbeat comments`, async () => {
    let release;
    const parked = new Promise((resolve) => (release = resolve));
    await withStreamUpstream(
      { "content-type": "text/event-stream" },
      async function* () {
        yield "data: one\n\n";
        // Silent for as long as the test likes: the origin is parked, so
        // anything that arrives now was emitted by the relay.
        await parked;
        yield "data: two\n\n";
      },
      async (upstreamBase) => {
        const res = await mod.handle(sseRequest(upstreamBase), sseEnv);
        assert.equal(res.status, 200);
        assert.equal(res.headers.get("content-type"), "text/event-stream");

        const reader = res.body.getReader();
        const first = await readUntil(reader, (text) => text.includes(SSE_PING), "a heartbeat");
        assert.ok(first.text.includes(SSE_PING), `no heartbeat while the upstream was silent`);
        // The upstream is still parked: the comment crossed the relay
        // without the origin having written anything else.
        assert.ok(
          !first.text.includes("data: two"),
          `the upstream's second chunk arrived before it was released: ${JSON.stringify(first.text)}`,
        );

        release();
        const rest = await readUntil(reader, () => false, "the stream to end");
        assert.ok(rest.done, "the relayed stream did not end with the upstream");
        const body = first.text + rest.text;

        // The upstream's own bytes are all present, unmodified and in order;
        // nothing else but heartbeat comments joined them; and nothing was
        // appended after the upstream's last byte.
        assert.equal(stripPings(body), "data: one\n\ndata: two\n\n");
        assert.ok(body.endsWith("data: two\n\n"), `bytes follow the upstream's last byte: ${body}`);
      },
    );
  });

  test(`${platform}: a non-SSE body is relayed byte-for-byte`, async () => {
    // The body IS the heartbeat text: an injection on a non-SSE response
    // would show up as added bytes, not as a needle in a haystack.
    const body = SSE_PING + "plain body\n";
    await withStreamUpstream(
      { "content-type": "text/plain" },
      async function* () {
        yield body;
        await sleep(4 * SSE_PING_MS);
      },
      async (upstreamBase) => {
        const res = await mod.handle(sseRequest(upstreamBase), sseEnv);
        assert.equal(res.status, 200);
        assert.equal(res.headers.get("content-type"), "text/plain");
        assert.equal(await res.text(), body);
      },
    );
  });

  test(`${platform}: an SSE body with a content-encoding is relayed byte-for-byte`, async () => {
    // An encoded body must never be touched — the gateway deliberately never
    // decodes relay traffic, so an injected byte would corrupt it. The guard
    // is the header's presence, whatever its value; the body below carries
    // the heartbeat text itself, so an injection would be visible as added
    // bytes. (identity keeps the comparison byte-exact: Node's fetch decodes
    // gzip/br/zstd before the worker's guard ever sees the body, so those
    // codings would hide what the worker actually forwarded.)
    const body = "data: one\n\n" + SSE_PING + "data: two\n\n";
    await withStreamUpstream(
      { "content-type": "text/event-stream", "content-encoding": "identity" },
      async function* () {
        yield body;
        await sleep(4 * SSE_PING_MS);
      },
      async (upstreamBase) => {
        const res = await mod.handle(sseRequest(upstreamBase), sseEnv);
        assert.equal(res.status, 200);
        assert.equal(res.headers.get("content-encoding"), "identity");
        assert.equal(await res.text(), body);
      },
    );
  });

  test(`${platform}: a bodyless response is relayed, never wrapped`, async () => {
    // The sharpest shape: a 204 that still carries the SSE content type has
    // nothing to wrap, and wrapping it would throw (a Response with a body
    // cannot hold a null-body status) — the wrap is gated on the body, so
    // the 204 relays as itself.
    const upstream = createServer((req, res) => {
      req.resume();
      res.writeHead(204, { "content-type": "text/event-stream" });
      res.end();
    });
    await new Promise((resolve) => upstream.listen(0, "127.0.0.1", resolve));
    try {
      const base = `http://127.0.0.1:${upstream.address().port}`;
      const res = await mod.handle(sseRequest(base), sseEnv);
      assert.equal(res.status, 204);
      assert.equal(res.headers.get("content-type"), "text/event-stream");
      assert.equal(await res.text(), "");
    } finally {
      upstream.closeAllConnections?.();
      await new Promise((resolve) => upstream.close(resolve));
    }
  });

  test(`${platform}: an upstream that breaks mid-stream errors the relayed body`, async () => {
    const upstream = createServer((req, res) => {
      req.resume();
      res.writeHead(200, { "content-type": "text/event-stream" });
      res.write("data: one\n\n");
      if (typeof res.flush === "function") res.flush();
      // Cut the connection mid-body: no terminating chunk ever follows, so
      // the relayed stream must fail rather than close cleanly — that error
      // is the mid-stream classification the gateway relies on.
      setTimeout(() => res.socket?.destroy(), 50);
    });
    await new Promise((resolve) => upstream.listen(0, "127.0.0.1", resolve));
    try {
      const base = `http://127.0.0.1:${upstream.address().port}`;
      const res = await mod.handle(sseRequest(base), sseEnv);
      assert.equal(res.status, 200);
      let failed = null;
      try {
        await res.text();
      } catch (err) {
        failed = err;
      }
      assert.ok(
        failed !== null,
        "the relayed body ended cleanly after the upstream broke mid-stream",
      );
    } finally {
      upstream.closeAllConnections?.();
      await new Promise((resolve) => upstream.close(resolve));
    }
  });

  test(`${platform}: no heartbeat is emitted after the upstream's final byte`, async () => {
    await withStreamUpstream(
      { "content-type": "text/event-stream" },
      async function* () {
        yield "data: start\n\n";
        // Silent long enough for several heartbeats to have gone out, so the
        // final assertion below is about the end of the stream, not about a
        // heartbeat that never happened.
        await sleep(4 * SSE_PING_MS);
        yield "data: end\n\n";
      },
      async (upstreamBase) => {
        const res = await mod.handle(sseRequest(upstreamBase), sseEnv);
        assert.equal(res.status, 200);
        const text = await res.text();
        assert.ok(text.includes(SSE_PING), "the heartbeat never fired, so the case proves nothing");
        assert.equal(stripPings(text), "data: start\n\ndata: end\n\n");
        assert.ok(text.endsWith("data: end\n\n"), `bytes follow the upstream's last byte: ${text}`);
      },
    );
  });

  test(`${platform}: a slow SSE origin gets the response opened early with the marker`, async () => {
    let release;
    const parked = new Promise((resolve) => (release = resolve));
    await withStreamUpstream(
      { "content-type": "text/event-stream" },
      async function* () {
        // Silent for 5× the grace: the relay must open the response while
        // the origin has not written a byte.
        await parked;
        yield "data: late\n\n";
      },
      async (upstreamBase) => {
        const res = await mod.handle(sseAcceptRequest(upstreamBase), earlyOpenEnv);
        assert.equal(res.status, 200);
        assert.equal(res.headers.get("content-type"), "text/event-stream");

        const reader = res.body.getReader();
        // The response opened at the grace mark — before the origin's first
        // byte. Every byte crossing so far is the relay's own.
        const first = await readUntil(
          reader,
          (text) => text.includes(SSE_OPEN),
          "the early-open marker",
        );
        assert.ok(first.text.includes(SSE_OPEN), `no marker under the origin's silence`);
        assert.ok(
          !first.text.includes("data: late"),
          `the origin's body arrived before it was released: ${JSON.stringify(first.text)}`,
        );

        release();
        const rest = await readUntil(reader, () => false, "the stream to end");
        assert.ok(rest.done, "the relayed stream did not end with the upstream");
        const body = first.text + rest.text;

        // The marker appears exactly once, at the head, and the origin's data
        // is unmodified and in order — followed only by heartbeat comments
        // the strip removes, never by a second marker.
        assert.ok(
          body.startsWith(SSE_OPEN),
          `the marker is not at the head: ${JSON.stringify(body)}`,
        );
        assert.equal(
          body.split(SSE_OPEN).length - 1,
          1,
          `marker repeated: ${JSON.stringify(body)}`,
        );
        assert.equal(stripPings(body), SSE_OPEN + "data: late\n\n");
        assert.ok(body.endsWith("data: late\n\n"), `bytes follow the origin's last byte: ${body}`);
      },
    );
  });

  test(`${platform}: an origin that settles inside the grace is relayed unchanged, no marker`, async () => {
    await withStreamUpstream(
      { "content-type": "text/event-stream" },
      async function* () {
        yield "data: quick\n\n";
      },
      async (upstreamBase) => {
        const res = await mod.handle(sseAcceptRequest(upstreamBase), earlyOpenEnv);
        assert.equal(res.status, 200);
        assert.equal(res.headers.get("content-type"), "text/event-stream");
        const text = await res.text();
        assert.equal(text, "data: quick\n\n");
      },
    );
  });

  test(`${platform}: a fast origin 204 inside the grace stays 204 and is never wrapped`, async () => {
    const upstream = createServer((req, res) => {
      req.resume();
      res.writeHead(204, { "content-type": "text/event-stream" });
      res.end();
    });
    await new Promise((resolve) => upstream.listen(0, "127.0.0.1", resolve));
    try {
      const base = `http://127.0.0.1:${upstream.address().port}`;
      const res = await mod.handle(sseAcceptRequest(base), earlyOpenEnv);
      assert.equal(res.status, 204);
      assert.equal(res.headers.get("content-type"), "text/event-stream");
      assert.equal(await res.text(), "");
    } finally {
      upstream.closeAllConnections?.();
      await new Promise((resolve) => upstream.close(resolve));
    }
  });

  test(`${platform}: a non-SSE caller is never opened early`, async () => {
    // A parked origin answers nothing, not even the response headers. An SSE
    // caller would get a volunteered 200 with the marker at the grace mark;
    // this caller does not ask for SSE, so the relay has no reason to open —
    // it must stay pending on the origin, far past the grace.
    let release;
    const parked = new Promise((resolve) => (release = resolve));
    const upstream = createServer((req, res) => {
      req.resume();
      void parked.then(() => {
        res.writeHead(200, { "content-type": "text/event-stream" });
        res.write("data: late\n\n");
        res.end();
      });
    });
    await new Promise((resolve) => upstream.listen(0, "127.0.0.1", resolve));
    try {
      const base = `http://127.0.0.1:${upstream.address().port}`;
      const fetched = mod.handle(sseRequest(base), earlyOpenEnv);
      const outcome = await Promise.race([
        fetched.then(() => "settled"),
        sleep(4 * SSE_OPEN_MS).then(() => "pending"),
      ]);
      assert.equal(outcome, "pending", "the relay opened a response for a non-SSE caller");

      release();
      const res = await fetched;
      assert.equal(res.status, 200);
      assert.equal(res.headers.get("content-type"), "text/event-stream");
      const text = await res.text();
      // The response opened only when the origin answered: no marker reached
      // this caller (heartbeat comments may — they follow the RESPONSE's
      // media type, which is the origin's, already established behavior).
      assert.ok(
        !text.includes(SSE_OPEN),
        `a non-SSE caller saw the marker: ${JSON.stringify(text)}`,
      );
      assert.equal(text.replaceAll(SSE_PING, ""), "data: late\n\n");
    } finally {
      upstream.closeAllConnections?.();
      await new Promise((resolve) => upstream.close(resolve));
    }
  });

  test(`${platform}: a settled non-SSE body past the grace rides the opened stream unchanged`, async () => {
    // The opened response commits text/event-stream (the relay's own
    // promise, made while the origin was silent). When the late origin
    // answers with plain data, its bytes still feed through verbatim after
    // the marker — the relay never rewrites what the origin wrote, it only
    // ever added the marker and heartbeat comments.
    let release;
    const parked = new Promise((resolve) => (release = resolve));
    await withStreamUpstream(
      { "content-type": "text/plain" },
      async function* () {
        await parked;
        yield "plain answer\n";
      },
      async (upstreamBase) => {
        const res = await mod.handle(sseAcceptRequest(upstreamBase), earlyOpenEnv);
        assert.equal(res.status, 200);
        assert.equal(res.headers.get("content-type"), "text/event-stream");
        const reader = res.body.getReader();
        const first = await readUntil(
          reader,
          (text) => text.includes(SSE_OPEN),
          "the early-open marker",
        );
        release();
        const rest = await readUntil(reader, () => false, "the stream to end");
        const body = first.text + rest.text;
        assert.ok(
          body.startsWith(SSE_OPEN),
          `the marker is not at the head: ${JSON.stringify(body)}`,
        );
        assert.equal(
          body.split(SSE_OPEN).length - 1,
          1,
          `marker repeated: ${JSON.stringify(body)}`,
        );
        assert.equal(
          body.replace(SSE_OPEN, "").replaceAll(SSE_PING, ""),
          "plain answer\n",
          `origin bytes altered past the grace: ${JSON.stringify(body)}`,
        );
      },
    );
  });

  test(`${platform}: a refusal inside the grace keeps the origin's status`, async () => {
    // A fast 502: the origin refused before the grace ran out, so it is the
    // origin's own status that relays — never a volunteered 200.
    const dead = createServer();
    await new Promise((resolve) => dead.listen(0, "127.0.0.1", resolve));
    const deadPort = dead.address().port;
    await new Promise((resolve) => dead.close(resolve));
    const res = await mod.handle(sseAcceptRequest(`http://127.0.0.1:${deadPort}`), earlyOpenEnv);
    assert.equal(res.status, 502);
    assert.deepEqual(await res.json(), { error: "upstream fetch failed" });
  });

  test(`${platform}: an SSE origin that settles past the grace with a 204 closes cleanly`, async () => {
    const upstream = createServer((req, res) => {
      req.resume();
      // Silent past the grace, then a real 204: the origin answered, but the
      // caller already holds the opened 200 — the body-less answer just ends
      // the stream, without a status the caller is waiting on.
      setTimeout(() => {
        res.writeHead(204, { "content-type": "text/event-stream" });
        res.end();
      }, 5 * SSE_OPEN_MS);
    });
    await new Promise((resolve) => upstream.listen(0, "127.0.0.1", resolve));
    try {
      const base = `http://127.0.0.1:${upstream.address().port}`;
      const res = await mod.handle(sseAcceptRequest(base), earlyOpenEnv);
      assert.equal(res.status, 200);
      assert.equal(res.headers.get("content-type"), "text/event-stream");
      const body = await res.text();
      assert.ok(
        body.startsWith(SSE_OPEN),
        `no marker under the origin's silence: ${JSON.stringify(body)}`,
      );
      assert.equal(body.split(SSE_OPEN).length - 1, 1, `marker repeated: ${JSON.stringify(body)}`);
    } finally {
      upstream.closeAllConnections?.();
      await new Promise((resolve) => upstream.close(resolve));
    }
  });

  test(`${platform}: an origin that breaks after the response opened errors the body, marker first`, async () => {
    const upstream = createServer((req, res) => {
      req.resume();
      // The relay opened the response at the grace mark, then the origin
      // breaks mid-body — a relayed stream that fails, never one that ends
      // cleanly, so the gateway still records a mid-stream failure.
      setTimeout(() => {
        res.writeHead(200, { "content-type": "text/event-stream" });
        res.write("data: one\n\n");
        if (typeof res.flush === "function") res.flush();
        res.socket?.destroy();
      }, 5 * SSE_OPEN_MS);
    });
    await new Promise((resolve) => upstream.listen(0, "127.0.0.1", resolve));
    try {
      const base = `http://127.0.0.1:${upstream.address().port}`;
      const res = await mod.handle(sseAcceptRequest(base), earlyOpenEnv);
      assert.equal(res.status, 200);
      let failed = null;
      try {
        await res.text();
      } catch (err) {
        failed = err;
      }
      assert.ok(failed !== null, "the opened body ended cleanly after the origin broke mid-stream");
    } finally {
      upstream.closeAllConnections?.();
      await new Promise((resolve) => upstream.close(resolve));
    }
  });
}
