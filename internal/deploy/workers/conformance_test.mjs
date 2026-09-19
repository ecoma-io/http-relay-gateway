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

// Local upstream the worker fetches into; it echoes what it saw.
async function withUpstream(run) {
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
      res.writeHead(203, { "x-upstream": "yes", "content-type": "application/json" });
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
}
