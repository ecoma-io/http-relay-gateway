// Vercel edge-function entry. The deployer uploads the assembled file as
// api/relay.js with RELAY_VERSION and RELAY_AUTH_TOKEN injected as
// deployment env, which the edge runtime exposes on process.env.
// `handle` is the test surface: the conformance suite calls it with a plain
// object env, no platform involved.
export const config = { runtime: "edge" };

export default function vercelRelay(request) {
  return handle(request, {
    RELAY_VERSION: process.env.RELAY_VERSION,
    RELAY_AUTH_TOKEN: process.env.RELAY_AUTH_TOKEN,
  });
}

export function handle(request, env) {
  return relayFetch(request, env);
}
