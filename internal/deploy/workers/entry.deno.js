// Deno Deploy entry. envVars surface through Deno.env; the serve call is
// guarded so the conformance suite can import this file under plain Node.
// `handle` is the test surface, like every entry.
export function handle(request, env) {
  return relayFetch(request, env);
}

function denoEnv() {
  return {
    RELAY_VERSION: Deno.env.get("RELAY_VERSION"),
    RELAY_AUTH_TOKEN: Deno.env.get("RELAY_AUTH_TOKEN"),
  };
}

if (typeof Deno !== "undefined") {
  Deno.serve((request) => handle(request, denoEnv()));
}
