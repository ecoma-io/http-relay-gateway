// Cloudflare module-worker entry. The deployer uploads the assembled file as
// the script's main module with RELAY_VERSION and RELAY_AUTH_TOKEN riding as
// plain_text bindings, which the runtime hands the fetch handler as string
// properties of env. `handle` is the test surface, like every entry.
export default {
  fetch(request, env) {
    return handle(request, env);
  },
};

export function handle(request, env) {
  return relayFetch(request, env);
}
