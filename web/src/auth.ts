import { reactive, readonly } from "vue";
import { api } from "./api";

// Session state for the router guards and the topbar. `ready` is false until
// the first status probe resolves, so the router never decides on unknown
// state.
const state = reactive({
  ready: false,
  setupRequired: false,
  authed: false,
  version: "",
});

async function refresh(): Promise<void> {
  const status = await api.getStatus();
  state.setupRequired = status.setupRequired;
  state.version = status.version;
  // Status is public and cannot reveal whether this browser holds a valid
  // session; a cheap authenticated call settles it.
  if (status.setupRequired) {
    state.authed = false;
  } else {
    try {
      await api.listRelays();
      state.authed = true;
    } catch (error) {
      if (
        error instanceof Error &&
        "status" in error &&
        (error as { status: number }).status === 401
      ) {
        state.authed = false;
      } else {
        throw error;
      }
    }
  }
  state.ready = true;
}

function onLogin(): void {
  state.authed = true;
  state.setupRequired = false;
}

function onLogout(): void {
  state.authed = false;
}

export const session = readonly(state);

export const auth = { refresh, onLogin, onLogout };
