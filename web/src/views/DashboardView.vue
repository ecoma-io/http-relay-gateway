<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from "vue";
import { api, type FleetVersion, type Relay } from "../api";
import { session } from "../auth";

const relays = ref<Relay[]>([]);
const fleet = ref<FleetVersion | null>(null);
const error = ref("");
const notice = ref("");
const acting = ref(false);

const active = computed(() => relays.value.filter((r) => r.active).length);
const managed = computed(() => relays.value.filter((r) => r.origin === "managed").length);
const providers = computed(() => new Set(relays.value.map((r) => r.provider)).size);
const drifted = computed(
  () =>
    relays.value.filter((r) => r.deployment !== null && r.deployment.status !== "active").length,
);

let timer: number | undefined;

async function poll(): Promise<void> {
  try {
    relays.value = await api.listRelays();
    error.value = "";
  } catch {
    error.value = "Could not reach the admin API.";
    return;
  }
  // Fleet info is supplementary — an unreachable reconciler must not turn
  // the whole board into an error.
  try {
    fleet.value = await api.fleetVersion();
  } catch {
    fleet.value = null;
  }
}

async function fleetAction(kind: "check" | "reconcile"): Promise<void> {
  acting.value = true;
  notice.value = "";
  try {
    if (kind === "check") {
      await api.fleetCheck();
      notice.value = "Fleet check queued — deployment statuses refresh as probes land.";
    } else {
      await api.fleetReconcile();
      notice.value = "Fleet reconcile queued — stale deployments redeploy in the background.";
    }
  } catch {
    error.value = "Could not reach the admin API.";
  } finally {
    acting.value = false;
  }
}

onMounted(() => {
  void poll();
  timer = window.setInterval(() => void poll(), 5000);
});

onUnmounted(() => {
  window.clearInterval(timer);
});
</script>

<template>
  <div class="page-head">
    <h1>Dashboard</h1>
    <div>
      <button type="button" class="btn" :disabled="acting" @click="fleetAction('check')">
        Check fleet
      </button>
      <button
        type="button"
        class="btn primary"
        :disabled="acting"
        @click="fleetAction('reconcile')"
      >
        Reconcile fleet
      </button>
    </div>
  </div>
  <p v-if="error" class="error">{{ error }}</p>
  <p v-if="notice" class="muted">{{ notice }}</p>
  <section class="panel stat-row" style="margin-bottom: 16px">
    <div class="stat">
      <div class="value">{{ session.version || "—" }}</div>
      <div class="label">gateway version</div>
    </div>
    <div class="stat">
      <div class="value">{{ fleet?.workerVersion ?? "—" }}</div>
      <div class="label">worker version</div>
    </div>
    <div class="stat">
      <div class="value">{{ relays.length }}</div>
      <div class="label">relays</div>
    </div>
    <div class="stat">
      <div class="value">{{ active }}</div>
      <div class="label">active</div>
    </div>
    <div class="stat">
      <div class="value">{{ managed }}</div>
      <div class="label">managed</div>
    </div>
    <div class="stat">
      <div class="value">{{ drifted }}</div>
      <div class="label">drifted deployments</div>
    </div>
    <div class="stat">
      <div class="value">{{ providers }}</div>
      <div class="label">providers</div>
    </div>
  </section>
  <section class="panel">
    <table>
      <thead>
        <tr>
          <th>Relay</th>
          <th>Provider</th>
          <th>Origin</th>
          <th>State</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="relay in relays" :key="relay.id">
          <td>{{ relay.name }}</td>
          <td>{{ relay.provider }}</td>
          <td class="muted">{{ relay.origin }}</td>
          <td>
            <span class="badge" :class="relay.active ? 'ok' : 'off'">
              {{ relay.active ? "active" : "inactive" }}
            </span>
          </td>
        </tr>
        <tr v-if="relays.length === 0">
          <td colspan="4" class="muted">No relays yet — create the first one under Relays.</td>
        </tr>
      </tbody>
    </table>
  </section>
</template>
