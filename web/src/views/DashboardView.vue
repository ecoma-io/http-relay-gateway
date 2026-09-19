<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from "vue";
import { api, type Relay } from "../api";
import { session } from "../auth";

const relays = ref<Relay[]>([]);
const error = ref("");

const active = computed(() => relays.value.filter((r) => r.active).length);
const managed = computed(() => relays.value.filter((r) => r.origin === "managed").length);
const providers = computed(() => new Set(relays.value.map((r) => r.provider)).size);

let timer: number | undefined;

async function poll(): Promise<void> {
  try {
    relays.value = await api.listRelays();
    error.value = "";
  } catch {
    error.value = "Could not reach the admin API.";
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
  </div>
  <p v-if="error" class="error">{{ error }}</p>
  <section class="panel stat-row" style="margin-bottom: 16px">
    <div class="stat">
      <div class="value">{{ session.version || "—" }}</div>
      <div class="label">gateway version</div>
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
