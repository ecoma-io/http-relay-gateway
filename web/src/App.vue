<script setup lang="ts">
import { computed } from "vue";
import { useRoute, useRouter } from "vue-router";
import { api } from "./api";
import { auth, session } from "./auth";

const route = useRoute();
const router = useRouter();

// The chrome (nav + logout) only exists on authenticated pages; setup and
// login render bare.
const chrome = computed(() => session.ready && !session.setupRequired && session.authed);

const navItems = [
  { name: "dashboard", label: "Dashboard" },
  { name: "relays", label: "Relays" },
  { name: "settings", label: "Settings" },
] as const;

async function logout(): Promise<void> {
  await api.logout();
  auth.onLogout();
  await router.push({ name: "login" });
}
</script>

<template>
  <div v-if="chrome" class="shell">
    <header class="topbar">
      <span class="brand">relay-gateway</span>
      <nav class="nav">
        <RouterLink
          v-for="item in navItems"
          :key="item.name"
          :to="{ name: item.name }"
          class="nav-link"
          :class="{ active: route.name === item.name }"
          >{{ item.label }}</RouterLink
        >
      </nav>
      <div class="topbar-right">
        <span v-if="session.version" class="version" :title="'gateway version'"
          >v{{ session.version }}</span
        >
        <button type="button" class="btn ghost" @click="logout">Log out</button>
      </div>
    </header>
    <main class="content">
      <RouterView />
    </main>
  </div>
  <main v-else class="bare">
    <RouterView />
  </main>
</template>
