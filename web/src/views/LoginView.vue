<script setup lang="ts">
import { reactive, ref } from "vue";
import { useRouter } from "vue-router";
import { ApiError, api } from "../api";
import { auth } from "../auth";

const router = useRouter();

const form = reactive({ password: "" });
const busy = ref(false);
const error = ref("");

async function submit(): Promise<void> {
  busy.value = true;
  error.value = "";
  try {
    await api.login(form.password);
    auth.onLogin();
    await router.push({ name: "dashboard" });
  } catch (err) {
    if (err instanceof ApiError && err.status === 429) {
      error.value = "Too many failed attempts — wait a few minutes and try again.";
    } else {
      error.value = err instanceof ApiError ? "Wrong password." : "Login failed";
    }
  } finally {
    busy.value = false;
  }
}
</script>

<template>
  <form class="card" @submit.prevent="submit">
    <h1>Sign in</h1>
    <p class="sub">Enter the admin password.</p>
    <p v-if="error" class="error">{{ error }}</p>
    <label class="field">
      <span>Password</span>
      <input v-model="form.password" type="password" required autocomplete="current-password" />
    </label>
    <button class="btn primary" type="submit" :disabled="busy">
      {{ busy ? "Signing in…" : "Sign in" }}
    </button>
  </form>
</template>
