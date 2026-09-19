<script setup lang="ts">
import { reactive, ref } from "vue";
import { useRouter } from "vue-router";
import { ApiError, api } from "../api";
import { auth } from "../auth";

const router = useRouter();

const form = reactive({ password: "", confirm: "" });
const busy = ref(false);
const error = ref("");

async function submit(): Promise<void> {
  busy.value = true;
  error.value = "";
  try {
    await api.setup(form.password, form.confirm);
    auth.onLogin();
    await router.push({ name: "dashboard" });
  } catch (err) {
    error.value = err instanceof ApiError ? err.message : "Setup failed";
  } finally {
    busy.value = false;
  }
}
</script>

<template>
  <form class="card" @submit.prevent="submit">
    <h1>Welcome</h1>
    <p class="sub">Choose the admin password for this gateway. This runs once.</p>
    <p v-if="error" class="error">{{ error }}</p>
    <label class="field">
      <span>Password (min 8 characters)</span>
      <input
        v-model="form.password"
        type="password"
        required
        minlength="8"
        autocomplete="new-password"
      />
    </label>
    <label class="field">
      <span>Confirm password</span>
      <input v-model="form.confirm" type="password" required autocomplete="new-password" />
    </label>
    <button class="btn primary" type="submit" :disabled="busy">
      {{ busy ? "Setting up…" : "Set up" }}
    </button>
  </form>
</template>
