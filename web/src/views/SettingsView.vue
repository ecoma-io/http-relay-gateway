<script setup lang="ts">
import { onMounted, reactive, ref } from "vue";
import { ApiError, api, type Settings, type SettingsPatch } from "../api";

const logLevels = ["debug", "info", "warn", "error"] as const;

// Every knob the settings API accepts, in display order. Numbers are edited
// as strings so a half-typed value never becomes a silent 0; each field owns
// its read/apply pair because a union-keyed record cannot be assigned
// through generically.
interface Field {
  label: string;
  hint: string;
  kind: "number" | "select";
  options?: readonly string[];
  read(settings: Settings): string;
  apply(patch: SettingsPatch, raw: string): void;
}

const fields: Field[] = [
  {
    label: "Log level",
    hint: "Applied to the running process immediately.",
    kind: "select",
    options: logLevels,
    read: (s) => s.logLevel,
    apply: (p, raw) => {
      p.logLevel = raw as Settings["logLevel"];
    },
  },
  {
    label: "Max retries",
    hint: "Failover attempts per request.",
    kind: "number",
    read: (s) => String(s.maxRetries),
    apply: (p, raw) => {
      p.maxRetries = Number(raw);
    },
  },
  {
    label: "Failure threshold",
    hint: "Consecutive failures before cooldown.",
    kind: "number",
    read: (s) => String(s.failureThreshold),
    apply: (p, raw) => {
      p.failureThreshold = Number(raw);
    },
  },
  {
    label: "Cooldown (ms)",
    hint: "How long a relay sits out after tripping.",
    kind: "number",
    read: (s) => String(s.cooldownMs),
    apply: (p, raw) => {
      p.cooldownMs = Number(raw);
    },
  },
  {
    label: "Streaming threshold (bytes)",
    hint: "Bodies above this stream through without failover. 0 buffers everything.",
    kind: "number",
    read: (s) => String(s.streamThresholdBytes),
    apply: (p, raw) => {
      p.streamThresholdBytes = Number(raw);
    },
  },
  {
    label: "Dial timeout (ms)",
    hint: "TCP connect budget per relay leg.",
    kind: "number",
    read: (s) => String(s.dialTimeoutMs),
    apply: (p, raw) => {
      p.dialTimeoutMs = Number(raw);
    },
  },
  {
    label: "Response header timeout (ms)",
    hint: "0 disables. Applies while waiting for relay response headers.",
    kind: "number",
    read: (s) => String(s.responseHeaderTimeoutMs),
    apply: (p, raw) => {
      p.responseHeaderTimeoutMs = Number(raw);
    },
  },
  {
    label: "Reconcile interval (s)",
    hint: "Reserved for fleet reconciliation. 0 disables.",
    kind: "number",
    read: (s) => String(s.reconcileIntervalSeconds),
    apply: (p, raw) => {
      p.reconcileIntervalSeconds = Number(raw);
    },
  },
];

const values = reactive<Record<string, string>>({});
const error = ref("");
const saved = ref(false);
const busy = ref(false);

async function load(): Promise<void> {
  try {
    const settings = await api.getSettings();
    for (const field of fields) {
      values[field.label] = field.read(settings);
    }
    error.value = "";
  } catch (err) {
    error.value = err instanceof ApiError ? err.message : "Could not load settings.";
  }
}

async function save(): Promise<void> {
  busy.value = true;
  error.value = "";
  saved.value = false;
  const patch: SettingsPatch = {};
  for (const field of fields) {
    const raw = values[field.label] ?? "";
    if (field.kind === "number") {
      const parsed = Number(raw);
      if (!Number.isInteger(parsed) || parsed < 0) {
        error.value = `${field.label} must be a whole number ≥ 0.`;
        busy.value = false;
        return;
      }
    }
    field.apply(patch, raw);
  }
  try {
    await api.patchSettings(patch);
    saved.value = true;
    await load();
  } catch (err) {
    error.value = err instanceof ApiError ? err.message : "Could not save settings.";
  } finally {
    busy.value = false;
  }
}

onMounted(() => void load());
</script>

<template>
  <div class="page-head">
    <h1>Settings</h1>
    <button type="button" class="btn primary" :disabled="busy" @click="save">Save</button>
  </div>
  <p v-if="error" class="error">{{ error }}</p>
  <p v-else-if="saved" class="muted">Saved — changes apply without a restart.</p>
  <section class="panel" style="padding: 20px">
    <div class="settings-grid">
      <label v-for="field in fields" :key="field.label" class="field">
        <span>{{ field.label }}</span>
        <select v-if="field.kind === 'select'" v-model="values[field.label]">
          <option v-for="option in field.options" :key="option" :value="option">
            {{ option }}
          </option>
        </select>
        <input v-else v-model="values[field.label]" type="number" min="0" step="1" />
        <span class="muted" style="font-weight: 400">{{ field.hint }}</span>
      </label>
    </div>
  </section>
</template>
