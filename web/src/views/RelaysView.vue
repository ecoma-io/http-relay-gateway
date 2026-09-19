<script setup lang="ts">
import { computed, onMounted, reactive, ref } from "vue";
import { ApiError, api, type Relay, type RelayInput } from "../api";

interface RelayForm {
  name: string;
  provider: string;
  url: string;
  active: boolean;
  headerPolicy: string;
}

const relays = ref<Relay[]>([]);
const error = ref("");
const busy = ref(false);

const modalOpen = ref(false);
const editing = ref<Relay | null>(null);
const modalError = ref("");

const form = reactive<RelayForm>({
  name: "",
  provider: "",
  url: "",
  active: true,
  headerPolicy: "",
});

const urlShown = (relay: Relay): string => relay.deployment?.url ?? relay.url;

const sorted = computed(() => [...relays.value].sort((a, b) => a.id - b.id));

async function load(): Promise<void> {
  try {
    relays.value = await api.listRelays();
    error.value = "";
  } catch (err) {
    error.value = err instanceof ApiError ? err.message : "Could not load relays.";
  }
}

function openCreate(): void {
  editing.value = null;
  Object.assign(form, { name: "", provider: "", url: "", active: true, headerPolicy: "" });
  modalError.value = "";
  modalOpen.value = true;
}

function openEdit(relay: Relay): void {
  editing.value = relay;
  Object.assign(form, {
    name: relay.name,
    provider: relay.provider,
    url: urlShown(relay),
    active: relay.active,
    headerPolicy: relay.headerPolicy ?? "",
  });
  modalError.value = "";
  modalOpen.value = true;
}

async function submit(): Promise<void> {
  modalError.value = "";
  busy.value = true;
  const input: RelayInput = {
    name: form.name,
    provider: form.provider,
    url: form.url,
    active: form.active,
    headerPolicy: form.headerPolicy === "" ? null : form.headerPolicy,
  };
  try {
    if (editing.value === null) {
      await api.createRelay(input);
    } else {
      await api.patchRelay(editing.value.id, input);
    }
    modalOpen.value = false;
    await load();
  } catch (err) {
    modalError.value = err instanceof ApiError ? err.message : "The request failed.";
  } finally {
    busy.value = false;
  }
}

async function toggle(relay: Relay): Promise<void> {
  busy.value = true;
  try {
    await api.patchRelay(relay.id, { active: !relay.active });
    await load();
  } catch (err) {
    error.value = err instanceof ApiError ? err.message : "The request failed.";
  } finally {
    busy.value = false;
  }
}

async function remove(relay: Relay): Promise<void> {
  if (!window.confirm(`Delete relay "${relay.name}"? The pool reconfigures immediately.`)) {
    return;
  }
  busy.value = true;
  try {
    await api.deleteRelay(relay.id);
    await load();
  } catch (err) {
    error.value = err instanceof ApiError ? err.message : "The request failed.";
  } finally {
    busy.value = false;
  }
}

onMounted(() => void load());
</script>

<template>
  <div class="page-head">
    <h1>Relays</h1>
    <button type="button" class="btn primary" @click="openCreate">New relay</button>
  </div>
  <p v-if="error" class="error">{{ error }}</p>
  <section class="panel">
    <table>
      <thead>
        <tr>
          <th>Name</th>
          <th>Provider</th>
          <th>URL</th>
          <th>Origin</th>
          <th>State</th>
          <th></th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="relay in sorted" :key="relay.id">
          <td>{{ relay.name }}</td>
          <td>{{ relay.provider }}</td>
          <td class="mono">{{ urlShown(relay) }}</td>
          <td class="muted">
            {{ relay.origin }}
            <span
              v-if="relay.origin === 'legacy'"
              class="badge off"
              title="This relay has a public URL and no authentication token."
              >unprotected</span
            >
          </td>
          <td>
            <span class="badge" :class="relay.active ? 'ok' : 'off'">
              {{ relay.active ? "active" : "inactive" }}
            </span>
          </td>
          <td>
            <div class="row-actions">
              <button type="button" class="btn small" :disabled="busy" @click="openEdit(relay)">
                Edit
              </button>
              <button type="button" class="btn small" :disabled="busy" @click="toggle(relay)">
                {{ relay.active ? "Deactivate" : "Activate" }}
              </button>
              <button
                type="button"
                class="btn small danger"
                :disabled="busy"
                @click="remove(relay)"
              >
                Delete
              </button>
            </div>
          </td>
        </tr>
        <tr v-if="relays.length === 0">
          <td colspan="6" class="muted">
            No relays yet — the data plane serves 503 until one exists.
          </td>
        </tr>
      </tbody>
    </table>
  </section>

  <div v-if="modalOpen" class="modal-backdrop" @click.self="modalOpen = false">
    <form class="modal" @submit.prevent="submit">
      <h2>{{ editing === null ? "New relay" : `Edit ${editing.name}` }}</h2>
      <p v-if="modalError" class="error">{{ modalError }}</p>
      <label class="field">
        <span>Name (unique)</span>
        <input v-model="form.name" type="text" required />
      </label>
      <label class="field">
        <span>Provider label (vercel, cloudflare, deno, …)</span>
        <input v-model="form.provider" type="text" required />
      </label>
      <label class="field">
        <span>Relay URL</span>
        <input v-model="form.url" type="text" required placeholder="https://my-relay.vercel.app" />
      </label>
      <label class="field">
        <span>Header policy — optional JSON, e.g. {"strip":["x-internal-trace"]}</span>
        <textarea v-model="form.headerPolicy" spellcheck="false" class="mono"></textarea>
      </label>
      <label class="check">
        <input v-model="form.active" type="checkbox" />
        <span>Active (joins the rotation)</span>
      </label>
      <div class="modal-actions">
        <button type="button" class="btn" @click="modalOpen = false">Cancel</button>
        <button type="submit" class="btn primary" :disabled="busy">
          {{ editing === null ? "Create" : "Save" }}
        </button>
      </div>
    </form>
  </div>
</template>
