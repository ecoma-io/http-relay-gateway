<script setup lang="ts">
import { computed, onMounted, onUnmounted, reactive, ref } from "vue";
import { ApiError, api, type Account, type Relay, type RelayInput } from "../api";

interface RelayForm {
  name: string;
  provider: string;
  url: string;
  active: boolean;
  headerPolicy: string;
  accountId: number | null;
}

const relays = ref<Relay[]>([]);
const accounts = ref<Account[]>([]);
const error = ref("");
const notice = ref("");
const busy = ref(false);

const modalOpen = ref(false);
const editing = ref<Relay | null>(null);
const modalError = ref("");

const adoptTarget = ref<Relay | null>(null);
const adoptAccountId = ref<number | null>(null);
const adoptError = ref("");

const deleteTarget = ref<Relay | null>(null);
const deleteRemote = ref(false);
const deleteError = ref("");

const form = reactive<RelayForm>({
  name: "",
  provider: "",
  url: "",
  active: true,
  headerPolicy: "",
  accountId: null,
});

const urlShown = (relay: Relay): string => relay.deployment?.url ?? relay.url;

const sorted = computed(() => [...relays.value].sort((a, b) => a.id - b.id));

// A managed relay rides on a platform account whose platform must equal the
// relay's provider label, so both pickers narrow to the matching accounts.
const matchingAccounts = computed(() =>
  accounts.value.filter((a) => a.platform === form.provider.trim().toLowerCase()),
);

const adoptCandidates = computed(() =>
  adoptTarget.value === null
    ? []
    : accounts.value.filter((a) => a.platform === adoptTarget.value?.provider),
);

async function load(): Promise<void> {
  try {
    const [relayList, accountList] = await Promise.all([api.listRelays(), api.listAccounts()]);
    relays.value = relayList;
    accounts.value = accountList;
    error.value = "";
  } catch (err) {
    error.value = err instanceof ApiError ? err.message : "Could not load relays.";
  }
}

function openCreate(): void {
  editing.value = null;
  Object.assign(form, {
    name: "",
    provider: "",
    url: "",
    active: true,
    headerPolicy: "",
    accountId: null,
  });
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
    accountId: relay.accountId,
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
      // An account only births a managed relay at creation; PATCH never
      // re-parents one (adoption and redeploy own the lifecycle after that).
      input.accountId = form.accountId ?? undefined;
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

function openAdopt(relay: Relay): void {
  adoptTarget.value = relay;
  adoptAccountId.value = null;
  adoptError.value = "";
}

async function submitAdopt(): Promise<void> {
  if (adoptTarget.value === null || adoptAccountId.value === null) {
    return;
  }
  adoptError.value = "";
  busy.value = true;
  try {
    await api.adoptRelay(adoptTarget.value.id, adoptAccountId.value);
    notice.value = `Adoption of "${adoptTarget.value.name}" queued — the worker deploys in the background.`;
    adoptTarget.value = null;
    await load();
  } catch (err) {
    adoptError.value = err instanceof ApiError ? err.message : "The request failed.";
  } finally {
    busy.value = false;
  }
}

async function redeploy(relay: Relay): Promise<void> {
  busy.value = true;
  error.value = "";
  try {
    await api.redeployRelay(relay.id);
    notice.value = `Redeploy of "${relay.name}" queued — status updates as it progresses.`;
    await load();
  } catch (err) {
    error.value = err instanceof ApiError ? err.message : "The request failed.";
  } finally {
    busy.value = false;
  }
}

function openDelete(relay: Relay): void {
  if (relay.deployment === null) {
    void removeLocal(relay);
    return;
  }
  deleteTarget.value = relay;
  deleteRemote.value = false;
  deleteError.value = "";
}

async function removeLocal(relay: Relay): Promise<void> {
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

async function confirmDelete(): Promise<void> {
  if (deleteTarget.value === null) {
    return;
  }
  deleteError.value = "";
  busy.value = true;
  try {
    await api.deleteRelay(deleteTarget.value.id, deleteRemote.value);
    deleteTarget.value = null;
    await load();
  } catch (err) {
    deleteError.value = err instanceof ApiError ? err.message : "The request failed.";
  } finally {
    busy.value = false;
  }
}

let pollTimer: number | undefined;

onMounted(() => {
  void load();
  // Deployment statuses move asynchronously (deploying → active), so the
  // table polls like the dashboard; paused while a modal is open so a form
  // is never re-rendered under the user.
  pollTimer = window.setInterval(() => {
    if (!busy.value && !modalOpen.value && !deleteTarget.value && !adoptTarget.value) {
      void load();
    }
  }, 5000);
});

onUnmounted(() => window.clearInterval(pollTimer));
</script>

<template>
  <div class="page-head">
    <h1>Relays</h1>
    <button type="button" class="btn primary" @click="openCreate">New relay</button>
  </div>
  <p v-if="error" class="error">{{ error }}</p>
  <p v-if="notice" class="muted">{{ notice }}</p>
  <section class="panel">
    <table>
      <thead>
        <tr>
          <th>Name</th>
          <th>Provider</th>
          <th>URL</th>
          <th>Origin</th>
          <th>Deployment</th>
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
            <template v-if="relay.deployment">
              <span
                class="badge"
                :class="relay.deployment.status === 'active' ? 'ok' : 'off'"
                :title="relay.deployment.lastError || relay.deployment.status"
                >{{ relay.deployment.status }}</span
              >
              <span class="mono muted" :title="'worker token (last 4)'"
                >••••{{ relay.deployment.tokenLast4 }}</span
              >
            </template>
            <span v-else class="muted">—</span>
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
                v-if="relay.origin === 'managed'"
                type="button"
                class="btn small"
                :disabled="busy"
                @click="redeploy(relay)"
              >
                {{ relay.deployment === null ? "Deploy" : "Redeploy" }}
              </button>
              <button
                v-if="relay.origin === 'legacy'"
                type="button"
                class="btn small"
                :disabled="busy || accounts.length === 0"
                title="Deploy the embedded worker through a platform account"
                @click="openAdopt(relay)"
              >
                Adopt
              </button>
              <button
                type="button"
                class="btn small danger"
                :disabled="busy"
                @click="openDelete(relay)"
              >
                Delete
              </button>
            </div>
          </td>
        </tr>
        <tr v-if="relays.length === 0">
          <td colspan="7" class="muted">
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
      <label v-if="editing === null" class="field">
        <span>
          Platform account — optional; picks a managed relay deployed by this gateway
          <template v-if="form.provider !== '' && matchingAccounts.length === 0">
            (no account for “{{ form.provider }}” yet)
          </template>
        </span>
        <select v-model="form.accountId">
          <option :value="null">None — external URL, stays unmanaged</option>
          <option v-for="account in matchingAccounts" :key="account.id" :value="account.id">
            {{ account.name }} ({{ account.platform }})
          </option>
        </select>
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

  <div v-if="adoptTarget !== null" class="modal-backdrop" @click.self="adoptTarget = null">
    <form class="modal" @submit.prevent="submitAdopt">
      <h2>Adopt {{ adoptTarget.name }}</h2>
      <p class="muted">
        Deploys the embedded worker through a platform account and puts this relay under gateway
        management. Until it succeeds the current URL keeps serving unprotected.
      </p>
      <p v-if="adoptError" class="error">{{ adoptError }}</p>
      <label v-if="adoptCandidates.length > 0" class="field">
        <span>Platform account ({{ adoptTarget.provider }})</span>
        <select v-model="adoptAccountId" required>
          <option v-for="account in adoptCandidates" :key="account.id" :value="account.id">
            {{ account.name }}
          </option>
        </select>
      </label>
      <p v-else class="muted">
        No {{ adoptTarget.provider }} account exists yet — create one under Accounts first.
      </p>
      <div class="modal-actions">
        <button type="button" class="btn" @click="adoptTarget = null">Cancel</button>
        <button
          v-if="adoptCandidates.length > 0"
          type="submit"
          class="btn primary"
          :disabled="busy || adoptAccountId === null"
        >
          Adopt
        </button>
      </div>
    </form>
  </div>

  <div v-if="deleteTarget !== null" class="modal-backdrop" @click.self="deleteTarget = null">
    <div class="modal">
      <h2>Delete {{ deleteTarget.name }}</h2>
      <p class="muted">
        The relay leaves the pool immediately. Deleting the platform worker too removes the public
        URL — anything still pointed at it starts failing.
      </p>
      <p v-if="deleteError" class="error">{{ deleteError }}</p>
      <label class="check">
        <input v-model="deleteRemote" type="checkbox" />
        <span>
          Also delete the worker from
          {{ deleteTarget.deployment?.platform ?? "the platform" }}
          ({{ deleteTarget.deployment?.url ?? "" }})
        </span>
      </label>
      <div class="modal-actions">
        <button type="button" class="btn" @click="deleteTarget = null">Cancel</button>
        <button type="button" class="btn danger" :disabled="busy" @click="confirmDelete">
          Delete
        </button>
      </div>
    </div>
  </div>
</template>
