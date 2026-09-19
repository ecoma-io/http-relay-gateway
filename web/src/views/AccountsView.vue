<script setup lang="ts">
import { computed, onMounted, reactive, ref } from "vue";
import { ApiError, api, type Account } from "../api";

const platforms = ["vercel", "cloudflare", "deno"] as const;

const accounts = ref<Account[]>([]);
const error = ref("");
const notice = ref("");
const busy = ref(false);

const modalOpen = ref(false);
const modalError = ref("");
const modal = reactive<{
  name: string;
  platform: string;
  token: string;
  accountRef: string;
}>({ name: "", platform: "vercel", token: "", accountRef: "" });

const sorted = computed(() => [...accounts.value].sort((a, b) => a.id - b.id));

async function load(): Promise<void> {
  try {
    accounts.value = await api.listAccounts();
    error.value = "";
  } catch (err) {
    error.value = err instanceof ApiError ? err.message : "Could not load accounts.";
  }
}

function openCreate(): void {
  Object.assign(modal, { name: "", platform: "vercel", token: "", accountRef: "" });
  modalError.value = "";
  modalOpen.value = true;
}

async function submit(): Promise<void> {
  modalError.value = "";
  busy.value = true;
  try {
    await api.createAccount({
      name: modal.name,
      platform: modal.platform,
      token: modal.token,
      accountRef: modal.accountRef === "" ? undefined : modal.accountRef,
    });
    modalOpen.value = false;
    await load();
  } catch (err) {
    modalError.value = err instanceof ApiError ? err.message : "The request failed.";
  } finally {
    busy.value = false;
  }
}

async function verify(account: Account): Promise<void> {
  busy.value = true;
  notice.value = "";
  try {
    await api.verifyAccount(account.id);
    notice.value = `"${account.name}" verified against ${account.platform}.`;
    await load();
  } catch (err) {
    error.value = err instanceof ApiError ? err.message : "The request failed.";
  } finally {
    busy.value = false;
  }
}

async function remove(account: Account, force: boolean): Promise<void> {
  const warning = force
    ? `Force-delete "${account.name}"? Managed relays on it lose their deployments and serve their own URLs.`
    : `Delete account "${account.name}"?`;
  if (!window.confirm(warning)) {
    return;
  }
  busy.value = true;
  error.value = "";
  try {
    await api.deleteAccount(account.id, force);
    await load();
  } catch (err) {
    if (err instanceof ApiError && err.status === 409 && !force) {
      error.value =
        `"${account.name}" is still referenced by managed relays. ` +
        "Retry with force to detach them (the confirm explains the consequences).";
    } else {
      error.value = err instanceof ApiError ? err.message : "The request failed.";
    }
  } finally {
    busy.value = false;
  }
}

onMounted(() => void load());
</script>

<template>
  <div class="page-head">
    <h1>Accounts</h1>
    <button type="button" class="btn primary" @click="openCreate">New account</button>
  </div>
  <p v-if="error" class="error">{{ error }}</p>
  <p v-if="notice" class="muted">{{ notice }}</p>
  <section class="panel">
    <table>
      <thead>
        <tr>
          <th>Name</th>
          <th>Platform</th>
          <th>Account</th>
          <th>Token</th>
          <th>Verified</th>
          <th></th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="account in sorted" :key="account.id">
          <td>{{ account.name }}</td>
          <td>{{ account.platform }}</td>
          <td class="mono muted">{{ account.accountRef || "—" }}</td>
          <td class="mono muted">••••{{ account.tokenLast4 }}</td>
          <td>
            <span v-if="account.verifiedAt" class="badge ok">verified</span>
            <span v-else class="badge off">unverified</span>
          </td>
          <td>
            <div class="row-actions">
              <button type="button" class="btn small" :disabled="busy" @click="verify(account)">
                Verify
              </button>
              <button
                type="button"
                class="btn small danger"
                :disabled="busy"
                @click="remove(account, false)"
              >
                Delete
              </button>
            </div>
          </td>
        </tr>
        <tr v-if="accounts.length === 0">
          <td colspan="6" class="muted">
            No platform accounts yet — managed relays need one to deploy.
          </td>
        </tr>
      </tbody>
    </table>
  </section>

  <div v-if="modalOpen" class="modal-backdrop" @click.self="modalOpen = false">
    <form class="modal" @submit.prevent="submit">
      <h2>New platform account</h2>
      <p class="muted">
        The token is checked against the platform before it is stored and is never shown again.
      </p>
      <p v-if="modalError" class="error">{{ modalError }}</p>
      <label class="field">
        <span>Name (unique)</span>
        <input v-model="modal.name" type="text" required />
      </label>
      <label class="field">
        <span>Platform</span>
        <select v-model="modal.platform">
          <option v-for="p in platforms" :key="p" :value="p">{{ p }}</option>
        </select>
      </label>
      <label class="field">
        <span>API token</span>
        <input v-model="modal.token" type="password" required autocomplete="off" />
      </label>
      <label class="field">
        <span>Account reference — optional (account id / team slug)</span>
        <input v-model="modal.accountRef" type="text" spellcheck="false" class="mono" />
      </label>
      <div class="modal-actions">
        <button type="button" class="btn" @click="modalOpen = false">Cancel</button>
        <button type="submit" class="btn primary" :disabled="busy">Create</button>
      </div>
    </form>
  </div>
</template>
