// Typed client for the admin API (/api/v1). Cookies are same-origin, so
// every request just needs credentials: "same-origin" — the default — and
// CSRF is covered by SameSite=Strict on the session cookie.

export class ApiError extends Error {
  readonly status: number;
  readonly field: string | null;

  constructor(status: number, message: string, field: string | null) {
    super(message);
    this.status = status;
    this.field = field;
  }
}

async function request<T>(method: string, path: string, body?: unknown): Promise<T> {
  const res = await fetch(`/api/v1${path}`, {
    method,
    headers: body === undefined ? undefined : { "Content-Type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const raw = await res.text();
  let parsed: unknown = null;
  if (raw !== "") {
    try {
      parsed = JSON.parse(raw);
    } catch {
      // A non-JSON body from a proxy or error page; fall through to the
      // status-based error below with the raw text as the message.
    }
  }
  if (!res.ok) {
    const record = (parsed ?? {}) as Record<string, unknown>;
    const message = typeof record["error"] === "string" ? record["error"] : raw || res.statusText;
    const field = typeof record["field"] === "string" ? record["field"] : null;
    throw new ApiError(res.status, message, field);
  }
  return parsed as T;
}

export interface Status {
  version: string;
  setupRequired: boolean;
  setupAt: number | null;
  hasSetupAt: boolean;
}

export interface Deployment {
  platform: string;
  project: string;
  url: string;
  version: string;
  status: string;
  tokenLast4: string;
  lastError: string;
  deployedAt: number;
}

export interface Relay {
  id: number;
  name: string;
  provider: string;
  url: string;
  active: boolean;
  origin: string;
  accountId: number | null;
  headerPolicy: string | null;
  deployment: Deployment | null;
  createdAt: number;
  updatedAt: number;
}

export interface RelayInput {
  name: string;
  provider: string;
  url: string;
  active: boolean;
  headerPolicy: string | null;
  accountId?: number;
}

export interface Account {
  id: number;
  name: string;
  platform: string;
  accountRef: string;
  verifiedAt: number;
  tokenLast4: string;
  createdAt: number;
  updatedAt: number;
}

export interface AccountInput {
  name: string;
  platform: string;
  token: string;
  accountRef?: string;
}

export interface FleetVersion {
  workerVersion: string;
  deployments: Record<string, number>;
}

export interface ImportItem {
  name: string;
  provider: string;
  url: string;
  active?: boolean;
}

export interface ImportReply {
  imported: number;
  rejected: { name: string; error: string }[];
}

export interface Provider {
  name: string;
  maxBody: number;
  headerPolicy: string | null;
}

export interface Settings {
  logLevel: string;
  maxRetries: number;
  failureThreshold: number;
  cooldownMs: number;
  streamThresholdBytes: number;
  dialTimeoutMs: number;
  responseHeaderTimeoutMs: number;
  reconcileIntervalSeconds: number;
}

export type SettingsPatch = Partial<Settings>;

export const api = {
  getStatus: () => request<Status>("GET", "/status"),

  setup: (password: string, confirm: string) =>
    request<Record<string, never>>("POST", "/setup", { password, confirm }),

  login: (password: string) => request<Record<string, never>>("POST", "/login", { password }),

  logout: () => request<Record<string, never>>("POST", "/logout"),

  listRelays: () => request<Relay[]>("GET", "/relays"),

  createRelay: (input: RelayInput) => request<Relay>("POST", "/relays", input),

  importRelays: (items: ImportItem[]) => request<ImportReply>("POST", "/relays/import", { items }),

  patchRelay: (id: number, patch: Partial<RelayInput>) =>
    request<Relay>("PATCH", `/relays/${id}`, patch),

  deleteRelay: (id: number, deleteRemote = false) =>
    request<Record<string, never>>(
      "DELETE",
      `/relays/${id}${deleteRemote ? "?deleteRemote=true" : ""}`,
    ),

  redeployRelay: (id: number) => request<{ queued: string }>("POST", `/relays/${id}/redeploy`),

  adoptRelay: (id: number, accountId: number) =>
    request<{ queued: string }>("POST", `/relays/${id}/adopt`, { accountId }),

  listAccounts: () => request<Account[]>("GET", "/accounts"),

  createAccount: (input: AccountInput) => request<Account>("POST", "/accounts", input),

  patchAccount: (id: number, patch: Partial<Omit<AccountInput, "platform">>) =>
    request<Account>("PATCH", `/accounts/${id}`, patch),

  verifyAccount: (id: number) => request<Account>("POST", `/accounts/${id}/verify`),

  deleteAccount: (id: number, force = false) =>
    request<Record<string, never>>("DELETE", `/accounts/${id}${force ? "?force=true" : ""}`),

  fleetVersion: () => request<FleetVersion>("GET", "/fleet/version"),

  fleetCheck: () => request<{ queued: string }>("POST", "/fleet/check"),

  fleetReconcile: () => request<{ queued: string }>("POST", "/fleet/reconcile"),

  listProviders: () => request<Provider[]>("GET", "/providers"),

  putProviders: (providers: Provider[]) => request<Provider[]>("PUT", "/providers", providers),

  getSettings: () => request<Settings>("GET", "/settings"),

  patchSettings: (patch: SettingsPatch) => request<Settings>("PATCH", "/settings", patch),
};
