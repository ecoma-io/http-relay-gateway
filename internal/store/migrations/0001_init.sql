-- Initial schema. Shipped migration files are append-only: a file that has
-- been released is never edited — a schema change gets the next number.
-- Timestamps are unix seconds written by strftime('%s','now').

CREATE TABLE schema_version (
    version    INTEGER PRIMARY KEY,
    applied_at INTEGER NOT NULL
);

CREATE TABLE settings (
    key        TEXT PRIMARY KEY,
    value      TEXT    NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE providers (
    name          TEXT PRIMARY KEY,
    max_body      INTEGER NOT NULL,
    -- Raw JSON {"strip":[...],"set":{...}} or NULL for verbatim forwarding.
    header_policy TEXT,
    updated_at    INTEGER NOT NULL
);

CREATE TABLE platform_accounts (
    id          INTEGER PRIMARY KEY,
    name        TEXT    NOT NULL UNIQUE,
    platform    TEXT    NOT NULL CHECK (platform IN ('vercel', 'cloudflare', 'deno')),
    -- Plaintext v1 by design; every read and write must go through
    -- store.Tokens so an at-rest encryption later touches one file only.
    token       TEXT    NOT NULL,
    account_ref TEXT    NOT NULL DEFAULT '',
    verified_at INTEGER NOT NULL DEFAULT 0,
    created_at  INTEGER NOT NULL,
    updated_at  INTEGER NOT NULL
);

CREATE TABLE relays (
    id            INTEGER PRIMARY KEY,
    name          TEXT    NOT NULL UNIQUE,
    provider      TEXT    NOT NULL,
    url           TEXT    NOT NULL,
    active        INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0, 1)),
    origin        TEXT    NOT NULL DEFAULT 'legacy' CHECK (origin IN ('legacy', 'managed')),
    account_id    INTEGER REFERENCES platform_accounts (id) ON DELETE RESTRICT,
    header_policy TEXT,
    created_at    INTEGER NOT NULL,
    updated_at    INTEGER NOT NULL
);

CREATE INDEX idx_relays_provider ON relays (provider);

-- Exactly one deployment row per relay: a redeploy overwrites it. History is
-- the structured log's job; a deployment_events table can be added later as
-- an append-only migration if it ever earns its keep.
CREATE TABLE deployments (
    id               INTEGER PRIMARY KEY,
    relay_id         INTEGER NOT NULL UNIQUE REFERENCES relays (id) ON DELETE CASCADE,
    account_id       INTEGER NOT NULL REFERENCES platform_accounts (id) ON DELETE RESTRICT,
    platform         TEXT    NOT NULL CHECK (platform IN ('vercel', 'cloudflare', 'deno')),
    project          TEXT    NOT NULL DEFAULT '',
    external_id      TEXT    NOT NULL DEFAULT '',
    url              TEXT    NOT NULL,
    version          TEXT    NOT NULL DEFAULT '',
    auth_token       TEXT    NOT NULL,
    status           TEXT    NOT NULL DEFAULT 'pending' CHECK (status IN
        ('pending', 'deploying', 'active', 'stale', 'unreachable', 'error')),
    last_error       TEXT    NOT NULL DEFAULT '',
    last_checked_at  INTEGER NOT NULL DEFAULT 0,
    deployed_at      INTEGER NOT NULL DEFAULT 0,
    created_at       INTEGER NOT NULL,
    updated_at       INTEGER NOT NULL
);

CREATE INDEX idx_deployments_account ON deployments (account_id);

-- The migration runner owns this table's rows: it inserts the version entry
-- in the same transaction as the DDL above.
