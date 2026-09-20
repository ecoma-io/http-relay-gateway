-- v2: deployments gains the 'paused' status — the platform answered a probe
-- but no relay worker is behind the URL (a free-tier suspension above all:
-- Vercel answers 402 + x-vercel-error DEPLOYMENT_DISABLED on every path).
-- A paused relay must leave the pool and wait for revival; it is never
-- redeployed, because a redeploy cannot lift a platform-side suspension.
-- SQLite cannot alter a CHECK constraint: rebuild the table, copy, drop.

CREATE TABLE deployments_v2 (
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
        ('pending', 'deploying', 'active', 'stale', 'unreachable', 'error', 'paused')),
    last_error       TEXT    NOT NULL DEFAULT '',
    last_checked_at  INTEGER NOT NULL DEFAULT 0,
    deployed_at      INTEGER NOT NULL DEFAULT 0,
    created_at       INTEGER NOT NULL,
    updated_at       INTEGER NOT NULL
);

INSERT INTO deployments_v2
    SELECT id, relay_id, account_id, platform, project, external_id, url,
           version, auth_token, status, last_error, last_checked_at,
           deployed_at, created_at, updated_at
    FROM deployments;

DROP TABLE deployments;
ALTER TABLE deployments_v2 RENAME TO deployments;
CREATE INDEX idx_deployments_account ON deployments (account_id);
