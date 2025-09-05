// lib/db.js
const { Pool } = require("pg");

// Nutzt DATABASE_URL (z.B. postgres://user:pass@host:5432/dbname)
// oder die Einzelwerte, wenn gewünscht.
const pool = new Pool({
  connectionString: process.env.DATABASE_URL || undefined,
  // Optional in DEV:
  // ssl: process.env.NODE_ENV === "production" ? { rejectUnauthorized: false } : false,
});

/** Low-level Query helper mit auto client release. */
async function q(text, params) {
  const res = await pool.query(text, params);
  return res;
}

/** Ping: wirft, wenn die DB nicht erreichbar ist. */
async function ping() {
  await q("SELECT 1");
}

/* ======================= USERS ======================= */
/**
 * Upsert eines Users (Spotify /me).
 * Erwartete Tabellenspalten:
 *   users(id TEXT PRIMARY KEY, display_name TEXT, email TEXT, country TEXT, created_at TIMESTAMPTZ DEFAULT now())
 */
async function upsertUser({ id, display_name = null, email = null, country = null }) {
  const sql = `
    INSERT INTO users (id, display_name, email, country)
    VALUES ($1, $2, $3, $4)
    ON CONFLICT (id)
    DO UPDATE SET display_name = EXCLUDED.display_name,
                  email = EXCLUDED.email,
                  country = EXCLUDED.country;
  `;
  await q(sql, [id, display_name, email, country]);
}

/* ======================= SESSIONS ======================= */
/**
 * Neue Session anlegen.
 * Erwartete Tabellenspalten:
 *   sessions(
 *     session_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
 *     user_id TEXT REFERENCES users(id),
 *     token_hash TEXT UNIQUE,
 *     refresh_token TEXT,
 *     access_token TEXT,
 *     access_expires_at TIMESTAMPTZ,
 *     scopes TEXT[],
 *     created_at TIMESTAMPTZ DEFAULT now(),
 *     revoked_at TIMESTAMPTZ
 *   )
 */
async function createSession({
  user_id,
  token_hash,
  refresh_token,
  access_token,
  access_expires_at, // JS Date oder ISO
  scopes = [],
}) {
  const sql = `
    INSERT INTO sessions (user_id, token_hash, refresh_token, access_token, access_expires_at, scopes)
    VALUES ($1, $2, $3, $4, $5, $6)
    RETURNING session_id, user_id, created_at
  `;
  const res = await q(sql, [user_id, token_hash, refresh_token, access_token, access_expires_at, scopes]);
  return res.rows[0];
}

/** Session anhand Token-Hash holen (nur valide, nicht revoked, nicht abgelaufen). */
async function findActiveSessionByHash(token_hash) {
  const sql = `
    SELECT s.*, u.display_name, u.email
    FROM sessions s
    JOIN users u ON u.id = s.user_id
    WHERE s.token_hash = $1
      AND s.revoked_at IS NULL
      AND (s.access_expires_at IS NULL OR s.access_expires_at > now())
    LIMIT 1
  `;
  const res = await q(sql, [token_hash]);
  return res.rows[0] || null;
}

/** Session als revoked markieren. */
async function revokeSessionByHash(token_hash) {
  await q(`UPDATE sessions SET revoked_at = now() WHERE token_hash = $1`, [token_hash]);
}

/** Access-Token/Expiry aktualisieren (nach Refresh). */
async function updateSessionAccess(token_hash, { access_token, access_expires_at }) {
  await q(
    `UPDATE sessions SET access_token = $1, access_expires_at = $2 WHERE token_hash = $3`,
    [access_token, access_expires_at, token_hash]
  );
}

/* ======================= SNAPSHOTS (letzter Stand) ======================= */
/**
 * Letzten Playback-Snapshot des Senders speichern (JSONB).
 * Erwartete Spalten:
 *   snapshots(id BIGSERIAL PK, user_id TEXT, payload JSONB, created_at TIMESTAMPTZ DEFAULT now())
 */
async function saveSnapshot(user_id, payload) {
  await q(`INSERT INTO snapshots (user_id, payload) VALUES ($1, $2)`, [user_id, payload]);
}

/** Letzten Snapshot eines Users holen. */
async function getLatestSnapshot(user_id) {
  const res = await q(
    `SELECT payload, created_at FROM snapshots WHERE user_id = $1 ORDER BY created_at DESC LIMIT 1`,
    [user_id]
  );
  return res.rows[0] || null;
}

/* ======================= SUBSCRIPTIONS (Follower/Follows) ======================= */
/**
 * subscriptions(subscriber_id TEXT, target_id TEXT, created_at DEFAULT now(), PRIMARY KEY(subscriber_id, target_id))
 * – Wer folgt wem (z.B. Auto-Follow bei „Mitspielen“ optional).
 */
async function addSubscription(subscriber_id, target_id) {
  const sql = `
    INSERT INTO subscriptions (subscriber_id, target_id)
    VALUES ($1, $2)
    ON CONFLICT (subscriber_id, target_id) DO NOTHING
  `;
  await q(sql, [subscriber_id, target_id]);
}
async function removeSubscription(subscriber_id, target_id) {
  await q(`DELETE FROM subscriptions WHERE subscriber_id = $1 AND target_id = $2`, [subscriber_id, target_id]);
}
async function getFollowers(target_id) {
  const res = await q(
    `SELECT s.subscriber_id AS id, u.display_name
     FROM subscriptions s
     LEFT JOIN users u ON u.id = s.subscriber_id
     WHERE s.target_id = $1`,
    [target_id]
  );
  return res.rows;
}

/* ======================= DEVICES ======================= */
/**
 * devices(user_id TEXT, device_id TEXT, name TEXT, type TEXT, is_active BOOLEAN, last_seen TIMESTAMPTZ, created_at DEFAULT now(),
 *         PRIMARY KEY(user_id, device_id))
 */
async function upsertDevice({ user_id, device_id, name = null, type = null, is_active = false }) {
  const sql = `
    INSERT INTO devices (user_id, device_id, name, type, is_active, last_seen)
    VALUES ($1, $2, $3, $4, $5, now())
    ON CONFLICT (user_id, device_id)
    DO UPDATE SET name = EXCLUDED.name,
                  type = EXCLUDED.type,
                  is_active = EXCLUDED.is_active,
                  last_seen = now()
  `;
  await q(sql, [user_id, device_id, name, type, !!is_active]);
}
async function getLastActiveDevice(user_id) {
  const res = await q(
    `SELECT * FROM devices WHERE user_id = $1 ORDER BY is_active DESC, last_seen DESC LIMIT 1`,
    [user_id]
  );
  return res.rows[0] || null;
}

/* ======================= EXPORTS ======================= */
module.exports = {
  pool,
  q,
  ping,

  upsertUser,

  createSession,
  findActiveSessionByHash,
  revokeSessionByHash,
  updateSessionAccess,

  saveSnapshot,
  getLatestSnapshot,

  addSubscription,
  removeSubscription,
  getFollowers,

  upsertDevice,
  getLastActiveDevice,
};
