-- Immer ins richtige Schema
CREATE SCHEMA IF NOT EXISTS public;
SET search_path TO public;

-- USERS: Spotify-User (PK = Spotify-ID als TEXT)
CREATE TABLE IF NOT EXISTS users (
  id                TEXT PRIMARY KEY,                 -- Spotify-ID!
  display_name      TEXT,
  refresh_token_enc TEXT,
  access_token      TEXT,
  access_expires_at TIMESTAMPTZ,
  email             TEXT,
  country           TEXT,
  product           TEXT,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- SESSIONS: aktive Sharing-Sessions (FK auf users)
CREATE TABLE IF NOT EXISTS sessions (
  id                UUID PRIMARY KEY,
  sender_spotify_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  is_active         BOOLEAN NOT NULL DEFAULT TRUE,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_snapshot_at  TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_sessions_active ON sessions (is_active);
CREATE INDEX IF NOT EXISTS idx_sessions_last   ON sessions (last_snapshot_at DESC);
CREATE INDEX IF NOT EXISTS idx_sessions_sender ON sessions (sender_spotify_id);

-- SNAPSHOTS: leichte Verlaufstabelle (FK auf sessions)
CREATE TABLE IF NOT EXISTS snapshots (
  id            BIGSERIAL PRIMARY KEY,
  session_id    UUID REFERENCES sessions(id) ON DELETE CASCADE,
  track_id      TEXT,
  progress_ms   INT,
  is_playing    BOOLEAN,
  title         TEXT,
  artists       TEXT,
  image         TEXT,
  ts            TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_snapshots_session_ts ON snapshots(session_id, ts DESC);

-- DEVICES: Push-Geräte (FK auf users)
CREATE TABLE IF NOT EXISTS devices (
  id              BIGSERIAL PRIMARY KEY,
  user_spotify_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  push_token      TEXT NOT NULL,
  platform        TEXT,
  updated_at      TIMESTAMPTZ DEFAULT now(),
  UNIQUE(user_spotify_id, push_token)
);

-- SUBSCRIPTIONS: wer folgt welcher Session (FKs auf sessions & users)
CREATE TABLE IF NOT EXISTS subscriptions (
  id              BIGSERIAL PRIMARY KEY,
  session_id      UUID REFERENCES sessions(id) ON DELETE CASCADE,
  user_spotify_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  created_at      TIMESTAMPTZ DEFAULT now(),
  UNIQUE(session_id, user_spotify_id)
);
CREATE INDEX IF NOT EXISTS idx_subs_session_user ON subscriptions(session_id, user_spotify_id);

COMMIT;
