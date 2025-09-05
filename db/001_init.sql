-- Nutzer (Spotify)
CREATE TABLE IF NOT EXISTS users (
  id SERIAL PRIMARY KEY,
  spotify_id TEXT UNIQUE NOT NULL,
  display_name TEXT,
  refresh_token_enc TEXT NOT NULL,
  access_token TEXT,
  access_expires_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

-- Aktive Sharing-Sessions (Sender)
CREATE TABLE IF NOT EXISTS sessions (
  id UUID PRIMARY KEY,
  sender_spotify_id TEXT NOT NULL,
  is_active BOOLEAN DEFAULT TRUE,
  created_at TIMESTAMPTZ DEFAULT now(),
  last_snapshot_at TIMESTAMPTZ
);

-- Letzter Snapshot (leichtgewichtiger Verlauf)
CREATE TABLE IF NOT EXISTS snapshots (
  id BIGSERIAL PRIMARY KEY,
  session_id UUID REFERENCES sessions(id) ON DELETE CASCADE,
  track_id TEXT,
  progress_ms INT,
  is_playing BOOLEAN,
  title TEXT,
  artists TEXT,
  image TEXT,
  ts TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_snapshots_session_ts ON snapshots(session_id, ts DESC);

-- Push-Geräte (Empfänger)
CREATE TABLE IF NOT EXISTS devices (
  id BIGSERIAL PRIMARY KEY,
  user_spotify_id TEXT NOT NULL,
  push_token TEXT NOT NULL,
  platform TEXT,
  updated_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE(user_spotify_id, push_token)
);

-- Subscription: wer folgt welcher Session
CREATE TABLE IF NOT EXISTS subscriptions (
  id BIGSERIAL PRIMARY KEY,
  session_id UUID REFERENCES sessions(id) ON DELETE CASCADE,
  user_spotify_id TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE(session_id, user_spotify_id)
);
