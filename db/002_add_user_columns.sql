-- Immer ins richtige Schema
SET search_path TO public;

-- Guard: nur ausführen, wenn 'users' existiert
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema='public' AND table_name='users'
  ) THEN
    RAISE NOTICE 'Table public.users not found – skipping 002_add_user_columns.sql';
    RETURN;
  END IF;
END $$;

-- optionale Felder & Index; idempotent
ALTER TABLE users
  ADD COLUMN IF NOT EXISTS email        TEXT,
  ADD COLUMN IF NOT EXISTS country      TEXT,
  ADD COLUMN IF NOT EXISTS product      TEXT,
  ADD COLUMN IF NOT EXISTS display_name TEXT,
  ADD COLUMN IF NOT EXISTS created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  ADD COLUMN IF NOT EXISTS updated_at   TIMESTAMPTZ NOT NULL DEFAULT now();

CREATE INDEX IF NOT EXISTS idx_users_email ON users (email);
