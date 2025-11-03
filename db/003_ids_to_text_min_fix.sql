-- Immer ins richtige Schema
SET search_path TO public;

DO $$
DECLARE
  col_type TEXT;
  has_spotify_id BOOLEAN;
BEGIN
  -- existiert users?
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema='public' AND table_name='users'
  ) THEN
    RAISE NOTICE 'Table public.users not found – skipping 003_ids_to_text_min_fix.sql';
    RETURN;
  END IF;

  -- Typ von users.id prüfen
  SELECT data_type INTO col_type
  FROM information_schema.columns
  WHERE table_schema='public' AND table_name='users' AND column_name='id';

  -- Altspalte spotify_id vorhanden?
  SELECT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='public' AND table_name='users' AND column_name='spotify_id'
  ) INTO has_spotify_id;

  -- id ggf. auf TEXT umstellen
  IF col_type IS NOT NULL AND col_type <> 'text' THEN
    EXECUTE 'ALTER TABLE users ALTER COLUMN id DROP DEFAULT';
    EXECUTE 'ALTER TABLE users ALTER COLUMN id TYPE text USING id::text';
  END IF;

  -- Falls spotify_id existiert: Inhalte migrieren (id = spotify_id), falls id noch "alt"
  IF has_spotify_id THEN
    EXECUTE $SQL$
      UPDATE users
      SET id = spotify_id::text
      WHERE spotify_id IS NOT NULL
        AND (id IS NULL OR id ~ '^[0-9]+$')
    $SQL$;

    -- spotify_id darf null sein (Backend schreibt nur id)
    EXECUTE 'ALTER TABLE users ALTER COLUMN spotify_id DROP NOT NULL';
  END IF;
END $$;

-- FKs sicherstellen, falls Tabelle früher ohne FKs angelegt wurde
DO $$
BEGIN
  -- sessions.sender_spotify_id -> users(id)
  IF NOT EXISTS (
    SELECT 1
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
      ON tc.constraint_name = kcu.constraint_name
    WHERE tc.table_schema='public' AND tc.table_name='sessions'
      AND tc.constraint_type='FOREIGN KEY'
      AND kcu.column_name='sender_spotify_id'
  ) THEN
    ALTER TABLE sessions
      ADD CONSTRAINT sessions_sender_fk
      FOREIGN KEY (sender_spotify_id) REFERENCES users(id) ON DELETE CASCADE;
  END IF;

  -- devices.user_spotify_id -> users(id)
  IF NOT EXISTS (
    SELECT 1
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
      ON tc.constraint_name = kcu.constraint_name
    WHERE tc.table_schema='public' AND tc.table_name='devices'
      AND tc.constraint_type='FOREIGN KEY'
      AND kcu.column_name='user_spotify_id'
  ) THEN
    ALTER TABLE devices
      ADD CONSTRAINT devices_user_fk
      FOREIGN KEY (user_spotify_id) REFERENCES users(id) ON DELETE CASCADE;
  END IF;

  -- subscriptions.user_spotify_id -> users(id)
  IF NOT EXISTS (
    SELECT 1
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
      ON tc.constraint_name = kcu.constraint_name
    WHERE tc.table_schema='public' AND tc.table_name='subscriptions'
      AND tc.constraint_type='FOREIGN KEY'
      AND kcu.column_name='user_spotify_id'
  ) THEN
    ALTER TABLE subscriptions
      ADD CONSTRAINT subs_user_fk
      FOREIGN KEY (user_spotify_id) REFERENCES users(id) ON DELETE CASCADE;
  END IF;
END $$;
