-- 003_ids_to_text_min_fix.sql
-- Minimal: users.id von INTEGER -> TEXT, ohne PK neu zu setzen

-- Falls es ein SERIAL/IDENTITY-Default gibt, entfernen
ALTER TABLE public.users
  ALTER COLUMN id DROP DEFAULT;

-- Typwechsel auf TEXT
ALTER TABLE public.users
  ALTER COLUMN id TYPE text USING id::text;

-- Optional: die evtl. noch vorhandene Sequence entkoppeln (falls es users_id_seq gibt)
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_class WHERE relkind = 'S' AND relname = 'users_id_seq'
  ) THEN
    ALTER SEQUENCE public.users_id_seq OWNED BY NONE;
  END IF;
END $$;
