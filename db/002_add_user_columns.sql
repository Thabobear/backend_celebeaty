-- E-Mail des Spotify-Accounts
ALTER TABLE public.users ADD COLUMN IF NOT EXISTS email text;

-- Anzeigename, Land, Abo-Typ (free/premium)
ALTER TABLE public.users ADD COLUMN IF NOT EXISTS display_name text;
ALTER TABLE public.users ADD COLUMN IF NOT EXISTS country text;
ALTER TABLE public.users ADD COLUMN IF NOT EXISTS product text;

-- Timestamps (falls noch nicht vorhanden)
ALTER TABLE public.users ADD COLUMN IF NOT EXISTS created_at timestamptz DEFAULT now();
ALTER TABLE public.users ADD COLUMN IF NOT EXISTS updated_at timestamptz DEFAULT now();

-- Optional: kleine Hilfs-Indexe
CREATE INDEX IF NOT EXISTS idx_users_email ON public.users (email);
