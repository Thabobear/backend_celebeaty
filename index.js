/**
 * Celebeaty – Single-Origin Backend (Express + WS + React Build)
 * Serverseitiges Polling für Sender, damit Events auch bei Hintergrund/Sperre weiterlaufen.
 */

const express = require("express");
const axios = require("axios");
const path = require("path");
const http = require("http");
const WebSocket = require("ws");
const cookieParser = require("cookie-parser");
const { v4: uuidv4 } = require("uuid");
const { Pool } = require("pg");
require("dotenv").config();

const rawUrl = process.env.DATABASE_URL || "";
const safeUrl = rawUrl.replace(/:\/\/([^:@]+):[^@]+@/, "://$1:****@");
console.log("[DB] Using:", safeUrl, "ssl=", process.env.DB_SSL);
const PAUSE_GRACE_MS = Number(process.env.PAUSE_GRACE_MS || 0); 
const PAUSE_TIMEOUT_MS = Number(process.env.PAUSE_TIMEOUT_MS || 0); 
const APP_GONE_TIMEOUT_MS = Number(process.env.APP_GONE_TIMEOUT_MS || 0); 


const app = express();

// ---- Serverseitige Steuerung aus DB-Events ----
// Live-Fanout per Direct-API (optional; standardmäßig AUS – vermeidet Doppelsteuerung)
const SERVER_FANOUT = process.env.SERVER_FANOUT === "1";
// DB-Replayer (Server steuert Follower aus der Event-Tabelle)
const REPLAY_POLL_MS = Number(process.env.REPLAY_POLL_MS || 600);   // 0.6 s
const REPLAY_LAG_MS  = Number(process.env.REPLAY_LAG_MS  || 1000);  // 0.8–1.5 s ist sweet spot
// Registry: pro (senderId:followerId) ein Timer
const replayers = new Map(); // key: `${senderId}:${followerId}` -> { timer, afterId }
// WS Präsenz
const socketsByUser = new Map(); // userId -> Set<WebSocket>
const appGoneTimers = new Map(); // userId -> Timeout

/* -------------------- Basics -------------------- */
app.set("trust proxy", 1);
app.use(express.json());
app.use(cookieParser());


// ---- Tuning: Seek-Erkennung nur bei großen Sprüngen
// ENV überschreibbar: SEEK_MIN_MS=7000 z.B. für 7s
const SEEK_MIN_MS = Number(process.env.SEEK_MIN_MS || 5000); // >=5s Sprung => seek-Event


/* -------------------- DB Pool ------------------- */
const dbUrl = process.env.DATABASE_URL || "";
const useSSL =
  /render\.com|railway\.app|neon\.tech|supabase\.co/i.test(dbUrl) ||
  (process.env.NODE_ENV === "production" && !/localhost|127\.0\.0\.1/.test(dbUrl));
const pool = new Pool({
  connectionString: dbUrl || undefined,
  ssl: useSSL ? { rejectUnauthorized: false } : false,
});

// wie weit vor Ablauf wir den Access-Token als „abgelaufen“ behandeln (serverseitig)
const ACCESS_EXP_LEEWAY_MS = Number(process.env.ACCESS_EXP_LEEWAY_MS || 120000); // 2 min


/* -------------------- Playback-Event Storage ------------------- */
async function ensurePlaybackEventsTable() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS public.playback_events (
        id BIGSERIAL PRIMARY KEY,
        sender_id   TEXT NOT NULL,
        kind        TEXT NOT NULL CHECK (kind IN ('trackchange','seek','playstate')),
        track_id    TEXT,
        progress_ms INTEGER NOT NULL DEFAULT 0,
        is_playing  BOOLEAN NOT NULL DEFAULT false,
        name        TEXT,
        artists     TEXT[],
        image       TEXT,
        ts_ms       BIGINT NOT NULL,
        created_at  TIMESTAMP NOT NULL DEFAULT now()
      )
    `);
    await pool.query(`
      CREATE INDEX IF NOT EXISTS ix_events_sender_id ON public.playback_events(sender_id, id)
    `);
  } catch (e) {
    console.warn("[DB] ensurePlaybackEventsTable failed:", e.message);
  }
}

async function storePlaybackEvent({
  sender_id, kind, track_id, progress_ms, is_playing, name, artists, image, ts_ms
}) {
  try {
    await pool.query(
      `INSERT INTO public.playback_events
       (sender_id, kind, track_id, progress_ms, is_playing, name, artists, image, ts_ms)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
      [
        sender_id,
        kind,
        track_id,
        Math.max(0, Number(progress_ms) || 0),
        !!is_playing,
        name || null,
        Array.isArray(artists) ? artists : (artists ? [String(artists)] : []),
        image || null,
        Number.isFinite(ts_ms) ? Math.round(ts_ms) : Math.round(Date.now())
      ]
    );
  } catch (e) {
    console.warn("[STORE] playback_events insert failed:", e.message);
  }
}

// Tabelle beim Start sicherstellen (kein await nötig zum Bootblocken)
ensurePlaybackEventsTable()
  .then(() => console.log("[DB] playback_events ready"))
  .catch((e) => console.warn("[DB] playback_events init error:", e.message));

/* -------------------- Push Helpers ------------------- */
async function ensurePushTable() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS push_tokens (
      user_id TEXT PRIMARY KEY,
      expo_token TEXT NOT NULL,
      updated_at TIMESTAMP NOT NULL DEFAULT now()
    )
  `);
}
async function upsertPushToken(userId, expoToken) {
  await ensurePushTable();
  await pool.query(
    `INSERT INTO push_tokens (user_id, expo_token, updated_at)
     VALUES ($1, $2, now())
     ON CONFLICT (user_id) DO UPDATE
       SET expo_token = EXCLUDED.expo_token,
           updated_at = now()`,
    [userId, expoToken]
  );
}
async function getPushTokensForUsers(userIds = []) {
  if (!userIds.length) return [];
  await ensurePushTable();
  const { rows } = await pool.query(
    `SELECT expo_token FROM push_tokens WHERE user_id = ANY($1)`,
    [userIds]
  );
  return rows.map(r => r.expo_token).filter(Boolean);
}
async function sendExpoPush(expoTokens, title, body) {
  if (!expoTokens.length) return;
  const payloads = expoTokens.map(to => ({ to, title, body, sound: null, priority: "high" }));
  try {
    await axios.post("https://exp.host/--/api/v2/push/send", payloads, {
      headers: { "Content-Type": "application/json" },
      timeout: 8000,
      validateStatus: () => true,
    });
  } catch (e) {
    console.log("[PUSH] send failed:", e.message);
  }
}

/* -------------------- Helpers ------------------- */
function cookieBase(req) {
  const isProd = process.env.NODE_ENV === "production";
  return { httpOnly: true, secure: isProd, sameSite: "none", path: "/" };
}
function getSelfOrigin(req) {
  const proto = (req.headers["x-forwarded-proto"] || "https").toString();
  const host = (req.headers["x-forwarded-host"] || req.headers.host || "").toString();
  return `${proto}://${host}`;
}

// ---- State (return_to) helper ----
function b64uEncode(obj) {
  try {
    return Buffer.from(JSON.stringify(obj), "utf8").toString("base64url");
  } catch {
    return "";
  }
}
function b64uDecode(str) {
  try {
    return JSON.parse(Buffer.from(String(str), "base64url").toString("utf8"));
  } catch {
    return null;
  }
}
// sehr einfache Whitelist: nur App/Custom-Schemes erlauben (kein http/https)
function isAllowedReturnTo(urlStr = "") {
  return /^[a-z][a-z0-9+\-.]*:\/\//i.test(urlStr) && !/^https?:\/\//i.test(urlStr);
}

function buildAuthUrl({ forceDialog = false, stateObj = null } = {}) {
  const scope = [
    "user-read-playback-state",
    "user-read-currently-playing",
    "user-modify-playback-state",
    "user-read-email",
    "user-read-private",
  ].join(" ");
  const params = new URLSearchParams({
    response_type: "code",
    client_id: process.env.SPOTIFY_CLIENT_ID,
    scope,
    redirect_uri: process.env.REDIRECT_URI,
  });
  if (forceDialog) params.set("show_dialog", "true");
  if (stateObj) params.set("state", b64uEncode(stateObj));
  return `https://accounts.spotify.com/authorize?${params.toString()}`;
}

async function refreshAccessToken(refreshToken) {
  return axios.post(
    "https://accounts.spotify.com/api/token",
    new URLSearchParams({
      grant_type: "refresh_token",
      refresh_token: refreshToken,
      client_id: process.env.SPOTIFY_CLIENT_ID,
      client_secret: process.env.SPOTIFY_CLIENT_SECRET,
    }),
    { headers: { "Content-Type": "application/x-www-form-urlencoded" }, validateStatus: () => true }
  );
}
async function spotifyGet(url, token) {
  return axios.get(url, { headers: { Authorization: `Bearer ${token}` }, validateStatus: () => true });
}
async function spotifyPut(url, token, body) {
  return axios.put(url, body, {
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
    validateStatus: () => true,
  });
}
/** Token aus Cookies, ggf. refresh */
async function withValidAccessToken(req, res) {
  // Access-Token: erst Header, dann Cookie
  let accessToken =
    (req.headers.authorization || "").replace(/^Bearer\s+/i, "") ||
    req.cookies.sp_at;

  // Refresh-Token: erst Header (mobil), dann Cookie (Web)
  const refreshTokenHeader = req.headers["x-refresh-token"];
  const refreshTokenCookie = req.cookies.sp_rt;
  const refreshToken = refreshTokenHeader || refreshTokenCookie;

  if (!accessToken && !refreshToken) {
    return { error: { status: 401, body: { error: "no_token" } } };
  }

  // Falls kein AT da, aber ein RT → refreshe
  if (!accessToken && refreshToken) {
    const rr = await refreshAccessToken(refreshToken);
    if (rr.status !== 200) {
      return { error: { status: rr.status, body: rr.data || { error: "refresh_failed" } } };
    }
    accessToken = rr.data.access_token;
    const expires_in = rr.data.expires_in || 3600;
    const base = cookieBase(req);

    // Für Web legen wir Cookies, mobil ist es egal (App sendet Header)
    res.cookie("sp_at", accessToken, { ...base, maxAge: (expires_in - 30) * 1000 });
    if (rr.data.refresh_token) {
      res.cookie("sp_rt", rr.data.refresh_token, { ...base, maxAge: 30 * 24 * 3600 * 1000 });
    }
  }
  return { accessToken };
}


/* -------------------- Health -------------------- */
app.get("/health", async (req, res) => {
  let dbUp = "down";
  let dbReason = null;
  try {
    await pool.query("SELECT 1");
    dbUp = "up";
  } catch (e) {
    dbReason = e.message;
  }
  res.json({ ok: true, db: dbUp, db_reason: dbReason, ts: Date.now(), env: process.env.NODE_ENV || "dev" });
});

// ===== Debug: Direktabfrage für einen Sender-Account
app.get("/__debug/sender-now", async (req, res) => {
  try {
    const senderId = String(req.query.sender_id || "").trim();
    if (!senderId) return res.status(400).json({ error: "missing_sender_id" });
    const at = await getFreshTokenForUser(senderId);
    const r = await spotifyGet("https://api.spotify.com/v1/me/player/currently-playing", at);
    return res.status(200).json({ status: r.status, data: r.data || null });
  } catch (e) {
    return res.status(500).json({ error: "debug_sender_now_failed", message: e.message });
  }
});

/* -------------------- Push Register ------------------- */
app.post("/push/register", async (req, res) => {
  try {
    const t = await withValidAccessToken(req, res);
    if (t.error) return res.status(t.error.status).json(t.error.body);
    const me = await spotifyGet("https://api.spotify.com/v1/me", t.accessToken);
    if (me.status !== 200) return res.status(401).json({ error: "me_failed" });
    const userId = me.data.id;
    // akzeptiere { expo_token } ODER { token } aus dem Frontend
    const expoToken =
      (req.body && (req.body.expo_token || req.body.token))
        ? String(req.body.expo_token || req.body.token)
        : "";
    if (!expoToken) {
      return res.status(400).json({ error: "missing_expo_token" });
    }
    // einfache Plausibilitätsprüfung
    if (!/^ExponentPushToken\[\S+\]$/.test(expoToken)) {
      return res.status(400).json({ error: "invalid_token_format" });
    }
    await upsertPushToken(userId, expoToken);
    res.json({ ok: true });
  } catch (e) {
    console.error("push/register error:", e.message);
    res.status(500).json({ error: "push_register_failed" });
  }
});

/* -------------------- Auth ---------------------- */
// /login und /force-login nehmen optional ?return_to=celebeaty://auth entgegen.
// Wir legen das in den OAuth "state", damit /callback zur App zurückleiten kann.
app.get("/login", (req, res) => {
  const rt = typeof req.query.return_to === "string" && isAllowedReturnTo(req.query.return_to)
    ? req.query.return_to
    : null;
  const url = buildAuthUrl({ forceDialog: false, stateObj: rt ? { rt } : null });
  return res.redirect(url);
});

app.get("/force-login", (req, res) => {
  const rt = typeof req.query.return_to === "string" && isAllowedReturnTo(req.query.return_to)
    ? req.query.return_to
    : null;
  const url = buildAuthUrl({ forceDialog: true, stateObj: rt ? { rt } : null });
  return res.redirect(url);
});

/* -------------------- Callback ------------------ */
app.get("/callback", async (req, res) => {
  const code = req.query.code;
  const stateStr = req.query.state;
  const stateObj = stateStr ? b64uDecode(stateStr) : null;
  const returnTo = stateObj && typeof stateObj.rt === "string" && isAllowedReturnTo(stateObj.rt) ? stateObj.rt : null;

  if (!code) {
    // Falls Fehler + return_to vorhanden → zurück in die App mit error
    if (returnTo) {
      const u = new URL(returnTo);
      u.searchParams.set("ok", "0");
      u.searchParams.set("err", "missing_code");
      return res.redirect(u.toString());
    }
    return res.status(400).send("Missing 'code'");
  }

  try {
    const tokenRes = await axios.post(
      "https://accounts.spotify.com/api/token",
      new URLSearchParams({
        grant_type: "authorization_code",
        code,
        redirect_uri: process.env.REDIRECT_URI,
        client_id: process.env.SPOTIFY_CLIENT_ID,
        client_secret: process.env.SPOTIFY_CLIENT_SECRET,
      }),
      { headers: { "Content-Type": "application/x-www-form-urlencoded" } }
    );
    const { access_token, refresh_token, expires_in } = tokenRes.data || {};
    if (!access_token) {
      if (returnTo) {
        const u = new URL(returnTo);
        u.searchParams.set("ok", "0");
        u.searchParams.set("err", "no_access_token");
        return res.redirect(u.toString());
      }
      return res.status(500).json({ error: "No access_token from Spotify", details: tokenRes.data });
    }

    // Wer bin ich?
    const meRes = await spotifyGet("https://api.spotify.com/v1/me", access_token);
    if (meRes.status !== 200) {
      if (returnTo) {
        const u = new URL(returnTo);
        u.searchParams.set("ok", "0");
        u.searchParams.set("err", "me_failed");
        return res.redirect(u.toString());
      }
      return res.status(500).json({ error: "me_failed", details: meRes.data });
    }
    const me = meRes.data || {};
    const userId = me.id;
    const displayName = me.display_name || userId || null;

    // Cookies
    const base = cookieBase(req);
    res.cookie("sp_at", access_token, { ...base, maxAge: Math.max(1, (expires_in || 3600) - 30) * 1000 });
    if (refresh_token) res.cookie("sp_rt", refresh_token, { ...base, maxAge: 30 * 24 * 3600 * 1000 });

    // User upsert (refresh_token nur überschreiben, wenn neu)
    await pool.query(
      `
      INSERT INTO users (id, display_name, refresh_token_enc, access_token, access_expires_at, email, country, product, created_at, updated_at)
      VALUES ($1, $2, $3, $4, now() + ($5 || ' seconds')::interval, $6, $7, $8, now(), now())
      ON CONFLICT (id)
      DO UPDATE SET
        display_name = EXCLUDED.display_name,
        access_token = EXCLUDED.access_token,
        access_expires_at = EXCLUDED.access_expires_at,
        email = EXCLUDED.email,
        country = EXCLUDED.country,
        product = EXCLUDED.product,
        updated_at = now(),
        refresh_token_enc = COALESCE(EXCLUDED.refresh_token_enc, users.refresh_token_enc)
      `,
      [
        userId,
        displayName,
        refresh_token || null,
        access_token,
        Math.max(30, expires_in || 3600),
        me.email || null,
        me.country || null,
        me.product || null,
      ]
    );

    // Erfolg: Priorität App-Deep-Link, sonst Frontend/UI
    if (returnTo) {
      const u = new URL(returnTo);
      u.searchParams.set("ok", "1");
      return res.redirect(u.toString());
    }
    const front = (process.env.FRONTEND_URI || getSelfOrigin(req)).replace(/\/+$/, "");
    return res.redirect(`${front}/`);
  } catch (err) {
    console.error("Token exchange failed:", err.response?.data || err.message);
    // Fehlerfall → wenn return_to da ist, dorthin mit Fehlercode
    const returnTo = (() => {
      const s = req.query.state ? b64uDecode(req.query.state) : null;
      const rt = s && typeof s.rt === "string" ? s.rt : null;
      return rt && isAllowedReturnTo(rt) ? rt : null;
    })();
    if (returnTo) {
      const u = new URL(returnTo);
      u.searchParams.set("ok", "0");
      u.searchParams.set("err", "callback_failed");
      return res.redirect(u.toString());
    }
    return res.status(500).json({ error: "callback_failed", details: err.response?.data || err.message });
  }
});

/* -------------------- Info APIs ---------------- */
app.get("/whoami", async (req, res) => {
  try {
    const t = await withValidAccessToken(req, res);
    if (t.error) return res.status(t.error.status).json(t.error.body);

    let r = await spotifyGet("https://api.spotify.com/v1/me", t.accessToken);
    if (r.status === 401 && req.cookies.sp_rt) {
      const rr = await refreshAccessToken(req.cookies.sp_rt);
      if (rr.status === 200) {
        const at = rr.data.access_token;
        const expires_in = rr.data.expires_in || 3600;
        const base = cookieBase(req);
        res.cookie("sp_at", at, { ...base, maxAge: (expires_in - 30) * 1000 });
        if (rr.data.refresh_token) res.cookie("sp_rt", rr.data.refresh_token, { ...base, maxAge: 30 * 24 * 3600 * 1000 });
        r = await spotifyGet("https://api.spotify.com/v1/me", at);
      }
    }
    if (r.status === 429) {
      const retry = Number(r.headers["retry-after"] || 1);
      res.set("Retry-After", String(retry));
      return res.status(429).json({ error: "rate_limited", retry_after: retry });
    }
    if (r.status >= 400) return res.status(r.status).json({ error: "spotify_error", details: r.data });

    const j = r.data || {};
    return res.json({
      id: j.id,
      display_name: j.display_name || j.id || null,
      email: j.email || null,
      country: j.country || null,
      product: j.product || null,
    });
  } catch (e) {
    console.error("whoami error:", e.message);
    return res.status(500).json({ error: "whoami_failed" });
  }
});

app.get("/currently-playing", async (req, res) => {
  try {
    const t = await withValidAccessToken(req, res);
    if (t.error) return res.status(t.error.status).json(t.error.body);

    let r = await spotifyGet("https://api.spotify.com/v1/me/player/currently-playing", t.accessToken);
    if (r.status === 401 && req.cookies.sp_rt) {
      const rr = await refreshAccessToken(req.cookies.sp_rt);
      if (rr.status === 200) {
        const at = rr.data.access_token;
        const expires_in = rr.data.expires_in || 3600;
        const base = cookieBase(req);
        res.cookie("sp_at", at, { ...base, maxAge: (expires_in - 30) * 1000 });
        if (rr.data.refresh_token) res.cookie("sp_rt", rr.data.refresh_token, { ...base, maxAge: 30 * 24 * 3600 * 1000 });
        r = await spotifyGet("https://api.spotify.com/v1/me/player/currently-playing", at);
      }
    }

    if (r.status === 429) {
      const retry = Number(r.headers["retry-after"] || 1);
      res.set("Retry-After", String(retry));
      return res.status(429).json({ error: "rate_limited", retry_after: retry });
    }

    if (r.status === 204 || !r.data) return res.json({ message: "Kein Song wird gerade gespielt.", reason: "no_item" });

    if (r.status === 200 && r.data) {
      const data = r.data;
      const item = data.item;
      if (!item) {
        return res.json({
          message: "Kein item. Evtl. Werbung oder private session.",
          reason: data.currently_playing_type || "no_item",
        });
      }
      return res.json({
        is_playing: !!data.is_playing,
        progress_ms: data.progress_ms || 0,
        track: {
          id: item.id,
          name: item.name,
          artists: (item.artists || []).map((a) => a.name),
          album: {
            name: item.album?.name,
            images: item.album?.images || [],
            spotify_url: item.album?.external_urls?.spotify || null,
          },
          spotify_url: item.external_urls?.spotify || null,
          duration_ms: item.duration_ms || 0,
        },
      });
    }

    return res.status(r.status).json({ error: "spotify_error", details: r.data });
  } catch (e) {
    console.error("currently-playing error:", e.response?.data || e.message);
    return res.status(500).json({ error: "currently_playing_failed" });
  }
});

/* -------------------- Spotify Control Proxy -------------- */
app.get("/spotify/devices", async (req, res) => {
  try {
    const t = await withValidAccessToken(req, res);
    if (t.error) return res.status(t.error.status).json(t.error.body);
    const r = await spotifyGet("https://api.spotify.com/v1/me/player/devices", t.accessToken);
    return res.status(r.status).json(r.data);
  } catch (e) {
    console.error("devices error:", e.message);
    return res.status(500).json({ error: "devices_failed" });
  }
});
app.post("/spotify/transfer", async (req, res) => {
  try {
    const t = await withValidAccessToken(req, res);
    if (t.error) return res.status(t.error.status).json(t.error.body);
    const { device_id, play = true } = req.body || {};
    const r = await spotifyPut("https://api.spotify.com/v1/me/player", t.accessToken, {
      device_ids: [device_id],
      play,
    });
    return res.status(r.status).json(r.data || {});
  } catch (e) {
    console.error("transfer error:", e.message);
    return res.status(500).json({ error: "transfer_failed" });
  }
});
app.post("/spotify/play", async (req, res) => {
  try {
    const t = await withValidAccessToken(req, res);
    if (t.error) return res.status(t.error.status).json(t.error.body);
    const body = req.body || {};
    const r = await spotifyPut("https://api.spotify.com/v1/me/player/play", t.accessToken, body);
    return res.status(r.status).json(r.data || {});
  } catch (e) {
    console.error("play error:", e.message);
    return res.status(500).json({ error: "play_failed" });
  }
});
app.post("/spotify/pause", async (req, res) => {
  try {
    const t = await withValidAccessToken(req, res);
    if (t.error) return res.status(t.error.status).json(t.error.body);
    const r = await spotifyPut("https://api.spotify.com/v1/me/player/pause", t.accessToken, {});
    return res.status(r.status).json(r.data || {});
  } catch (e) {
    console.error("pause error:", e.message);
    return res.status(500).json({ error: "pause_failed" });
  }
});

/* -------------------- Sessions API ----------------------- */
async function getCurrentSpotifyId(req, res) {
  const t = await withValidAccessToken(req, res);
  if (t.error) return { error: t.error };
  const me = await spotifyGet("https://api.spotify.com/v1/me", t.accessToken);
  if (me.status !== 200) return { error: { status: 401, body: { error: "me_failed" } } };
  return { id: me.data.id, name: me.data.display_name || me.data.id };
}




app.get("/sessions/active", async (req, res) => {
  try {
    const { rows } = await pool.query(
      `
      SELECT 
        s.id AS session_id,
        s.sender_spotify_id AS user_id,
        COALESCE(u.display_name, s.sender_spotify_id) AS display_name,
        s.created_at,
        s.last_snapshot_at
      FROM sessions s
      LEFT JOIN users u ON u.id = s.sender_spotify_id
      WHERE s.is_active = true
      ORDER BY COALESCE(s.last_snapshot_at, s.created_at) DESC
      LIMIT 50
      `
    );
    res.json({
      active: rows.length,
      sessions: rows.map((r) => ({
        session_id: r.session_id,
        user_id: r.user_id,
        name: r.display_name,
        since: r.created_at,
        last: r.last_snapshot_at,
      })),
    });
  } catch (e) {
    console.error("listActiveSessions error", e.message);
    res.json({ active: 0, sessions: [] });
  }
});

/* ===== Polling-Manager (Server sendet Events, wenn Sender teilt) ===== */
const pollers = new Map(); // key: sender_spotify_id -> { timer, lastSnapshot }
// Follower-Registry: senderId -> Set<followerId>
const followersBySender = new Map(); 

async function getFreshTokenForUser(userId) {
  const uRes = await pool.query(
    `SELECT id, refresh_token_enc, access_token, access_expires_at FROM users WHERE id = $1`,
    [userId]
  );
  if (!uRes.rowCount) throw new Error("user not found");
  const u = uRes.rows[0];

  const now = Date.now();
  const exp = u.access_expires_at ? new Date(u.access_expires_at).getTime() : 0;
   // 🔁 proaktiv früher refreshen, damit der Wechsel nicht „spürbar“ wird
  if (u.access_token && exp > now + ACCESS_EXP_LEEWAY_MS) {
    return u.access_token;
  }
  if (!u.refresh_token_enc) throw new Error("no refresh token");

  const rr = await refreshAccessToken(u.refresh_token_enc);
  if (rr.status !== 200) throw new Error("refresh failed: " + rr.status);
  const newAT = rr.data.access_token;
  const expires_in = rr.data.expires_in || 3600;
  await pool.query(
    `UPDATE users SET access_token=$1, access_expires_at=now()+($2||' seconds')::interval, updated_at=now() WHERE id=$3`,
    [newAT, Math.max(30, expires_in), userId]
  );
  if (rr.data.refresh_token) {
    await pool.query(`UPDATE users SET refresh_token_enc=$1 WHERE id=$2`, [rr.data.refresh_token, userId]);
  }
  return newAT;
}

/** Spotify-Helfer für einen beliebigen Nutzer (Follower) */
async function spGetForUser(userId, url) {
  const at = await getFreshTokenForUser(userId);
  return spotifyGet(url, at);
}
async function spPutForUser(userId, url, body) {
  const at = await getFreshTokenForUser(userId);
  return spotifyPut(url, at, body);
}

/** Playback für einen Follower an den Sender-Zustand angleichen */
async function alignFollowerPlayback(followerId, { trackId, progress, shouldPlay, forcePosition }) {
  try {
    // 1) Geräte checken
    const devRes = await spGetForUser(followerId, "https://api.spotify.com/v1/me/player/devices");
    const devices = (devRes.status === 200 && devRes.data && devRes.data.devices) ? devRes.data.devices : [];
    if (!devices.length) {
      // Kein Device sichtbar → serverseitig nicht lösbar (App kann Spotify nicht „öffnen“)
      return;
    }
    // iPhone/Smartphone bevorzugen
    const device =
      devices.find(d => /iPhone|iOS|Smartphone/i.test(d?.name) || d?.type === "Smartphone") ||
      devices.find(d => d?.is_active) ||
      devices.find(d => !d?.is_restricted) ||
      devices[0];
    if (!device) return;

    // 2) ggf. auf dieses Device transferieren (aktivieren)
    if (!device.is_active) {
      await spPutForUser(followerId, "https://api.spotify.com/v1/me/player", {
        device_ids: [device.id],
        play: !!shouldPlay,
      });
      await new Promise(r => setTimeout(r, 600));
    }

    // 3) Play/Pause senden
    if (!shouldPlay) {
      await spPutForUser(followerId, "https://api.spotify.com/v1/me/player/pause", {});
      return;
    }
    // Play – je nach Event Position setzen
    const body = forcePosition
      ? { uris: [`spotify:track:${trackId}`], position_ms: Math.max(0, progress || 0) }
      : {};
    await spPutForUser(followerId, "https://api.spotify.com/v1/me/player/play", body);
  } catch (e) {
    console.warn(`[ALIGN][ERR] follower=${followerId} track=${trackId} play=${shouldPlay} pos=${progress}ms force=${forcePosition} → ${e?.message || e}`);
  }
}

/** DB-Event-Replayer: folgt den gespeicherten Events des Senders für genau einen Follower */
function startDbReplayer(senderId, followerId) {
  const key = `${senderId}:${followerId}`;
  if (replayers.has(key)) return;
  const state = { afterId: 0 };

  const tick = async () => {
    try {
      // Follower noch registriert?
      const set = followersBySender.get(senderId);
      if (!set || !set.has(followerId)) { stopDbReplayer(senderId, followerId); return; }

      // Neueste relevanten Events seit afterId, mit Lag
      const { rows } = await pool.query(
        `
        WITH base AS (
          SELECT id, sender_id, kind, track_id, progress_ms, is_playing, name, artists, image, ts_ms, created_at
          FROM public.playback_events
          WHERE sender_id = $1
            AND id > $2
            AND kind IN ('trackchange','seek','playstate')
            AND created_at <= now() - ($3::text || ' milliseconds')::interval
          ORDER BY id ASC
          LIMIT 100
        ),
        dedup_playstate AS (
          SELECT *
          FROM (
            SELECT b.*,
                   LAG(is_playing) OVER (ORDER BY id) AS prev_is_playing
            FROM base b
          ) t
          WHERE kind <> 'playstate' OR (prev_is_playing IS DISTINCT FROM is_playing)
        ),
        thin_seek AS (
          SELECT *
          FROM (
            SELECT d.*,
                   /* Letzter SEEK-Zeitstempel pro Track, vor der aktuellen Zeile */
                   MAX(CASE WHEN kind = 'seek' THEN ts_ms END)
                     OVER (
                       PARTITION BY track_id
                       ORDER BY id
                       ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                     ) AS prev_seek_ts
            FROM dedup_playstate d
          ) s
          WHERE kind <> 'seek' OR prev_seek_ts IS NULL OR (ts_ms - prev_seek_ts) > 600
        )
        SELECT * FROM thin_seek
        ORDER BY id ASC
        `,
        [senderId, state.afterId, REPLAY_LAG_MS]
      );
      if (!rows.length) { /* keine neuen Events */ return; }
      console.log(`[REPLAY] ${senderId}→${followerId} fetched ${rows.length} evts after_id=${state.afterId} lag=${REPLAY_LAG_MS}ms`);

      // Nur das letzte Event anwenden
      const ev = rows[rows.length - 1];
      state.afterId = Math.max(state.afterId, Number(ev.id || 0));

      const kind = ev.kind;
      const trackId = ev.track_id;
      if (!trackId) return;

      // Effektive Zielposition aus Eventzeit + Drift
      const now = Date.now();
      const base = Math.max(0, ev.progress_ms || 0);
      const drift = Math.max(0, now - (ev.ts_ms || now));
      const effective = Math.floor(base + drift);

      let forcePosition = false;
      let shouldPlay = true;
      if (kind === 'trackchange') { forcePosition = true;  shouldPlay = true; }
      else if (kind === 'seek')   { forcePosition = true;  shouldPlay = true; }
      else if (kind === 'playstate') { forcePosition = false; shouldPlay = !!ev.is_playing; }

      await alignFollowerPlayback(followerId, {
        trackId,
        progress: effective,
        shouldPlay,
        forcePosition
      });
      console.log(`[REPLAY][APPLY] ${senderId}→${followerId} ${kind} track=${trackId} pos=${effective}ms play=${shouldPlay} force=${forcePosition}`);
    } catch (e) {
      console.warn(`[REPLAY][ERR] ${senderId}→${followerId}: ${e?.message || e}`);
    }
  };

  const timer = setInterval(tick, REPLAY_POLL_MS);
  replayers.set(key, { timer, afterId: 0 });
}

function stopDbReplayer(senderId, followerId) {
  const key = `${senderId}:${followerId}`;
  const r = replayers.get(key);
  if (r) { clearInterval(r.timer); replayers.delete(key); }
}

function stopAllReplayersForSender(senderId) {
  const keys = Array.from(replayers.keys()).filter(k => k.startsWith(`${senderId}:`));
  for (const k of keys) {
    try { clearInterval(replayers.get(k).timer); } catch {}
    replayers.delete(k);
  }
}

/** Follower eines Senders synchronisieren (nur auf „relevante“ Events reagieren) */
async function fanoutToFollowers(senderId, payload) {
  const set = followersBySender.get(senderId);
  if (!set || !set.size) return;
  const { kind, trackId, progress_ms, is_playing } = payload || {};
  if (!trackId) return;
  if (!["trackchange", "seek", "playstate"].includes(kind)) return; // keepalive ignorieren

  const jobs = Array.from(set).map(fid =>
    alignFollowerPlayback(fid, {
      trackId,
      progress: progress_ms || 0,
      shouldPlay: !!is_playing,
      forcePosition: kind === "trackchange" || kind === "seek",
    })
  );
  await Promise.allSettled(jobs);
}

function broadcastJSON(obj) {
  const msg = JSON.stringify(obj);
  if (!wss || !wss.clients) return;
  wss.clients.forEach((c) => {
    if (c.readyState === WebSocket.OPEN) {
      try { c.send(msg); } catch {}
    }
  });
}

// Helper: Session-Ende-Event
function broadcastSessionEnded(senderId, senderName, reason) {
  const payload = {
    type: "session",
    kind: "ended",
    user: { id: senderId, name: senderName || senderId },
    ts: Date.now(),
    reason: reason || null,
  };
  try {
    console.log("[WS] Broadcast session ended →", senderId);
    broadcastJSON(payload);
  } catch {}
}

// Expo Push an alle aktuell registrierten Follower schicken
async function pushSessionEndedToFollowers(senderId, { senderName, trackId, progressMs }) {
  try {
    const set = followersBySender.get(senderId);
    if (!set || !set.size) return;
    const followerIds = Array.from(set);
    const q = await pool.query(
      `SELECT expo_token FROM push_tokens WHERE user_id = ANY($1::text[])`,
      [followerIds]
    );
    if (!q.rowCount) return;
    const tokens = q.rows.map(r => r.expo_token).filter(t => /^ExponentPushToken\[\S+\]$/.test(t));
    if (!tokens.length) return;

    const body = {
      to: tokens,
      title: "Session beendet",
      body: `${senderName || "Sender"} hat die Live-Session beendet. Spotify wurde pausiert.`,
      data: {
        type: "session_ended",
        senderId,
        trackId: trackId || null,
        progressMs: progressMs || 0,
      },
      sound: null,
      priority: "high",
    };
    // Expo erlaubt Batch-POST mit Array; bei vielen Tokens ggf. in Chunks splitten (<=100)
    const chunks = [];
    for (let i = 0; i < tokens.length; i += 100) {
      chunks.push(tokens.slice(i, i + 100));
    }
    for (const group of chunks) {
      await axios.post(
        "https://exp.host/--/api/v2/push/send",
        group.map(t => ({ ...body, to: t })),
        { timeout: 10000, validateStatus: () => true }
      );
    }
  } catch (e) {
    console.warn("push fanout failed:", e.message);
  }
}


// Helper: explizit „playstate=false“ an alle (Receiver filtern selbst)
function broadcastPlaystateFalse({ senderId, senderName, trackId, progressMs }) {
  const payload = {
    type: "track",
    kind: "playstate",
    user: { id: senderId, name: senderName || senderId },
    trackId: trackId || null,
    progress_ms: progressMs || 0,
    is_playing: false,
    ts: Date.now(),
  };
  try {
    console.log("[WS] Broadcast playstate=false →", senderId, "track", trackId || "n/a");
    broadcastJSON(payload);
  } catch {}
}

function startPollingForSender(senderId, senderName) {
  if (pollers.has(senderId)) return;

  const state = {
    lastTrackId: null,
    lastIsPlaying: null,
    lastProgress: 0,
    lastKeepalive: 0,
    noItemSince: 0,        // ms-Timestamp, ab wann wir 204/kein item sehen
  };

  console.log(`[POLL] startPollingForSender for ${senderName} (${senderId})`);

  const timer = setInterval(async () => {
    try {
      const at = await getFreshTokenForUser(senderId);
      const r = await spotifyGet("https://api.spotify.com/v1/me/player/currently-playing", at);
      if (r.status !== 200 && r.status !== 204) {
        console.warn(`[POLL] ${senderId} currently-playing status=${r.status}`, r.data || "");
      }

      if (r.status === 204 || !r.data || !r.data.item) {
        // 204 oder kein item: kann Werbung, private session, inaktives Gerät etc. sein
        const why = r?.data?.currently_playing_type || "no_item";
        const nowMs = Date.now();

        // 🕐 merken, seit wann kein Item mehr kam
        if (!state.noItemSince) state.noItemSince = nowMs;
        const silentFor = nowMs - state.noItemSince;

        console.log(
          `[POLL] ${senderId} no item (status=${r.status}) reason=${why} silentFor=${silentFor}ms`
        );

        // Nur wenn wirklich LANGE (PAUSE_GRACE_MS) nichts kommt → einmalig Pause broadcasten & persistieren
        if (
          silentFor >= PAUSE_GRACE_MS &&
          state.lastTrackId &&
          state.lastIsPlaying === true
        ) {
          broadcastJSON({
            type: "track",
            kind: "playstate",
            user: { id: senderId, name: senderName || senderId },
            trackId: state.lastTrackId,
            progress_ms: state.lastProgress || 0,
            is_playing: false,
            ts: nowMs,
          });
          try {
            await storePlaybackEvent({
              sender_id: senderId,
              kind: "playstate",
              track_id: state.lastTrackId,
              progress_ms: state.lastProgress || 0,
              is_playing: false,
              name: null,
              artists: [],
              image: null,
              ts_ms: nowMs,
            });
            console.log(`[EVT][STORE] playstate ${senderId} is_playing=false @${state.lastProgress || 0}`);
          } catch {}
          state.lastIsPlaying = false;
          state.wasSilentlyPaused = true; // merken: Pause kam aus „Silence“
          // kein Reset von noItemSince – wir bleiben in „Pause“, bis wieder ein echtes Item kommt
        }

        // Harte Kante: sehr lange Pause → Session sauber beenden (einmalig)
        if (silentFor >= PAUSE_TIMEOUT_MS && !state.timedOut) {
          state.timedOut = true;
          console.log(
            `[POLL] ${senderId} Pause >= ${PAUSE_TIMEOUT_MS}ms → stopPolling (timeout)`
          );
          // 👉 ECHTER Timeout ⇒ Push an den Sender
          try {
            const tokens = await getPushTokensForUsers([senderId]);
            if (tokens.length) {
            await sendExpoPush(
              tokens,
              "Session beendet",
              "Spotify angehalten - deine Session wurde beendet"
            );
            }
          } catch {}
          await stopPollingForSender(senderId, senderName, "timeout");
        }
        return;
      } else {
        // wir haben wieder ein echtes Item → Stille-Timer zurücksetzen
        state.noItemSince = 0;
        state.timedOut = false; // Reset, weil wieder ein echtes Item da ist
      }

      const data = r.data;
      const item = data.item;
      if (!item) {
        console.log(`[POLL] ${senderId} has data but no item. type=${data.currently_playing_type} is_playing=${data.is_playing}`);
      }
      const trackId = item.id;
      const progress = data.progress_ms || 0;
      const is_playing = !!data.is_playing;
      console.log(`[POLL] ${senderId} item=${trackId} play=${is_playing} @${progress}ms type=${data.currently_playing_type}`);

      await pool.query(
        `UPDATE sessions SET last_snapshot_at = now() WHERE sender_spotify_id = $1 AND is_active = true`,
        [senderId]
      );

      const trackChanged = trackId !== state.lastTrackId;
      const playStateChanged = (is_playing ? 1 : 0) !== (state.lastIsPlaying ? 1 : 0);
      // Nur "echte" Sprünge melden: absolute Differenz >= SEEK_MIN_MS
      const seekDetected =
        !trackChanged &&
        !playStateChanged &&
        Math.abs(progress - (state.lastProgress || 0)) >= SEEK_MIN_MS;

      const now = Date.now();
      const needKeepalive = now - (state.lastKeepalive || 0) > 15000;

      if (trackChanged) {
        const msg = {
          type: "track",
          kind: "trackchange",
          user: { id: senderId, name: senderName || senderId },
          trackId,
          progress_ms: progress,
          name: item.name,
          artists: (item.artists || []).map((a) => a.name),
          image: item.album?.images?.[0]?.url || null,
          is_playing,
          ts: now,
        };

        broadcastJSON(msg);
        if (SERVER_FANOUT) fanoutToFollowers(senderId, msg);
        // persistieren (nur „relevante“ Events)
        await storePlaybackEvent({
          sender_id: senderId, kind: "trackchange", track_id: trackId,
          progress_ms: progress, is_playing, name: msg.name, artists: msg.artists, image: msg.image, ts_ms: now
        });
        console.log(`[EVT][STORE] trackchange ${senderId} ${trackId} @${progress}`);
      } else if (playStateChanged) {
        const msg = {
          type: "track",
          kind: "playstate",
          user: { id: senderId, name: senderName || senderId },
          trackId,
          progress_ms: progress,
          name: item.name,
          artists: (item.artists || []).map((a) => a.name),
          image: item.album?.images?.[0]?.url || null,
          is_playing,
          ts: now,
        };

        broadcastJSON(msg);
        if (SERVER_FANOUT) fanoutToFollowers(senderId, msg);
        // persistieren (nur „relevante“ Events)
        await storePlaybackEvent({
          sender_id: senderId, kind: "playstate", track_id: trackId,
          progress_ms: progress, is_playing, name: msg.name, artists: msg.artists, image: msg.image, ts_ms: now
        });
        console.log(`[EVT][STORE] playstate ${senderId} is_playing=${is_playing} @${progress}`);
      } else if (seekDetected || needKeepalive) {
        const msg = {
          type: "track",
          kind: seekDetected ? "seek" : "keepalive",
          user: { id: senderId, name: senderName || senderId },
          trackId,
          progress_ms: progress,
          name: item.name,
          artists: (item.artists || []).map((a) => a.name),
          image: item.album?.images?.[0]?.url || null,
          is_playing,
          ts: now,
        };

        broadcastJSON(msg);
        // keepalive NICHT fanouten (würde zu viel Traffic erzeugen)
        if (needKeepalive) state.lastKeepalive = now;
        if (seekDetected) {
          await storePlaybackEvent({
            sender_id: senderId, kind: "seek", track_id: trackId,
            progress_ms: progress, is_playing, name: msg.name, artists: msg.artists, image: msg.image, ts_ms: now
          });
          console.log(`[EVT][STORE] seek ${senderId} ${trackId} @${progress}`);
        }
      }

      state.lastTrackId = trackId;
      state.lastIsPlaying = is_playing;
      state.lastProgress = progress;

 // 🩵 FIX: Wenn Spotify nach längerer Pause "aufwacht" → fehlendes Play-Event nachreichen
     if (!state.hasAnnouncedResume && is_playing && !trackChanged) {
       const since = Date.now() - (state.lastKeepalive || 0);
       if (since > 10000) {
         console.log(`[Celebeaty] Detected resume after long pause → broadcast playstate:true for ${senderName}`);
         const msg = {
           type: "track",
           kind: "playstate",
           user: { id: senderId, name: senderName || senderId },
           trackId,
           progress_ms: progress,
           name: item.name,
           artists: (item.artists || []).map(a => a.name),
           image: item.album?.images?.[0]?.url || null,
           is_playing: true,
           ts: Date.now(),
         };
         broadcastJSON(msg);
         if (SERVER_FANOUT) fanoutToFollowers(senderId, msg);
         // ❗ WICHTIG: auch in DB persistieren, damit /events/since dieses Playstate bekommt
         await storePlaybackEvent({
           sender_id: senderId, kind: "playstate", track_id: trackId,
           progress_ms: progress, is_playing: true,
           name: msg.name, artists: msg.artists, image: msg.image, ts_ms: msg.ts
         });
         state.hasAnnouncedResume = true;
       }
     } else if (!is_playing) {
       state.hasAnnouncedResume = false; // reset, damit nächster Resume erkannt wird
     }

    } catch (e) {
      console.warn(`[POLL][ERR] ${senderId} ${e?.message || e}`);
    }
  }, 2000);

  pollers.set(senderId, { timer, state });
}

async function stopPollingForSender(senderId, senderName, reason = null) {
  const p = pollers.get(senderId);
  if (p) {
    // vor dem Stoppen ein letztes „Pause“-Signal senden (falls wir noch State haben)
    const lastTrackId = p.state?.lastTrackId || null;
    const lastProgress = p.state?.lastProgress || 0;
    broadcastPlaystateFalse({
      senderId,
      senderName,
      trackId: lastTrackId,
      progressMs: lastProgress,
    });
    // 🔔 echte Pushes an alle aktuellen Follower
    pushSessionEndedToFollowers(senderId, {
      senderName,
      trackId: lastTrackId,
      progressMs: lastProgress,
    });
    // danach Session-Ende signalisieren
    broadcastSessionEnded(senderId, senderName, reason);
    clearInterval(p.timer);
    pollers.delete(senderId);
  } else {
    // Falls kein Poller (z.B. direkte Beendigung), trotzdem Events senden
    broadcastPlaystateFalse({ senderId, senderName, trackId: null, progressMs: 0 });
    pushSessionEndedToFollowers(senderId, { senderName, trackId: null, progressMs: 0 });
    broadcastSessionEnded(senderId, senderName, reason);
  }


  // ➜ Alle DB-Replayer dieses Senders stoppen
  stopAllReplayersForSender(senderId);

  // (Optional, aber empfohlen) Pause-Kante persistieren, damit DB-Consumer sie sehen
  try {
    await storePlaybackEvent({
      sender_id: senderId,
      kind: "playstate",
      track_id: p?.state?.lastTrackId || null,
      progress_ms: p?.state?.lastProgress || 0,
      is_playing: false,
      name: null,
      artists: [],
      image: null,
      ts_ms: Date.now(),
    });
  } catch (e) { /* ruhig */ }



  // 👇 NEU: alle aktiven Follower serverseitig PAUSIEREN + Push schicken
  (async () => {
    try {
      const set = followersBySender.get(senderId);
      const followerIds = set ? Array.from(set) : [];
      if (followerIds.length) {
        // 1) Spotify-Pause für jeden Follower
        await Promise.allSettled(
          followerIds.map(fid =>
            spPutForUser(fid, "https://api.spotify.com/v1/me/player/pause", {})
          )
        );
        // 2) Expo Push an diese Follower
        const tokens = await getPushTokensForUsers(followerIds);
        if (tokens.length) {
          await sendExpoPush(tokens, "Session beendet", `${senderName || "Sender"} hat die Live-Session beendet. Spotify wurde pausiert.`);
        }
      }
    } catch (e) {
      console.log("[END] fanout pause/push failed:", e.message);
    }
  })();

}

/* ---- Follow-Registration: Receiver meldet sich/ab ---- */
app.post("/follow/start", async (req, res) => {
  try {
    const who = await getCurrentSpotifyId(req, res); // ← das ist der Follower (aktueller User)
    if (who.error) return res.status(who.error.status || 401).json(who.error.body || { error: "no_me" });

    const { sender_id } = req.body || {};
    if (!sender_id) return res.status(400).json({ error: "missing_sender_id" });

    // Refresh/Access-Token des Followers persistieren (wie bei share/start)
    const rtHeader = (req.headers["x-refresh-token"] || "").toString() || null;
    const atHeader = (req.headers.authorization || "").replace(/^Bearer\s+/i, "") || null;
    // ⬇️  NEU: Cookie-Fallback, damit Web-Client Tokens persistiert
    const rtCookie = (req.cookies?.sp_rt || null);
    const atCookie = (req.cookies?.sp_at || null);
    const rt = rtHeader || rtCookie;
    const at = atHeader || atCookie;
    await pool.query(
      `
      INSERT INTO users (id, display_name, refresh_token_enc, access_token, access_expires_at, created_at, updated_at, spotify_id)
      VALUES ($1, $2, $3, $4, now() + interval '55 minutes', now(), now(), $1)
      ON CONFLICT (id)
      DO UPDATE SET
        display_name        = EXCLUDED.display_name,
        access_token        = EXCLUDED.access_token,
        access_expires_at   = EXCLUDED.access_expires_at,
        updated_at          = now(),
        refresh_token_enc   = COALESCE(EXCLUDED.refresh_token_enc, users.refresh_token_enc),
        spotify_id          = COALESCE(users.spotify_id, EXCLUDED.spotify_id)
      `,
      [who.id, who.name || who.id, rt, at]
    );

    // In-Memory-Registry aktualisieren
    if (!followersBySender.has(sender_id)) followersBySender.set(sender_id, new Set());
    followersBySender.get(sender_id).add(who.id);

    // ➜ DB-Replayer für diesen (sender, follower) starten
    startDbReplayer(sender_id, who.id);
    console.log(`[FOLLOW] start follower=${who.id} sender=${sender_id} rt=${!!rt} at=${!!at}`);

    return res.json({ ok: true });
  } catch (e) {
    return res.status(500).json({ error: "follow_start_failed" });
  }
});

app.post("/follow/stop", async (req, res) => {
  try {
    const who = await getCurrentSpotifyId(req, res); // Follower
    if (who.error) return res.status(who.error.status || 401).json(who.error.body || { error: "no_me" });
    const { sender_id } = req.body || {};
    if (!sender_id) return res.status(400).json({ error: "missing_sender_id" });
    const set = followersBySender.get(sender_id);
    if (set) set.delete(who.id);
    // ➜ Replayer stoppen
    stopDbReplayer(sender_id, who.id);
    return res.json({ ok: true });
  } catch (e) {
    return res.status(500).json({ error: "follow_stop_failed" });
  }
});



/* ---- Share-Endpoints binden Polling ein ---- */
app.post("/share/start", async (req, res) => {
  try {
    const who = await getCurrentSpotifyId(req, res);
    if (who.error) return res.status(who.error.status || 401).json(who.error.body || { error: "no_me" });

    const rtHeader = (req.headers["x-refresh-token"] || "").toString() || null;
    const atHeader = (req.headers.authorization || "").replace(/^Bearer\s+/i, "") || null;
    const rtCookie = (req.cookies?.sp_rt || null);
    const atCookie = (req.cookies?.sp_at || null);
    const rt = rtHeader || rtCookie;
    const at = atHeader || atCookie;

    // User auf PRIMARY KEY "id" upserten (FK der sessions zeigt auf users.id)
    await pool.query(
      `
      INSERT INTO users (id, display_name, refresh_token_enc, access_token, access_expires_at, created_at, updated_at, spotify_id)
      VALUES ($1, $2, $3, $4, now() + interval '55 minutes', now(), now(), $1)
      ON CONFLICT (id)
      DO UPDATE SET
        display_name        = EXCLUDED.display_name,
        access_token        = EXCLUDED.access_token,
        access_expires_at   = EXCLUDED.access_expires_at,
        updated_at          = now(),
        refresh_token_enc   = COALESCE(EXCLUDED.refresh_token_enc, users.refresh_token_enc),
        spotify_id          = COALESCE(users.spotify_id, EXCLUDED.spotify_id)
      `,
      [who.id, who.name || who.id, rt, at]
    );

    // Prüfen anhand users.id (nicht spotify_id)
    const chk = await pool.query(
      `SELECT refresh_token_enc FROM users WHERE id = $1`,
      [who.id]
    );

    if (!chk.rowCount || !chk.rows[0].refresh_token_enc) {
      return res.status(400).json({
        error: "no_refresh_token",
        message: "Kein Refresh-Token vorhanden. Bitte neu einloggen und das Teilen erneut starten.",
      });
    }

    const existing = await pool.query(
      `SELECT id FROM sessions WHERE sender_spotify_id = $1 LIMIT 1`,
      [who.id]
    );

    let sessionId;
    if (existing.rowCount) {
      const upd = await pool.query(
        `UPDATE sessions SET is_active = true, last_snapshot_at = now() WHERE id = $1 RETURNING id`,
        [existing.rows[0].id]
      );
      sessionId = upd.rows[0].id;
    } else {
      sessionId = uuidv4();
      await pool.query(
        `INSERT INTO sessions (id, sender_spotify_id, is_active, created_at, last_snapshot_at)
         VALUES ($1, $2, true, now(), now())`,
        [sessionId, who.id]
      );
    }

    startPollingForSender(who.id, who.name);
    res.json({ ok: true, session_id: sessionId, user_id: who.id });
  } catch (e) {
    console.error("share/start error:", e.message);
    res.status(500).json({ error: "share_start_failed" });
  }
});

app.post("/share/stop", async (req, res) => {
  try {
    const who = await getCurrentSpotifyId(req, res);
    if (who.error) return res.status(who.error.status || 401).json(who.error.body || { error: "no_me" });

    await pool.query(`UPDATE sessions SET is_active = false WHERE sender_spotify_id = $1`, [who.id]);
    // stopPollingForSender: WS playstate=false, session/ended, Pause für Follower, Push
    await stopPollingForSender(who.id, who.name, "manual");
    res.json({ ok: true });
  } catch (e) {
    console.error("share/stop error:", e.message);
    res.status(500).json({ error: "share_stop_failed" });
  }
});

/* -------------------- Buffer-Feed für Receiver ------------------ */
// Liefert Events eines Senders nach ID (aufwärts), optional mit LAG (ms),
// damit Receiver „versetzt“ abspielen können.
// GET /events/since?sender_id=...&after_id=0&limit=100&lag_ms=800
app.get("/events/since", async (req, res) => {
  try {
    const senderId = String(req.query.sender_id || "").trim();
    if (!senderId) return res.status(400).json({ error: "missing_sender_id" });
    const afterId = Number(req.query.after_id || 0);
    const limit = Math.min(500, Math.max(1, Number(req.query.limit || 100)));
    const lagMs = Math.max(0, Number(req.query.lag_ms || 800)); // Standard-Puffer 800 ms

    // Relevante Events + Dedupe/Konsolidierung:
    // - nur Kanten bei playstate (Änderung von is_playing)
    // - seek-Events pro Track auf >=600ms Abstand drosseln
    const { rows } = await pool.query(
      `
      WITH base AS (
        SELECT id, sender_id, kind, track_id, progress_ms, is_playing, name, artists, image,
               ts_ms, created_at
        FROM public.playback_events
        WHERE sender_id = $1
          AND id > $2
          AND kind IN ('trackchange','seek','playstate')
          AND created_at <= now() - ($3::text || ' milliseconds')::interval
        ORDER BY id ASC
      ),
      dedup_playstate AS (
        SELECT *
        FROM (
          SELECT b.*,
                 LAG(is_playing) OVER (ORDER BY id)         AS prev_is_playing,
                 LAG(kind)       OVER (ORDER BY id)         AS prev_kind
          FROM base b
        ) t
        WHERE
          -- immer behalten:
          kind <> 'playstate'
          -- nur Play/Pause-Kanten (echter Wechsel):
          OR (prev_is_playing IS DISTINCT FROM is_playing)
      ),
      thin_seek AS (
        SELECT *
        FROM (
          SELECT d.*,
                 /* letzter Seek-Zeitstempel pro Track (vor aktueller Zeile) */
                 MAX(CASE WHEN kind = 'seek' THEN ts_ms END)
                   OVER (
                     PARTITION BY track_id
                     ORDER BY id
                     ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                   ) AS prev_seek_ts
          FROM dedup_playstate d
        ) s
        WHERE
          -- nur seeks mit >=600ms Abstand, alles andere durchlassen
          kind <> 'seek' OR prev_seek_ts IS NULL OR (ts_ms - prev_seek_ts) > 600
      )
      SELECT 
        id,
        sender_id,
        kind,
        track_id      AS "trackId",
        progress_ms,
        is_playing,
        name,
        artists,
        image,
        ts_ms,
        created_at
      FROM thin_seek
      ORDER BY id ASC
      LIMIT $4
      `,
      [senderId, afterId, lagMs, limit]
    );
    console.log(
      "[EVT][SINCE] sender=" + senderId +
      " after_id=" + afterId +
      " lag=" + lagMs + "ms -> " + rows.length + " evts"
    );
    res.json({
      ok: true,
      after_id: afterId,
      count: rows.length,
      events: rows,
    });
  } catch (e) {
    console.error("/events/since error:", e.message);
    res.status(500).json({ error: "events_since_failed" });
  }
});


/* -------------------- Sender-Status (aktueller Track, inkl. Metadaten) --- */
app.get("/sender/current", async (req, res) => {
  try {
    const senderId = (req.query.id || "").toString().trim();
    if (!senderId) return res.status(400).json({ error: "missing_sender_id" });

    // Live vom Sender-Account abfragen – robust, auch wenn Poller-State älter ist
    const at = await getFreshTokenForUser(senderId);
    const r = await spotifyGet("https://api.spotify.com/v1/me/player/currently-playing", at);

    if (r.status === 204 || !r.data || !r.data.item) {
      return res.json({ ok: true, track: null });
    }

    const data = r.data;
    const item = data.item;
    return res.json({
      ok: true,
      track: {
        id: item.id,
        name: item.name,
        artists: (item.artists || []).map(a => a.name),
        album: { images: item.album?.images || [] },
        progress_ms: data.progress_ms || 0,
        is_playing: !!data.is_playing,
      },
    });
  } catch (e) {
    console.error("sender/current error:", e.message);
    return res.status(500).json({ error: "sender_current_failed" });
  }
});


/* -------------------- Static + SPA Fallback -------------- */
app.use(express.static(path.join(__dirname, "public")));
app.get("*", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

/* -------------------- WebSocket -------------------------- */
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

wss.on("connection", (ws) => {
  ws.on("message", (raw) => {
    let data;
    try { data = JSON.parse(raw); } catch { return; }

    // Präsenz-Tracking: Sender meldet sich mit hello
    if (data?.type === "hello" && data?.userId) {
     ws.__userId = String(data.userId);
      if (!socketsByUser.has(ws.__userId)) socketsByUser.set(ws.__userId, new Set());
      socketsByUser.get(ws.__userId).add(ws);
      // falls es einen App-Gone-Timer gab → löschen
      const t = appGoneTimers.get(ws.__userId);
      if (t) { clearTimeout(t); appGoneTimers.delete(ws.__userId); }
    }

    // optional: Keepalive vom Client
    if (data?.type === "app_alive" && data?.userId && ws.__userId === String(data.userId)) {
      // nichts zu tun – die Existenz der offenen WS-Verbindung reicht als Präsenzsignal
    }
    wss.clients.forEach((client) => {
      if (client !== ws && client.readyState === WebSocket.OPEN) {
        try { client.send(JSON.stringify(data)); } catch {}
      }
    });

    if (data?.type === "follow" && data.followingUserId) {
      const count = Array.from(wss.clients).filter((c) => c !== ws && c.readyState === WebSocket.OPEN).length;
      try { ws.send(JSON.stringify({ type: "listener_count", count, ts: Date.now() })); } catch {}
    }
  });
  // 💡 NEU: erkennen, wenn App (WS) geschlossen wurde → Session nach Grace beenden
  ws.on("close", () => {
    const uid = ws.__userId ? String(ws.__userId) : null;
    if (!uid) return;

    // Verbindung aus Registry entfernen
    const set = socketsByUser.get(uid);
    if (set) {
      set.delete(ws);
      if (set.size === 0) socketsByUser.delete(uid);
    }

    const stillOnline = socketsByUser.get(uid)?.size > 0;
    if (!stillOnline) {
      if (appGoneTimers.has(uid)) { clearTimeout(appGoneTimers.get(uid)); }
      const t = setTimeout(async () => {
        appGoneTimers.delete(uid);
        try {
          const r = await pool.query(`SELECT display_name FROM users WHERE id = $1`, [uid]);
          const name = r.rows?.[0]?.display_name || uid;
          await stopPollingForSender(uid, name, "app_closed");
        } catch (e) {
          console.warn("[WS] app_closed stopPolling failed:", e?.message || e);
        }
      }, 0); // keine Gnadenfrist mehr – sofortiges Session-Ende bei App-Schließen
      appGoneTimers.set(uid, t);
    }
  });
});


/* -------------------- Start ------------------------------ */
const PORT = process.env.PORT || 3001;
server.listen(PORT, () => {
  console.log(`✅ Celebeaty listening on :${PORT}`);
});
