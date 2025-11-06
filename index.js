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

const app = express();

/* -------------------- Basics -------------------- */
app.set("trust proxy", 1);
app.use(express.json());
app.use(cookieParser());

/* -------------------- DB Pool ------------------- */
const dbUrl = process.env.DATABASE_URL || "";
const useSSL =
  /render\.com|railway\.app|neon\.tech|supabase\.co/i.test(dbUrl) ||
  (process.env.NODE_ENV === "production" && !/localhost|127\.0\.0\.1/.test(dbUrl));
const pool = new Pool({
  connectionString: dbUrl || undefined,
  ssl: useSSL ? { rejectUnauthorized: false } : false,
});

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
  let accessToken =
    (req.headers.authorization || "").replace(/^Bearer\s+/i, "") ||
    req.cookies.sp_at;
  const refreshTokenHeader = req.headers["x-refresh-token"];
  const refreshTokenCookie = req.cookies.sp_rt;
  const refreshToken = refreshTokenHeader || refreshTokenCookie;
  if (!accessToken && !refreshToken) {
    return { error: { status: 401, body: { error: "no_token" } } };
  }
  if (!accessToken && refreshToken) {
    const rr = await refreshAccessToken(refreshToken);
    if (rr.status !== 200) {
      return { error: { status: rr.status, body: rr.data || { error: "refresh_failed" } } };
    }
    accessToken = rr.data.access_token;
    const expires_in = rr.data.expires_in || 3600;
    const base = cookieBase(req);
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

/* -------------------- Auth ---------------------- */
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

app.get("/callback", async (req, res) => {
  const code = req.query.code;
  const stateStr = req.query.state;
  const stateObj = stateStr ? b64uDecode(stateStr) : null;
  const returnTo = stateObj && typeof stateObj.rt === "string" && isAllowedReturnTo(stateObj.rt) ? stateObj.rt : null;

  if (!code) {
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

    const base = cookieBase(req);
    res.cookie("sp_at", access_token, { ...base, maxAge: Math.max(1, (expires_in || 3600) - 30) * 1000 });
    if (refresh_token) res.cookie("sp_rt", refresh_token, { ...base, maxAge: 30 * 24 * 3600 * 1000 });

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

    if (returnTo) {
      const u = new URL(returnTo);
      u.searchParams.set("ok", "1");
      return res.redirect(u.toString());
    }
    const front = (process.env.FRONTEND_URI || getSelfOrigin(req)).replace(/\/+$/, "");
    return res.redirect(`${front}/`);
  } catch (err) {
    console.error("Token exchange failed:", err.response?.data || err.message);
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
app.get("/currently-playing", async (req, res) => {
  try {
    const t = await withValidAccessToken(req, res);
    if (t.error) return res.status(t.error.status).json(t.error.body);
    let r = await spotifyGet("https://api.spotify.com/v1/me/player/currently-playing", t.accessToken);
    if (r.status === 204 || !r.data) return res.json({ message: "Kein Song wird gerade gespielt.", reason: "no_item" });
    return res.json(r.data);
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

/* -------------------- Sessions + Polling ----------------------- */

async function getCurrentSpotifyId(req, res) {
  const t = await withValidAccessToken(req, res);
  if (t.error) return { error: t.error };
  const me = await spotifyGet("https://api.spotify.com/v1/me", t.accessToken);
  if (me.status !== 200) return { error: { status: 401, body: { error: "me_failed" } } };
  return { id: me.data.id, name: me.data.display_name || me.data.id };
}

app.get("/sessions/active", async (req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT s.id AS session_id, s.sender_spotify_id AS user_id,
             COALESCE(u.display_name, s.sender_spotify_id) AS display_name,
             s.created_at, s.last_snapshot_at
      FROM sessions s
      LEFT JOIN users u ON u.id = s.sender_spotify_id
      WHERE s.is_active = true
      ORDER BY COALESCE(s.last_snapshot_at, s.created_at) DESC
      LIMIT 50
    `);
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

const pollers = new Map();

async function getFreshTokenForUser(userId) {
  const uRes = await pool.query(
    `SELECT id, refresh_token_enc, access_token, access_expires_at FROM users WHERE id = $1`,
    [userId]
  );
  if (!uRes.rowCount) throw new Error("user not found");
  const u = uRes.rows[0];
  const now = Date.now();
  const exp = u.access_expires_at ? new Date(u.access_expires_at).getTime() : 0;
  if (u.access_token && exp > now + 30 * 1000) {
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

function broadcastJSON(obj) {
  const msg = JSON.stringify(obj);
  wss.clients.forEach((c) => {
    if (c.readyState === WebSocket.OPEN) {
      try { c.send(msg); } catch {}
    }
  });
}

function startPollingForSender(senderId, senderName) {
  if (pollers.has(senderId)) return;
  const state = { lastTrackId: null, lastIsPlaying: null, lastProgress: 0, lastKeepalive: 0 };
  const timer = setInterval(async () => {
    try {
      const at = await getFreshTokenForUser(senderId);
      const r = await spotifyGet("https://api.spotify.com/v1/me/player/currently-playing", at);
      if (r.status === 204 || !r.data || !r.data.item) return;
      const item = r.data.item;
      const trackId = item.id;
      const progress = r.data.progress_ms || 0;
      const is_playing = !!r.data.is_playing;
      await pool.query(
        `UPDATE sessions SET last_snapshot_at = now() WHERE sender_spotify_id = $1 AND is_active = true`,
        [senderId]
      );
      broadcastJSON({
        type: "track",
        kind: "keepalive",
        user: { id: senderId, name: senderName || senderId },
        trackId,
        progress_ms: progress,
        is_playing,
        ts: Date.now(),
      });
      state.lastTrackId = trackId;
      state.lastIsPlaying = is_playing;
      state.lastProgress = progress;
    } catch {}
  }, 2000);
  pollers.set(senderId, { timer, state });
}

function stopPollingForSender(senderId) {
  const p = pollers.get(senderId);
  if (p) {
    clearInterval(p.timer);
    pollers.delete(senderId);
  }
}

/* ---- Share-Endpoints binden Polling ein ---- */
app.post("/share/start", async (req, res) => {
  try {
    const who = await getCurrentSpotifyId(req, res);
    if (who.error) return res.status(who.error.status || 401).json(who.error.body || { error: "no_me" });

    const rtHeader = (req.headers["x-refresh-token"] || "").toString() || null;
    const atHeader = (req.headers.authorization || "").replace(/^Bearer\s+/i, "") || null;

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
      [who.id, who.name || who.id, rtHeader, atHeader]
    );

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
    stopPollingForSender(who.id);
    broadcastJSON({ type: "session_end", userId: who.id, ts: Date.now() }); // 🧩 NEW: Info an Clients
    res.json({ ok: true });
  } catch (e) {
    console.error("share/stop error:", e.message);
    res.status(500).json({ error: "share_stop_failed" });
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

    // 🧩 NEW: userId merken, falls "hello" gesendet wurde
    if (data?.type === "hello" && data.userId) {
      ws.userId = data.userId;
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

  // 🧩 NEW: Session Cleanup, wenn Socket geschlossen wird
  ws.on("close", async () => {
    if (ws.userId) {
      try {
        await pool.query(`UPDATE sessions SET is_active = false WHERE sender_spotify_id = $1`, [ws.userId]);
        stopPollingForSender(ws.userId);
        broadcastJSON({ type: "session_end", userId: ws.userId, ts: Date.now() });
        console.log("[Cleanup] Session von", ws.userId, "deaktiviert (WS close)");
      } catch (err) {
        console.error("[Cleanup] Fehler beim Session-Cleanup:", err.message);
      }
    }
  });
});

// 🧩 NEW: Automatischer Cleanup alter Sessions
setInterval(async () => {
  try {
    const res = await pool.query(`
      UPDATE sessions
      SET is_active = false
      WHERE is_active = true
        AND (now() - COALESCE(last_snapshot_at, created_at)) > interval '30 minutes'
      RETURNING sender_spotify_id
    `);
    if (res.rowCount > 0) {
      console.log("[Cleanup] Alte Sessions geschlossen:", res.rowCount);
      res.rows.forEach((r) => {
        stopPollingForSender(r.sender_spotify_id);
        broadcastJSON({ type: "session_end", userId: r.sender_spotify_id, ts: Date.now() });
      });
    }
  } catch (e) {
    console.error("[Cleanup] Fehler beim Auto-Cleanup:", e.message);
  }
}, 5 * 60 * 1000);

/* -------------------- Start ------------------------------ */
const PORT = process.env.PORT || 3001;
server.listen(PORT, () => {
  console.log(`✅ Celebeaty listening on :${PORT}`);
});
