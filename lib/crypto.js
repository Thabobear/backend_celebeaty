// lib/crypto.js
const crypto = require("crypto");

/**
 * Erzeugt ein undurchschaubares Session-Token (Base64URL, 32 Bytes).
 * Dieses Token gibst du dem Client NIE als Cookie aus – falls du httpOnly-Cookies
 * nutzt, landet nur ein kurzer Opaque-String dort. In der DB speichern wir ausschließlich den Hash.
 */
function generateToken(bytes = 32) {
  const raw = crypto.randomBytes(bytes);
  return base64url(raw);
}

/** Hash eines Tokens (SHA-256, Base64URL) – so wird es in der DB gespeichert. */
function hashToken(token) {
  const h = crypto.createHash("sha256").update(token, "utf8").digest();
  return base64url(h);
}

/** Zeitkonstante Gleichheit (gegen Timing-Angriffe). */
function safeEqual(a, b) {
  const ba = Buffer.from(String(a));
  const bb = Buffer.from(String(b));
  if (ba.length !== bb.length) return false;
  return crypto.timingSafeEqual(ba, bb);
}

/** Hilfsfunktionen: Base64URL */
function base64url(buf) {
  return Buffer.from(buf)
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");
}

module.exports = { generateToken, hashToken, safeEqual };
