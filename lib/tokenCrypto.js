/**
 * Endpoint encryption — AES-256-GCM
 *
 * Packs a proxy target (endpoint + url + optional headers) into a single
 * opaque, URL-safe token so a request looks like `/x/<TOKEN>` instead of
 * `/m3u8-proxy?url=…`. GCM is authenticated: the 16-byte tag makes tokens
 * unforgeable, so a client cannot craft a token for an arbitrary URL — only
 * this server (holder of the key) can mint valid ones. That doubles as SSRF
 * protection for every URL the proxy itself generates when rewriting a
 * playlist (segments, keys, nested playlists).
 *
 * Native `crypto` only — no deps, microsecond-scale per token.
 */

import crypto from 'node:crypto';

const ALGO = 'aes-256-gcm';
const IV_LEN = 12; // 96-bit nonce, the GCM standard
const TAG_LEN = 16; // 128-bit auth tag

// ---------------------------------------------------------------------------
// Key — from PROXY_ENCRYPT_KEY. Accepts a 64-char hex string (a raw 32-byte
// key) or any passphrase (hashed to 32 bytes via SHA-256). Loaded lazily so
// importing this module doesn't blow up before your .env is loaded.
// ---------------------------------------------------------------------------

let cachedKey = null;

function loadKey() {
  if (cachedKey) return cachedKey;

  const raw = process.env.PROXY_ENCRYPT_KEY;
  if (!raw) throw new Error('PROXY_ENCRYPT_KEY is not set — cannot encrypt endpoints');

  cachedKey = /^[0-9a-fA-F]{64}$/.test(raw)
    ? Buffer.from(raw, 'hex')
    : crypto.createHash('sha256').update(raw, 'utf8').digest();

  return cachedKey;
}

// ---------------------------------------------------------------------------
// Payload — what a token decrypts to
// ---------------------------------------------------------------------------

/**
 * @typedef {Object} ProxyPayload
 * @property {string} e - endpoint, e.g. 'm3u8-proxy'
 * @property {string} u - absolute target url
 * @property {string} [h] - optional headers, as a raw JSON string
 */

// ---------------------------------------------------------------------------
// encrypt / decrypt
// ---------------------------------------------------------------------------

/** @param {ProxyPayload} payload @returns {string} */
export function encrypt(payload) {
  const key = loadKey();
  const iv = crypto.randomBytes(IV_LEN);
  const cipher = crypto.createCipheriv(ALGO, key, iv);
  const pt = Buffer.from(JSON.stringify(payload), 'utf8');
  const ct = Buffer.concat([cipher.update(pt), cipher.final()]);
  const tag = cipher.getAuthTag();
  // iv | tag | ciphertext  ->  base64url (path-safe: A-Z a-z 0-9 - _)
  return Buffer.concat([iv, tag, ct]).toString('base64url');
}

/** @param {string} token @returns {ProxyPayload|null} */
export function decrypt(token) {
  try {
    const key = loadKey();
    const buf = Buffer.from(token, 'base64url');
    if (buf.length < IV_LEN + TAG_LEN + 2) return null;

    const iv = buf.subarray(0, IV_LEN);
    const tag = buf.subarray(IV_LEN, IV_LEN + TAG_LEN);
    const ct = buf.subarray(IV_LEN + TAG_LEN);

    const decipher = crypto.createDecipheriv(ALGO, key, iv);
    decipher.setAuthTag(tag);
    const pt = Buffer.concat([decipher.update(ct), decipher.final()]); // throws on bad tag

    const payload = JSON.parse(pt.toString('utf8'));
    if (typeof payload?.e !== 'string' || typeof payload?.u !== 'string') return null;
    return payload;
  } catch {
    return null; // bad token, tampered, wrong key, or malformed JSON
  }
}
