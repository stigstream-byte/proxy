/**
 * Endpoint encryption — AES-256-GCM
 *
 * Packs the proxy target (endpoint + url + optional headers) into a single
 * opaque, URL-safe token so a request looks like `/<TOKEN>` instead of
 * `/m3u8-proxy?url=…`. GCM is authenticated: the 16-byte tag makes tokens
 * unforgeable, so a client cannot craft a token for an arbitrary URL — only
 * this server (holder of the key) can mint valid ones. That doubles as SSRF
 * protection for the open-proxy routes.
 *
 * Native `crypto` only — no deps, microsecond-scale per token.
 */

import * as crypto from 'crypto';

const ALGO    = 'aes-256-gcm';
const IV_LEN  = 12; // 96-bit nonce, the GCM standard
const TAG_LEN = 16; // 128-bit auth tag

// ---------------------------------------------------------------------------
// Key — from PROXY_ENCRYPT_KEY. Accepts a 64-char hex string (a raw 32-byte
// key) or any passphrase (hashed to 32 bytes via SHA-256).
// ---------------------------------------------------------------------------

function loadKey(): Buffer {
  const raw = process.env.PROXY_ENCRYPT_KEY;
  if (!raw) throw new Error('PROXY_ENCRYPT_KEY is not set — cannot encrypt endpoints');
  if (/^[0-9a-fA-F]{64}$/.test(raw)) return Buffer.from(raw, 'hex');
  return crypto.createHash('sha256').update(raw, 'utf8').digest();
}

const KEY = loadKey();

// ---------------------------------------------------------------------------
// Payload — what a token decrypts to
// ---------------------------------------------------------------------------

export interface ProxyPayload {
  e: string;  // endpoint, e.g. '/m3u8-proxy'
  u: string;  // absolute target url
  h?: string; // optional headers, as the raw JSON string clients already pass
}

// ---------------------------------------------------------------------------
// encrypt / decrypt
// ---------------------------------------------------------------------------

export function encrypt(payload: ProxyPayload): string {
  const iv     = crypto.randomBytes(IV_LEN);
  const cipher = crypto.createCipheriv(ALGO, KEY, iv);
  const pt     = Buffer.from(JSON.stringify(payload), 'utf8');
  const ct     = Buffer.concat([cipher.update(pt), cipher.final()]);
  const tag    = cipher.getAuthTag();
  // iv | tag | ciphertext  →  base64url (path-safe: A-Z a-z 0-9 - _)
  return Buffer.concat([iv, tag, ct]).toString('base64url');
}

export function decrypt(token: string): ProxyPayload | null {
  try {
    const buf = Buffer.from(token, 'base64url');
    if (buf.length < IV_LEN + TAG_LEN + 2) return null;

    const iv  = buf.subarray(0, IV_LEN);
    const tag = buf.subarray(IV_LEN, IV_LEN + TAG_LEN);
    const ct  = buf.subarray(IV_LEN + TAG_LEN);

    const decipher = crypto.createDecipheriv(ALGO, KEY, iv);
    decipher.setAuthTag(tag);
    const pt = Buffer.concat([decipher.update(ct), decipher.final()]); // throws on bad tag

    const payload = JSON.parse(pt.toString('utf8')) as ProxyPayload;
    if (typeof payload?.e !== 'string' || typeof payload?.u !== 'string') return null;
    return payload;
  } catch {
    return null; // bad token, tampered, wrong key, or malformed JSON
  }
}
