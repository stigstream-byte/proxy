import { encrypt } from './tokenCrypto.js';

/**
 * Builds a URL back to *this* proxy server, pointing at one of its own
 * endpoints (fetch / ts-proxy / m3u8-proxy) for a given target + headers.
 *
 * Instead of a plaintext `?url=&headers=` query string, the target and
 * headers are sealed into a single encrypted token (see tokenCrypto.js),
 * so rewritten segment/key/nested-playlist URLs neither leak the upstream
 * URL or headers, nor can be tampered with or forged by a client.
 */
export function buildProxyUrl(req, endpoint, targetUrl, headers) {
  const base = `${req.protocol}://${req.get('host')}`;

  const token = encrypt({
    e: endpoint,
    u: targetUrl,
    ...(headers && Object.keys(headers).length ? { h: JSON.stringify(headers) } : {}),
  });

  return `${base}/x/${token}`;
}
