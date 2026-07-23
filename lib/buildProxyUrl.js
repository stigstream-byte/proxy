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
function getRequestBaseUrl(req) {
  const forwardedProto = req.headers?.['x-forwarded-proto'] || req.headers?.['x-forwarded-protocol'];
  const forwardedHost = req.headers?.['x-forwarded-host'] || req.headers?.['host'];
  const proto = forwardedProto
    ? String(forwardedProto).split(',')[0].trim()
    : req.protocol || 'http';
  const host = forwardedHost
    ? String(forwardedHost).split(',')[0].trim()
    : req.get?.('host') || 'localhost';

  return `${proto}://${host}`;
}

export function buildProxyUrl(req, endpoint, targetUrl, headers) {
  const base = getRequestBaseUrl(req);

  const token = encrypt({
    e: endpoint,
    u: targetUrl,
    ...(headers && Object.keys(headers).length ? { h: JSON.stringify(headers) } : {}),
  });

  return `${base}/x/${token}`;
}
