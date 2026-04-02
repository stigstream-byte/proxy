/**
 * Lightweight Streaming Proxy Server
 * Forwards requests to upstream URLs, bypassing CORS.
 * Rewrites M3U8 playlists so all segment/playlist URLs stay proxied.
 */

import * as dotenv from 'dotenv';
dotenv.config();

import express, { Request, Response, NextFunction } from 'express';
import fetch from 'node-fetch';
import * as http from 'http';
import * as https from 'https';

const app = express();
app.set('trust proxy', true);

const IS_DEV = (process.env.NODE_ENV ?? 'development') !== 'production';
const PORT: number = process.env.PORT ? parseInt(process.env.PORT, 10) : IS_DEV ? 3003 : 3000;

// Keep-alive agents — reuse TCP/TLS connections across requests
const httpAgent  = new http.Agent({ keepAlive: true });
const httpsAgent = new https.Agent({ keepAlive: true });

// ---------------------------------------------------------------------------
// CORS — first middleware, handles preflight immediately
// ---------------------------------------------------------------------------

app.use((_req: Request, res: Response, next: NextFunction) => {
  res.header('Access-Control-Allow-Origin',   '*');
  res.header('Access-Control-Allow-Methods',  '*');
  res.header('Access-Control-Allow-Headers',  '*');
  res.header('Access-Control-Expose-Headers', '*');
  res.header('Timing-Allow-Origin',           '*');
  if (_req.method === 'OPTIONS') return res.sendStatus(200);
  next();
});

// ---------------------------------------------------------------------------
// URL validation — block private/loopback IPs (SSRF protection)
// ---------------------------------------------------------------------------

function validateUrl(raw: string): string | null {
  try {
    const url = new URL(raw);
    if (!['http:', 'https:'].includes(url.protocol)) return 'Only http/https allowed';
    const h = url.hostname.toLowerCase();
    if (
      h === 'localhost' || h === '[::1]' ||
      /^127\./.test(h) || /^10\./.test(h) ||
      /^192\.168\./.test(h) || /^169\.254\./.test(h) ||
      /^172\.(1[6-9]|2\d|3[01])\./.test(h)
    ) return 'Private/reserved IPs not allowed';
    return null;
  } catch {
    return 'Invalid URL';
  }
}

// ---------------------------------------------------------------------------
// Response headers to strip before forwarding to client
// ---------------------------------------------------------------------------

const STRIP_RESPONSE_HEADERS = new Set([
  'access-control-allow-origin',
  'access-control-allow-headers',
  'access-control-allow-methods',
  'access-control-expose-headers',
  'content-encoding',   // node-fetch decompresses; forwarding this causes double-decompress
  'transfer-encoding',
]);

// ---------------------------------------------------------------------------
// M3U8 rewriter — rewrites every URL in a playlist to go through this proxy
// ---------------------------------------------------------------------------

function rewriteM3U8(content: string, baseUrl: string, proxyBase: string, headersParam: string): string {
  // Build absolute URL from a possibly-relative href
  function abs(href: string): string {
    try { return new URL(href, baseUrl).href; } catch { return href; }
  }

  // Wrap an absolute upstream URL in a proxy URL
  function proxied(href: string, endpoint: string): string {
    const absolute = abs(href);
    let qs = `url=${encodeURIComponent(absolute)}`;
    if (headersParam) qs += `&headers=${encodeURIComponent(headersParam)}`;
    return `${proxyBase}${endpoint}?${qs}`;
  }

  const lines = content.split('\n');
  const out: string[] = [];
  let nextLineIsSegment = false;

  for (const line of lines) {
    const t = line.trim();

    // Blank lines pass through
    if (!t) { out.push(line); continue; }

    // After #EXT-X-STREAM-INF, the next non-comment line is a variant playlist URL
    if (nextLineIsSegment && !t.startsWith('#')) {
      out.push(proxied(t, '/m3u8-proxy'));
      nextLineIsSegment = false;
      continue;
    }

    if (t.startsWith('#EXT-X-STREAM-INF')) {
      out.push(line);
      nextLineIsSegment = true;
      continue;
    }

    // Rewrite URI="..." attributes in tags like #EXT-X-KEY, #EXT-X-MAP, #EXT-X-MEDIA, etc.
    if (t.startsWith('#') && t.includes('URI="')) {
      const rewritten = t.replace(/URI="([^"]+)"/g, (_match, href) => {
        const isPlaylist = href.includes('.m3u8') || href.includes('/playlist') || href.includes('/master');
        return `URI="${proxied(href, isPlaylist ? '/m3u8-proxy' : '/fetch')}"`;
      });
      out.push(rewritten);
      continue;
    }

    // Non-comment, non-empty lines that aren't a variant URL are segment URLs
    if (!t.startsWith('#')) {
      out.push(proxied(t, '/ts-proxy'));
      continue;
    }

    out.push(line);
  }

  return out.join('\n');
}

// ---------------------------------------------------------------------------
// Core proxy handler
// ---------------------------------------------------------------------------

async function proxy(req: Request, res: Response): Promise<void> {
  const targetUrl = req.query.url as string | undefined;
  if (!targetUrl) { res.status(400).json({ error: 'Missing url parameter' }); return; }

  const validationError = validateUrl(targetUrl);
  if (validationError) { res.status(400).json({ error: validationError }); return; }

  // Optional custom headers passed as ?headers=<JSON>
  let customHeaders: Record<string, string> = {};
  const headersParam = req.query.headers as string | undefined;
  if (headersParam) {
    try { customHeaders = JSON.parse(headersParam); } catch { /* ignore malformed */ }
  }

  const upstreamHeaders: Record<string, string> = { ...customHeaders };
  const range = req.headers['range'];
  if (range) upstreamHeaders['range'] = range;

  const agent = targetUrl.startsWith('https') ? httpsAgent : httpAgent;

  const controller = new AbortController();
  req.on('close', () => controller.abort());

  try {
    const upstream = await fetch(targetUrl, {
      headers: upstreamHeaders,
      signal:  controller.signal as any,
      agent,
    });

    // For M3U8 routes, buffer and rewrite the playlist before sending
    const isM3U8Route = req.path === '/m3u8-proxy';
    const contentType = upstream.headers.get('content-type') || '';
    const looksLikeM3U8 = isM3U8Route || contentType.includes('mpegurl') || targetUrl.includes('.m3u8');

    if (looksLikeM3U8 && upstream.ok) {
      const text = await upstream.text();
      if (text.includes('#EXTM3U')) {
        const proto    = req.get('x-forwarded-proto') || req.protocol;
        const proxyBase = `${proto}://${req.get('host')}`;
        const baseUrl   = targetUrl.substring(0, targetUrl.lastIndexOf('/') + 1);
        const rewritten = rewriteM3U8(text, baseUrl, proxyBase, headersParam || '');

        res.status(upstream.status);
        res.setHeader('Content-Type', 'application/vnd.apple.mpegurl');
        res.send(rewritten);
        return;
      }
      // Not actually an M3U8 — fall through and send as-is
      res.status(upstream.status);
      upstream.headers.forEach((value, key) => {
        if (!STRIP_RESPONSE_HEADERS.has(key.toLowerCase())) res.setHeader(key, value);
      });
      res.send(text);
      return;
    }

    // Everything else: stream straight through
    res.status(upstream.status);
    upstream.headers.forEach((value, key) => {
      if (!STRIP_RESPONSE_HEADERS.has(key.toLowerCase())) res.setHeader(key, value);
    });

    if (upstream.body) {
      upstream.body.pipe(res);
      upstream.body.on('error', () => res.destroy());
    } else {
      res.end();
    }

  } catch (e: any) {
    if (e?.name === 'AbortError') return;
    console.error('[proxy error]', e?.message);
    if (!res.headersSent) res.status(502).json({ error: 'Upstream fetch failed', message: e?.message });
  }
}

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------

app.get('/health',     (_req, res) => res.json({ status: 'ok', timestamp: new Date().toISOString() }));
app.get('/proxy',      proxy);
app.get('/m3u8-proxy', proxy);
app.get('/ts-proxy',   proxy);
app.get('/mp4-proxy',  proxy);
app.get('/fetch',      proxy);
app.get('/subtitle',   proxy);

// ---------------------------------------------------------------------------
// Start
// ---------------------------------------------------------------------------

app.listen(PORT, () => {
  console.log(`Proxy running on port ${PORT} [${IS_DEV ? 'development' : 'production'}]`);
});