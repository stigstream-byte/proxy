/**
 * Lightweight Streaming Proxy Server
 * Forwards requests to upstream URLs, bypassing CORS.
 */

import * as dotenv from 'dotenv';
dotenv.config();

import express, { Request, Response, NextFunction } from 'express';
import fetch from 'node-fetch';
import * as http from 'http';
import * as https from 'https';

const app = express();
app.set('trust proxy', true);

const IS_DEV   = (process.env.NODE_ENV ?? 'development') !== 'production';
const PORT: number = process.env.PORT ? parseInt(process.env.PORT, 10) : IS_DEV ? 3003 : 3000;

// ---------------------------------------------------------------------------
// Keep-alive agents — reuse TCP/TLS connections across requests
// ---------------------------------------------------------------------------

const httpAgent  = new http.Agent({ keepAlive: true });
const httpsAgent = new https.Agent({ keepAlive: true });

// ---------------------------------------------------------------------------
// CORS — must be first
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
// Headers to strip from the upstream response before forwarding to client
// ---------------------------------------------------------------------------

const STRIP_RESPONSE_HEADERS = new Set([
  'access-control-allow-origin',
  'access-control-allow-headers',
  'access-control-allow-methods',
  'access-control-expose-headers',
  'content-encoding',   // node-fetch decompresses; forwarding causes double-decompress
  'transfer-encoding',
]);

// ---------------------------------------------------------------------------
// Core proxy handler — used by all routes
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

  // Build upstream request headers; pass through Range for video seeking
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

    // Forward status + upstream headers (minus stripped ones)
    res.status(upstream.status);
    upstream.headers.forEach((value, key) => {
      if (!STRIP_RESPONSE_HEADERS.has(key.toLowerCase())) {
        res.setHeader(key, value);
      }
    });

    // Stream body straight through — no buffering
    if (upstream.body) {
      upstream.body.pipe(res);
      upstream.body.on('error', () => res.destroy());
    } else {
      res.end();
    }

  } catch (e: any) {
    if (e?.name === 'AbortError') return; // client disconnected — nothing to do
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