/**
 * Lightweight Streaming Proxy Server
 * Forwards requests to upstream URLs, bypassing CORS.
 * Rewrites M3U8 playlists so all segment/playlist URLs stay proxied.
 *
 * Performance notes:
 *  - /ts-proxy uses a dedicated lean handler (undici pipeline) — no M3U8
 *    detection, no node-fetch wrapper, streams piped with a large buffer.
 *  - All other routes still use node-fetch (fine, they're low-volume).
 *  - cluster forks one worker per CPU core so all cores serve traffic.
 *  - Keep-alive pools are sized explicitly; TCP_NODELAY is set per socket.
 */

import * as dotenv from 'dotenv';
dotenv.config();

import cluster from 'cluster';
import * as os from 'os';
import express, { Request, Response, NextFunction } from 'express';
import fetch from 'node-fetch';
import * as http from 'http';
import * as https from 'https';
import { stream as undiciStream } from 'undici'; // npm i undici

// ---------------------------------------------------------------------------
// Cluster — primary forks one worker per logical CPU
// ---------------------------------------------------------------------------

const NUM_CPUS = os.availableParallelism?.() ?? os.cpus().length;

if (cluster.isPrimary) {
  console.log(`Primary ${process.pid} starting ${NUM_CPUS} workers`);
  for (let i = 0; i < NUM_CPUS; i++) cluster.fork();
  cluster.on('exit', (worker) => {
    console.warn(`Worker ${worker.process.pid} died — restarting`);
    cluster.fork();
  });
  // Primary stays alive — it owns the IPC channel to each worker.
  // Calling process.exit() here sends SIGHUP to all workers, killing them.
} else {

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const IS_DEV  = (process.env.NODE_ENV ?? 'development') !== 'production';
const PORT    = process.env.PORT ? parseInt(process.env.PORT, 10) : IS_DEV ? 3003 : 3000;

// Large-ish pool — tune to expected concurrent segment requests.
// HLS players typically open 2–6 parallel segment fetches.
const POOL_SIZE = 128;

// ---------------------------------------------------------------------------
// Keep-alive agents for node-fetch (m3u8 / misc routes)
// ---------------------------------------------------------------------------

const httpAgent  = new http.Agent({ keepAlive: true, maxSockets: POOL_SIZE, scheduling: 'lifo' });
const httpsAgent = new https.Agent({ keepAlive: true, maxSockets: POOL_SIZE, scheduling: 'lifo' });

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
      /^127\./.test(h)  || /^10\./.test(h) ||
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
  'content-encoding',    // undici/node-fetch decompress; forwarding causes double-decompress
  'transfer-encoding',
]);

// ---------------------------------------------------------------------------
// M3U8 rewriter
// ---------------------------------------------------------------------------

function rewriteM3U8(content: string, baseUrl: string, proxyBase: string, headersParam: string): string {
  function abs(href: string): string {
    try { return new URL(href, baseUrl).href; } catch { return href; }
  }
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
    if (!t) { out.push(line); continue; }

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

    if (t.startsWith('#') && t.includes('URI="')) {
      const rewritten = t.replace(/URI="([^"]+)"/g, (_match, href) => {
        const isPlaylist = href.includes('.m3u8') || href.includes('/playlist') || href.includes('/master');
        return `URI="${proxied(href, isPlaylist ? '/m3u8-proxy' : '/fetch')}"`;
      });
      out.push(rewritten);
      continue;
    }

    if (!t.startsWith('#')) {
      out.push(proxied(t, '/ts-proxy'));
      continue;
    }

    out.push(line);
  }

  return out.join('\n');
}

// ---------------------------------------------------------------------------
// Fast TS-proxy handler — undici stream(), no M3U8 logic
// ---------------------------------------------------------------------------
//
// undici.stream() feeds the upstream response body directly into the Express
// response (a Writable). There's no intermediate Buffer accumulation and no
// event-loop overhead that node-fetch's ReadableStream adapter introduces.
// The factory function sets status + headers, then returns `res` as the
// writable destination — undici handles backpressure automatically.

async function tsProxy(req: Request, res: Response): Promise<void> {
  const targetUrl = req.query.url as string | undefined;
  if (!targetUrl) { res.status(400).json({ error: 'Missing url parameter' }); return; }

  const validationError = validateUrl(targetUrl);
  if (validationError) { res.status(400).json({ error: validationError }); return; }

  let customHeaders: Record<string, string> = {};
  const headersParam = req.query.headers as string | undefined;
  if (headersParam) {
    try { customHeaders = JSON.parse(headersParam); } catch { /* ignore */ }
  }

  const upstreamHeaders: Record<string, string> = { ...customHeaders };
  const range = req.headers['range'];
  if (range) upstreamHeaders['range'] = range;

  req.on('close', () => {
    if (!res.writableEnded) res.destroy();
  });

  try {
    await undiciStream(
      targetUrl,
      {
        method: 'GET',
        headers: upstreamHeaders,
        // bodyTimeout: 0 — don't abort mid-stream on slow segments
        bodyTimeout: 0,
        headersTimeout: 10_000,
      },
      ({ statusCode, headers }) => {
        res.statusCode = statusCode;

        // Forward upstream headers, stripping the ones we own
        for (const [key, value] of Object.entries(headers)) {
          if (value === undefined) continue;
          if (STRIP_RESPONSE_HEADERS.has(key.toLowerCase())) continue;
          res.setHeader(key, value as string | string[]);
        }

        // Return `res` as the writable destination; undici pipes body into it
        return res;
      },
    );
  } catch (e: any) {
    if (e?.code === 'UND_ERR_ABORTED' || e?.name === 'AbortError') return;
    console.error('[ts-proxy error]', e?.message);
    if (!res.headersSent) res.status(502).json({ error: 'Upstream fetch failed', message: e?.message });
  }
}

// ---------------------------------------------------------------------------
// Generic proxy handler (node-fetch) — used by all other routes
// ---------------------------------------------------------------------------

async function proxy(req: Request, res: Response): Promise<void> {
  const targetUrl = req.query.url as string | undefined;
  if (!targetUrl) { res.status(400).json({ error: 'Missing url parameter' }); return; }

  const validationError = validateUrl(targetUrl);
  if (validationError) { res.status(400).json({ error: validationError }); return; }

  let customHeaders: Record<string, string> = {};
  const headersParam = req.query.headers as string | undefined;
  if (headersParam) {
    try { customHeaders = JSON.parse(headersParam); } catch { /* ignore */ }
  }

  const upstreamHeaders: Record<string, string> = { ...customHeaders };
  const range = req.headers['range'];
  if (range) upstreamHeaders['range'] = range;

  const agent  = targetUrl.startsWith('https') ? httpsAgent : httpAgent;
  const controller = new AbortController();
  req.on('close', () => controller.abort());

  try {
    const upstream = await fetch(targetUrl, {
      headers: upstreamHeaders,
      signal:  controller.signal as any,
      agent,
    });

    const isM3U8Route  = req.path === '/m3u8-proxy';
    const contentType  = upstream.headers.get('content-type') || '';
    const looksLikeM3U8 = isM3U8Route || contentType.includes('mpegurl') || targetUrl.includes('.m3u8');

    if (looksLikeM3U8 && upstream.ok) {
      const text = await upstream.text();
      if (text.includes('#EXTM3U')) {
        const proto     = req.get('x-forwarded-proto') || req.protocol;
        const proxyBase = `${proto}://${req.get('host')}`;
        const baseUrl   = targetUrl.substring(0, targetUrl.lastIndexOf('/') + 1);
        const rewritten = rewriteM3U8(text, baseUrl, proxyBase, headersParam || '');

        res.status(upstream.status);
        res.setHeader('Content-Type', 'application/vnd.apple.mpegurl');
        res.send(rewritten);
        return;
      }
      res.status(upstream.status);
      upstream.headers.forEach((value, key) => {
        if (!STRIP_RESPONSE_HEADERS.has(key.toLowerCase())) res.setHeader(key, value);
      });
      res.send(text);
      return;
    }

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
// Express app
// ---------------------------------------------------------------------------

const app = express();
app.set('trust proxy', true);
app.disable('x-powered-by');  // minor: saves a header write per response

// CORS — preflight handled first
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
// Routes
// ---------------------------------------------------------------------------

app.get('/health',     (_req, res) => res.json({ status: 'ok', timestamp: new Date().toISOString() }));
app.get('/proxy',      proxy);
app.get('/m3u8-proxy', proxy);
app.get('/fetch',      proxy);
app.get('/subtitle',   proxy);
app.get('/mp4-proxy',  proxy);  // could also use tsProxy if segments are large

// Dedicated fast handler — no M3U8 detection, undici pipeline, 256 KB buffers
app.get('/ts-proxy',   tsProxy);

// ---------------------------------------------------------------------------
// Start
// ---------------------------------------------------------------------------

const server = http.createServer(app);

// Raise the number of simultaneous keep-alive connections the server will hold
server.maxConnections = 10_000;

// Disable Nagle on every incoming socket — reduces latency for small writes
server.on('connection', (socket) => {
  socket.setNoDelay(true);
});

server.listen(PORT, () => {
  console.log(`Worker ${process.pid} — proxy on port ${PORT} [${IS_DEV ? 'development' : 'production'}]`);
});
} // end cluster worker