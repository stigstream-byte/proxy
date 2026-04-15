/**
 * Lightweight Streaming Proxy Server
 * Forwards requests to upstream URLs, bypassing CORS.
 * Rewrites M3U8 playlists so all segment/playlist URLs stay proxied.
 *
 * Performance notes:
 *  - /ts-proxy uses native http/https.request() piped directly into the
 *    Express response — zero-copy, no library overhead, connection reuse
 *    via the shared keep-alive agents.
 *  - All routes share the same two keep-alive agents (http + https), so
 *    every route benefits from connection pooling automatically.
 *  - cluster forks one worker per logical CPU; TCP_NODELAY set per socket.
 *
 * No undici dependency — native Node.js only.
 */

import * as dotenv from 'dotenv';
dotenv.config();

import cluster from 'cluster';
import * as os from 'os';
import express, { Request, Response, NextFunction } from 'express';
import fetch from 'node-fetch';
import * as http from 'http';
import * as https from 'https';

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
} else {

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const IS_DEV  = (process.env.NODE_ENV ?? 'development') !== 'production';
const PORT    = process.env.PORT ? parseInt(process.env.PORT, 10) : IS_DEV ? 3003 : 3000;

// Sized for 2 cores: HLS players open 2–6 parallel fetches per worker.
// 64 sockets per agent is more than enough headroom.
const POOL_SIZE = 64;

// ---------------------------------------------------------------------------
// Shared keep-alive agents — used by every route.
//
// A single agent per protocol is sufficient; Node's http.Agent multiplexes
// all requests to the same origin over its pooled sockets automatically.
// fifo scheduling spreads load evenly so sockets don't idle and close.
// ---------------------------------------------------------------------------

const httpAgent  = new http.Agent({ keepAlive: true, maxSockets: POOL_SIZE, scheduling: 'fifo' });
const httpsAgent = new https.Agent({ keepAlive: true, maxSockets: POOL_SIZE, scheduling: 'fifo' });

function agentFor(url: string): http.Agent | https.Agent {
  return url.startsWith('https') ? httpsAgent : httpAgent;
}

function modFor(url: string): typeof http | typeof https {
  return url.startsWith('https') ? https : http;
}

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
  'content-encoding',    // node decompresses; forwarding causes double-decompress
  'transfer-encoding',
  'content-length',      // <-- ADD THIS
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

    // Init segment for fMP4/CMAF streams — must go through /ts-proxy.
    if (t.startsWith('#EXT-X-MAP')) {
      const rewritten = t.replace(/URI="([^"]+)"/g, (_match, href) =>
        `URI="${proxied(href, '/ts-proxy')}"`
      );
      out.push(rewritten);
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
// Fast TS-proxy handler — native http/https, direct pipe, one retry
// ---------------------------------------------------------------------------
//
// http.request() with the keep-alive agent reuses open TCP connections to
// the same CDN origin, giving the same pooling benefit as undici with zero
// extra dependencies. The upstream response body is piped straight into the
// Express response — no intermediate buffering, backpressure handled by Node.
//
// Timeouts:
//   headersTimeout: 20s — aborts if the CDN doesn't respond in time
//   bodyTimeout:    30s — aborts a stalled mid-stream transfer
//
// One retry fires before headers are sent so the player never sees the error.

function tsProxyAttempt(
  targetUrl: string,
  upstreamHeaders: Record<string, string>,
  res: Response,
): Promise<void> {
  return new Promise((resolve, reject) => {
    const parsed = new URL(targetUrl);
    const mod    = modFor(targetUrl);
    const agent  = agentFor(targetUrl);

    const req = mod.request(
      {
        hostname: parsed.hostname,
        port:     parsed.port || (parsed.protocol === 'https:' ? 443 : 80),
        path:     parsed.pathname + parsed.search,
        method:   'GET',
        headers:  upstreamHeaders,
        agent,
      },
      (upstream) => {
        // Cancel the headers timeout now that we have a response
        req.socket?.setTimeout(0);

        res.statusCode = upstream.statusCode ?? 502;

        for (const [key, value] of Object.entries(upstream.headers)) {
          if (!value) continue;
          if (STRIP_RESPONSE_HEADERS.has(key.toLowerCase())) continue;
          res.setHeader(key, value as string | string[]);
        }

        upstream.pipe(res);

        // Body stall guard — 30s without new data triggers an abort
        let bodyTimer = setTimeout(() => {
          req.destroy(new Error('Body timeout'));
        }, 30_000);

        upstream.on('data', () => {
          clearTimeout(bodyTimer);
          bodyTimer = setTimeout(() => req.destroy(new Error('Body timeout')), 30_000);
        });

        upstream.on('end',   () => { clearTimeout(bodyTimer); resolve(); });
        upstream.on('error', (err) => { clearTimeout(bodyTimer); reject(err); });
        res.on('error',      () => { clearTimeout(bodyTimer); req.destroy(); resolve(); });
      },
    );

    // Headers timeout — 20s to first byte of response headers
    req.setTimeout(20_000, () => req.destroy(new Error('Headers timeout')));

    req.on('error', reject);
    req.end();
  });
}

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

  req.on('close', () => { if (!res.writableEnded) res.destroy(); });

  try {
    await tsProxyAttempt(targetUrl, upstreamHeaders, res);
  } catch (e: any) {
    if (e?.code === 'UND_ERR_ABORTED' || e?.name === 'AbortError') return;

    // One retry before giving up — prevents ABR quality ladder-down on
    // transient CDN hiccups. Only safe to retry if nothing has been sent yet.
    if (!res.headersSent) {
      try {
        await tsProxyAttempt(targetUrl, upstreamHeaders, res);
        return;
      } catch (retryErr: any) {
        if (retryErr?.code === 'UND_ERR_ABORTED' || retryErr?.name === 'AbortError') return;
        console.error('[ts-proxy retry failed]', retryErr?.message);
        if (!res.headersSent) res.status(502).json({ error: 'Upstream fetch failed', message: retryErr?.message });
        return;
      }
    }

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

  const agent      = agentFor(targetUrl);
  const controller = new AbortController();
  res.on('close', () => controller.abort());

  try {
    const upstream = await fetch(targetUrl, {
      headers: upstreamHeaders,
      signal:  controller.signal as any,
      agent,
    });

    const isM3U8Route   = req.path === '/m3u8-proxy';
    const contentType   = upstream.headers.get('content-type') || '';
    const looksLikeM3U8 = isM3U8Route || contentType.includes('mpegurl') || targetUrl.includes('.m3u8');

    if (looksLikeM3U8 && upstream.ok) {
      const buf = await upstream.arrayBuffer();
      const text = Buffer.from(buf).toString("utf8");
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
app.disable('x-powered-by');

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
app.get('/mp4-proxy',  proxy);

// Fast segment handler — native http/https, keep-alive pooling, direct pipe
app.get('/ts-proxy',   tsProxy);

// ---------------------------------------------------------------------------
// Start
// ---------------------------------------------------------------------------

const server = http.createServer(app);

server.maxConnections = 10_000;

server.on('connection', (socket) => {
  socket.setNoDelay(true);
});

server.listen(PORT, () => {
  console.log(`Worker ${process.pid} — proxy on port ${PORT} [${IS_DEV ? 'development' : 'production'}]`);
});
} // end cluster worker