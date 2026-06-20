/**
 * Lightweight Streaming Proxy Server (lean build)
 * - Single process (no cluster)
 * - Small keep-alive pools sized for a low-resource VPS
 * - Native http/https throughout — no node-fetch
 * - SSRF protection, CORS, header forwarding, M3U8 rewriting preserved
 */

import * as dotenv from 'dotenv';
dotenv.config();

import express, { Request, Response, NextFunction } from 'express';
import * as http  from 'http';
import * as https from 'https';

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const IS_DEV   = (process.env.NODE_ENV ?? 'development') !== 'production';
const PORT = process.env.PORT ? parseInt(process.env.PORT, 10) : IS_DEV ? 3003 : 3000;

// Small pools — plenty for a personal/low-traffic VPS
const httpAgent  = new http.Agent ({ keepAlive: true, maxSockets: 32, maxFreeSockets: 8 });
const httpsAgent = new https.Agent({ keepAlive: true, maxSockets: 32, maxFreeSockets: 8 });

function agentFor(url: string): http.Agent | https.Agent {
  return url.startsWith('https') ? httpsAgent : httpAgent;
}
function modFor(url: string): typeof http | typeof https {
  return url.startsWith('https') ? https : http;
}

// ---------------------------------------------------------------------------
// Live metrics — in-process counters powering /stats
// ---------------------------------------------------------------------------

const startedAt = Date.now();

interface EndpointMetric { inFlight: number; total: number; errors: number; }

const metrics = {
  inFlight: 0,            // requests being proxied right now (app-level "active connections")
  clientConnections: 0,  // open client TCP sockets (socket-level)
  totalRequests: 0,
  totalErrors: 0,
  byEndpoint: {} as Record<string, EndpointMetric>,
};

// Collapse arbitrary paths down to the known proxy routes so the map stays small.
const KNOWN_ROUTES = new Set([
  '/health', '/stats', '/fetch', '/subtitle', '/mp4-proxy', '/proxy',
  '/m3u8-proxy', '/m3u8-only-proxy', '/ts-proxy',
]);
function endpointKey(path: string): string {
  return KNOWN_ROUTES.has(path) ? path : 'other';
}

// Express middleware: count a request while it's in flight, decrement when it
// finishes or the client disconnects. /stats is skipped so polling it doesn't
// skew the numbers.
function trackRequest(req: Request, res: Response, next: NextFunction): void {
  if (req.path === '/stats') { next(); return; }

  const key = endpointKey(req.path);
  const ep = (metrics.byEndpoint[key] ??= { inFlight: 0, total: 0, errors: 0 });

  metrics.inFlight++; metrics.totalRequests++;
  ep.inFlight++;      ep.total++;

  let settled = false;
  const done = () => {
    if (settled) return;
    settled = true;
    metrics.inFlight--;
    ep.inFlight--;
    if (res.statusCode >= 500) { metrics.totalErrors++; ep.errors++; }
  };
  res.on('finish', done);
  res.on('close', done);
  next();
}

// Snapshot of a keep-alive agent's socket pool (active / idle / queued upstream).
function agentStats(agent: http.Agent) {
  const count = (m: unknown) =>
    m ? Object.values(m as Record<string, unknown[]>).reduce((s, arr) => s + arr.length, 0) : 0;
  const a = agent as unknown as { sockets?: unknown; freeSockets?: unknown; requests?: unknown };
  return {
    activeSockets:  count(a.sockets),
    freeSockets:    count(a.freeSockets),
    queuedRequests: count(a.requests),
  };
}

// ---------------------------------------------------------------------------
// URL validation — SSRF protection
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
// Headers to strip before forwarding to client
// ---------------------------------------------------------------------------

const STRIP_RESPONSE_HEADERS = new Set([
  'access-control-allow-origin',
  'access-control-allow-headers',
  'access-control-allow-methods',
  'access-control-expose-headers',
  'content-encoding',   // node decompresses; re-forwarding causes double-decompress
  'transfer-encoding',
  'content-length',
]);

// Redirect statuses we transparently follow upstream (the client never sees them)
const REDIRECT_STATUSES = new Set([301, 302, 303, 307, 308]);

// ---------------------------------------------------------------------------
// Native proxy — pipes upstream response directly into Express res
// Follows up to `maxRedirects` upstream redirects (off by default).
// ---------------------------------------------------------------------------

function nativeProxy(
  targetUrl: string,
  upstreamHeaders: Record<string, string>,
  res: Response,
  headersTimeoutMs = 15_000,
  bodyTimeoutMs    = 30_000,
  method           = 'GET',
  incomingReq?:    Request,
  maxRedirects     = 0,
): Promise<void> {
  return new Promise((resolve, reject) => {
    const parsed = new URL(targetUrl);
    const upstreamReq = modFor(targetUrl).request(
      {
        hostname: parsed.hostname,
        port:     parsed.port || (parsed.protocol === 'https:' ? 443 : 80),
        path:     parsed.pathname + parsed.search,
        method,
        headers:  upstreamHeaders,
        agent:    agentFor(targetUrl),
      },
      (upstream) => {
        const status = upstream.statusCode ?? 502;

        // ---- Follow upstream redirects transparently ----
        const location = upstream.headers.location;
        if (REDIRECT_STATUSES.has(status) && location && maxRedirects > 0) {
          // Drain this response so the socket can return to the pool
          upstream.resume();

          let nextUrl: string;
          try {
            nextUrl = new URL(Array.isArray(location) ? location[0] : location, targetUrl).href;
          } catch {
            reject(new Error('Invalid redirect Location'));
            return;
          }

          // Re-validate the redirect target — SSRF protection must apply here too
          const ssrf = validateUrl(nextUrl);
          if (ssrf) { reject(new Error(`Blocked redirect target: ${ssrf}`)); return; }

          // 303 always becomes GET; 301/302 conventionally become GET for non-GET/HEAD
          let nextMethod = method;
          if (status === 303 ||
              ((status === 301 || status === 302) && method !== 'GET' && method !== 'HEAD')) {
            nextMethod = 'GET';
          }

          nativeProxy(
            nextUrl, upstreamHeaders, res,
            headersTimeoutMs, bodyTimeoutMs, nextMethod,
            undefined,                 // body (if any) was already consumed on the first hop
            maxRedirects - 1,
          ).then(resolve, reject);
          return;
        }

        res.statusCode = status;

        for (const [key, value] of Object.entries(upstream.headers)) {
          if (value && !STRIP_RESPONSE_HEADERS.has(key.toLowerCase()))
            res.setHeader(key, value as string | string[]);
        }

        // Single body timeout; no per-chunk timer churn
        const bodyTimer = setTimeout(() => upstreamReq.destroy(new Error('Body timeout')), bodyTimeoutMs);

        upstream.pipe(res);
        upstream.on('end',   () => { clearTimeout(bodyTimer); resolve(); });
        upstream.on('error', (e) => { clearTimeout(bodyTimer); reject(e); });
        res.on('error',      () => { clearTimeout(bodyTimer); upstreamReq.destroy(); resolve(); });
      },
    );

    upstreamReq.setTimeout(headersTimeoutMs, () => upstreamReq.destroy(new Error('Headers timeout')));
    upstreamReq.on('error', reject);

    // Pipe request body for methods that carry one
    if (incomingReq && ['POST', 'PUT', 'PATCH'].includes(method.toUpperCase())) {
      incomingReq.pipe(upstreamReq);
    } else {
      upstreamReq.end();
    }
  });
}

// ---------------------------------------------------------------------------
// Parse common query params
// ---------------------------------------------------------------------------

// Headers we never forward upstream (hop-by-hop / proxy-internal)
const STRIP_REQUEST_HEADERS = new Set([
  'host', 'connection', 'keep-alive', 'proxy-authenticate',
  'proxy-authorization', 'te', 'trailers', 'transfer-encoding', 'upgrade',
]);

function parseProxyParams(req: Request): {
  targetUrl: string | undefined;
  upstreamHeaders: Record<string, string>;
  headersParam: string | undefined;
} {
  const targetUrl    = req.query.url as string | undefined;
  const headersParam = req.query.headers as string | undefined;

  // Start with the incoming request headers (minus hop-by-hop)
  const upstreamHeaders: Record<string, string> = {};
  for (const [key, value] of Object.entries(req.headers)) {
    if (STRIP_REQUEST_HEADERS.has(key.toLowerCase())) continue;
    if (Array.isArray(value)) upstreamHeaders[key] = value.join(', ');
    else if (value)           upstreamHeaders[key] = value;
  }

  // Custom headers from ?headers=... override/extend the above
  if (headersParam) {
    try {
      const custom = JSON.parse(headersParam) as Record<string, string>;
      Object.assign(upstreamHeaders, custom);
    } catch { /* ignore malformed */ }
  }

  // Force uncompressed response — we don't decompress, so gzip/brotli
  // bodies would arrive as garbled binary. identity ensures plain text/bytes.
  upstreamHeaders['accept-encoding'] = 'identity';

  return { targetUrl, upstreamHeaders, headersParam };
}

// ---------------------------------------------------------------------------
// General proxy handler — /fetch, /subtitle, /mp4-proxy, /proxy
// ---------------------------------------------------------------------------

async function fetchProxy(req: Request, res: Response): Promise<void> {
  const { targetUrl, upstreamHeaders } = parseProxyParams(req);
  if (!targetUrl) { res.status(400).json({ error: 'Missing url parameter' }); return; }

  const err = validateUrl(targetUrl);
  if (err) { res.status(400).json({ error: err }); return; }

  req.on('close', () => { if (!res.writableEnded) res.destroy(); });

  try {
    await nativeProxy(targetUrl, upstreamHeaders, res, 15_000, 30_000, req.method, req);
  } catch (e: any) {
    if (!res.headersSent) res.status(502).json({ error: 'Upstream error', message: e?.message });
  }
}

// ---------------------------------------------------------------------------
// TS-segment proxy — one retry before giving up
// ---------------------------------------------------------------------------

async function tsProxy(req: Request, res: Response): Promise<void> {
  const { targetUrl, upstreamHeaders } = parseProxyParams(req);
  if (!targetUrl) { res.status(400).json({ error: 'Missing url parameter' }); return; }

  const err = validateUrl(targetUrl);
  if (err) { res.status(400).json({ error: err }); return; }

  req.on('close', () => { if (!res.writableEnded) res.destroy(); });

  try {
    await nativeProxy(targetUrl, upstreamHeaders, res, 20_000, 30_000, req.method, req, 5);
  } catch (e: any) {
    if (e?.name === 'AbortError') return;
    if (!res.headersSent) {
      try {
        await nativeProxy(targetUrl, upstreamHeaders, res, 20_000, 30_000, req.method, req, 5);
      } catch (re: any) {
        if (!res.headersSent) res.status(502).json({ error: 'Upstream error', message: re?.message });
      }
    }
  }
}

// ---------------------------------------------------------------------------
// M3U8 rewriter
// ---------------------------------------------------------------------------

function rewriteM3U8(content: string, baseUrl: string, proxyBase: string, headersParam: string): string {
  function proxied(href: string, endpoint: string): string {
    let abs: string;
    try { abs = new URL(href, baseUrl).href; } catch { abs = href; }
    let qs = `url=${encodeURIComponent(abs)}`;
    if (headersParam) qs += `&headers=${encodeURIComponent(headersParam)}`;
    return `${proxyBase}${endpoint}?${qs}`;
  }

  const out: string[] = [];
  let nextIsSegment = false;

  for (const line of content.split('\n')) {
    const t = line.trim();
    if (!t) { out.push(line); continue; }

    if (nextIsSegment && !t.startsWith('#')) {
      out.push(proxied(t, '/m3u8-proxy'));
      nextIsSegment = false;
      continue;
    }

    if (t.startsWith('#EXT-X-STREAM-INF')) { out.push(line); nextIsSegment = true; continue; }

    if (t.startsWith('#EXT-X-MAP')) {
      out.push(t.replace(/URI="([^"]+)"/g, (_m, h) => `URI="${proxied(h, '/ts-proxy')}"`));
      continue;
    }

    if (t.startsWith('#') && t.includes('URI="')) {
      out.push(t.replace(/URI="([^"]+)"/g, (_m, h) => {
        const isPlaylist = h.includes('.m3u8') || h.includes('/playlist') || h.includes('/master');
        return `URI="${proxied(h, isPlaylist ? '/m3u8-proxy' : '/fetch')}"`;
      }));
      continue;
    }

    out.push(!t.startsWith('#') ? proxied(t, '/ts-proxy') : line);
  }

  return out.join('\n');
}

// ---------------------------------------------------------------------------
// M3U8 proxy — must buffer full body to rewrite URLs
// ---------------------------------------------------------------------------

async function nativeFetch(
  targetUrl: string,
  headers: Record<string, string>,
  timeoutMs = 20_000,
): Promise<{ status: number; headers: http.IncomingMessage['headers']; body: string }> {
  return new Promise((resolve, reject) => {
    const parsed = new URL(targetUrl);
    const req = modFor(targetUrl).request(
      {
        hostname: parsed.hostname,
        port:     parsed.port || (parsed.protocol === 'https:' ? 443 : 80),
        path:     parsed.pathname + parsed.search,
        method:   'GET',
        headers,
        agent:    agentFor(targetUrl),
      },
      (upstream) => {
        const chunks: Buffer[] = [];
        const timer = setTimeout(() => req.destroy(new Error('Timeout')), timeoutMs);
        upstream.on('data',  (c) => chunks.push(c));
        upstream.on('end',   () => { clearTimeout(timer); resolve({ status: upstream.statusCode ?? 502, headers: upstream.headers, body: Buffer.concat(chunks).toString('utf8') }); });
        upstream.on('error', (e) => { clearTimeout(timer); reject(e); });
      },
    );
    req.setTimeout(timeoutMs, () => req.destroy(new Error('Headers timeout')));
    req.on('error', reject);
    req.end();
  });
}

async function m3u8Proxy(req: Request, res: Response): Promise<void> {
  const { targetUrl, upstreamHeaders, headersParam } = parseProxyParams(req);
  if (!targetUrl) { res.status(400).json({ error: 'Missing url parameter' }); return; }

  const err = validateUrl(targetUrl);
  if (err) { res.status(400).json({ error: err }); return; }

  try {
    const upstream = await nativeFetch(targetUrl, upstreamHeaders);
    const ct       = (upstream.headers['content-type'] as string) ?? '';
    const isM3U8   = ct.includes('mpegurl') || targetUrl.includes('.m3u8');

    if (isM3U8 && upstream.body.includes('#EXTM3U')) {
      const proto     = req.get('x-forwarded-proto') || req.protocol;
      const proxyBase = `${proto}://${req.get('host')}`;
      const baseUrl   = targetUrl.substring(0, targetUrl.lastIndexOf('/') + 1);
      res.status(upstream.status);
      res.setHeader('Content-Type', 'application/vnd.apple.mpegurl');
      res.send(rewriteM3U8(upstream.body, baseUrl, proxyBase, headersParam ?? ''));
      return;
    }

    res.status(upstream.status);
    for (const [key, value] of Object.entries(upstream.headers)) {
      if (value && !STRIP_RESPONSE_HEADERS.has(key.toLowerCase()))
        res.setHeader(key, value as string | string[]);
    }
    res.send(upstream.body);
  } catch (e: any) {
    if (!res.headersSent) res.status(502).json({ error: 'Upstream error', message: e?.message });
  }
}

// ---------------------------------------------------------------------------
// M3U8-only rewriter — rewrites nested playlist URLs only, TS segments untouched
// ---------------------------------------------------------------------------

function rewriteM3U8PlaylistsOnly(content: string, baseUrl: string, proxyBase: string, headersParam: string): string {
  function proxiedPlaylist(href: string): string {
    let abs: string;
    try { abs = new URL(href, baseUrl).href; } catch { abs = href; }
    let qs = `url=${encodeURIComponent(abs)}`;
    if (headersParam) qs += `&headers=${encodeURIComponent(headersParam)}`;
    return `${proxyBase}/m3u8-only-proxy?${qs}`;
  }

  function absoluteSegment(href: string): string {
    try { return new URL(href, baseUrl).href; } catch { return href; }
  }

  const out: string[] = [];
  let nextIsVariant = false;

  for (const line of content.split('\n')) {
    const t = line.trim();
    if (!t) { out.push(line); continue; }

    // The line after #EXT-X-STREAM-INF is a variant playlist URI
    if (nextIsVariant && !t.startsWith('#')) {
      out.push(proxiedPlaylist(t));
      nextIsVariant = false;
      continue;
    }

    if (t.startsWith('#EXT-X-STREAM-INF')) {
      out.push(line);
      nextIsVariant = true;
      continue;
    }

    // #EXT-X-MAP — initialization segment, leave URL absolute but don't proxy
    if (t.startsWith('#EXT-X-MAP')) {
      out.push(t.replace(/URI="([^"]+)"/g, (_m, h) => `URI="${absoluteSegment(h)}"`));
      continue;
    }

    // Other tags with URI="" — proxy if it looks like a playlist, absolutise otherwise
    if (t.startsWith('#') && t.includes('URI="')) {
      out.push(t.replace(/URI="([^"]+)"/g, (_m, h) => {
        const isPlaylist = h.includes('.m3u8') || h.includes('/playlist') || h.includes('/master');
        return `URI="${isPlaylist ? proxiedPlaylist(h) : absoluteSegment(h)}"`;
      }));
      continue;
    }

    // Plain segment lines — make absolute, do NOT proxy
    if (!t.startsWith('#')) {
      out.push(absoluteSegment(t));
      continue;
    }

    out.push(line);
  }

  return out.join('\n');
}

async function m3u8OnlyProxy(req: Request, res: Response): Promise<void> {
  const { targetUrl, upstreamHeaders, headersParam } = parseProxyParams(req);
  if (!targetUrl) { res.status(400).json({ error: 'Missing url parameter' }); return; }

  const err = validateUrl(targetUrl);
  if (err) { res.status(400).json({ error: err }); return; }

  try {
    const upstream = await nativeFetch(targetUrl, upstreamHeaders);
    const ct       = (upstream.headers['content-type'] as string) ?? '';
    const isM3U8   = ct.includes('mpegurl') || targetUrl.includes('.m3u8');

    if (isM3U8 && upstream.body.includes('#EXTM3U')) {
      const proto     = req.get('x-forwarded-proto') || req.protocol;
      const proxyBase = `${proto}://${req.get('host')}`;
      const baseUrl   = targetUrl.substring(0, targetUrl.lastIndexOf('/') + 1);
      res.status(upstream.status);
      res.setHeader('Content-Type', 'application/vnd.apple.mpegurl');
      res.send(rewriteM3U8PlaylistsOnly(upstream.body, baseUrl, proxyBase, headersParam ?? ''));
      return;
    }

    // Not an M3U8 — pass through as-is
    res.status(upstream.status);
    for (const [key, value] of Object.entries(upstream.headers)) {
      if (value && !STRIP_RESPONSE_HEADERS.has(key.toLowerCase()))
        res.setHeader(key, value as string | string[]);
    }
    res.send(upstream.body);
  } catch (e: any) {
    if (!res.headersSent) res.status(502).json({ error: 'Upstream error', message: e?.message });
  }
}

// ---------------------------------------------------------------------------
// Express app
// ---------------------------------------------------------------------------

const app = express();
app.set('trust proxy', true);
app.disable('x-powered-by');
app.disable('etag');

// CORS + preflight
app.use((_req: Request, res: Response, next: NextFunction) => {
  res.header('Access-Control-Allow-Origin', '*');
  if (_req.method === 'OPTIONS') return res.sendStatus(200);
  next();
});

// In-flight request accounting (must run before the route handlers)
app.use(trackRequest);

// Routes
app.get('/health',     (_req, res) => res.json({ ok: true }));
app.get('/stats',      (req, res) => {
  // Optional guard: set STATS_TOKEN to require ?token= or an x-stats-token header.
  const token = process.env.STATS_TOKEN;
  if (token && req.get('x-stats-token') !== token && req.query.token !== token) {
    res.status(401).json({ error: 'unauthorized' });
    return;
  }

  const mem = process.memoryUsage();
  res.json({
    ok: true,
    timestamp: new Date().toISOString(),
    uptimeSeconds: Math.round((Date.now() - startedAt) / 1000),
    // Two views of "active connections" — pick whichever matches your model:
    //   activeRequests    = requests currently being proxied (app-level)
    //   clientConnections = open client TCP sockets (socket-level)
    activeRequests: metrics.inFlight,
    clientConnections: metrics.clientConnections,
    totalRequests: metrics.totalRequests,
    totalErrors: metrics.totalErrors,
    byEndpoint: metrics.byEndpoint,
    upstreamPools: {
      http:  agentStats(httpAgent),
      https: agentStats(httpsAgent),
    },
    memory: {
      rssMB:      +(mem.rss / 1048576).toFixed(1),
      heapUsedMB: +(mem.heapUsed / 1048576).toFixed(1),
    },
  });
});
app.all('/fetch',      fetchProxy);
app.all('/subtitle',   fetchProxy);
app.all('/mp4-proxy',  fetchProxy);
app.all('/proxy',      fetchProxy);
app.all('/m3u8-proxy',      m3u8Proxy);
app.all('/m3u8-only-proxy', m3u8OnlyProxy);
app.all('/ts-proxy',        tsProxy);

// ---------------------------------------------------------------------------
// Start — single process, modest connection ceiling
// ---------------------------------------------------------------------------

const server = http.createServer(app);
server.maxConnections = 512;
server.on('connection', (s) => {
  s.setNoDelay(true);
  metrics.clientConnections++;
  s.on('close', () => { metrics.clientConnections--; });
});
server.listen(PORT, () => console.log(`Proxy on port ${PORT}`));