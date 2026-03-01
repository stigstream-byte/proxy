/**
 * Node.js Express Streaming Proxy Server (TypeScript)
 */

import * as dotenv from 'dotenv';
dotenv.config(); // Load .env before anything else reads process.env

import express, { Request, Response, NextFunction } from 'express';
import fetch, { Response as FetchResponse, RequestInit } from 'node-fetch';
import * as http from 'http';
import * as https from 'https';
import { Readable } from 'stream';

// Use Node's built-in AbortController (Node 16+) to avoid type conflicts with node-fetch
const { AbortController } = globalThis as unknown as { AbortController: typeof globalThis.AbortController };

// ---------------------------------------------------------------------------
// Environment
// ---------------------------------------------------------------------------

const IS_DEV  = (process.env.NODE_ENV ?? 'development') !== 'production';
const ENV_LABEL = IS_DEV ? 'development' : 'production';

// Port: explicit .env value → environment default (3000 prod / 3003 dev)
const PORT: number = process.env.PORT
  ? parseInt(process.env.PORT, 10)
  : IS_DEV ? 3003 : 3000;

// ---------------------------------------------------------------------------
// Logger — silenced entirely in production
// ---------------------------------------------------------------------------

const logger = {
  log:   (...args: unknown[]): void => { if (IS_DEV) console.log(...args);   },
  warn:  (...args: unknown[]): void => { if (IS_DEV) console.warn(...args);  },
  error: (...args: unknown[]): void => { if (IS_DEV) console.error(...args); },
};

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface RetryConfig {
  maxRetries: number;
  segmentMaxRetries: number;
  initialDelay: number;
  maxDelay: number;
  backoffMultiplier: number;
}

interface ValidateUrlResult {
  valid: boolean;
  error?: string;
}

interface SubtitleEntry {
  start: string;
  end: string;
  text: string;
}

/**
 * Extends FetchResponse with a custom timer handle attached by _fetchWithRetryCore
 * so that streamResponse can clear the TTFB timeout once body streaming begins.
 */
interface ExtendedResponse extends FetchResponse {
  _bodyTimeoutId?: ReturnType<typeof setTimeout>;
}

interface SecCHUAMap {
  [version: string]: string;
}

// ---------------------------------------------------------------------------
// App setup
// ---------------------------------------------------------------------------

const app = express();

// Trust reverse proxy headers (e.g. X-Forwarded-Proto from nginx/Cloudflare)
app.set('trust proxy', true);

// Keep-alive agents for connection reuse — dramatically reduces latency for repeated upstream requests.
// maxSockets raised to 128: movies/TV at high quality generate many concurrent segment requests
// (video + audio + subtitles at multiple qualities during ABR switching).
const httpAgent  = new http.Agent({ keepAlive: true, maxSockets: 128, maxFreeSockets: 32 });
const httpsAgent = new https.Agent({ keepAlive: true, maxSockets: 128, maxFreeSockets: 32 });

// ---------------------------------------------------------------------------
// Agent selection
// ---------------------------------------------------------------------------
// When a Host override is in play the shared pool MUST NOT be used because:
//   1. TLS SNI — the shared httpsAgent derives servername from the URL hostname.
//      If the real vhost differs (e.g. hitting a CDN IP), the TLS handshake
//      fails or returns the wrong certificate.
//   2. Pool contamination — a bad/mismatched connection stored in the shared
//      pool gets re-used for normal requests, causing intermittent failures
//      across ALL endpoints even when no host override is set.
//
// The fix: for host-override HTTPS requests, create a short-lived agent with
// the correct servername and keepAlive disabled so it never touches the pool.
// ---------------------------------------------------------------------------

function selectAgent(url: string, hostOverride: string): http.Agent | https.Agent {
  const isHttps = url.startsWith('https');

  if (hostOverride && isHttps) {
    // Fresh one-shot agent: correct SNI servername, never pooled.
    return new https.Agent({
      keepAlive:  false,
      servername: hostOverride, // TLS SNI matches the virtual host, not the URL IP
    });
  }

  if (hostOverride) {
    // HTTP with host override — isolated pool keyed to this override only,
    // so it can't contaminate the shared httpAgent pool.
    return new http.Agent({ keepAlive: false });
  }

  return isHttps ? httpsAgent : httpAgent;
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const RETRY_CONFIG: RetryConfig = {
  maxRetries:        2,   // M3U8 / generic
  segmentMaxRetries: 1,   // TS segments — one retry only; stale URLs won't improve
  initialDelay:      25,
  maxDelay:          300,
  backoffMultiplier: 2,
};

const TIMEOUT_MS         = 15000; // M3U8 / MPD / generic — time-to-first-byte (TTFB) only
const SEGMENT_TIMEOUT_MS =  8000; // TS / fMP4 TTFB — once streaming begins the stall timeout takes over
const MP4_TIMEOUT_MS     = 30000; // MP4 TTFB — large files may take a moment to start

// After headers arrive, the TTFB timeout is replaced by a per-chunk idle/stall
// timeout. This allows arbitrarily large or slow streams to complete while still
// killing connections that have genuinely gone silent mid-transfer.
const SEGMENT_STALL_MS   = 45000; // 45 s idle → stalled TS/fMP4 segment (1080p can be 4-8 MB on slow CDNs)
const MP4_STALL_MS       = 60000; // 60 s idle → stalled MP4
const DEFAULT_STALL_MS   = 30000; // generic fetch stall

// ---------------------------------------------------------------------------
// Quality / bandwidth detection helpers
// ---------------------------------------------------------------------------

// Extracts bandwidth from an #EXT-X-STREAM-INF line for logging purposes.
function parseBandwidth(streamInfLine: string): string {
  const m = streamInfLine.match(/BANDWIDTH=(\d+)/i);
  return m ? Math.round(parseInt(m[1], 10) / 1000) + ' kbps' : 'unknown';
}

// Maps a rough bandwidth (bps) to a human-readable quality label.
function bandwidthToQuality(bps: number): string {
  if (!bps) return '';
  if (bps >= 4_000_000) return '1080p+';
  if (bps >= 2_000_000) return '1080p';
  if (bps >= 1_200_000) return '720p';
  if (bps >= 600_000)   return '480p';
  if (bps >= 300_000)   return '360p';
  return '240p or lower';
}

// Detects fMP4 segment MIME type by URL extension.
function segmentContentType(url: string): string {
  if (/\.mp4(\?|$)/i.test(url) || /\.m4s(\?|$)/i.test(url) || /\.m4v(\?|$)/i.test(url)) {
    return 'video/mp4';
  }
  if (/\.aac(\?|$)/i.test(url) || /\.m4a(\?|$)/i.test(url)) {
    return 'audio/mp4';
  }
  if (/\.vtt(\?|$)/i.test(url)) return 'text/vtt';
  if (/\.webm(\?|$)/i.test(url)) return 'video/webm';
  return 'video/MP2T'; // default: MPEG-TS
}

// ---------------------------------------------------------------------------
// Anti-bot: rotating User-Agents and Accept-Language variants
// ---------------------------------------------------------------------------

const USER_AGENTS: string[] = [
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0',
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
];

const ACCEPT_LANGUAGES: string[] = [
  'en-US,en;q=0.9',
  'en-US,en;q=0.9,es;q=0.8',
  'en-GB,en;q=0.9',
  'en-US,en;q=0.8',
];

// sec-ch-ua strings paired to the Chrome UAs above (index-matched where applicable)
const SEC_CH_UA_MAP: SecCHUAMap = {
  '124': '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
  '123': '"Chromium";v="123", "Google Chrome";v="123", "Not-A.Brand";v="99"',
};

function pickRandom<T>(arr: T[]): T {
  return arr[Math.floor(Math.random() * arr.length)];
}

// ---------------------------------------------------------------------------
// Pending-request deduplication (M3U8 / text only — never for streams)
// ---------------------------------------------------------------------------

const pendingRequests = new Map<string, Promise<ExtendedResponse>>();

// ---------------------------------------------------------------------------
// Headers that must never be forwarded downstream
// ---------------------------------------------------------------------------

const BLOCKED_RESPONSE_HEADERS = new Set<string>([
  'content-type',
  'content-length',        // managed explicitly — never trust upstream's declared size
  'content-encoding',      // node-fetch decompresses transparently; forwarding this causes the player to double-decompress → garbage
  'access-control-allow-origin',
  'access-control-allow-headers',
  'access-control-allow-methods',
  'x-upstream-status',
  'transfer-encoding',
]);

// ---------------------------------------------------------------------------
// CORS middleware
// ---------------------------------------------------------------------------

app.use((req: Request, res: Response, next: NextFunction) => {
  res.header('Access-Control-Allow-Origin',   '*');
  res.header('Access-Control-Allow-Methods',  '*');
  res.header('Access-Control-Allow-Headers',  '*');
  res.header('Access-Control-Expose-Headers', 'Content-Length, Content-Range, Content-Type, Date, Server, X-Cache-Hit, X-Upstream-Status');
  res.header('Access-Control-Max-Age',        '86400');
  res.header('Timing-Allow-Origin',           '*');

  if (req.method === 'OPTIONS') return res.sendStatus(200);
  next();
});

app.use(express.json());

// ---------------------------------------------------------------------------
// URL validation (SSRF protection)
// ---------------------------------------------------------------------------

function validateUrl(urlString: string): ValidateUrlResult {
  try {
    const url = new URL(urlString);

    if (!['http:', 'https:'].includes(url.protocol)) {
      return { valid: false, error: 'Only HTTP/HTTPS protocols allowed' };
    }

    const h = url.hostname.toLowerCase();

    // RFC-1918 private ranges + loopback + link-local
    if (
      h === 'localhost'          ||
      h === '[::1]'              ||
      /^127\./.test(h)           ||
      /^10\./.test(h)            ||
      /^192\.168\./.test(h)      ||
      /^169\.254\./.test(h)      ||
      // 172.16.0.0/12 → 172.16.x.x – 172.31.x.x
      /^172\.(1[6-9]|2\d|3[01])\./.test(h)
    ) {
      return { valid: false, error: 'Private/reserved IPs not allowed' };
    }

    return { valid: true };
  } catch {
    return { valid: false, error: 'Invalid URL format' };
  }
}

// ---------------------------------------------------------------------------
// Custom-headers parser
// ---------------------------------------------------------------------------

function parseCustomHeaders(query: Request['query']): Record<string, string> {
  const customHeaders: Record<string, string> = {};

  const headersParam = query.headers as string | undefined;
  if (headersParam) {
    try {
      let obj: Record<string, string>;
      try {
        obj = JSON.parse(headersParam);
      } catch {
        obj = JSON.parse(Buffer.from(headersParam, 'base64').toString('utf-8'));
      }
      Object.assign(customHeaders, obj);
    } catch (err) {
      logger.warn('✗ Failed to parse headers param:', (err as Error).message);
    }
  }

  for (const [key, value] of Object.entries(query)) {
    if (key.startsWith('header_')) {
      customHeaders[key.slice(7).replace(/_/g, '-')] = value as string;
    }
  }

  return customHeaders;
}

// ---------------------------------------------------------------------------
// Request-header builder with anti-bot rotation
// ---------------------------------------------------------------------------

function buildRequestHeaders(
  customHeaders: Record<string, string> = {},
  includeReferer = true,
): Record<string, string> {
  const ua = customHeaders['User-Agent'] || customHeaders['user-agent'] || pickRandom(USER_AGENTS);

  // Derive sec-ch-ua from Chrome version in the UA string
  const chromeVerMatch = ua.match(/Chrome\/(\d+)/);
  const chromeVer      = chromeVerMatch ? chromeVerMatch[1] : null;
  const secCHUA        = (chromeVer && SEC_CH_UA_MAP[chromeVer]) || '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"';

  const headers: Record<string, string> = {
    'User-Agent':         ua,
    'Accept':             '*/*',
    'Accept-Language':    pickRandom(ACCEPT_LANGUAGES),
    'Accept-Encoding':    'identity', // Disable compression — gzip would cause Content-Length mismatch after decompression
    'Cache-Control':      'no-cache',
    'Pragma':             'no-cache',
    'Connection':         'keep-alive',
    'Sec-Fetch-Dest':     'empty',
    'Sec-Fetch-Mode':     'cors',
    'Sec-Fetch-Site':     'cross-site',
    'sec-ch-ua':          secCHUA,
    'sec-ch-ua-mobile':   '?0',
    'sec-ch-ua-platform': ua.includes('Macintosh') ? '"macOS"' : ua.includes('Linux') ? '"Linux"' : '"Windows"',
  };

  for (const [key, value] of Object.entries(customHeaders)) {
    if (key.toLowerCase() === 'referer' && !includeReferer) continue;
    headers[key] = value;
  }

  return headers;
}

// ---------------------------------------------------------------------------
// Retry core
// ---------------------------------------------------------------------------

async function _fetchWithRetryCore(
  url: string,
  options: RequestInit & { headers?: Record<string, string>; signal?: AbortController['signal'] },
  retries: number,
  timeoutMs: number,
): Promise<ExtendedResponse> {
  // Extract the Host override (if any) so we can select the right agent.
  const hostOverride = (options?.headers?.['Host']) ?? '';
  const agent = selectAgent(url, hostOverride);
  let lastError: Error | undefined;
  let delay = RETRY_CONFIG.initialDelay;

  for (let attempt = 0; attempt <= retries; attempt++) {
    const attemptController = new AbortController();

    const callerSignal = options?.signal;
    if (callerSignal?.aborted) throw new Error('Request aborted by caller');

    const onCallerAbort = () => attemptController.abort();
    callerSignal?.addEventListener('abort', onCallerAbort);

    // The timeout covers the TTFB only — the caller is responsible for clearing
    // response._bodyTimeoutId once the body is consumed.
    const timeoutId = setTimeout(() => attemptController.abort(), timeoutMs);

    try {
      const response = await fetch(url, {
        ...options,
        agent,
        signal: attemptController.signal,
      }) as ExtendedResponse;

      // Headers arrived — remove the one-shot caller-abort listener and re-wire
      // client disconnect to also clear the still-running body timeout.
      callerSignal?.removeEventListener('abort', onCallerAbort);
      if (callerSignal) {
        callerSignal.addEventListener('abort', () => {
          clearTimeout(timeoutId);
          attemptController.abort();
        }, { once: true });
      }

      if (response.ok || (response.status >= 400 && response.status < 500 && response.status !== 429)) {
        // Attach the body-timeout handle so the caller clears it after body read.
        response._bodyTimeoutId = timeoutId;
        return response;
      }

      clearTimeout(timeoutId);
      lastError = new Error(`HTTP ${response.status}: ${response.statusText}`);
      logger.warn(`⚠️  Attempt ${attempt + 1} failed: ${lastError.message}`);

    } catch (error) {
      clearTimeout(timeoutId);
      callerSignal?.removeEventListener('abort', onCallerAbort);

      lastError = error as Error;
      logger.warn(`⚠️  Attempt ${attempt + 1} failed: ${lastError.message}`);

      if (callerSignal?.aborted) break;
      if (lastError.name === 'AbortError') break;
    }

    if (attempt < retries) {
      await new Promise(r => setTimeout(r, Math.min(delay, RETRY_CONFIG.maxDelay)));
      delay *= RETRY_CONFIG.backoffMultiplier;
    }
  }

  throw lastError;
}

// ---------------------------------------------------------------------------
// Retry wrapper with optional request deduplication (text/buffered only)
// ---------------------------------------------------------------------------

async function fetchWithRetry(
  url: string,
  options: RequestInit & { headers?: Record<string, string>; signal?: AbortController['signal'] },
  retries  = RETRY_CONFIG.maxRetries,
  timeoutMs = TIMEOUT_MS,
  deduplicate = false,
): Promise<ExtendedResponse> {
  if (!deduplicate) return _fetchWithRetryCore(url, options, retries, timeoutMs);

  const requestKey = `${url}:${JSON.stringify(options?.headers ?? {})}`;

  if (pendingRequests.has(requestKey)) {
    try { return await pendingRequests.get(requestKey)!; } catch { /* fall through */ }
  }

  let resolve!: (value: ExtendedResponse) => void;
  let reject!:  (reason?: unknown) => void;
  const promise = new Promise<ExtendedResponse>((res, rej) => { resolve = res; reject = rej; });
  pendingRequests.set(requestKey, promise);

  try {
    const result = await _fetchWithRetryCore(url, options, retries, timeoutMs);
    resolve(result);
    return result;
  } catch (err) {
    reject(err);
    throw err;
  } finally {
    pendingRequests.delete(requestKey);
  }
}

// ---------------------------------------------------------------------------
// Stream helper
// ---------------------------------------------------------------------------

function forwardResponseHeaders(upstreamResponse: FetchResponse, res: Response): void {
  for (const [key, value] of upstreamResponse.headers.entries()) {
    if (!BLOCKED_RESPONSE_HEADERS.has(key.toLowerCase())) {
      res.setHeader(key, value);
    }
  }
}

function streamResponse(
  upstreamResponse: ExtendedResponse,
  res: Response,
  abortController: InstanceType<typeof AbortController>,
  stallTimeoutMs = DEFAULT_STALL_MS,
): void {
  const body = upstreamResponse.body as unknown as Readable;

  // ── Phase 1 complete: TTFB timeout is no longer needed ──────────────────
  // The TTFB timer was set in _fetchWithRetryCore to abort stalled connections
  // before headers arrived. Headers are here now — clear it immediately.
  // Leaving it running is the root cause of ERR_INCOMPLETE_CHUNKED_ENCODING.
  clearTimeout(upstreamResponse._bodyTimeoutId);

  // ── Phase 2: per-chunk idle / stall timeout ──────────────────────────────
  let stallTimer: ReturnType<typeof setTimeout>;

  function scheduleStall(): void {
    clearTimeout(stallTimer);
    stallTimer = setTimeout(() => {
      logger.error(`❌ Upstream stall — no data for ${stallTimeoutMs}ms, aborting`);
      abortController?.abort();
      body?.destroy(new Error('Upstream stall timeout'));
    }, stallTimeoutMs);
  }

  function cancelStall(): void {
    clearTimeout(stallTimer);
  }

  scheduleStall(); // arm immediately when streaming begins

  body?.on('data',  scheduleStall); // reset the clock on every chunk
  body?.on('end',   cancelStall);
  body?.on('error', cancelStall);

  res.on('close', () => {
    cancelStall();
    abortController?.abort();
    body?.destroy();
  });

  body?.on('error', (err: NodeJS.ErrnoException) => {
    cancelStall();
    // AbortError means the client disconnected or we intentionally aborted —
    // either way the response is already gone so there is nothing to do.
    if (err.name === 'AbortError' || err.code === 'ABORT_ERR') return;
    logger.error('❌ Upstream stream error:', err.message);
    if (!res.headersSent) {
      res.status(502).json({ error: 'Stream error', message: err.message });
    } else {
      // Use res.destroy() — NOT res.end() — for mid-stream binary errors.
      // res.end() would send a clean HTTP close, making the player think it
      // received a complete valid segment. res.destroy() issues a TCP RST.
      res.destroy();
    }
  });

  body?.pipe(res);
}

// ---------------------------------------------------------------------------
// M3U8 rewriter
// ---------------------------------------------------------------------------

function rewriteM3U8Content(
  m3u8Content: string,
  baseUrl: string,
  proxyBaseUrl: string,
  customHeaders: Record<string, string> = {},
  hostOverride = '',
): string {
  const headersParam = Object.keys(customHeaders).length > 0
    ? `&headers=${encodeURIComponent(JSON.stringify(customHeaders))}`
    : '';

  const hostParam = hostOverride ? `&host=${encodeURIComponent(hostOverride)}` : '';
  const suffix = headersParam + hostParam;

  function abs(href: string): string {
    try { return new URL(href, baseUrl).href; } catch { return href; }
  }

  function endpointFor(resolvedUrl: string): string {
    if (
      /\.m3u8(\?|$)/i.test(resolvedUrl) ||
      /[?&]type=(video|audio|subtitle)(&|$)/i.test(resolvedUrl) ||
      resolvedUrl.includes('/playlist/')  ||
      resolvedUrl.includes('/master/')    ||
      resolvedUrl.includes('/index.m3u8')
    ) return '/m3u8-proxy';
    return '/ts-proxy';
  }

  function proxyUrl(href: string): string {
    const resolved = abs(href);
    const ep = endpointFor(resolved);
    return `${proxyBaseUrl}${ep}?url=${encodeURIComponent(resolved)}${suffix}`;
  }

  function rewriteUriAttr(line: string, forcedEndpoint?: string): string {
    return line.replace(/URI="([^"]+)"/gi, (_, href: string) => {
      const resolved = abs(href);
      const ep = forcedEndpoint || endpointFor(resolved);
      return `URI="${proxyBaseUrl}${ep}?url=${encodeURIComponent(resolved)}${suffix}"`;
    });
  }

  const lines  = m3u8Content.split('\n');
  const out: string[]   = [];
  let   expectVariantUrl = false;

  for (let i = 0; i < lines.length; i++) {
    const raw = lines[i];
    const t   = raw.trim();

    if (!t) { out.push(raw); continue; }

    // ── Master playlist tags ──────────────────────────────────────────────

    if (t.startsWith('#EXT-X-STREAM-INF')) {
      const bwMatch  = t.match(/BANDWIDTH=(\d+)/i);
      const resMatch = t.match(/RESOLUTION=(\d+x\d+)/i);
      const bw       = bwMatch ? parseInt(bwMatch[1], 10) : 0;
      logger.log(`  📊 Variant: ${resMatch ? resMatch[1] : '?'} — ${bandwidthToQuality(bw)} (${parseBandwidth(t)})`);
      out.push(raw);
      expectVariantUrl = true;
      continue;
    }

    if (t.startsWith('#EXT-X-I-FRAME-STREAM-INF')) {
      out.push(rewriteUriAttr(t, '/m3u8-proxy'));
      continue;
    }

    if (t.startsWith('#EXT-X-MEDIA:')) {
      out.push(rewriteUriAttr(t, '/m3u8-proxy'));
      continue;
    }

    if (t.startsWith('#EXT-X-SESSION-DATA:')) {
      out.push(rewriteUriAttr(t));
      continue;
    }

    // ── Media playlist tags ───────────────────────────────────────────────

    if (t.startsWith('#EXT-X-KEY:')) {
      out.push(rewriteUriAttr(t, '/fetch'));
      continue;
    }

    if (t.startsWith('#EXT-X-MAP:')) {
      out.push(rewriteUriAttr(t, '/ts-proxy'));
      continue;
    }

    if (t.startsWith('#')) { out.push(raw); continue; }

    // ── URL lines (variant playlists or media segments) ───────────────────

    if (expectVariantUrl) {
      out.push(`${proxyBaseUrl}/m3u8-proxy?url=${encodeURIComponent(abs(t))}${suffix}`);
      expectVariantUrl = false;
      continue;
    }

    try {
      out.push(proxyUrl(t));
    } catch (err) {
      logger.warn('⚠️  Failed to rewrite segment line:', t, (err as Error).message);
      out.push(raw);
    }
  }

  return out.join('\n');
}

// ---------------------------------------------------------------------------
// Shared route handlers
// ---------------------------------------------------------------------------

async function handleM3U8(req: Request, res: Response, includeReferer: boolean): Promise<void> {
  const targetUrl = req.query.url as string | undefined;
  if (!targetUrl) { res.status(400).json({ error: 'Missing url parameter' }); return; }

  const { valid, error } = validateUrl(targetUrl);
  if (!valid) { res.status(400).json({ error }); return; }

  const hostOverride = ((req.query.host as string) || '').trim();

  logger.log(`📺 M3U8 Request (referer=${includeReferer}${hostOverride ? ', host=' + hostOverride : ''}):`, targetUrl);

  try {
    const customHeaders = parseCustomHeaders(req.query);
    if (!includeReferer) { delete customHeaders['Referer']; delete customHeaders['referer']; }

    const requestHeaders = buildRequestHeaders(customHeaders, includeReferer);

    if (hostOverride) {
      requestHeaders['Host'] = hostOverride;
    }

    const targetResponse = await fetchWithRetry(targetUrl, { headers: requestHeaders }, RETRY_CONFIG.maxRetries, TIMEOUT_MS, true);

    if (!targetResponse.ok) {
      logger.error('❌ M3U8 fetch failed:', targetResponse.status);
      res.status(targetResponse.status).json({ error: 'Failed to fetch M3U8', status: targetResponse.status });
      return;
    }

    let m3u8Content = await targetResponse.text();

    const isMaster = m3u8Content.includes('#EXT-X-STREAM-INF') || m3u8Content.includes('#EXT-X-I-FRAME-STREAM-INF');
    if (isMaster) {
      const variantCount = (m3u8Content.match(/#EXT-X-STREAM-INF/gi) || []).length;
      logger.log(`  🎬 Master playlist — ${variantCount} quality variant(s)`);
    } else {
      const segCount = m3u8Content.split('\n').filter(l => l.trim() && !l.trim().startsWith('#')).length;
      const duration = (() => {
        const m = m3u8Content.match(/#EXT-X-TARGETDURATION:(\d+)/i);
        return m ? `${m[1]}s target duration` : '';
      })();
      logger.log(`  📼 Media playlist — ~${segCount} segment(s)${duration ? ', ' + duration : ''}`);
    }

    const baseUrl    = targetUrl.substring(0, targetUrl.lastIndexOf('/') + 1);
    const proxyBase  = `${req.protocol}://${req.get('host')}`;
    m3u8Content = rewriteM3U8Content(m3u8Content, baseUrl, proxyBase, customHeaders, hostOverride);

    res.setHeader('Content-Type',  'application/vnd.apple.mpegurl');
    res.setHeader('Cache-Control', 'no-cache');
    res.send(m3u8Content);

  } catch (err) {
    logger.error('❌ M3U8 proxy error:', err);
    if (!res.headersSent) res.status(502).json({ error: 'Proxy error', message: (err as Error).message, type: (err as Error).name });
  }
}

async function handleFetch(req: Request, res: Response, includeReferer: boolean): Promise<void> {
  const targetUrl = req.query.url as string | undefined;
  if (!targetUrl) { res.status(400).json({ error: 'Missing url parameter' }); return; }

  const { valid, error } = validateUrl(targetUrl);
  if (!valid) { res.status(400).json({ error }); return; }

  logger.log(`🌐 Fetch Request (referer=${includeReferer}):`, targetUrl);

  const clientController = new AbortController();
  req.on('close', () => clientController.abort());

  try {
    const customHeaders = parseCustomHeaders(req.query);
    if (!includeReferer) { delete customHeaders['Referer']; delete customHeaders['referer']; }

    const requestHeaders = buildRequestHeaders(customHeaders, includeReferer);
    const rangeHeader    = req.get('Range');
    if (rangeHeader) requestHeaders['Range'] = rangeHeader;

    const targetResponse = await fetchWithRetry(targetUrl, { method: req.method, headers: requestHeaders, signal: clientController.signal });

    if (!targetResponse.ok) {
      res.status(targetResponse.status).json({ error: 'Failed to fetch resource', status: targetResponse.status });
      return;
    }

    const contentType     = targetResponse.headers.get('content-type')   || 'application/octet-stream';
    const contentLength   = targetResponse.headers.get('content-length');
    const isRangeResponse = targetResponse.status === 206;

    res.setHeader('Content-Type',       contentType);
    res.setHeader('Accept-Ranges',      'bytes');
    res.setHeader('X-Upstream-Status',  targetResponse.status);
    if (isRangeResponse && contentLength) res.setHeader('Content-Length', contentLength);

    forwardResponseHeaders(targetResponse, res);
    streamResponse(targetResponse, res, clientController, DEFAULT_STALL_MS);

  } catch (err) {
    if ((err as Error).name === 'AbortError' || (err as NodeJS.ErrnoException).code === 'ABORT_ERR') return;
    logger.error('❌ Fetch proxy error:', err);
    if (!res.headersSent) res.status(502).json({ error: 'Proxy error', message: (err as Error).message, type: (err as Error).name });
  }
}

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------

// Health check
app.get('/health', (_req: Request, res: Response) => {
  res.json({
    status:      'ok',
    environment: ENV_LABEL,
    timestamp:   new Date().toISOString(),
  });
});

// M3U8
app.get('/m3u8-proxy', (req: Request, res: Response) => handleM3U8(req, res, true));

// TS / segment proxy
app.get('/ts-proxy', async (req: Request, res: Response) => {
  const targetUrl = req.query.url as string | undefined;
  if (!targetUrl) { res.status(400).json({ error: 'Missing url parameter' }); return; }

  const { valid, error } = validateUrl(targetUrl);
  if (!valid) { res.status(400).json({ error }); return; }

  const hostOverride = ((req.query.host as string) || '').trim();

  const clientController = new AbortController();
  req.on('close', () => clientController.abort());

  try {
    const customHeaders  = parseCustomHeaders(req.query);
    const requestHeaders = buildRequestHeaders(customHeaders, true);

    if (hostOverride) {
      requestHeaders['Host'] = hostOverride;
    }

    const rangeHeader = req.get('Range');
    if (rangeHeader) requestHeaders['Range'] = rangeHeader;

    const MAX_SEGMENT_RETRIES = 3;
    let targetResponse: ExtendedResponse | undefined;
    let lastErr: Error | undefined;

    for (let attempt = 1; attempt <= MAX_SEGMENT_RETRIES; attempt++) {
      if (clientController.signal.aborted) return;

      try {
        targetResponse = await _fetchWithRetryCore(
          targetUrl,
          { headers: requestHeaders, signal: clientController.signal, compress: true },
          0,
          SEGMENT_TIMEOUT_MS
        );
        lastErr = undefined;
        break;
      } catch (err) {
        if ((err as Error).name === 'AbortError' || (err as NodeJS.ErrnoException).code === 'ABORT_ERR') return;
        lastErr = err as Error;
        logger.warn(`⚠️  Segment attempt ${attempt}/${MAX_SEGMENT_RETRIES} failed: ${lastErr.message} — ${targetUrl}`);
        if (attempt < MAX_SEGMENT_RETRIES) {
          await new Promise(r => setTimeout(r, 250 * attempt));
        }
      }
    }

    if (lastErr || !targetResponse) {
      logger.error('❌ Segment all retries exhausted:', lastErr?.message);
      if (!res.headersSent) res.status(502).json({ error: 'Proxy error', message: lastErr?.message, type: lastErr?.name });
      return;
    }

    if (!targetResponse.ok && targetResponse.status !== 206) {
      logger.error('❌ Segment fetch failed:', targetResponse.status, targetUrl);
      res.status(targetResponse.status).json({ error: 'Failed to fetch segment', status: targetResponse.status });
      return;
    }

    const upstreamType  = targetResponse.headers.get('content-type') || '';
    const derivedType   = segmentContentType(targetUrl);
    const contentType   = (derivedType !== 'video/MP2T')
      ? derivedType
      : (upstreamType && upstreamType !== 'application/octet-stream' && upstreamType !== 'text/javascript')
        ? upstreamType
        : derivedType;
    const contentLength = targetResponse.headers.get('content-length');
    const contentRange  = targetResponse.headers.get('content-range');
    const isRangeResponse = targetResponse.status === 206;

    res.status(targetResponse.status);
    res.setHeader('Content-Type',      contentType);
    res.setHeader('Accept-Ranges',     'bytes');
    res.setHeader('X-Upstream-Status', targetResponse.status);
    if (isRangeResponse && contentLength) res.setHeader('Content-Length', contentLength);
    if (contentRange)  res.setHeader('Content-Range',  contentRange);

    forwardResponseHeaders(targetResponse, res);
    streamResponse(targetResponse, res, clientController, SEGMENT_STALL_MS);

  } catch (err) {
    if ((err as Error).name === 'AbortError' || (err as NodeJS.ErrnoException).code === 'ABORT_ERR') return;
    logger.error('❌ Segment proxy error:', (err as Error).message);
    if (!res.headersSent) res.status(502).json({ error: 'Proxy error', message: (err as Error).message, type: (err as Error).name });
  }
});

// MP4 proxy
app.get('/mp4-proxy', async (req: Request, res: Response) => {
  const targetUrl = req.query.url as string | undefined;
  if (!targetUrl) { res.status(400).json({ error: 'Missing url parameter' }); return; }

  const { valid, error } = validateUrl(targetUrl);
  if (!valid) { res.status(400).json({ error }); return; }

  const hostOverride = ((req.query.host as string) || '').trim();

  logger.log('🎬 MP4 Request:', targetUrl);

  const clientController = new AbortController();
  req.on('close', () => clientController.abort());

  try {
    const customHeaders  = parseCustomHeaders(req.query);
    const requestHeaders = buildRequestHeaders(customHeaders, true);

    if (hostOverride) requestHeaders['Host'] = hostOverride;

    const rangeHeader = req.get('Range');
    if (rangeHeader) requestHeaders['Range'] = rangeHeader;

    const targetResponse = await _fetchWithRetryCore(
      targetUrl,
      { headers: requestHeaders, signal: clientController.signal },
      RETRY_CONFIG.maxRetries,
      MP4_TIMEOUT_MS
    );

    if (!targetResponse.ok && targetResponse.status !== 206) {
      logger.error('❌ MP4 fetch failed:', targetResponse.status);
      res.status(targetResponse.status).json({ error: 'Failed to fetch MP4', status: targetResponse.status });
      return;
    }

    const contentType     = targetResponse.headers.get('content-type')   || 'video/mp4';
    const contentLength   = targetResponse.headers.get('content-length');
    const contentRange    = targetResponse.headers.get('content-range');
    const acceptRanges    = targetResponse.headers.get('accept-ranges');
    const isRangeResponse = targetResponse.status === 206;

    res.status(targetResponse.status);
    res.setHeader('Content-Type',      contentType);
    res.setHeader('Accept-Ranges',     acceptRanges || 'bytes');
    res.setHeader('X-Upstream-Status', targetResponse.status);
    if (isRangeResponse && contentLength) res.setHeader('Content-Length', contentLength);
    if (contentRange)  res.setHeader('Content-Range',  contentRange);

    forwardResponseHeaders(targetResponse, res);
    streamResponse(targetResponse, res, clientController, MP4_STALL_MS);

  } catch (err) {
    if ((err as Error).name === 'AbortError' || (err as NodeJS.ErrnoException).code === 'ABORT_ERR') return;
    logger.error('❌ MP4 proxy error:', err);
    if (!res.headersSent) res.status(502).json({ error: 'Proxy error', message: (err as Error).message, type: (err as Error).name });
  }
});

// Generic fetch (with referer)
app.get('/fetch', (req: Request, res: Response) => handleFetch(req, res, true));

// Subtitle proxy
app.get('/subtitle', async (req: Request, res: Response) => {
  const targetUrl = req.query.url as string | undefined;
  if (!targetUrl) { res.status(400).json({ error: 'Missing url parameter' }); return; }

  const { valid, error } = validateUrl(targetUrl);
  if (!valid) { res.status(400).json({ error }); return; }

  try {
    const customHeaders  = parseCustomHeaders(req.query);
    const requestHeaders = buildRequestHeaders(customHeaders, true);

    const response = await fetchWithRetry(targetUrl, { headers: requestHeaders }, 1, 15000);
    if (!response.ok) { res.status(502).json({ error: 'Failed to fetch subtitle', status: response.status }); return; }

    const buffer = await response.buffer();
    let text = buffer.toString('utf-8');
    if ((text.match(/\uFFFD/g) || []).length > 10) text = buffer.toString('latin1');

    const entries = parseSRTorVTT(text);
    if (!entries || entries.length === 0) { res.status(415).json({ error: 'Unsupported subtitle format or failed to parse.' }); return; }

    res.setHeader('Content-Type', 'text/plain; charset=utf-8');
    res.send(entriesToSRT(entries));

  } catch (err) {
    res.status(500).json({ error: 'Failed to fetch or convert subtitle', message: (err as Error).message });
  }
});

// ---------------------------------------------------------------------------
// Subtitle helpers
// ---------------------------------------------------------------------------

function parseSRTorVTT(text: string): SubtitleEntry[] {
  text = text.replace(/^\uFEFF/, '').replace(/\r\n|\r/g, '\n').replace(/^WEBVTT.*?\n+/, '');
  return text.split(/\n{2,}/).reduce<SubtitleEntry[]>((acc, block) => {
    const lines = block.split('\n').filter(Boolean);
    if (lines.length < 2) return acc;
    const idx = /^\d+$/.test(lines[0]) ? 1 : 0;
    const m = lines[idx]?.match(/(\d{2}:\d{2}:\d{2}[.,]\d{3})\s*-->\s*(\d{2}:\d{2}:\d{2}[.,]\d{3})/);
    if (!m) return acc;
    acc.push({ start: m[1].replace(',', '.'), end: m[2].replace(',', '.'), text: lines.slice(idx + 1).join('\n') });
    return acc;
  }, []);
}

function entriesToSRT(entries: SubtitleEntry[]): string {
  return entries.map((e, i) =>
    `${i + 1}\n${e.start.replace('.', ',')} --> ${e.end.replace('.', ',')}\n${e.text}\n`
  ).join('\n');
}

// ---------------------------------------------------------------------------
// 404 / error handlers
// ---------------------------------------------------------------------------

app.use((_req: Request, res: Response) => {
  res.status(404).json({
    error: 'Not Found',
    availableEndpoints: [
      '/health',
      '/m3u8-proxy?url=<url>&headers=<json>&host=<hostname>          — HLS master or media playlist',
      '/ts-proxy?url=<url>&headers=<json>&host=<hostname>            — TS / fMP4 / .m4s segments',
      '/mp4-proxy?url=<url>&headers=<json>&host=<hostname>           — Progressive MP4 with seek/range',
      '/fetch?url=<url>&headers=<json>                               — Generic resource (keys, etc.)',
      '/subtitle?url=<url>&headers=<json>                            — SRT / VTT subtitle conversion',
    ],
  });
});

app.use((err: Error, _req: Request, res: Response, _next: NextFunction) => {
  logger.error('Server error:', err);
  res.status(500).json({ error: 'Internal Server Error', message: err.message });
});

// ---------------------------------------------------------------------------
// Start
// ---------------------------------------------------------------------------

app.listen(PORT, () => {
  // Startup banner always prints regardless of environment so you know it's running
  console.log(`🚀 Proxy server [${ENV_LABEL}] running on port ${PORT}`);
  if (IS_DEV) {
    console.log(`📍 Health check: http://localhost:${PORT}/health`);
    console.log(`🎬 Endpoints: /m3u8-proxy  /ts-proxy  /mp4-proxy  /fetch  /subtitle`);
  }
});