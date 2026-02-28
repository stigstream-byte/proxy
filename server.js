/**
 * Node.js Express Streaming Proxy Server
 */

const express = require('express');
const fetch = require('node-fetch');
const AbortController = require('abort-controller');
const http = require('http');
const https = require('https');

const app = express();
const PORT = process.env.PORT || 3000;

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

function selectAgent(url, hostOverride) {
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

const RETRY_CONFIG = {
  maxRetries:        2,   // M3U8 / generic
  segmentMaxRetries: 1,   // TS segments — one retry only; stale URLs won't improve
  initialDelay:      25,
  maxDelay:          300,
  backoffMultiplier: 2,
};

const TIMEOUT_MS         = 15000; // M3U8 / MPD / generic requests
const SEGMENT_TIMEOUT_MS =  8000; // TS / fMP4 segments — balanced for 1080p on slow CDNs
const MP4_TIMEOUT_MS     = 30000; // Progressive MP4 — large files, range requests for seeking

// ---------------------------------------------------------------------------
// Quality / bandwidth detection helpers
// ---------------------------------------------------------------------------
// Extracts bandwidth from an #EXT-X-STREAM-INF line for logging purposes.
function parseBandwidth(streamInfLine) {
  const m = streamInfLine.match(/BANDWIDTH=(\d+)/i);
  return m ? Math.round(parseInt(m[1], 10) / 1000) + ' kbps' : 'unknown';
}

// Maps a rough bandwidth (bps) to a human-readable quality label.
function bandwidthToQuality(bps) {
  if (!bps) return '';
  if (bps >= 4_000_000) return '1080p+';
  if (bps >= 2_000_000) return '1080p';
  if (bps >= 1_200_000) return '720p';
  if (bps >= 600_000)   return '480p';
  if (bps >= 300_000)   return '360p';
  return '240p or lower';
}

// Detects fMP4 segment MIME type by URL extension.
function segmentContentType(url) {
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

// (LRU segment cache removed — segments are streamed directly to avoid
//  buffering latency and the cache was not the source of slowness anyway)

// ---------------------------------------------------------------------------
// Anti-bot: rotating User-Agents and Accept-Language variants
// ---------------------------------------------------------------------------

const USER_AGENTS = [
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0',
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
];

const ACCEPT_LANGUAGES = [
  'en-US,en;q=0.9',
  'en-US,en;q=0.9,es;q=0.8',
  'en-GB,en;q=0.9',
  'en-US,en;q=0.8',
];

// sec-ch-ua strings paired to the Chrome UAs above (index-matched where applicable)
const SEC_CH_UA_MAP = {
  '124': '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
  '123': '"Chromium";v="123", "Google Chrome";v="123", "Not-A.Brand";v="99"',
};

function pickRandom(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

// ---------------------------------------------------------------------------
// Pending-request deduplication (M3U8 / text only — never for streams)
// ---------------------------------------------------------------------------

const pendingRequests = new Map();

// ---------------------------------------------------------------------------
// Headers that must never be forwarded downstream
// ---------------------------------------------------------------------------

const BLOCKED_RESPONSE_HEADERS = new Set([
  'content-type',
  'content-length',        // managed explicitly — never trust upstream's declared size
  'access-control-allow-origin',
  'access-control-allow-headers',
  'access-control-allow-methods',
  'x-upstream-status',
  'transfer-encoding',
]);

// ---------------------------------------------------------------------------
// CORS middleware
// ---------------------------------------------------------------------------

app.use((req, res, next) => {
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

function validateUrl(urlString) {
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

function parseCustomHeaders(query) {
  const customHeaders = {};

  const headersParam = query.headers;
  if (headersParam) {
    try {
      let obj;
      try {
        obj = JSON.parse(headersParam);
      } catch {
        obj = JSON.parse(Buffer.from(headersParam, 'base64').toString('utf-8'));
      }
      Object.assign(customHeaders, obj);
    } catch (err) {
      console.warn('✗ Failed to parse headers param:', err.message);
    }
  }

  for (const [key, value] of Object.entries(query)) {
    if (key.startsWith('header_')) {
      customHeaders[key.slice(7).replace(/_/g, '-')] = value;
    }
  }

  return customHeaders;
}

// ---------------------------------------------------------------------------
// Request-header builder with anti-bot rotation
// ---------------------------------------------------------------------------

function buildRequestHeaders(customHeaders = {}, includeReferer = true) {
  const ua = customHeaders['User-Agent'] || customHeaders['user-agent'] || pickRandom(USER_AGENTS);

  // Derive sec-ch-ua from Chrome version in the UA string
  const chromeVerMatch = ua.match(/Chrome\/(\d+)/);
  const chromeVer      = chromeVerMatch ? chromeVerMatch[1] : null;
  const secCHUA        = (chromeVer && SEC_CH_UA_MAP[chromeVer]) || '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"';

  const headers = {
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

async function _fetchWithRetryCore(url, options, retries, timeoutMs) {
  // Extract the Host override (if any) so we can select the right agent.
  // We read it from headers here rather than threading a separate param through
  // the whole call stack.
  const hostOverride = options?.headers?.['Host'] || '';
  const agent = selectAgent(url, hostOverride);
  let lastError;
  let delay = RETRY_CONFIG.initialDelay;

  for (let attempt = 0; attempt <= retries; attempt++) {
    const attemptController = new AbortController();

    const callerSignal = options?.signal;
    if (callerSignal?.aborted) throw new Error('Request aborted by caller');

    const onCallerAbort = () => attemptController.abort();
    callerSignal?.addEventListener('abort', onCallerAbort);

    // The timeout covers the FULL round-trip including body — the caller is
    // responsible for clearing response._bodyTimeoutId once the body is consumed.
    const timeoutId = setTimeout(() => attemptController.abort(), timeoutMs);

    try {
      const response = await fetch(url, {
        ...options,
        agent,
        signal: attemptController.signal,
      });

      // Headers arrived — remove the one-shot caller-abort listener (it was for
      // this attempt's controller) and re-wire client disconnect to also clear
      // the still-running body timeout so we don't leave dangling timers.
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
      console.warn(`⚠️  Attempt ${attempt + 1} failed: ${lastError.message}`);

    } catch (error) {
      clearTimeout(timeoutId);
      callerSignal?.removeEventListener('abort', onCallerAbort);

      lastError = error;
      console.warn(`⚠️  Attempt ${attempt + 1} failed: ${error.message}`);

      if (callerSignal?.aborted) break;
      if (error.name === 'AbortError') break;
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

async function fetchWithRetry(url, options, retries = RETRY_CONFIG.maxRetries, timeoutMs = TIMEOUT_MS, deduplicate = false) {
  if (!deduplicate) return _fetchWithRetryCore(url, options, retries, timeoutMs);

  const requestKey = `${url}:${JSON.stringify(options?.headers || {})}`;

  if (pendingRequests.has(requestKey)) {
    try { return await pendingRequests.get(requestKey); } catch { /* fall through */ }
  }

  let resolve, reject;
  const promise = new Promise((res, rej) => { resolve = res; reject = rej; });
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

function forwardResponseHeaders(upstreamResponse, res) {
  for (const [key, value] of upstreamResponse.headers.entries()) {
    if (!BLOCKED_RESPONSE_HEADERS.has(key.toLowerCase())) {
      res.setHeader(key, value);
    }
  }
}

function streamResponse(upstreamResponse, res, abortController) {
  const body = upstreamResponse.body;

  const clearBodyTimeout = () => clearTimeout(upstreamResponse._bodyTimeoutId);

  res.on('close', () => {
    clearBodyTimeout();
    abortController?.abort();
    body.destroy();
  });

  body.on('error', (err) => {
    clearBodyTimeout();
    console.error('❌ Upstream stream error:', err.message);
    if (!res.headersSent) res.status(502).json({ error: 'Stream error', message: err.message });
    else res.destroy();
  });

  body.on('end', clearBodyTimeout);

  body.pipe(res);
}

// ---------------------------------------------------------------------------
// M3U8 rewriter
// ---------------------------------------------------------------------------
// Handles both master playlists (multi-quality) and media playlists (segments).
//
// Master playlist lines to rewrite:
//   #EXT-X-STREAM-INF   — variant stream (video at a given quality)
//   #EXT-X-I-FRAME-STREAM-INF — I-frame trick-play variants (URI= attribute)
//   #EXT-X-MEDIA        — alternate renditions: audio tracks, subtitles, CC
//   #EXT-X-SESSION-DATA — external JSON data referenced by URI
//
// Media playlist lines to rewrite:
//   #EXT-X-KEY          — AES-128 / SAMPLE-AES encryption key
//   #EXT-X-MAP          — fMP4 initialisation segment (EXT-X-MAP URI)
//   Bare URL lines      — actual media segments (.ts, .m4s, .mp4, .aac …)
//
// Sub-playlists (*.m3u8 / variant indicators) are routed to /m3u8-proxy so
// the proxy can rewrite them in turn.  Everything else goes to /ts-proxy.
// ---------------------------------------------------------------------------

function rewriteM3U8Content(m3u8Content, baseUrl, proxyBaseUrl, customHeaders = {}, hostOverride = '') {
  const headersParam = Object.keys(customHeaders).length > 0
    ? `&headers=${encodeURIComponent(JSON.stringify(customHeaders))}`
    : '';

  const hostParam = hostOverride ? `&host=${encodeURIComponent(hostOverride)}` : '';

  const suffix = headersParam + hostParam;

  // Resolve a potentially-relative URL against the playlist base.
  function abs(href) {
    try { return new URL(href, baseUrl).href; } catch { return href; }
  }

  // Decide the proxy endpoint for a resolved URL.
  function endpointFor(resolvedUrl) {
    if (
      /\.m3u8(\?|$)/i.test(resolvedUrl) ||
      /[?&]type=(video|audio|subtitle)(&|$)/i.test(resolvedUrl) ||
      resolvedUrl.includes('/playlist/')  ||
      resolvedUrl.includes('/master/')    ||
      resolvedUrl.includes('/index.m3u8')
    ) return '/m3u8-proxy';
    return '/ts-proxy';
  }

  function proxyUrl(href) {
    const resolved = abs(href);
    const ep = endpointFor(resolved);
    return `${proxyBaseUrl}${ep}?url=${encodeURIComponent(resolved)}${suffix}`;
  }

  // Rewrite a URI="…" attribute inside a tag line.
  function rewriteUriAttr(line, forcedEndpoint) {
    return line.replace(/URI="([^"]+)"/gi, (_, href) => {
      const resolved = abs(href);
      const ep = forcedEndpoint || endpointFor(resolved);
      return `URI="${proxyBaseUrl}${ep}?url=${encodeURIComponent(resolved)}${suffix}"`;
    });
  }

  const lines  = m3u8Content.split('\n');
  const out    = [];
  let   expectVariantUrl = false; // true when next non-comment line is a variant URL

  for (let i = 0; i < lines.length; i++) {
    const raw = lines[i];
    const t   = raw.trim();

    if (!t) { out.push(raw); continue; }

    // ── Master playlist tags ──────────────────────────────────────────────

    if (t.startsWith('#EXT-X-STREAM-INF')) {
      // Log quality info for debugging
      const bwMatch = t.match(/BANDWIDTH=(\d+)/i);
      const res     = t.match(/RESOLUTION=(\d+x\d+)/i);
      const bw      = bwMatch ? parseInt(bwMatch[1], 10) : 0;
      console.log(`  📊 Variant: ${res ? res[1] : '?'} — ${bandwidthToQuality(bw)} (${parseBandwidth(t)})`);
      out.push(raw);
      expectVariantUrl = true;
      continue;
    }

    if (t.startsWith('#EXT-X-I-FRAME-STREAM-INF')) {
      // Inline URI= attribute — rewrite in place, no following URL line
      out.push(rewriteUriAttr(t, '/m3u8-proxy'));
      continue;
    }

    if (t.startsWith('#EXT-X-MEDIA:')) {
      // Audio tracks, subtitle tracks, closed captions — URI is optional
      out.push(rewriteUriAttr(t, '/m3u8-proxy'));
      continue;
    }

    if (t.startsWith('#EXT-X-SESSION-DATA:')) {
      out.push(rewriteUriAttr(t));
      continue;
    }

    // ── Media playlist tags ───────────────────────────────────────────────

    if (t.startsWith('#EXT-X-KEY:')) {
      // Encryption key — always fetch via /fetch so we don't alter the binary
      out.push(rewriteUriAttr(t, '/fetch'));
      continue;
    }

    if (t.startsWith('#EXT-X-MAP:')) {
      // fMP4 initialisation segment — always a binary resource → /ts-proxy
      out.push(rewriteUriAttr(t, '/ts-proxy'));
      continue;
    }

    // Pass all other tags through unchanged
    if (t.startsWith('#')) { out.push(raw); continue; }

    // ── URL lines (variant playlists or media segments) ───────────────────

    if (expectVariantUrl) {
      out.push(`${proxyBaseUrl}/m3u8-proxy?url=${encodeURIComponent(abs(t))}${suffix}`);
      expectVariantUrl = false;
      continue;
    }

    // Regular segment line
    try {
      out.push(proxyUrl(t));
    } catch (err) {
      console.warn('⚠️  Failed to rewrite segment line:', t, err.message);
      out.push(raw);
    }
  }

  return out.join('\n');
}

// ---------------------------------------------------------------------------
// Shared route handlers (reduces duplication between */no-referer variants)
// ---------------------------------------------------------------------------

async function handleM3U8(req, res, includeReferer) {
  const targetUrl = req.query.url;
  if (!targetUrl) return res.status(400).json({ error: 'Missing url parameter' });

  const { valid, error } = validateUrl(targetUrl);
  if (!valid) return res.status(400).json({ error });

  const hostOverride = (req.query.host || '').trim();

  console.log(`📺 M3U8 Request (referer=${includeReferer}${hostOverride ? ', host=' + hostOverride : ''}):`, targetUrl);

  try {
    const customHeaders = parseCustomHeaders(req.query);
    if (!includeReferer) { delete customHeaders['Referer']; delete customHeaders['referer']; }

    const requestHeaders = buildRequestHeaders(customHeaders, includeReferer);

    if (hostOverride) {
      requestHeaders['Host'] = hostOverride;
    }

    const targetResponse = await fetchWithRetry(targetUrl, { headers: requestHeaders }, RETRY_CONFIG.maxRetries, TIMEOUT_MS, true);

    if (!targetResponse.ok) {
      console.error('❌ M3U8 fetch failed:', targetResponse.status);
      return res.status(targetResponse.status).json({ error: 'Failed to fetch M3U8', status: targetResponse.status });
    }

    let m3u8Content = await targetResponse.text();

    // Detect master vs media playlist and log accordingly
    const isMaster = m3u8Content.includes('#EXT-X-STREAM-INF') || m3u8Content.includes('#EXT-X-I-FRAME-STREAM-INF');
    if (isMaster) {
      const variantCount = (m3u8Content.match(/#EXT-X-STREAM-INF/gi) || []).length;
      console.log(`  🎬 Master playlist — ${variantCount} quality variant(s)`);
    } else {
      const segCount = m3u8Content.split('\n').filter(l => l.trim() && !l.trim().startsWith('#')).length;
      const duration = (() => {
        const m = m3u8Content.match(/#EXT-X-TARGETDURATION:(\d+)/i);
        return m ? `${m[1]}s target duration` : '';
      })();
      console.log(`  📼 Media playlist — ~${segCount} segment(s)${duration ? ', ' + duration : ''}`);
    }

    const baseUrl   = targetUrl.substring(0, targetUrl.lastIndexOf('/') + 1);
    const proxyBase = `https://${req.get('host')}`;
    m3u8Content = rewriteM3U8Content(m3u8Content, baseUrl, proxyBase, customHeaders, hostOverride);

    res.setHeader('Content-Type',  'application/vnd.apple.mpegurl');
    res.setHeader('Cache-Control', 'no-cache');
    res.send(m3u8Content);

  } catch (err) {
    console.error('❌ M3U8 proxy error:', err);
    if (!res.headersSent) res.status(502).json({ error: 'Proxy error', message: err.message, type: err.name });
  }
}

async function handleFetch(req, res, includeReferer) {
  const targetUrl = req.query.url;
  if (!targetUrl) return res.status(400).json({ error: 'Missing url parameter' });

  const { valid, error } = validateUrl(targetUrl);
  if (!valid) return res.status(400).json({ error });

  console.log(`🌐 Fetch Request (referer=${includeReferer}):`, targetUrl);

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
      return res.status(targetResponse.status).json({ error: 'Failed to fetch resource', status: targetResponse.status });
    }

    const contentType   = targetResponse.headers.get('content-type')   || 'application/octet-stream';
    const contentLength = targetResponse.headers.get('content-length');
    const isRangeResponse = targetResponse.status === 206;

    res.setHeader('Content-Type',       contentType);
    res.setHeader('Accept-Ranges',      'bytes');
    res.setHeader('X-Upstream-Status',  targetResponse.status);
    // Only forward Content-Length for range responses (206) where accuracy is
    // guaranteed. For full-content streams, omit it to prevent
    // ERR_CONTENT_LENGTH_MISMATCH when upstream returns fewer/more bytes than
    // declared (e.g. due to ignored Accept-Encoding: identity, CDN quirks, etc.)
    if (isRangeResponse && contentLength) res.setHeader('Content-Length', contentLength);

    forwardResponseHeaders(targetResponse, res);
    streamResponse(targetResponse, res, clientController);

  } catch (err) {
    console.error('❌ Fetch proxy error:', err);
    if (!res.headersSent) res.status(502).json({ error: 'Proxy error', message: err.message, type: err.name });
  }
}

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------

// Health check
app.get('/health', (req, res) => {
  res.json({
    status:    'ok',
    timestamp: new Date().toISOString(),
  });
});

// M3U8
app.get('/m3u8-proxy',           (req, res) => handleM3U8(req, res, true));
app.get('/m3u8-proxy-no-referer',(req, res) => handleM3U8(req, res, false));

// TS / segment proxy — handles MPEG-TS (.ts), fMP4 (.m4s/.mp4/.m4v), AAC (.aac/.m4a),
// WebM (.webm), and VTT subtitle segments. Streams directly, no caching.
app.get('/ts-proxy', async (req, res) => {
  const targetUrl = req.query.url;
  if (!targetUrl) return res.status(400).json({ error: 'Missing url parameter' });

  const { valid, error } = validateUrl(targetUrl);
  if (!valid) return res.status(400).json({ error });

  const hostOverride = (req.query.host || '').trim();

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

    const targetResponse = await _fetchWithRetryCore(
      targetUrl,
      { headers: requestHeaders, signal: clientController.signal },
      RETRY_CONFIG.segmentMaxRetries,
      SEGMENT_TIMEOUT_MS
    );

    // Accept both 200 OK and 206 Partial Content
    if (!targetResponse.ok && targetResponse.status !== 206) {
      console.error('❌ Segment fetch failed:', targetResponse.status, targetUrl);
      return res.status(targetResponse.status).json({ error: 'Failed to fetch segment', status: targetResponse.status });
    }

    const upstreamType  = targetResponse.headers.get('content-type') || '';
    const contentType   = (upstreamType && upstreamType !== 'application/octet-stream')
      ? upstreamType
      : segmentContentType(targetUrl);
    const contentLength = targetResponse.headers.get('content-length');
    const contentRange  = targetResponse.headers.get('content-range');
    const isRangeResponse = targetResponse.status === 206;

    res.status(targetResponse.status);
    res.setHeader('Content-Type',      contentType);
    res.setHeader('Accept-Ranges',     'bytes');
    res.setHeader('X-Upstream-Status', targetResponse.status);
    // Only forward Content-Length for 206 range responses where the upstream
    // guarantees the byte count. Omitting it for 200 responses lets Express
    // use chunked transfer encoding and avoids ERR_CONTENT_LENGTH_MISMATCH
    // when the actual stream length diverges from the declared header.
    if (isRangeResponse && contentLength) res.setHeader('Content-Length', contentLength);
    if (contentRange)  res.setHeader('Content-Range',  contentRange);

    forwardResponseHeaders(targetResponse, res);

    // Stream directly — data flows to the client as soon as first bytes arrive.
    streamResponse(targetResponse, res, clientController);

  } catch (err) {
    console.error('❌ Segment proxy error:', err.message);
    if (!res.headersSent) res.status(502).json({ error: 'Proxy error', message: err.message, type: err.name });
  }
});

// MP4 proxy — progressive download with full Range/seek support for movies & TV.
// Uses a longer timeout (MP4_TIMEOUT_MS) since a single range chunk from a
// 1080p movie can be several MB over a slow upstream CDN.
app.get('/mp4-proxy', async (req, res) => {
  const targetUrl = req.query.url;
  if (!targetUrl) return res.status(400).json({ error: 'Missing url parameter' });

  const { valid, error } = validateUrl(targetUrl);
  if (!valid) return res.status(400).json({ error });

  const hostOverride = (req.query.host || '').trim();

  console.log('🎬 MP4 Request:', targetUrl);

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

    // Accept 200 (full file) and 206 (range/seek)
    if (!targetResponse.ok && targetResponse.status !== 206) {
      console.error('❌ MP4 fetch failed:', targetResponse.status);
      return res.status(targetResponse.status).json({ error: 'Failed to fetch MP4', status: targetResponse.status });
    }

    const contentType   = targetResponse.headers.get('content-type')   || 'video/mp4';
    const contentLength = targetResponse.headers.get('content-length');
    const contentRange  = targetResponse.headers.get('content-range');
    const acceptRanges  = targetResponse.headers.get('accept-ranges');
    const isRangeResponse = targetResponse.status === 206;

    res.status(targetResponse.status);
    res.setHeader('Content-Type',      contentType);
    res.setHeader('Accept-Ranges',     acceptRanges || 'bytes');
    res.setHeader('X-Upstream-Status', targetResponse.status);
    // Only forward Content-Length for 206 range responses. For full 200
    // responses, suppress it to prevent ERR_CONTENT_LENGTH_MISMATCH — upstreams
    // sometimes misreport the byte count (ignored identity encoding, CDN quirks).
    if (isRangeResponse && contentLength) res.setHeader('Content-Length', contentLength);
    if (contentRange)  res.setHeader('Content-Range',  contentRange);

    forwardResponseHeaders(targetResponse, res);
    streamResponse(targetResponse, res, clientController);

  } catch (err) {
    console.error('❌ MP4 proxy error:', err);
    if (!res.headersSent) res.status(502).json({ error: 'Proxy error', message: err.message, type: err.name });
  }
});

// DASH / MPD proxy — rewrites all relative URLs in the manifest so that
// initialisation segments (.mp4 init), media segments (.m4s / .mp4 chunks),
// and sub-period manifests are all fetched through this proxy.
app.get('/mpd-proxy', async (req, res) => {
  const targetUrl = req.query.url;
  if (!targetUrl) return res.status(400).json({ error: 'Missing url parameter' });

  const { valid, error } = validateUrl(targetUrl);
  if (!valid) return res.status(400).json({ error });

  const hostOverride = (req.query.host || '').trim();

  console.log('📡 MPD (DASH) Request:', targetUrl);

  const clientController = new AbortController();
  req.on('close', () => clientController.abort());

  try {
    const customHeaders  = parseCustomHeaders(req.query);
    const requestHeaders = buildRequestHeaders(customHeaders, true);

    if (hostOverride) requestHeaders['Host'] = hostOverride;

    const targetResponse = await fetchWithRetry(targetUrl, { headers: requestHeaders, signal: clientController.signal }, RETRY_CONFIG.maxRetries, TIMEOUT_MS, true);

    if (!targetResponse.ok) {
      console.error('❌ MPD fetch failed:', targetResponse.status);
      return res.status(targetResponse.status).json({ error: 'Failed to fetch MPD', status: targetResponse.status });
    }

    let mpdContent = await targetResponse.text();

    const baseUrl   = targetUrl.substring(0, targetUrl.lastIndexOf('/') + 1);
    const proxyBase = `https://${req.get('host')}`;

    const headersParam = Object.keys(customHeaders).length > 0
      ? `&headers=${encodeURIComponent(JSON.stringify(customHeaders))}`
      : '';
    const hostParam = hostOverride ? `&host=${encodeURIComponent(hostOverride)}` : '';
    const suffix = headersParam + hostParam;

    function resolveAndProxy(href, endpoint) {
      try {
        const resolved = new URL(href, baseUrl).href;
        return `${proxyBase}${endpoint}?url=${encodeURIComponent(resolved)}${suffix}`;
      } catch { return href; }
    }

    // Rewrite initialization and media segment templates
    mpdContent = mpdContent
      // SegmentTemplate sourceURL and media/initialization attributes
      .replace(/\binitialization="([^"]+)"/gi, (_, u) => `initialization="${resolveAndProxy(u, '/ts-proxy')}"`)
      .replace(/\bmedia="([^"]+)"/gi,          (_, u) => `media="${resolveAndProxy(u, '/ts-proxy')}"`)
      // SegmentBase and SegmentList with explicit URLs
      .replace(/\bsourceURL="([^"]+)"/gi,      (_, u) => `sourceURL="${resolveAndProxy(u, '/ts-proxy')}"`)
      // BaseURL elements
      .replace(/<BaseURL>([^<]+)<\/BaseURL>/gi, (_, u) => {
        const proxied = resolveAndProxy(u.trim(), '/ts-proxy');
        return `<BaseURL>${proxied}</BaseURL>`;
      })
      // SegmentList SegmentURL media= and index= attributes
      .replace(/\bmediaRange="([^"]+)"/gi, (_, u) => `mediaRange="${u}"`) // keep ranges as-is
      ;

    // Log representation quality info
    const repMatches = [...mpdContent.matchAll(/bandwidth="(\d+)"/gi)];
    if (repMatches.length) {
      const bandwidths = repMatches.map(m => parseInt(m[1], 10)).sort((a, b) => a - b);
      console.log(`  📊 DASH representations: ${bandwidths.map(b => bandwidthToQuality(b)).join(', ')}`);
    }

    res.setHeader('Content-Type',  'application/dash+xml');
    res.setHeader('Cache-Control', 'no-cache');
    res.send(mpdContent);

  } catch (err) {
    console.error('❌ MPD proxy error:', err);
    if (!res.headersSent) res.status(502).json({ error: 'Proxy error', message: err.message, type: err.name });
  }
});

// Generic fetch (with / without referer)
app.get('/fetch',           (req, res) => handleFetch(req, res, true));
app.get('/fetch-no-referer',(req, res) => handleFetch(req, res, false));

// Subtitle proxy
app.get('/subtitle', async (req, res) => {
  const targetUrl = req.query.url;
  if (!targetUrl) return res.status(400).json({ error: 'Missing url parameter' });

  const { valid, error } = validateUrl(targetUrl);
  if (!valid) return res.status(400).json({ error });

  try {
    const customHeaders  = parseCustomHeaders(req.query);
    const requestHeaders = buildRequestHeaders(customHeaders, true);

    const response = await fetchWithRetry(targetUrl, { headers: requestHeaders }, 1, 15000);
    if (!response.ok) return res.status(502).json({ error: 'Failed to fetch subtitle', status: response.status });

    const buffer = await response.buffer();
    let text = buffer.toString('utf-8');
    if ((text.match(/\uFFFD/g) || []).length > 10) text = buffer.toString('latin1');

    const entries = parseSRTorVTT(text);
    if (!entries || entries.length === 0) return res.status(415).json({ error: 'Unsupported subtitle format or failed to parse.' });

    res.setHeader('Content-Type', 'text/plain; charset=utf-8');
    res.send(entriesToSRT(entries));

  } catch (err) {
    res.status(500).json({ error: 'Failed to fetch or convert subtitle', message: err.message });
  }
});

// Minimal SRT/VTT parser
function parseSRTorVTT(text) {
  text = text.replace(/^\uFEFF/, '').replace(/\r\n|\r/g, '\n').replace(/^WEBVTT.*?\n+/, '');
  return text.split(/\n{2,}/).reduce((acc, block) => {
    const lines = block.split('\n').filter(Boolean);
    if (lines.length < 2) return acc;
    let idx = /^\d+$/.test(lines[0]) ? 1 : 0;
    const m = lines[idx]?.match(/(\d{2}:\d{2}:\d{2}[.,]\d{3})\s*-->\s*(\d{2}:\d{2}:\d{2}[.,]\d{3})/);
    if (!m) return acc;
    acc.push({ start: m[1].replace(',', '.'), end: m[2].replace(',', '.'), text: lines.slice(idx + 1).join('\n') });
    return acc;
  }, []);
}

function entriesToSRT(entries) {
  return entries.map((e, i) =>
    `${i + 1}\n${e.start.replace('.', ',')} --> ${e.end.replace('.', ',')}\n${e.text}\n`
  ).join('\n');
}

// ---------------------------------------------------------------------------
// 404 / error handlers
// ---------------------------------------------------------------------------

app.use((req, res) => {
  res.status(404).json({
    error: 'Not Found',
    availableEndpoints: [
      '/health',
      '/m3u8-proxy?url=<url>&headers=<json>&host=<hostname>          — HLS master or media playlist',
      '/m3u8-proxy-no-referer?url=<url>&headers=<json>&host=<hostname>',
      '/mpd-proxy?url=<url>&headers=<json>&host=<hostname>           — MPEG-DASH manifest',
      '/ts-proxy?url=<url>&headers=<json>&host=<hostname>            — TS / fMP4 / .m4s segments',
      '/mp4-proxy?url=<url>&headers=<json>&host=<hostname>           — Progressive MP4 with seek/range',
      '/fetch?url=<url>&headers=<json>                               — Generic resource (keys, etc.)',
      '/fetch-no-referer?url=<url>&headers=<json>',
      '/subtitle?url=<url>&headers=<json>                            — SRT / VTT subtitle conversion',
    ],
  });
});

app.use((err, req, res, _next) => {
  console.error('Server error:', err);
  res.status(500).json({ error: 'Internal Server Error', message: err.message });
});

// ---------------------------------------------------------------------------
// Start
// ---------------------------------------------------------------------------

app.listen(PORT, () => {
  console.log(`🚀 Proxy server running on port ${PORT}`);
  console.log(`📍 Health check: http://localhost:${PORT}/health`);
  console.log(`🎬 Endpoints: /m3u8-proxy  /mpd-proxy  /ts-proxy  /mp4-proxy  /fetch  /subtitle`);
});
