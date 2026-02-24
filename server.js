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

// Keep-alive agents for connection reuse — dramatically reduces latency for repeated upstream requests
const httpAgent  = new http.Agent({ keepAlive: true, maxSockets: 64 });
const httpsAgent = new https.Agent({ keepAlive: true, maxSockets: 64 });

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const RETRY_CONFIG = {
  maxRetries:        2,
  initialDelay:      25,
  maxDelay:          300,
  backoffMultiplier: 2,
};

const TIMEOUT_MS         = 15000; // M3U8 / generic requests
const SEGMENT_TIMEOUT_MS = 10000; // TS segments

// Max memory budget for the TS segment cache (default 128 MB, override via env)
const CACHE_MAX_BYTES = parseInt(process.env.CACHE_MAX_BYTES, 10) || 128 * 1024 * 1024;

// Don't cache individual segments larger than 10 % of the total budget
const CACHE_MAX_SINGLE_BYTES = Math.floor(CACHE_MAX_BYTES * 0.1);

// ---------------------------------------------------------------------------
// LRU Segment Cache
// ---------------------------------------------------------------------------
// Uses a Map (whose iteration order is insertion order) as the backing store.
// On every cache hit the entry is moved to the end (most-recently-used).
// On every cache set we evict from the front (least-recently-used) until the
// byte budget is satisfied.
// ---------------------------------------------------------------------------

class LRUSegmentCache {
  constructor(maxBytes) {
    this.maxBytes   = maxBytes;
    this.totalBytes = 0;
    this.map        = new Map(); // url → { buf: Buffer, size: number, hits: number }
  }

  get(key) {
    const entry = this.map.get(key);
    if (!entry) return null;

    // Refresh position to "most recently used"
    this.map.delete(key);
    this.map.set(key, entry);
    entry.hits++;
    return entry.buf;
  }

  set(key, buf) {
    const size = buf.length;

    // Skip items that are individually too large
    if (size > CACHE_MAX_SINGLE_BYTES) return;

    // Remove stale entry for the same key first
    if (this.map.has(key)) {
      this.totalBytes -= this.map.get(key).size;
      this.map.delete(key);
    }

    // Evict LRU entries until there is room
    while (this.totalBytes + size > this.maxBytes && this.map.size > 0) {
      const lruKey = this.map.keys().next().value;
      this.totalBytes -= this.map.get(lruKey).size;
      this.map.delete(lruKey);
    }

    this.map.set(key, { buf, size, hits: 0 });
    this.totalBytes += size;
  }

  stats() {
    return {
      entries:      this.map.size,
      totalMB:      (this.totalBytes / 1024 / 1024).toFixed(2),
      maxMB:        (this.maxBytes   / 1024 / 1024).toFixed(2),
      usagePct:     Math.round((this.totalBytes / this.maxBytes) * 100),
    };
  }
}

const segmentCache = new LRUSegmentCache(CACHE_MAX_BYTES);

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
    'Accept-Encoding':    'identity', // Disable compression to avoid decoding issues when proxying
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
  const agent = url.startsWith('https') ? httpsAgent : httpAgent;
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

function rewriteM3U8Content(m3u8Content, baseUrl, proxyBaseUrl, customHeaders = {}) {
  const headersParam = Object.keys(customHeaders).length > 0
    ? `&headers=${encodeURIComponent(JSON.stringify(customHeaders))}`
    : '';

  return m3u8Content.split('\n').map(line => {
    const t = line.trim();
    if (!t) return line;

    if (t.startsWith('#EXT-X-KEY:') || t.startsWith('#EXT-X-MEDIA:') || t.startsWith('#EXT-X-MAP:')) {
      try {
        const match = t.match(/URI="([^"]+)"/);
        if (match) {
          const abs = new URL(match[1], baseUrl).href;
          let endpoint;
          if (t.startsWith('#EXT-X-KEY:'))   endpoint = '/fetch';
          else if (t.startsWith('#EXT-X-MAP:')) endpoint = '/ts-proxy';
          else                                 endpoint = '/m3u8-proxy';
          const proxied = `${proxyBaseUrl}${endpoint}?url=${encodeURIComponent(abs)}${headersParam}`;
          return t.replace(/URI="[^"]+"/, `URI="${proxied}"`);
        }
      } catch (err) {
        console.warn('Failed to rewrite tag URI:', err.message);
      }
      return line;
    }

    if (t.startsWith('#')) return line;

    try {
      const abs = new URL(t, baseUrl).href;
      const isPlaylist =
        /\.m3u8(\?.*)?$/i.test(abs) ||
        /[?&]type=(video|audio|subtitle)(&|$)/i.test(abs) ||
        abs.includes('/playlist/');
      const endpoint = isPlaylist ? '/m3u8-proxy' : '/ts-proxy';
      return `${proxyBaseUrl}${endpoint}?url=${encodeURIComponent(abs)}${headersParam}`;
    } catch (err) {
      console.warn('Failed to rewrite line:', t, err.message);
      return line;
    }
  }).join('\n');
}

// ---------------------------------------------------------------------------
// Shared route handlers (reduces duplication between */no-referer variants)
// ---------------------------------------------------------------------------

async function handleM3U8(req, res, includeReferer) {
  const targetUrl = req.query.url;
  if (!targetUrl) return res.status(400).json({ error: 'Missing url parameter' });

  const { valid, error } = validateUrl(targetUrl);
  if (!valid) return res.status(400).json({ error });

  console.log(`📺 M3U8 Request (referer=${includeReferer}):`, targetUrl);

  try {
    const customHeaders = parseCustomHeaders(req.query);
    if (!includeReferer) { delete customHeaders['Referer']; delete customHeaders['referer']; }

    const requestHeaders = buildRequestHeaders(customHeaders, includeReferer);
    const targetResponse = await fetchWithRetry(targetUrl, { headers: requestHeaders }, RETRY_CONFIG.maxRetries, TIMEOUT_MS, true);

    if (!targetResponse.ok) {
      console.error('❌ M3U8 fetch failed:', targetResponse.status);
      return res.status(targetResponse.status).json({ error: 'Failed to fetch M3U8', status: targetResponse.status });
    }

    let m3u8Content = await targetResponse.text();
    const baseUrl    = targetUrl.substring(0, targetUrl.lastIndexOf('/') + 1);
    const proxyBase  = `https://${req.get('host')}`;
    m3u8Content = rewriteM3U8Content(m3u8Content, baseUrl, proxyBase, customHeaders);

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

    res.setHeader('Content-Type',       contentType);
    res.setHeader('Accept-Ranges',      'bytes');
    res.setHeader('X-Upstream-Status',  targetResponse.status);
    if (contentLength) res.setHeader('Content-Length', contentLength);

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
    cache:     segmentCache.stats(),
  });
});

// M3U8
app.get('/m3u8-proxy',           (req, res) => handleM3U8(req, res, true));
app.get('/m3u8-proxy-no-referer',(req, res) => handleM3U8(req, res, false));

// TS / segment proxy — with LRU caching
app.get('/ts-proxy', async (req, res) => {
  const targetUrl = req.query.url;
  if (!targetUrl) return res.status(400).json({ error: 'Missing url parameter' });

  const { valid, error } = validateUrl(targetUrl);
  if (!valid) return res.status(400).json({ error });

  // --- Cache check ---
  const cacheKey = targetUrl;
  const cached   = segmentCache.get(cacheKey);
  if (cached) {
    res.setHeader('Content-Type',      'video/MP2T');
    res.setHeader('Content-Length',    cached.length);
    res.setHeader('Accept-Ranges',     'bytes');
    res.setHeader('X-Cache-Hit',       'HIT');
    res.setHeader('Cache-Control',     'public, max-age=3600');
    return res.end(cached);
  }

  // A single AbortController whose signal we hand to the fetch calls.
  // The timeout for each individual attempt is managed INSIDE _fetchWithRetryCore
  // with a per-attempt controller, so we only use this one for client-disconnect.
  const clientController = new AbortController();
  req.on('close', () => clientController.abort());

  try {
    const customHeaders  = parseCustomHeaders(req.query);
    const requestHeaders = buildRequestHeaders(customHeaders, true);

    const rangeHeader = req.get('Range');
    if (rangeHeader) requestHeaders['Range'] = rangeHeader;

    const targetResponse = await _fetchWithRetryCore(
      targetUrl,
      { headers: requestHeaders, signal: clientController.signal },
      RETRY_CONFIG.maxRetries,
      SEGMENT_TIMEOUT_MS
    );

    if (!targetResponse.ok) {
      console.error('❌ Segment fetch failed:', targetResponse.status);
      return res.status(targetResponse.status).json({ error: 'Failed to fetch segment', status: targetResponse.status });
    }

    const contentType   = targetResponse.headers.get('content-type')   || 'video/MP2T';
    const contentLength = targetResponse.headers.get('content-length');

    res.setHeader('Content-Type',      contentType);
    res.setHeader('Accept-Ranges',     'bytes');
    res.setHeader('X-Upstream-Status', targetResponse.status);
    res.setHeader('X-Cache-Hit',       'MISS');
    // Do NOT forward Content-Length yet — the upstream value can't be trusted until
    // we know whether node-fetch decoded/decompressed the body.  We'll set the real
    // value once we have the buffer (buffered path) or omit it entirely (streaming path).

    forwardResponseHeaders(targetResponse, res);

    const declaredSize = contentLength ? parseInt(contentLength, 10) : Infinity;

    if (!rangeHeader && declaredSize <= CACHE_MAX_SINGLE_BYTES) {
      const chunks = [];
      targetResponse.body.on('data',  chunk => chunks.push(chunk));
      targetResponse.body.on('end',   () => {
        clearTimeout(targetResponse._bodyTimeoutId);
        const buf = Buffer.concat(chunks);
        segmentCache.set(cacheKey, buf);
        if (!res.writableEnded) {
          // Set the real Content-Length now that we know the exact byte count.
          res.setHeader('Content-Length', buf.length);
          res.end(buf);
        }
      });
      targetResponse.body.on('error', err => {
        clearTimeout(targetResponse._bodyTimeoutId);
        console.error('❌ Segment stream error:', err.message);
        if (!res.headersSent) res.status(502).json({ error: 'Stream error', message: err.message });
        else res.destroy();
      });
      res.on('close', () => { clientController.abort(); targetResponse.body.destroy(); });
    } else {
      // Too large or range request — stream directly without caching
      streamResponse(targetResponse, res, clientController);
    }

  } catch (err) {
    console.error('❌ Segment proxy error:', err);
    if (!res.headersSent) res.status(502).json({ error: 'Proxy error', message: err.message, type: err.name });
  }
});

// MP4 proxy
app.get('/mp4-proxy', async (req, res) => {
  const targetUrl = req.query.url;
  if (!targetUrl) return res.status(400).json({ error: 'Missing url parameter' });

  const { valid, error } = validateUrl(targetUrl);
  if (!valid) return res.status(400).json({ error });

  console.log('🎬 MP4 Request:', targetUrl);

  const clientController = new AbortController();
  req.on('close', () => clientController.abort());

  try {
    const customHeaders  = parseCustomHeaders(req.query);
    const requestHeaders = buildRequestHeaders(customHeaders, true);

    const rangeHeader = req.get('Range');
    if (rangeHeader) requestHeaders['Range'] = rangeHeader;

    const targetResponse = await fetchWithRetry(targetUrl, { headers: requestHeaders, signal: clientController.signal });

    if (!targetResponse.ok) {
      console.error('❌ MP4 fetch failed:', targetResponse.status);
      return res.status(targetResponse.status).json({ error: 'Failed to fetch MP4', status: targetResponse.status });
    }

    const contentType   = targetResponse.headers.get('content-type')   || 'video/mp4';
    const contentLength = targetResponse.headers.get('content-length');

    res.setHeader('Content-Type',      contentType);
    res.setHeader('Accept-Ranges',     'bytes');
    res.setHeader('X-Upstream-Status', targetResponse.status);
    if (contentLength) res.setHeader('Content-Length', contentLength);

    forwardResponseHeaders(targetResponse, res);
    streamResponse(targetResponse, res, clientController);

  } catch (err) {
    console.error('❌ MP4 proxy error:', err);
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
      '/m3u8-proxy?url=<url>&headers=<json>',
      '/m3u8-proxy-no-referer?url=<url>&headers=<json>',
      '/ts-proxy?url=<url>&headers=<json>',
      '/mp4-proxy?url=<url>&headers=<json>',
      '/fetch?url=<url>&headers=<json>',
      '/fetch-no-referer?url=<url>&headers=<json>',
      '/subtitle?url=<url>&headers=<json>',
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
  console.log(`💾 TS segment cache: ${(CACHE_MAX_BYTES / 1024 / 1024).toFixed(0)} MB budget`);
});
