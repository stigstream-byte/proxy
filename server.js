/**
 * Node.js Express Streaming Proxy Server
 * Converted from Cloudflare Workers
 */

const express = require('express');
const fetch = require('node-fetch');
const AbortController = require('abort-controller');

const app = express();
const PORT = process.env.PORT || 3000;

// Trust reverse proxy headers (e.g. X-Forwarded-Proto from nginx/Cloudflare)
app.set('trust proxy', true);

// Retry configuration for robust fetching - optimized for speed
const RETRY_CONFIG = {
  maxRetries: 2,
  initialDelay: 25,
  maxDelay: 300,
  backoffMultiplier: 2
};

const TIMEOUT_MS = 15000; // 15 second timeout for M3U8
const SEGMENT_TIMEOUT_MS = 10000; // 10 second timeout for segments

// Request deduplication map
const pendingRequests = new Map();

// More realistic User-Agent (matches common browsers)
const DEFAULT_USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';

// Middleware for CORS
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', '*');
  res.header('Access-Control-Allow-Headers', '*');
  res.header('Access-Control-Expose-Headers', 'Content-Length, Content-Range, Content-Type, Date, Server, X-Cache-Hit');
  res.header('Access-Control-Max-Age', '86400');
  res.header('Timing-Allow-Origin', '*');
  
  if (req.method === 'OPTIONS') {
    return res.sendStatus(200);
  }
  next();
});

// Parse JSON bodies
app.use(express.json());

/**
 * Retry wrapper with exponential backoff, timeout protection, and request deduplication
 */
async function fetchWithRetry(url, options, retries = RETRY_CONFIG.maxRetries, timeoutMs = TIMEOUT_MS) {
  const requestKey = `${url}:${JSON.stringify(options?.headers || {})}`;
  if (pendingRequests.has(requestKey)) {
    try {
      return await pendingRequests.get(requestKey);
    } catch (error) {
      // If the pending request failed, continue to try again
    }
  }

  let lastError;
  let delay = RETRY_CONFIG.initialDelay;

  const fetchPromise = (async () => {
    try {
      for (let attempt = 0; attempt <= retries; attempt++) {
        try {
          const controller = new AbortController();
          const timeoutId = setTimeout(() => controller.abort(), timeoutMs);

          const response = await fetch(url, {
            ...options,
            signal: controller.signal
          });

          clearTimeout(timeoutId);

          // Only retry on specific status codes (5xx and 429)
          if (response.ok || (response.status >= 400 && response.status < 500 && response.status !== 429)) {
            return response;
          }

          lastError = new Error(`HTTP ${response.status}: ${response.statusText}`);
          console.warn(`⚠️ Attempt ${attempt + 1} failed: ${lastError.message}`);

        } catch (error) {
          lastError = error;
          console.warn(`⚠️ Attempt ${attempt + 1} failed: ${error.message}`);
          
          if (attempt === retries || error.name === 'AbortError') {
            break;
          }
        }

        // Wait before retrying (exponential backoff)
        if (attempt < retries) {
          await new Promise(resolve => setTimeout(resolve, Math.min(delay, RETRY_CONFIG.maxDelay)));
          delay *= RETRY_CONFIG.backoffMultiplier;
        }
      }

      throw lastError;
    } finally {
      // Always clean up - remove from pending requests when done (success or failure)
      pendingRequests.delete(requestKey);
    }
  })();

  pendingRequests.set(requestKey, fetchPromise);
  return fetchPromise;
}

/**
 * Validate URL before proxying to prevent SSRF attacks
 */
function validateUrl(urlString) {
  try {
    const url = new URL(urlString);
    
    if (!['http:', 'https:'].includes(url.protocol)) {
      return { valid: false, error: 'Only HTTP/HTTPS protocols allowed' };
    }

    const hostname = url.hostname.toLowerCase();
    if (
      hostname === 'localhost' ||
      hostname.startsWith('127.') ||
      hostname.startsWith('192.168.') ||
      hostname.startsWith('10.') ||
      hostname.startsWith('172.16.') ||
      hostname.startsWith('169.254.') ||
      hostname === '[::1]'
    ) {
      return { valid: false, error: 'Private IPs not allowed' };
    }

    return { valid: true };
  } catch (error) {
    return { valid: false, error: 'Invalid URL format' };
  }
}

/**
 * Parse custom headers from query parameters with proper error handling
 */
function parseCustomHeaders(query) {
  const customHeaders = {};
  
  const headersParam = query.headers;
  if (headersParam) {
    try {
      let headersObj;
      try {
        headersObj = JSON.parse(headersParam);
      } catch {
        // Try base64 decode
        const decoded = Buffer.from(headersParam, 'base64').toString('utf-8');
        headersObj = JSON.parse(decoded);
      }
      
      // Properly merge headers (case-sensitive)
      Object.assign(customHeaders, headersObj);
      console.log('✓ Parsed custom headers:', Object.keys(headersObj));
    } catch (error) {
      console.warn('✗ Failed to parse headers:', error.message);
    }
  }

  // Parse individual header_* parameters
  for (const [key, value] of Object.entries(query)) {
    if (key.startsWith('header_')) {
      const headerName = key.replace('header_', '').replace(/_/g, '-');
      customHeaders[headerName] = value;
    }
  }

  return customHeaders;
}

/**
 * Build request headers with anti-bot bypassing
 */
function buildRequestHeaders(customHeaders = {}, includeReferer = true) {
  const headers = {
    'User-Agent': customHeaders['User-Agent'] || customHeaders['user-agent'] || DEFAULT_USER_AGENT,
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'identity', // Explicitly disable compression to avoid decoding issues when proxying
    'Cache-Control': 'no-cache',
    'Pragma': 'no-cache',
    'Connection': 'keep-alive',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'cross-site',
    'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"'
  };

  // Merge custom headers
  for (const [key, value] of Object.entries(customHeaders)) {
    if (key.toLowerCase() === 'referer' && !includeReferer) {
      continue;
    }
    headers[key] = value;
  }

  return headers;
}

/**
 * Rewrite M3U8 content to proxy URLs
 */
function rewriteM3U8Content(m3u8Content, baseUrl, proxyBaseUrl, customHeaders = {}) {
  const lines = m3u8Content.split('\n');
  const rewrittenLines = [];
  
  const headersParam = Object.keys(customHeaders).length > 0 
    ? `&headers=${encodeURIComponent(JSON.stringify(customHeaders))}`
    : '';

  for (const line of lines) {
    const trimmedLine = line.trim();
    
    // Skip empty lines
    if (trimmedLine === '') {
      rewrittenLines.push(line);
      continue;
    }

    // Handle #EXT-X-KEY tags with URI attributes
    if (trimmedLine.startsWith('#EXT-X-KEY:')) {
      try {
        const uriMatch = trimmedLine.match(/URI="([^"]+)"/);
        if (uriMatch && uriMatch[1]) {
          const keyUrl = uriMatch[1];
          const absoluteKeyUrl = new URL(keyUrl, baseUrl).href;
          const proxiedKeyUrl = `${proxyBaseUrl}/fetch?url=${encodeURIComponent(absoluteKeyUrl)}${headersParam}`;
          const rewrittenLine = trimmedLine.replace(/URI="[^"]+"/, `URI="${proxiedKeyUrl}"`);
          rewrittenLines.push(rewrittenLine);
          console.log(`Rewrote [EXT-X-KEY]: ${keyUrl.substring(0, 60)}...`);
          continue;
        }
      } catch (error) {
        console.warn('Failed to rewrite EXT-X-KEY:', trimmedLine, error.message);
      }
      rewrittenLines.push(line);
      continue;
    }

    // Handle #EXT-X-MEDIA tags with URI attributes (audio, subtitles)
    if (trimmedLine.startsWith('#EXT-X-MEDIA:')) {
      try {
        const uriMatch = trimmedLine.match(/URI="([^"]+)"/);
        if (uriMatch && uriMatch[1]) {
          const mediaUrl = uriMatch[1];
          const absoluteMediaUrl = new URL(mediaUrl, baseUrl).href;
          const proxiedMediaUrl = `${proxyBaseUrl}/m3u8-proxy?url=${encodeURIComponent(absoluteMediaUrl)}${headersParam}`;
          const rewrittenLine = trimmedLine.replace(/URI="[^"]+"/, `URI="${proxiedMediaUrl}"`);
          rewrittenLines.push(rewrittenLine);
          console.log(`Rewrote [EXT-X-MEDIA]: ${mediaUrl.substring(0, 60)}...`);
          continue;
        }
      } catch (error) {
        console.warn('Failed to rewrite EXT-X-MEDIA:', trimmedLine, error.message);
      }
      rewrittenLines.push(line);
      continue;
    }

    // Handle #EXT-X-MAP tags with URI attributes (initialization segments)
    if (trimmedLine.startsWith('#EXT-X-MAP:')) {
      try {
        const uriMatch = trimmedLine.match(/URI="([^"]+)"/);
        if (uriMatch && uriMatch[1]) {
          const mapUrl = uriMatch[1];
          const absoluteMapUrl = new URL(mapUrl, baseUrl).href;
          const proxiedMapUrl = `${proxyBaseUrl}/ts-proxy?url=${encodeURIComponent(absoluteMapUrl)}${headersParam}`;
          const rewrittenLine = trimmedLine.replace(/URI="[^"]+"/, `URI="${proxiedMapUrl}"`);
          rewrittenLines.push(rewrittenLine);
          console.log(`Rewrote [EXT-X-MAP]: ${mapUrl.substring(0, 60)}...`);
          continue;
        }
      } catch (error) {
        console.warn('Failed to rewrite EXT-X-MAP:', trimmedLine, error.message);
      }
      rewrittenLines.push(line);
      continue;
    }

    // Skip other comment lines (tags starting with #)
    if (trimmedLine.startsWith('#')) {
      rewrittenLines.push(line);
      continue;
    }

    try {
      // Any non-comment line in M3U8 should be a URL
      // Convert to absolute URL
      const absoluteUrl = new URL(trimmedLine, baseUrl).href;
      
      // Determine if it's a playlist or segment
      // Playlists are:
      // - URLs ending with .m3u8
      // - URLs with type=video, type=audio, or type=subtitle query params (variant playlists)
      // - URLs containing /playlist/ path
      // Everything else is treated as a TS segment
      const isPlaylist = 
        absoluteUrl.match(/\.m3u8(\?.*)?$/i) ||
        absoluteUrl.match(/[?&]type=(video|audio|subtitle)(&|$)/i) ||
        absoluteUrl.includes('/playlist/');
      const endpoint = isPlaylist ? '/m3u8-proxy' : '/ts-proxy';
      
      const proxiedUrl = `${proxyBaseUrl}${endpoint}?url=${encodeURIComponent(absoluteUrl)}${headersParam}`;
      rewrittenLines.push(proxiedUrl);
      
      console.log(`Rewrote [${endpoint}]: ${trimmedLine.substring(0, 60)}...`);
    } catch (error) {
      console.warn('Failed to rewrite line:', trimmedLine, error.message);
      rewrittenLines.push(line);
    }
  }

  return rewrittenLines.join('\n');
}

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

// M3U8 proxy endpoint
app.get('/m3u8-proxy', async (req, res) => {
  const targetUrl = req.query.url;
  
  if (!targetUrl) {
    return res.status(400).json({ error: 'Missing url parameter' });
  }

  const urlValidation = validateUrl(targetUrl);
  if (!urlValidation.valid) {
    return res.status(400).json({ error: urlValidation.error });
  }

  console.log('📺 M3U8 Request:', targetUrl);

  try {
    const customHeaders = parseCustomHeaders(req.query);
    const requestHeaders = buildRequestHeaders(customHeaders, true);

    const targetResponse = await fetchWithRetry(targetUrl, {
      headers: requestHeaders
    });

    if (!targetResponse.ok) {
      console.error('❌ M3U8 fetch failed:', targetResponse.status);
      return res.status(targetResponse.status).json({
        error: 'Failed to fetch M3U8',
        status: targetResponse.status
      });
    }

    let m3u8Content = await targetResponse.text();
    console.log('✅ M3U8 fetched, size:', m3u8Content.length);

    // Rewrite M3U8 content
    const baseUrl = targetUrl.substring(0, targetUrl.lastIndexOf('/') + 1);
    // req.protocol is reliable now that trust proxy is enabled; fallback to https for safety
    //const protocol = req.protocol || 'https';
    const protocol = 'https'; // Force https for proxy URLs to avoid mixed content issues in browsers
    const proxyBaseUrl = `${protocol}://${req.get('host')}`;
    m3u8Content = rewriteM3U8Content(m3u8Content, baseUrl, proxyBaseUrl, customHeaders);

    res.setHeader('Content-Type', 'application/vnd.apple.mpegurl');
    res.setHeader('Cache-Control', 'no-cache');
    res.send(m3u8Content);

  } catch (error) {
    console.error('❌ M3U8 proxy error:', error);
    res.status(502).json({
      error: 'Proxy error',
      message: error.message,
      type: error.name
    });
  }
});

// M3U8 proxy endpoint (no referer)
app.get('/m3u8-proxy-no-referer', async (req, res) => {
  const targetUrl = req.query.url;
  
  if (!targetUrl) {
    return res.status(400).json({ error: 'Missing url parameter' });
  }

  const urlValidation = validateUrl(targetUrl);
  if (!urlValidation.valid) {
    return res.status(400).json({ error: urlValidation.error });
  }

  console.log('📺 M3U8 Request (No Referer):', targetUrl);

  try {
    let customHeaders = parseCustomHeaders(req.query);
    delete customHeaders['Referer'];
    delete customHeaders['referer'];

    const requestHeaders = buildRequestHeaders(customHeaders, false);

    const targetResponse = await fetchWithRetry(targetUrl, {
      headers: requestHeaders
    });

    if (!targetResponse.ok) {
      console.error('❌ M3U8 fetch failed:', targetResponse.status);
      return res.status(targetResponse.status).json({
        error: 'Failed to fetch M3U8',
        status: targetResponse.status
      });
    }

    let m3u8Content = await targetResponse.text();
    console.log('✅ M3U8 fetched (No Referer), size:', m3u8Content.length);

    // Rewrite M3U8 content
    const baseUrl = targetUrl.substring(0, targetUrl.lastIndexOf('/') + 1);
    // req.protocol is reliable now that trust proxy is enabled; fallback to https for safety
    //const protocol = req.protocol || 'https';
    const protocol = 'https'; // Force https for proxy URLs to avoid mixed content issues in browsers
    const proxyBaseUrl = `${protocol}://${req.get('host')}`;
    m3u8Content = rewriteM3U8Content(m3u8Content, baseUrl, proxyBaseUrl, customHeaders);

    res.setHeader('Content-Type', 'application/vnd.apple.mpegurl');
    res.setHeader('Cache-Control', 'no-cache');
    res.send(m3u8Content);

  } catch (error) {
    console.error('❌ M3U8 proxy error (No Referer):', error);
    res.status(502).json({
      error: 'Proxy error',
      message: error.message,
      type: error.name
    });
  }
});

// TS/Segment proxy endpoint
app.get('/ts-proxy', async (req, res) => {
  const targetUrl = req.query.url;
  
  if (!targetUrl) {
    return res.status(400).json({ error: 'Missing url parameter' });
  }

  const urlValidation = validateUrl(targetUrl);
  if (!urlValidation.valid) {
    return res.status(400).json({ error: urlValidation.error });
  }

  try {
    const customHeaders = parseCustomHeaders(req.query);
    const requestHeaders = buildRequestHeaders(customHeaders, true);

    // Forward Range header if present
    const rangeHeader = req.get('Range');
    if (rangeHeader) {
      requestHeaders['Range'] = rangeHeader;
    }

    const targetResponse = await fetchWithRetry(targetUrl, {
      headers: requestHeaders
    }, RETRY_CONFIG.maxRetries, SEGMENT_TIMEOUT_MS);

    if (!targetResponse.ok) {
      console.error('❌ Segment fetch failed:', targetResponse.status);
      return res.status(targetResponse.status).json({
        error: 'Failed to fetch segment',
        status: targetResponse.status
      });
    }

    const contentType = targetResponse.headers.get('content-type') || 'video/MP2T';

    // Set response headers
    res.setHeader('Content-Type', contentType);
    res.setHeader('Accept-Ranges', 'bytes');
    res.setHeader('X-Upstream-Status', targetResponse.status);

    // Forward other headers
    for (const [key, value] of targetResponse.headers.entries()) {
      const lowerKey = key.toLowerCase();
      if (
        lowerKey !== 'content-type' &&
        lowerKey !== 'access-control-allow-origin' &&
        lowerKey !== 'access-control-allow-headers' &&
        lowerKey !== 'access-control-allow-methods' &&
        lowerKey !== 'x-upstream-status'
      ) {
        res.setHeader(key, value);
      }
    }

    // Stream the response
    targetResponse.body.pipe(res);

  } catch (error) {
    console.error('❌ Segment proxy error:', error);
    res.status(502).json({
      error: 'Proxy error',
      message: error.message,
      type: error.name
    });
  }
});

// MP4 proxy endpoint
app.get('/mp4-proxy', async (req, res) => {
  const targetUrl = req.query.url;
  
  if (!targetUrl) {
    return res.status(400).json({ error: 'Missing url parameter' });
  }

  const urlValidation = validateUrl(targetUrl);
  if (!urlValidation.valid) {
    return res.status(400).json({ error: urlValidation.error });
  }

  console.log('🎬 MP4 Request:', targetUrl);

  try {
    const customHeaders = parseCustomHeaders(req.query);
    const requestHeaders = buildRequestHeaders(customHeaders, true);

    // Forward Range header if present
    const rangeHeader = req.get('Range');
    if (rangeHeader) {
      requestHeaders['Range'] = rangeHeader;
    }

    const targetResponse = await fetchWithRetry(targetUrl, {
      headers: requestHeaders
    });

    if (!targetResponse.ok) {
      console.error('❌ MP4 fetch failed:', targetResponse.status);
      return res.status(targetResponse.status).json({
        error: 'Failed to fetch MP4',
        status: targetResponse.status
      });
    }

    const contentType = targetResponse.headers.get('content-type') || 'video/mp4';
    console.log('✅ MP4 fetched:', contentType);

    // Set response headers
    res.setHeader('Content-Type', contentType);
    res.setHeader('Accept-Ranges', 'bytes');
    res.setHeader('X-Upstream-Status', targetResponse.status);

    // Forward other headers
    for (const [key, value] of targetResponse.headers.entries()) {
      const lowerKey = key.toLowerCase();
      if (
        lowerKey !== 'content-type' &&
        lowerKey !== 'access-control-allow-origin' &&
        lowerKey !== 'access-control-allow-headers' &&
        lowerKey !== 'access-control-allow-methods' &&
        lowerKey !== 'x-upstream-status'
      ) {
        res.setHeader(key, value);
      }
    }

    // Stream the response
    targetResponse.body.pipe(res);

  } catch (error) {
    console.error('❌ MP4 proxy error:', error);
    res.status(502).json({
      error: 'Proxy error',
      message: error.message,
      type: error.name
    });
  }
});

// Generic fetch endpoint
app.get('/fetch', async (req, res) => {
  const targetUrl = req.query.url;
  
  if (!targetUrl) {
    return res.status(400).json({ error: 'Missing url parameter' });
  }

  const urlValidation = validateUrl(targetUrl);
  if (!urlValidation.valid) {
    return res.status(400).json({ error: urlValidation.error });
  }

  console.log('🌐 Fetch Request:', targetUrl);

  try {
    const customHeaders = parseCustomHeaders(req.query);
    const requestHeaders = buildRequestHeaders(customHeaders, true);

    // Forward Range header if present
    const rangeHeader = req.get('Range');
    if (rangeHeader) {
      requestHeaders['Range'] = rangeHeader;
    }

    const targetResponse = await fetchWithRetry(targetUrl, {
      method: req.method,
      headers: requestHeaders
    });

    if (!targetResponse.ok) {
      console.error('❌ Fetch failed:', targetResponse.status);
      return res.status(targetResponse.status).json({
        error: 'Failed to fetch resource',
        status: targetResponse.status
      });
    }

    const contentType = targetResponse.headers.get('content-type') || 'application/octet-stream';
    console.log('✅ Fetched:', contentType);

    // Set response headers
    res.setHeader('Content-Type', contentType);
    res.setHeader('Accept-Ranges', 'bytes');
    res.setHeader('X-Upstream-Status', targetResponse.status);

    // Forward other headers
    for (const [key, value] of targetResponse.headers.entries()) {
      const lowerKey = key.toLowerCase();
      if (
        lowerKey !== 'content-type' &&
        lowerKey !== 'access-control-allow-origin' &&
        lowerKey !== 'access-control-allow-headers' &&
        lowerKey !== 'access-control-allow-methods' &&
        lowerKey !== 'x-upstream-status'
      ) {
        res.setHeader(key, value);
      }
    }

    // Stream the response
    targetResponse.body.pipe(res);

  } catch (error) {
    console.error('❌ Fetch proxy error:', error);
    res.status(502).json({
      error: 'Proxy error',
      message: error.message,
      type: error.name
    });
  }
});

// Generic fetch endpoint (no referer)
app.get('/fetch-no-referer', async (req, res) => {
  const targetUrl = req.query.url;
  
  if (!targetUrl) {
    return res.status(400).json({ error: 'Missing url parameter' });
  }

  const urlValidation = validateUrl(targetUrl);
  if (!urlValidation.valid) {
    return res.status(400).json({ error: urlValidation.error });
  }

  console.log('🌐 Fetch Request (No Referer)');

  try {
    let customHeaders = parseCustomHeaders(req.query);
    delete customHeaders['Referer'];
    delete customHeaders['referer'];

    const requestHeaders = buildRequestHeaders(customHeaders, false);

    // Forward Range header if present
    const rangeHeader = req.get('Range');
    if (rangeHeader) {
      requestHeaders['Range'] = rangeHeader;
    }

    const targetResponse = await fetchWithRetry(targetUrl, {
      method: req.method,
      headers: requestHeaders
    });

    if (!targetResponse.ok) {
      console.error('❌ Fetch failed:', targetResponse.status);
      return res.status(targetResponse.status).json({
        error: 'Failed to fetch resource',
        status: targetResponse.status
      });
    }

    const contentType = targetResponse.headers.get('content-type') || 'application/octet-stream';
    console.log('✅ Fetched (No Referer):', contentType);

    // Set response headers
    res.setHeader('Content-Type', contentType);
    res.setHeader('Accept-Ranges', 'bytes');
    res.setHeader('X-Upstream-Status', targetResponse.status);

    // Forward other headers
    for (const [key, value] of targetResponse.headers.entries()) {
      const lowerKey = key.toLowerCase();
      if (
        lowerKey !== 'content-type' &&
        lowerKey !== 'access-control-allow-origin' &&
        lowerKey !== 'access-control-allow-headers' &&
        lowerKey !== 'access-control-allow-methods' &&
        lowerKey !== 'x-upstream-status'
      ) {
        res.setHeader(key, value);
      }
    }

    // Stream the response
    targetResponse.body.pipe(res);

  } catch (error) {
    console.error('❌ Fetch proxy error (No Referer):', error);
    res.status(502).json({
      error: 'Proxy error',
      message: error.message,
      type: error.name
    });
  }
});

// Subtitle proxy endpoint
app.get('/subtitle', async (req, res) => {
  const targetUrl = req.query.url;
  
  if (!targetUrl) {
    return res.status(400).json({ error: 'Missing url parameter' });
  }

  const urlValidation = validateUrl(targetUrl);
  if (!urlValidation.valid) {
    return res.status(400).json({ error: urlValidation.error });
  }

  try {
    const customHeaders = parseCustomHeaders(req.query);
    const requestHeaders = buildRequestHeaders(customHeaders, true);

    const response = await fetchWithRetry(targetUrl, {
      headers: requestHeaders
    }, 1, 15000);

    if (!response.ok) {
      return res.status(502).json({ error: 'Failed to fetch subtitle', status: response.status });
    }

    const buffer = await response.buffer();
    
    // Try to decode as UTF-8, fallback to ISO-8859-1 if it looks wrong
    let text = '';
    try {
      text = buffer.toString('utf-8');
      if ((text.match(/�/g) || []).length > 10) {
        text = buffer.toString('latin1');
      }
    } catch (e) {
      text = buffer.toString('latin1');
    }

    // Try to parse as SRT or VTT
    let entries = parseSRTorVTT(text);
    if (!entries || entries.length === 0) {
      return res.status(415).json({ error: 'Unsupported subtitle format or failed to parse.' });
    }

    // Convert to SRT
    const srt = entriesToSRT(entries);
    
    res.setHeader('Content-Type', 'text/plain; charset=utf-8');
    res.send(srt);
  } catch (err) {
    res.status(500).json({ error: 'Failed to fetch or convert subtitle', message: err.message });
  }
});

// Minimal SRT/VTT parser
function parseSRTorVTT(text) {
  text = text.replace(/^\uFEFF/, '').replace(/\r\n|\r/g, '\n');
  text = text.replace(/^WEBVTT.*?\n+/, '');
  const blocks = text.split(/\n{2,}/);
  const entries = [];
  for (const block of blocks) {
    const lines = block.split('\n').filter(Boolean);
    if (lines.length < 2) continue;
    let idx = 0;
    if (/^\d+$/.test(lines[0])) idx = 1;
    const timeMatch = lines[idx].match(/(\d{2}:\d{2}:\d{2}[.,]\d{3})\s*-->\s*(\d{2}:\d{2}:\d{2}[.,]\d{3})/);
    if (!timeMatch) continue;
    const start = timeMatch[1].replace(',', '.');
    const end = timeMatch[2].replace(',', '.');
    const textLines = lines.slice(idx + 1).join('\n');
    entries.push({ start, end, text: textLines });
  }
  return entries;
}

// Convert parsed entries to SRT format
function entriesToSRT(entries) {
  return entries.map((e, i) => `${i + 1}\n${e.start.replace('.', ',')} --> ${e.end.replace('.', ',')}\n${e.text}\n`).join('\n');
}

// 404 handler
app.use((req, res) => {
  res.status(404).json({
    error: 'Not Found',
    availableEndpoints: [
      '/health',
      '/m3u8-proxy?url=<url>&headers=<json_headers>',
      '/m3u8-proxy-no-referer?url=<url>&headers=<json_headers>',
      '/ts-proxy?url=<url>&headers=<json_headers>',
      '/mp4-proxy?url=<url>&headers=<json_headers>',
      '/fetch?url=<url>&headers=<json_headers>',
      '/fetch-no-referer?url=<url>&headers=<json_headers>',
      '/subtitle?url=<subtitle_url>&headers=<json_headers>'
    ]
  });
});

// Error handler
app.use((err, req, res, next) => {
  console.error('Server error:', err);
  res.status(500).json({
    error: 'Internal Server Error',
    message: err.message
  });
});

// Start server
app.listen(PORT, () => {
  console.log(`🚀 Proxy server running on port ${PORT}`);
  console.log(`📍 Health check: http://localhost:${PORT}/health`);
});
