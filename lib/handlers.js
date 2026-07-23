import { Readable } from 'node:stream';
import { copyResponseHeaders } from './hopByHop.js';
import { rewritePlaylist } from './m3u8Rewriter.js';

/**
 * Generic passthrough: forwards method/headers/body upstream, streams the
 * response (status + headers + body) straight back.
 */
export async function runFetch(req, res, { url, headers }) {
  const method = req.method.toUpperCase();
  const init = { method, headers };

  const hasBody = !['GET', 'HEAD'].includes(method) && req.body && req.body.length > 0;
  if (hasBody) {
    init.body = req.body; // raw Buffer, captured by express.raw() in server.js
  }

  try {
    const upstream = await fetch(url, init);
    res.status(upstream.status);
    copyResponseHeaders(upstream.headers, res);

    const buffer = Buffer.from(await upstream.arrayBuffer());
    res.send(buffer);
  } catch (err) {
    res.status(502).json({ error: 'Upstream fetch failed', details: err.message });
  }
}

/**
 * Streams a binary resource (media segment, key file, init segment, etc.)
 * straight through. Forwards the client's Range header so seeking works.
 */
export async function runTsProxy(req, res, { url, headers }) {
  if (req.headers.range) {
    headers = { ...headers, Range: req.headers.range };
  }

  try {
    const upstream = await fetch(url, { headers });
    res.status(upstream.status);
    copyResponseHeaders(upstream.headers, res);

    if (!upstream.body) return res.end();
    Readable.fromWeb(upstream.body).pipe(res);
  } catch (err) {
    res.status(502).json({ error: 'Upstream fetch failed', details: err.message });
  }
}

/**
 * Fetches an m3u8 playlist and rewrites every URI inside it so segments,
 * nested playlists, and key/init resources route back through this proxy
 * (as encrypted tokens - see buildProxyUrl.js), carrying the same headers.
 */
export async function runM3u8Proxy(req, res, { url, headers }) {
  console.log('[runM3u8Proxy] hit, url:', url);
  try {
    const upstream = await fetch(url, { headers });

    if (!upstream.ok) {
      const body = await upstream.text();
      return res.status(upstream.status).send(body);
    }

    const playlistText = await upstream.text();
    const rewritten = rewritePlaylist(playlistText, url, headers, req);

    res.setHeader('Content-Type', 'application/vnd.apple.mpegurl');
    res.send(rewritten);
  } catch (err) {
    res.status(502).json({ error: 'Upstream fetch failed', details: err.message });
  }
}
