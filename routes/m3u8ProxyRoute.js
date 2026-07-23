import express from 'express';
import { parseHeaders } from '../lib/headers.js';
import { runM3u8Proxy } from '../lib/handlers.js';

const router = express.Router();

// GET /m3u8-proxy?url=<target>&headers=<json>
// Fetches the playlist text and rewrites every URI inside it so segments,
// nested playlists, and metadata resources all route back through this
// proxy as encrypted tokens - see lib/tokenCrypto.js and lib/handlers.js.
router.get('/m3u8-proxy', async (req, res) => {
  const { url, headers: headersParam } = req.query;
  if (!url) return res.status(400).json({ error: 'Missing "url" query parameter' });

  await runM3u8Proxy(req, res, { url, headers: parseHeaders(headersParam) });
});

export default router;
