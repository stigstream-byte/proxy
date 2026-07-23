import express from 'express';
import { parseHeaders } from '../lib/headers.js';
import { runTsProxy } from '../lib/handlers.js';

const router = express.Router();

// GET /ts-proxy?url=<target>&headers=<json>
// Streams a binary resource (video/audio segment, key file, etc.) straight
// through to the client. Forwards Range so seeking in players still works.
router.get('/ts-proxy', async (req, res) => {
  const { url, headers: headersParam } = req.query;
  if (!url) return res.status(400).json({ error: 'Missing "url" query parameter' });

  await runTsProxy(req, res, { url, headers: parseHeaders(headersParam) });
});

export default router;
