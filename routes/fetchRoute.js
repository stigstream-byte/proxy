import express from 'express';
import { parseHeaders } from '../lib/headers.js';
import { runFetch } from '../lib/handlers.js';

const router = express.Router();

// GET /fetch?url=<target>&headers=<json>
// Also accepts POST/PUT/PATCH/DELETE with a raw body, which gets
// forwarded upstream as-is. This is the generic "just proxy it" endpoint.
router.all('/fetch', async (req, res) => {
  const { url, headers: headersParam } = req.query;
  if (!url) return res.status(400).json({ error: 'Missing "url" query parameter' });

  await runFetch(req, res, { url, headers: parseHeaders(headersParam) });
});

export default router;
