import 'dotenv/config';
import express from 'express';
import fetchRoute from './routes/fetchRoute.js';
import tsProxyRoute from './routes/tsProxyRoute.js';
import m3u8ProxyRoute from './routes/m3u8ProxyRoute.js';
import tokenRoute from './routes/tokenRoute.js';
import { attachStatsMiddleware, getStatsSnapshot } from './lib/stats.js';

const app = express();

// Capture every request body as a raw Buffer regardless of Content-Type -
// we don't want to parse/transform it, just forward it upstream untouched.
app.use(express.raw({ type: () => true, limit: '50mb' }));

// Permissive CORS so the proxy can be called directly from a browser
// (e.g. an <hls.js> player or a <video> tag).
app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Headers', '*');
  res.setHeader('Access-Control-Allow-Methods', '*');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

const PORT = process.env.PORT || 3000;
const server = app.listen(PORT, () => {
  console.log(`lite-proxy listening on http://localhost:${PORT}`);
});

attachStatsMiddleware(app, server);

app.get('/health', (req, res) => {
  res.json({ status: 'ok' });
});

app.use(fetchRoute);
app.use(tsProxyRoute);
app.use(m3u8ProxyRoute);
app.use(tokenRoute);

app.get('/stats', (req, res) => {
  res.json(getStatsSnapshot());
});

app.get('/', (req, res) => {
  res.json({
    status: 'ok',
    endpoints: [
      'GET /health',
      'GET /stats',
      'GET|POST|PUT|PATCH|DELETE /fetch?url=&headers=',
      'GET /ts-proxy?url=&headers=',
      'GET /m3u8-proxy?url=&headers=',
      'GET /x/:token  (encrypted stand-in for any of the above - see lib/tokenCrypto.js)',
    ],
  });
});

