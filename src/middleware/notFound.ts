import { Request, Response } from 'express';

const ENV_LABEL = (process.env.NODE_ENV ?? 'development') !== 'production' ? 'development' : 'production';

function renderHtmlPage(): string {
  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Streaming Proxy</title>
  <style>
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #0f1117; color: #e2e8f0; min-height: 100vh; display: flex; align-items: center; justify-content: center; padding: 2rem; }
    .card { background: #1a1d27; border: 1px solid #2d3148; border-radius: 12px; padding: 2.5rem; max-width: 680px; width: 100%; }
    h1 { font-size: 1.4rem; font-weight: 600; color: #fff; margin-bottom: 0.4rem; }
    .badge { display: inline-block; background: #22c55e22; color: #22c55e; border: 1px solid #22c55e44; border-radius: 99px; font-size: 0.7rem; font-weight: 600; padding: 0.15rem 0.6rem; letter-spacing: 0.05em; vertical-align: middle; margin-left: 0.5rem; }
    p.sub { color: #64748b; font-size: 0.875rem; margin-bottom: 2rem; }
    ul { list-style: none; display: flex; flex-direction: column; gap: 0.6rem; }
    li { background: #0f1117; border: 1px solid #2d3148; border-radius: 8px; padding: 0.75rem 1rem; display: flex; align-items: baseline; gap: 0.75rem; }
    li:hover { border-color: #4f5882; }
    .ep { font-family: 'SF Mono', 'Fira Code', monospace; font-size: 0.8rem; color: #818cf8; white-space: nowrap; }
    .desc { font-size: 0.8rem; color: #94a3b8; }
    .param { color: #f59e0b; }
  </style>
</head>
<body>
  <div class="card">
    <h1>Streaming Proxy <span class="badge">ONLINE</span></h1>
    <p class="sub">Node ${process.version} &bull; ${ENV_LABEL}</p>
    <ul>
      <li><span class="ep">/health</span><span class="desc">Server status &amp; uptime</span></li>
      <li><span class="ep">/m3u8-proxy?<span class="param">url</span>=&amp;<span class="param">headers</span>=&amp;<span class="param">host</span>=</span><span class="desc">HLS master or media playlist</span></li>
      <li><span class="ep">/ts-proxy?<span class="param">url</span>=&amp;<span class="param">headers</span>=&amp;<span class="param">host</span>=</span><span class="desc">TS / fMP4 / .m4s segments</span></li>
      <li><span class="ep">/mp4-proxy?<span class="param">url</span>=&amp;<span class="param">headers</span>=&amp;<span class="param">host</span>=</span><span class="desc">Progressive MP4 with seek &amp; range</span></li>
      <li><span class="ep">/fetch?<span class="param">url</span>=&amp;<span class="param">headers</span>=</span><span class="desc">Generic resource (keys, manifests)</span></li>
      <li><span class="ep">/subtitle?<span class="param">url</span>=&amp;<span class="param">headers</span>=</span><span class="desc">SRT / VTT subtitle conversion</span></li>
    </ul>
  </div>
</body>
</html>`;
}

const JSON_ENDPOINTS = [
  '/health',
  '/m3u8-proxy?url=<url>&headers=<json>&host=<hostname>',
  '/ts-proxy?url=<url>&headers=<json>&host=<hostname>',
  '/mp4-proxy?url=<url>&headers=<json>&host=<hostname>',
  '/fetch?url=<url>&headers=<json>',
  '/subtitle?url=<url>&headers=<json>',
];

export function notFoundHandler(req: Request, res: Response): void {
  const acceptsHtml = req.headers['accept']?.includes('text/html');

  if (acceptsHtml) {
    res.status(404).setHeader('Content-Type', 'text/html; charset=utf-8').send(renderHtmlPage());
    return;
  }

  res.status(404).json({ error: 'Not Found', availableEndpoints: JSON_ENDPOINTS });
}