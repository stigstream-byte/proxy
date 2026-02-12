# Streaming Proxy Server

A Node.js Express proxy server for streaming video content (M3U8, HLS, TS segments, MP4) with header forwarding and anti-bot bypassing capabilities.

Converted from Cloudflare Workers to standard Node.js/Express.

## Features

- ✅ M3U8 playlist proxying with automatic URL rewriting
- ✅ TS/HLS segment proxying
- ✅ MP4 video proxying
- ✅ Subtitle (SRT/VTT) proxying and conversion
- ✅ Custom header forwarding
- ✅ Range request support
- ✅ Automatic retry with exponential backoff
- ✅ Request deduplication
- ✅ CORS enabled
- ✅ Anti-bot bypassing with realistic browser headers

## Installation

1. Install dependencies:
```bash
npm install
```

2. Start the server:
```bash
npm start
```

For development with auto-reload:
```bash
npm run dev
```

The server will start on port 3000 by default. You can change this by setting the `PORT` environment variable:
```bash
PORT=8080 npm start
```

## API Endpoints

### Health Check
```
GET /health
```
Returns server status and timestamp.

### M3U8 Proxy
```
GET /m3u8-proxy?url=<url>&headers=<json_headers>
```
Proxies M3U8 playlists and automatically rewrites URLs to go through the proxy.

**Parameters:**
- `url` (required): The M3U8 playlist URL to proxy
- `headers` (optional): JSON object of custom headers to send with the request

**Example:**
```
GET /m3u8-proxy?url=https://example.com/playlist.m3u8
GET /m3u8-proxy?url=https://example.com/playlist.m3u8&headers={"Referer":"https://example.com"}
```

### M3U8 Proxy (No Referer)
```
GET /m3u8-proxy-no-referer?url=<url>&headers=<json_headers>
```
Same as `/m3u8-proxy` but explicitly removes the Referer header.

### TS/Segment Proxy
```
GET /ts-proxy?url=<url>&headers=<json_headers>
```
Proxies video segments (TS, M4S, MP4 fragments).

**Parameters:**
- `url` (required): The segment URL to proxy
- `headers` (optional): JSON object of custom headers

**Example:**
```
GET /ts-proxy?url=https://example.com/segment0.ts
```

### MP4 Proxy
```
GET /mp4-proxy?url=<url>&headers=<json_headers>
```
Proxies MP4 video files with support for range requests.

**Parameters:**
- `url` (required): The MP4 file URL to proxy
- `headers` (optional): JSON object of custom headers

**Example:**
```
GET /mp4-proxy?url=https://example.com/video.mp4
```

### Generic Fetch
```
GET /fetch?url=<url>&headers=<json_headers>
```
Generic proxy endpoint for any resource.

**Parameters:**
- `url` (required): The resource URL to fetch
- `headers` (optional): JSON object of custom headers

### Generic Fetch (No Referer)
```
GET /fetch-no-referer?url=<url>&headers=<json_headers>
```
Same as `/fetch` but explicitly removes the Referer header.

### Subtitle Proxy
```
GET /subtitle?url=<url>&headers=<json_headers>
```
Proxies and converts subtitle files (SRT/VTT) to SRT format with proper UTF-8 encoding.

**Parameters:**
- `url` (required): The subtitle file URL to proxy
- `headers` (optional): JSON object of custom headers

**Example:**
```
GET /subtitle?url=https://example.com/subtitles.vtt
```

## Custom Headers

You can pass custom headers in two ways:

### 1. JSON Headers Parameter
Pass headers as a JSON object in the `headers` query parameter:
```
?headers={"Referer":"https://example.com","User-Agent":"Custom UA"}
```

Or base64-encoded:
```
?headers=eyJSZWZlcmVyIjoiaHR0cHM6Ly9leGFtcGxlLmNvbSJ9
```

### 2. Individual Header Parameters
Pass individual headers with the `header_` prefix:
```
?header_referer=https://example.com&header_user_agent=CustomUA
```

## Security Features

- **SSRF Protection**: Prevents proxying to private IP addresses and localhost
- **Protocol Validation**: Only allows HTTP/HTTPS protocols
- **URL Validation**: Validates all URLs before proxying

## Configuration

The following constants can be modified in `server.js`:

```javascript
const RETRY_CONFIG = {
  maxRetries: 2,              // Maximum retry attempts
  initialDelay: 25,           // Initial delay in ms
  maxDelay: 300,              // Maximum delay in ms
  backoffMultiplier: 2        // Exponential backoff multiplier
};

const TIMEOUT_MS = 15000;           // Timeout for M3U8 requests (15s)
const SEGMENT_TIMEOUT_MS = 10000;   // Timeout for segment requests (10s)
```

## Example Usage

### Video.js Integration
```javascript
const player = videojs('my-video');

const m3u8Url = 'https://example.com/playlist.m3u8';
const proxyUrl = `http://localhost:3000/m3u8-proxy?url=${encodeURIComponent(m3u8Url)}`;

player.src({
  src: proxyUrl,
  type: 'application/x-mpegURL'
});
```

### HLS.js Integration
```javascript
const video = document.getElementById('video');
const hls = new Hls();

const m3u8Url = 'https://example.com/playlist.m3u8';
const proxyUrl = `http://localhost:3000/m3u8-proxy?url=${encodeURIComponent(m3u8Url)}`;

hls.loadSource(proxyUrl);
hls.attachMedia(video);
```

### With Custom Headers
```javascript
const headers = {
  'Referer': 'https://example.com',
  'Origin': 'https://example.com'
};

const m3u8Url = 'https://example.com/playlist.m3u8';
const proxyUrl = `http://localhost:3000/m3u8-proxy?url=${encodeURIComponent(m3u8Url)}&headers=${encodeURIComponent(JSON.stringify(headers))}`;
```

## Deployment

### Local Development
```bash
npm run dev
```

### Production
```bash
npm start
```

### Using PM2
```bash
npm install -g pm2
pm2 start server.js --name "proxy-server"
pm2 save
pm2 startup
```

### Docker
Create a `Dockerfile`:
```dockerfile
FROM node:18-alpine
WORKDIR /app
COPY package*.json ./
RUN npm ci --only=production
COPY . .
EXPOSE 3000
CMD ["node", "server.js"]
```

Build and run:
```bash
docker build -t streaming-proxy .
docker run -p 3000:3000 streaming-proxy
```

### Environment Variables
- `PORT`: Server port (default: 3000)
- `NODE_ENV`: Environment (development/production)

## Differences from Cloudflare Workers

This Node.js version differs from the original Cloudflare Workers version in the following ways:

1. **No Cloudflare-specific features**: Removed `cf` fetch options and Cloudflare cache API
2. **No Turnstile verification**: Removed Cloudflare Turnstile bot protection
3. **Standard Node.js streaming**: Uses Node.js streams instead of Web Streams API
4. **Express framework**: Built on Express.js for easier routing and middleware
5. **Standard npm packages**: Uses `node-fetch` and `abort-controller` instead of global fetch

## License

MIT