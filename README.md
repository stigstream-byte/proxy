# lite-proxy

A tiny, modular HTTP proxy with three endpoints. No magic — it just
forwards requests upstream with whatever headers you give it, and for
m3u8 playlists it rewrites the internal URLs so segments/keys/nested
playlists keep flowing back through the proxy.

## Install & run

```bash
npm install
npm start
# -> lite-proxy listening on http://localhost:8080
```

Set `PROXY_ENCRYPT_KEY` in your `.env` (a 64-char hex string, or any passphrase)
before starting the server - it's required for the token layer described below.

## Endpoints

All three take:
- `url` (required) — the upstream URL to hit
- `headers` (optional) — a JSON object, URL-encoded, of headers to send upstream

### `GET|POST|PUT|PATCH|DELETE /fetch?url=&headers=`
Generic passthrough. Forwards method, headers, and raw request body
upstream, and streams the response (status + headers + body) straight
back. Use this for anything that isn't a media segment or a playlist.

```
/fetch?url=https://api.example.com/data&headers={"Authorization":"Bearer xyz"}
```

POST with a body just works — whatever you send in the request body is
forwarded upstream unchanged.

### `GET /ts-proxy?url=&headers=`
Streams a binary resource through (video/audio segments, key files,
etc). Forwards the client's `Range` header upstream so seeking works.

```
/ts-proxy?url=https://cdn.example.com/segment1.ts&headers={"Referer":"https://example.com"}
```

### `GET /m3u8-proxy?url=&headers=`
Fetches an m3u8 playlist and rewrites every URL inside it so the
player keeps calling back through this proxy (with the same headers):

- Media segments and other binary URIs -> `/ts-proxy`
- Nested/variant playlists (`.m3u8`) -> `/m3u8-proxy` (recursive)
- `#EXT-X-KEY` / `#EXT-X-MAP` metadata URIs -> `/fetch`

```
/m3u8-proxy?url=https://cdn.example.com/master.m3u8&headers={"Referer":"https://example.com"}
```

### `GET /x/:token`
An opaque, encrypted (AES-256-GCM) stand-in for any of the three endpoints
above plus its `url`/`headers`. **Every URL the m3u8 rewriter generates
now uses this** - segments and nested playlists go out as `/x/<token>`
instead of `/ts-proxy?url=...&headers=...`, so the upstream URL and your
headers never appear in plaintext, and a token can't be forged or edited
(GCM's auth tag makes tampering fail with a 400).

You can also mint tokens yourself from another service that shares the
same `PROXY_ENCRYPT_KEY`, using the same `encrypt()` from
`lib/tokenCrypto.js`:

```js
import { encrypt } from './lib/tokenCrypto.js';

const token = encrypt({
  e: 'm3u8-proxy',                       // 'fetch' | 'ts-proxy' | 'm3u8-proxy'
  u: 'https://cdn.example.com/master.m3u8',
  h: JSON.stringify({ Referer: 'https://example.com' }), // optional
});
// -> GET /x/<token>
```

The plain `?url=&headers=` endpoints are still there and unaffected -
useful for manual testing, or as the entrypoint your own trusted backend
calls to kick off the very first request.

## Layout

```
server.js                  - wires everything together
routes/fetchRoute.js        - /fetch
routes/tsProxyRoute.js      - /ts-proxy
routes/m3u8ProxyRoute.js    - /m3u8-proxy
routes/tokenRoute.js        - /x/:token (decrypts + dispatches to a handler)
lib/handlers.js             - the actual fetch/ts-proxy/m3u8 logic, shared
                              by both the plain routes and the token route
lib/headers.js              - safe JSON header parsing
lib/hopByHop.js              - strips connection-specific response headers
lib/buildProxyUrl.js         - builds encrypted /x/<token> proxy URLs
lib/m3u8Rewriter.js          - the playlist rewriting logic
lib/tokenCrypto.js           - AES-256-GCM encrypt/decrypt for tokens
```
