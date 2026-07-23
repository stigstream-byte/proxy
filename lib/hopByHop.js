/**
 * Headers that shouldn't be blindly forwarded from the upstream response
 * back to the client (either because they're connection-specific, or
 * because we've already changed the body encoding/length by proxying it).
 */
export const HOP_BY_HOP = new Set([
  'connection',
  'keep-alive',
  'transfer-encoding',
  'upgrade',
  'proxy-authenticate',
  'proxy-authorization',
  'te',
  'trailer',
  'content-encoding',
  'content-length',
]);

export function copyResponseHeaders(upstreamHeaders, res) {
  upstreamHeaders.forEach((value, key) => {
    if (!HOP_BY_HOP.has(key.toLowerCase())) {
      res.setHeader(key, value);
    }
  });
}
