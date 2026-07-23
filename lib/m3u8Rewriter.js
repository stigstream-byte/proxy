import { buildProxyUrl } from './buildProxyUrl.js';

const METADATA_TAGS = ['#EXT-X-KEY', '#EXT-X-MAP'];
const PLAYLIST_PRECEDING_TAGS = ['#EXT-X-STREAM-INF', '#EXT-X-I-FRAME-STREAM-INF'];
const PLAYLIST_ATTRIBUTE_TAGS = ['#EXT-X-MEDIA'];

function isPlaylistUrl(url) {
  return /\.m3u8(?:\?|$)/i.test(url);
}

function resolve(uri, baseUrl) {
  return new URL(uri, baseUrl).toString();
}

function pickEndpoint(url, forceFetch = false) {
  if (forceFetch) return 'fetch';
  return isPlaylistUrl(url) ? 'm3u8-proxy' : 'ts-proxy';
}

export function rewritePlaylist(playlistText, baseUrl, headers, req) {
  const lines = playlistText.split(/\r?\n/);
  let nextLineIsPlaylist = false;

  const rewritten = lines.map((line) => {
    const trimmed = line.trim();

    if (!trimmed) return line;

    // Rewrite URI="..." attributes
    if (trimmed.startsWith('#')) {
      const isPlaylistAttribute = PLAYLIST_ATTRIBUTE_TAGS.some(tag =>
        trimmed.startsWith(tag)
      );

      const uriMatch = line.match(/URI="([^\"]+)"/);

      if (!uriMatch) {
        if (PLAYLIST_PRECEDING_TAGS.some(tag => trimmed.startsWith(tag))) {
          nextLineIsPlaylist = true;
        }
        return line;
      }

      const originalUri = uriMatch[1];
      const resolvedUrl = resolve(originalUri, baseUrl);

      const forceFetch = METADATA_TAGS.some(tag =>
        trimmed.startsWith(tag)
      );

      const endpoint = forceFetch
        ? 'fetch'
        : isPlaylistAttribute || isPlaylistUrl(resolvedUrl)
          ? 'm3u8-proxy'
          : 'ts-proxy';

      const proxiedUrl = buildProxyUrl(
        req,
        endpoint,
        resolvedUrl,
        headers
      );

      //console.log('[rewrite]', originalUri, '->', proxiedUrl);

      return line.replace(originalUri, proxiedUrl);
    }

    // Rewrite bare URI lines (segments/playlists)
    const resolvedUrl = resolve(trimmed, baseUrl);

    const endpoint = nextLineIsPlaylist || isPlaylistUrl(resolvedUrl)
      ? 'm3u8-proxy'
      : 'ts-proxy';

    nextLineIsPlaylist = false;

    const proxiedUrl = buildProxyUrl(
      req,
      endpoint,
      resolvedUrl,
      headers
    );

    //console.log('[rewrite]', trimmed, '->', proxiedUrl);

    return proxiedUrl;
  });

  //console.log(
    //`[rewritePlaylist] Rewrote ${rewritten.length} lines`
  //);

  return rewritten.join('\n');
}