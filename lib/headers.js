/**
 * Parses the `headers` query param, which is expected to be a JSON object
 * encoded as a string, e.g. ?headers={"Referer":"https://example.com"}
 *
 * Always returns a plain object (empty if missing/invalid) so callers
 * never need to null-check.
 */
export function parseHeaders(raw) {
  if (!raw) return {};

  try {
    const parsed = JSON.parse(raw);
    if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
      return {};
    }
    return parsed;
  } catch {
    return {};
  }
}
