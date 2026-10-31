// The hub's JSON API, relative to the page so the dashboard also works behind a proxy that
// mounts the hub under a path. Phase 5a adds the dashboard token here; phase 3 adds peer hubs
// (one base URL per hub).

const BASE = new URL('../api/v1/', document.baseURI);

/** GET /api/v1/<name> as JSON; throws on a non-2xx answer or a network error. */
export async function get(name, { signal } = {}) {
  const response = await fetch(new URL(name, BASE), {
    headers: { accept: 'application/json' },
    cache: 'no-store',
    signal,
  });
  if (!response.ok) throw new Error(`${name}: HTTP ${response.status}`);
  return response.json();
}

/** URL of the server-sent events stream. */
export function streamUrl() {
  return new URL('stream', BASE).href;
}
