// The hub's JSON API, relative to the page so the dashboard also works behind a proxy that
// mounts the hub under a path. Phase 5a adds the dashboard token here. Peer hubs
// (KRAKEN_HUB_PEERS, ADR 0016) are read with `getFrom`, cross-origin, without credentials.

const BASE = new URL('../api/v1/', document.baseURI);

/** A peer that does not answer within this long counts as unreachable. */
const PEER_TIMEOUT_MS = 3000;

async function getJson(url, name, { signal } = {}) {
  const response = await fetch(url, {
    headers: { accept: 'application/json' },
    cache: 'no-store',
    credentials: 'omit',
    signal,
  });
  if (!response.ok) throw new Error(`${name}: HTTP ${response.status}`);
  return response.json();
}

/** GET /api/v1/<name> as JSON; throws on a non-2xx answer or a network error. */
export function get(name, { signal } = {}) {
  return getJson(new URL(name, BASE), name, { signal });
}

/**
 * GET <origin>/api/v1/<name> on a peer hub; `origin` comes from this hub's KRAKEN_HUB_PEERS
 * (validated at its startup). Throws on an error answer, a network error or a timeout.
 */
export async function getFrom(origin, name) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), PEER_TIMEOUT_MS);
  try {
    return await getJson(new URL(`api/v1/${name}`, `${origin}/`), name, { signal: controller.signal });
  } catch (error) {
    throw new Error(controller.signal.aborted ? `no answer within ${PEER_TIMEOUT_MS / 1000} s` : error.message);
  } finally {
    clearTimeout(timer);
  }
}

/** URL of the server-sent events stream. */
export function streamUrl() {
  return new URL('stream', BASE).href;
}
