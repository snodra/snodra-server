// The hub's JSON API, relative to the page so the dashboard also works behind a proxy that
// mounts the hub under a path. When the hub has a dashboard token (KRAKEN_HUB_DASHBOARD_TOKEN,
// ADR 0018) every call carries it as `Authorization: Bearer <token>`: the page asks for it once,
// keeps it in sessionStorage (this tab only, gone when it closes) and never puts it in a URL, a
// cookie or a log line. Peer hubs (KRAKEN_HUB_PEERS, ADR 0016) are read with `getFrom`,
// cross-origin, without cookies, with the same token (the hubs of one deployment share it).

const BASE = new URL('../api/v1/', document.baseURI);
const TOKEN_KEY = 'kraken-hub.token';

/** A peer that does not answer within this long counts as unreachable. */
const PEER_TIMEOUT_MS = 3000;

/** The hub answered 401: it wants a (new) dashboard token. */
export class AuthError extends Error {
  constructor(name) {
    super(`${name}: the hub wants a dashboard token`);
    this.name = 'AuthError';
  }
}

function readToken() {
  try {
    return sessionStorage.getItem(TOKEN_KEY) || '';
  } catch {
    return '';
  }
}

let token = readToken();
const listeners = new Set();

/** True when a token is held for this tab. */
export const hasToken = () => token !== '';

/** Keeps `value` for this tab (sessionStorage when it is available, else memory). */
export function setToken(value) {
  token = value.trim();
  try {
    if (token) sessionStorage.setItem(TOKEN_KEY, token);
    else sessionStorage.removeItem(TOKEN_KEY);
  } catch {
    // Storage may be blocked: the token then lives until the page is reloaded.
  }
}

export const clearToken = () => setToken('');

/** Calls `fn()` whenever the hub answers 401 to one of its own calls. */
export function onUnauthorized(fn) {
  listeners.add(fn);
}

function headers(accept) {
  const h = { accept };
  if (token) h.authorization = `Bearer ${token}`;
  return h;
}

function unauthorized(name) {
  for (const fn of listeners) fn();
  return new AuthError(name);
}

async function getJson(url, name, { signal, own }) {
  const response = await fetch(url, {
    headers: headers('application/json'),
    cache: 'no-store',
    credentials: 'omit',
    signal,
  });
  if (response.status === 401 && own) throw unauthorized(name);
  if (!response.ok) throw new Error(`${name}: HTTP ${response.status}`);
  return response.json();
}

/** GET /api/v1/<name> as JSON; throws on a non-2xx answer or a network error. */
export function get(name, { signal } = {}) {
  return getJson(new URL(name, BASE), name, { signal, own: true });
}

/**
 * GET <origin>/api/v1/<name> on a peer hub; `origin` comes from this hub's KRAKEN_HUB_PEERS
 * (validated at its startup). Throws on an error answer, a network error or a timeout.
 */
export async function getFrom(origin, name) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), PEER_TIMEOUT_MS);
  try {
    return await getJson(new URL(`api/v1/${name}`, `${origin}/`), name, { signal: controller.signal, own: false });
  } catch (error) {
    throw new Error(controller.signal.aborted ? `no answer within ${PEER_TIMEOUT_MS / 1000} s` : error.message);
  } finally {
    clearTimeout(timer);
  }
}

/** True when this browser can read the event stream with fetch. */
export const canStream = () => typeof fetch === 'function' && typeof TextDecoder === 'function';

/**
 * Reads the server-sent events of /api/v1/stream with fetch, so the token travels in a header
 * like on every other call. (EventSource cannot set headers; a token in the URL would reach
 * logs, history and Referer.) Calls `onOpen()` once the stream answered, `onEvent(name, data)`
 * for each event, and resolves when the hub ends the stream. Rejects with AuthError on 401, with
 * an Error on another non-2xx answer or a network error, and with the abort reason when
 * `signal` aborts.
 */
export async function readStream({ signal, onOpen, onEvent }) {
  const response = await fetch(new URL('stream', BASE), {
    headers: headers('text/event-stream'),
    cache: 'no-store',
    credentials: 'omit',
    signal,
  });
  if (response.status === 401) throw unauthorized('stream');
  if (!response.ok || !response.body) throw new Error(`stream: HTTP ${response.status}`);
  onOpen();
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  for (;;) {
    const { done, value } = await reader.read();
    if (done) return;
    buffer += decoder.decode(value, { stream: true }).replace(/\r\n?/g, '\n');
    for (let end = buffer.indexOf('\n\n'); end !== -1; end = buffer.indexOf('\n\n')) {
      const block = buffer.slice(0, end);
      buffer = buffer.slice(end + 2);
      let name = 'message';
      const data = [];
      for (const line of block.split('\n')) {
        if (line.startsWith(':')) continue;
        const colon = line.indexOf(':');
        const field = colon === -1 ? line : line.slice(0, colon);
        const text = colon === -1 ? '' : line.slice(colon + 1).replace(/^ /, '');
        if (field === 'event') name = text;
        else if (field === 'data') data.push(text);
      }
      if (data.length) onEvent(name, data.join('\n'));
    }
  }
}
