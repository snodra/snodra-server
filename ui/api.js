// The hub's JSON API, relative to the page so the dashboard also works behind a proxy that
// mounts the hub under a path. When the hub has a dashboard token (KRAKEN_HUB_DASHBOARD_TOKEN,
// ADR 0018) every call carries it as `Authorization: Bearer <token>`: the page asks for it once,
// keeps it in sessionStorage (this tab only, gone when it closes) and never puts it in a URL, a
// cookie or a log line. Peer hubs (KRAKEN_HUB_PEERS, ADR 0016) are read with `getFrom`,
// cross-origin, without cookies, with the same token (the hubs of one deployment share it).
// The processor controls (ADR 0023) send a second credential, the write token, kept the same way
// under its own key, only on `control` calls and only to this hub (it reaches every instance).
// The event browser (ADR 0025) reads this hub only, with the dashboard token, or the write token
// when the tab holds no dashboard token (the write token reads too, and a hub that has only a
// write token serves payloads to a request that presents it).

const BASE = new URL('../api/v1/', document.baseURI);
const TOKEN_KEY = 'kraken-hub.token';
const WRITE_TOKEN_KEY = 'kraken-hub.write-token';

/** A peer that does not answer within this long counts as unreachable. */
const PEER_TIMEOUT_MS = 3000;

/** The hub answered 401: it wants a (new) dashboard token. */
export class AuthError extends Error {
  constructor(name) {
    super(`${name}: the hub wants a dashboard token`);
    this.name = 'AuthError';
  }
}

function readStored(key) {
  try {
    return sessionStorage.getItem(key) || '';
  } catch {
    return '';
  }
}

/** Keeps `value` under `key` for this tab (sessionStorage when it is available, else memory). */
function store(key, value) {
  try {
    if (value) sessionStorage.setItem(key, value);
    else sessionStorage.removeItem(key);
  } catch {
    // Storage may be blocked: the token then lives until the page is reloaded.
  }
}

let token = readStored(TOKEN_KEY);
let writeToken = readStored(WRITE_TOKEN_KEY);
const listeners = new Set();

/** True when a token is held for this tab. */
export const hasToken = () => token !== '';

/** Keeps `value` for this tab (sessionStorage when it is available, else memory). */
export function setToken(value) {
  token = value.trim();
  store(TOKEN_KEY, token);
}

export const clearToken = () => setToken('');

/** True when a write credential (the write token or an ACL credential with control = true) is
 * held for this tab: the pages then show their controls. */
export const hasWriteToken = () => writeToken !== '';

/** Keeps the write credential for this tab, like the dashboard token. */
export function setWriteToken(value) {
  writeToken = value.trim();
  store(WRITE_TOKEN_KEY, writeToken);
}

export const clearWriteToken = () => setWriteToken('');

/** Calls `fn()` whenever the hub answers 401 to one of its own calls. */
export function onUnauthorized(fn) {
  listeners.add(fn);
}

function headers(accept, credential = token) {
  const h = { accept };
  if (credential) h.authorization = `Bearer ${credential}`;
  return h;
}

/** The credential of the event browser's reads: the dashboard token, else the write token. */
const browserCredential = () => token || writeToken;

/** An Error carrying the HTTP status of the answer (`status`), so a caller can tell an answer
 * that names nothing (404) from a failure. */
function httpError(message, status) {
  const error = new Error(message);
  error.status = status;
  return error;
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
  if (!response.ok) throw httpError(`${name}: HTTP ${response.status}`, response.status);
  return response.json();
}

/** GET /api/v1/<name> as JSON; throws on a non-2xx answer or a network error. */
export function get(name, { signal } = {}) {
  return getJson(new URL(name, BASE), name, { signal, own: true });
}

/**
 * GET <origin>/api/v1/<name> on a peer hub; `origin` comes from this hub's KRAKEN_HUB_PEERS
 * (validated at its startup). Throws on an error answer (with its `status`), a network error or
 * a timeout.
 */
export async function getFrom(origin, name) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), PEER_TIMEOUT_MS);
  try {
    return await getJson(new URL(`api/v1/${name}`, `${origin}/`), name, { signal: controller.signal, own: false });
  } catch (error) {
    if (controller.signal.aborted) throw new Error(`no answer within ${PEER_TIMEOUT_MS / 1000} s`);
    throw httpError(error.message, error.status);
  } finally {
    clearTimeout(timer);
  }
}

/**
 * A processor control call (ADR 0023): `method` (POST or DELETE) on /api/v1/<path> of this hub,
 * with the write credential as the Authorization header and `body` as JSON when given. Resolves
 * to `{ ok, status, data }` for every answer (`data`: its JSON, null when it has none), so the
 * page can show why the hub refused; a 401 here never signs the page out (the write credential
 * is not the dashboard token). Throws on a network error.
 */
export async function control(method, path, body) {
  const init = { method, headers: { accept: 'application/json' }, cache: 'no-store', credentials: 'omit' };
  if (writeToken) init.headers.authorization = `Bearer ${writeToken}`;
  if (body !== undefined) {
    init.headers['content-type'] = 'application/json';
    init.body = JSON.stringify(body);
  }
  const response = await fetch(new URL(path, BASE), init);
  let data = null;
  try {
    data = await response.json();
  } catch {
    data = null;
  }
  return { ok: response.ok, status: response.status, data };
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
export function readStream({ signal, onOpen, onEvent }) {
  return readEvents('stream', token, { signal, onOpen, onEvent });
}

/**
 * GET /api/v1/<path> of the events API (ADR 0025, ADR 0027), with its credential. Resolves to
 * `{ ok, status, data }` for every answer (`data`: its JSON, null when it has none), so the page
 * can show why the hub refused; throws AuthError on 401 and an Error on a network error.
 */
export async function browse(path, { signal } = {}) {
  const response = await fetch(new URL(path, BASE), {
    headers: headers('application/json', browserCredential()),
    cache: 'no-store',
    credentials: 'omit',
    signal,
  });
  if (response.status === 401) throw unauthorized(path.split('?')[0]);
  let data = null;
  try {
    data = await response.json();
  } catch {
    data = null;
  }
  return { ok: response.ok, status: response.status, data };
}

/**
 * Reads the event tail /api/v1/events/tail?<query> like `readStream`; `onRefused(status, data)`
 * is called instead of rejecting when the hub refuses it with JSON (429, 403, 503).
 */
export async function readTail(query, { signal, onOpen, onEvent, onRefused }) {
  return readEvents(`events/tail?${query}`, browserCredential(), { signal, onOpen, onEvent, onRefused });
}

async function readEvents(path, credential, { signal, onOpen, onEvent, onRefused }) {
  const response = await fetch(new URL(path, BASE), {
    headers: headers('text/event-stream', credential),
    cache: 'no-store',
    credentials: 'omit',
    signal,
  });
  if (response.status === 401) throw unauthorized(path.split('?')[0]);
  if (!response.ok && onRefused) {
    let data = null;
    try {
      data = await response.json();
    } catch {
      data = null;
    }
    onRefused(response.status, data);
    return;
  }
  if (!response.ok || !response.body) throw new Error(`${path.split('?')[0]}: HTTP ${response.status}`);
  onOpen();
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  for (;;) {
    const { done, value } = await reader.read();
    if (done) return;
    buffer += decoder.decode(value, { stream: true }).replace(/\r\n?/g, '\n');
    // One pass over what came: a busy stream brings hundreds of events per read.
    let start = 0;
    for (let end = buffer.indexOf('\n\n'); end !== -1; end = buffer.indexOf('\n\n', start)) {
      const block = buffer.slice(start, end);
      start = end + 2;
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
    buffer = buffer.slice(start);
  }
}
