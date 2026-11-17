// snodra-server dashboard: hash routing, the header, live updates; the processor controls need a
// write credential (ADR 0023); the Events page (ADR 0025, ADR 0027) shows only while an event
// browser add-on is connected to the hub. No third-party code.

import * as api from './api.js';
import { badge, fill, h } from './dom.js';
import * as fmt from './format.js';
import { hideTip } from './graph.js';
import deadLetters from './pages/deadletters.js';
import events from './pages/events.js';
import * as hubs from './hubs.js';
import { startLive } from './live.js';
import { showLogin } from './login.js';
import overview from './pages/overview.js';
import processors from './pages/processors.js';
import services from './pages/services.js';
import settings from './pages/settings.js';
import types from './pages/types.js';

/** This page's build (from its own module URL, /ui/v/<build>/app.js). */
const BUILD = (import.meta.url.match(/\/v\/([^/]+)\//) || [])[1] || null;

/** The fastest a live page refreshes, whatever the stream sends. */
const MIN_REFRESH_MS = 1000;

/** How often the header re-checks the peer hubs while the page shown does not read them. */
const PEER_CHECK_MS = 5000;

/** Where the reader's choice of "all hubs" or "this hub" is remembered (this browser only). */
const SCOPE_KEY = 'snodra-server.scope';

const PAGES = [
  overview,
  services,
  types,
  processors,
  deadLetters,
  events,
  settings,
];

/** False for an event browser page while no event browser is connected (its nav entry hides). */
const shown = (p) => !p.browser || Boolean(version?.eventBrowser?.installed);

const $ = (id) => document.getElementById(id);
const main = $('main');
const errorSlot = h('div', { role: 'alert' });
const content = h('div');

let route = null;
let view = null;
let generation = 0;
let refreshing = false;
let pendingRefresh = false;
let lastRefresh = 0;
let lastRevision = null;
let version = null;
let lastUpdate = null;
let scope = readScope();
let live = null;
let signedOut = false;

function readScope() {
  try {
    return localStorage.getItem(SCOPE_KEY) === 'this' ? 'this' : 'all';
  } catch {
    return 'all';
  }
}

function writeScope(value) {
  scope = value;
  try {
    localStorage.setItem(SCOPE_KEY, value);
  } catch {
    // Storage may be unavailable (private window, blocked site data): keep it for this tab.
  }
}

/** True when the pages merge every reachable peer hub (peers configured, "All hubs" chosen). */
const allHubs = () => hubs.enabled() && scope === 'all';

function parseHash() {
  const raw = location.hash.startsWith('#/') ? location.hash.slice(2) : '';
  const [path, query = ''] = raw.split('?');
  const page = PAGES.find((p) => p.id === path) || PAGES[0];
  return { page, params: new URLSearchParams(path === page.id ? query : '') };
}

/** Puts filter state into the URL without reloading the page. */
function setParams(values) {
  const params = new URLSearchParams(route.params);
  for (const [k, v] of Object.entries(values)) {
    if (v === null || v === undefined || v === '') params.delete(k);
    else params.set(k, v);
  }
  route.params = params;
  const query = params.toString();
  history.replaceState(null, '', `#/${route.page.id}${query ? `?${query}` : ''}`);
}

const ctx = () => ({ api, params: route.params, setParams, version, hubs, allHubs: allHubs(), refresh: () => refresh() });

function renderNav() {
  fill($('nav'), PAGES.filter(shown).map((p) => h('li', null,
    h('a', { href: `#/${p.id}`, 'aria-current': p === route.page ? 'page' : null }, p.title))));
}

function showError(error) {
  fill(errorSlot, h('p', { class: 'error-line' },
    `Could not refresh (${error.message}). Showing the last data.`));
}

function markReady() {
  document.documentElement.dataset.ready = 'true';
}

async function refresh() {
  if (!route.page.load) {
    markReady();
    return;
  }
  if (refreshing) {
    pendingRefresh = true;
    return;
  }
  refreshing = true;
  const mine = generation;
  try {
    const data = await route.page.load(ctx());
    if (mine === generation) {
      view.update(data);
      fill(errorSlot);
      markReady();
    }
  } catch (error) {
    if (mine === generation) showError(error);
  } finally {
    refreshing = false;
    lastRefresh = Date.now();
    if (pendingRefresh) {
      pendingRefresh = false;
      refresh();
    }
  }
}

function mount() {
  route = parseHash();
  generation += 1;
  view?.dispose?.();
  hideTip();
  fill(errorSlot);
  fill(content);
  document.title = `${route.page.title} · Snodra Server`;
  renderNav();
  view = route.page.mount(content, ctx());
  refresh();
}

function renderHeader(summary) {
  if (!version) return;
  const fact = (k, v) => h('div', null, h('dt', null, k), h('dd', null, v));
  const now = summary?.nowMs ?? version.nowMs;
  fill($('hub-meta'),
    fact('hub', h('span', { class: 'mono', title: 'SNODRA_SERVER_ID' }, version.hubId)),
    fact('version', version.version),
    fact('up', fmt.span(now - version.startedAtMs)));
  const grafana = $('grafana');
  if (version.grafanaUrl) {
    grafana.href = version.grafanaUrl;
    grafana.hidden = false;
  } else {
    grafana.hidden = true;
  }
}

const PEER_STATES = {
  reachable: ['good', 'reachable'],
  unreachable: ['crit', 'unreachable'],
  unknown: ['plain', 'checking'],
  duplicate: ['plain', 'listed twice'],
};

/** The hub indicator: this hub, every peer with its reachability, and the scope selector. */
function renderHubs(peers) {
  const el = $('hubs');
  if (!hubs.enabled()) {
    el.hidden = true;
    fill(el);
    return;
  }
  const select = h('select', { 'aria-label': 'Show data of', on: { change: () => {
    writeScope(select.value);
    mount();
  } } },
  h('option', { value: 'all', selected: scope === 'all' }, 'All hubs'),
  h('option', { value: 'this', selected: scope === 'this' }, 'This hub'));
  const items = [h('li', { class: 'hub self', title: 'The hub serving this page' },
    h('span', { class: 'mono' }, hubs.ownHubId() || 'this hub'), badge('good', 'this hub'))];
  for (const p of peers) {
    if (p.state === 'self') continue;
    const [level, label] = PEER_STATES[p.state] || PEER_STATES.unknown;
    items.push(h('li', { class: 'hub', title: p.error ? `${p.origin}: ${p.error}` : p.origin },
      h('span', { class: 'mono' }, p.hubId || p.origin), badge(level, label)));
  }
  fill(el, select, h('ul', { class: 'hub-list' }, items));
  el.hidden = false;
}

async function loadVersion() {
  try {
    version = await api.get('version');
    hubs.configure(version);
    renderHeader(null);
  } catch (error) {
    // A 401 already showed the sign-in form (see `api.onUnauthorized` below).
    if (!(error instanceof api.AuthError)) {
      fill($('hub-meta'), h('div', null, h('dd', null, 'hub unreachable')));
    }
  }
}

/** The hub wants a dashboard token: ask for it, then start the page over. */
function askForToken() {
  if (signedOut) return;
  signedOut = true;
  api.clearToken();
  $('signout').hidden = true;
  fill($('hub-meta'));
  showLogin(main, async (value) => {
    api.setToken(value);
    try {
      await api.get('version');
    } catch (error) {
      api.clearToken();
      return error instanceof api.AuthError ? 'The hub did not accept that token.' : `Could not reach the hub (${error.message}).`;
    }
    signedOut = false;
    main.replaceChildren(errorSlot, content);
    $('signout').hidden = false;
    await loadVersion();
    mount();
    if (!(route.page.live && allHubs())) hubs.check();
    live?.restart();
    return null;
  });
}

function showBanner(text, action) {
  const banner = $('banner');
  fill(banner, h('span', null, text), action);
  banner.hidden = false;
}

function onSummary(summary) {
  lastUpdate = Date.now();
  if (version && summary.hubId && summary.hubId !== version.hubId) loadVersion();
  renderHeader(summary);
  if (BUILD && summary.buildId && summary.buildId !== BUILD) {
    showBanner('This hub now serves a newer dashboard.',
      h('button', { type: 'button', on: { click: () => location.reload() } }, 'Reload'));
  }
  if (!route?.page.live) return;
  const changed = summary.revision !== null && summary.revision !== lastRevision;
  lastRevision = summary.revision;
  if (changed || Date.now() - lastRefresh >= MIN_REFRESH_MS) refresh();
}

function onState(state) {
  const el = $('live');
  el.dataset.state = state;
  el.textContent = {
    live: 'Live',
    polling: 'Polling every 2 s',
    offline: lastUpdate ? `Offline since ${fmt.clock(lastUpdate)}` : 'Offline',
  }[state];
}

api.onUnauthorized(askForToken);
hubs.onChange(renderHubs);
$('signout').hidden = !api.hasToken();
$('signout').addEventListener('click', () => {
  api.clearToken();
  location.reload();
});
// The header keeps the peers' reachability current; a page that merges the hubs reads them on
// every refresh anyway.
setInterval(() => {
  if (!(route?.page.live && allHubs())) hubs.check();
}, PEER_CHECK_MS);

main.replaceChildren(errorSlot, content);
$('skip').addEventListener('click', (event) => {
  event.preventDefault();
  main.focus();
});
window.addEventListener('hashchange', () => {
  if (location.hash.startsWith('#/') || location.hash === '') mount();
});
// The version tells the page about peer hubs, so it comes before the first page load.
loadVersion().then(() => {
  mount();
  if (!(route.page.live && allHubs())) hubs.check();
});
live = startLive({ onSummary, onState });
