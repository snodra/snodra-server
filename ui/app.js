// kraken-hub dashboard: hash routing, the header, live updates. Read-only; no third-party code.

import * as api from './api.js';
import { fill, h } from './dom.js';
import * as fmt from './format.js';
import { hideTip } from './graph.js';
import { startLive } from './live.js';
import overview from './pages/overview.js';
import services from './pages/services.js';
import settings from './pages/settings.js';
import types from './pages/types.js';
import { upcoming } from './pages/upcoming.js';

/** This page's build (from its own module URL, /ui/v/<build>/app.js). */
const BUILD = (import.meta.url.match(/\/v\/([^/]+)\//) || [])[1] || null;

/** The fastest a live page refreshes, whatever the stream sends. */
const MIN_REFRESH_MS = 1000;

const PAGES = [
  overview,
  services,
  types,
  settings,
  upcoming({
    id: 'processors', title: 'Processors', phase: 6,
    text: 'Event processors per service: state, partitions with positions and lag, pause, resume and replay.',
  }),
  upcoming({
    id: 'dead-letters', title: 'Dead letters', phase: 6,
    text: 'Events a handler failed on, parked per key, with retry and delete.',
  }),
  upcoming({
    id: 'events', title: 'Events', phase: 7,
    text: 'A read-only browser over the Kafka events topic: search by time, type, key and headers, consumer-group lag.',
  }),
];

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

const ctx = () => ({ api, params: route.params, setParams, version });

function renderNav() {
  const items = [];
  PAGES.forEach((p, i) => {
    if (p.phase && !PAGES[i - 1]?.phase) items.push(h('li', { class: 'sep', 'aria-hidden': 'true' }));
    items.push(h('li', { class: p.phase ? 'upcoming' : null },
      h('a', {
        href: `#/${p.id}`,
        'aria-current': p === route.page ? 'page' : null,
        title: p.phase ? `Available from phase ${p.phase}` : null,
      }, p.title, p.phase ? h('span', { class: 'phase' }, `P${p.phase}`) : null)));
  });
  fill($('nav'), items);
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
  document.title = `${route.page.title} · kraken-hub`;
  renderNav();
  view = route.page.mount(content, ctx());
  refresh();
}

function renderHeader(summary) {
  if (!version) return;
  const fact = (k, v) => h('div', null, h('dt', null, k), h('dd', null, v));
  const now = summary?.nowMs ?? version.nowMs;
  fill($('hub-meta'),
    fact('hub', h('span', { class: 'mono', title: 'KRAKEN_HUB_ID' }, version.hubId)),
    fact('version', version.version),
    fact('protocol', `v${version.protocolVersion}`),
    fact('up', fmt.span(now - version.startedAtMs)));
  const grafana = $('grafana');
  if (version.grafanaUrl) {
    grafana.href = version.grafanaUrl;
    grafana.hidden = false;
  } else {
    grafana.hidden = true;
  }
}

async function loadVersion() {
  try {
    version = await api.get('version');
    renderHeader(null);
  } catch {
    fill($('hub-meta'), h('div', null, h('dd', null, 'hub unreachable')));
  }
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
  if (!route.page.live) return;
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

main.replaceChildren(errorSlot, content);
$('skip').addEventListener('click', (event) => {
  event.preventDefault();
  main.focus();
});
window.addEventListener('hashchange', () => {
  if (location.hash.startsWith('#/') || location.hash === '') mount();
});
mount();
loadVersion();
startLive({ onSummary, onState });
