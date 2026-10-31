// Overview: totals, the traffic graph (senders -> types -> owners) and the same edges as a table.

import { MAX_ROWS, badge, capNote, fill, h, keepScroll, num, selectionIn, table } from '../dom.js';
import * as fmt from '../format.js';
import { buildModel, hideTip, renderGraph, structureKey } from '../graph.js';

function tile(label, value, sub) {
  return h('div', { class: 'tile' },
    h('div', { class: 'tile-label' }, label),
    h('div', { class: 'tile-value' }, value),
    h('div', { class: 'tile-sub' }, sub));
}

function renderTiles(el, t, windowMs) {
  const win = fmt.windowLabel(windowMs);
  fill(el,
    tile('Services', fmt.count(t.services), null),
    tile('Instances', fmt.count(t.instances),
      t.staleInstances ? badge('warn', `${t.staleInstances} stale`, 'Heartbeat older than a third of the eviction timeout') : 'none stale'),
    tile('Types', fmt.count(t.types), `${fmt.int(t.commandTypes)} commands, ${fmt.int(t.queryTypes)} queries`),
    tile('In flight', fmt.count(t.inFlight), 'queued requests included'),
    tile('Queued', fmt.count(t.queued), 'waiting for permits'),
    tile(`Requests, last ${win}`, fmt.count(t.requests),
      t.errors ? badge(fmt.errorLevel(t.errors, t.requests) === 'ok' ? 'plain' : fmt.errorLevel(t.errors, t.requests),
        `${fmt.int(t.errors)} errors (${fmt.errorRate(t.errors, t.requests)})`) : 'no errors'));
}

function errorCell(e) {
  const level = fmt.errorLevel(e.errors, e.count);
  const text = `${fmt.int(e.errors)} (${fmt.errorRate(e.errors, e.count)})`;
  return num(level === 'warn' || level === 'crit' ? badge(level, text) : text);
}

function renderEdges(el, model, windowMs) {
  const owner = (type) => model.types.get(type)?.owners.join(', ') || '';
  // Every edge the filter keeps (not only the drawn ones), busiest first, at most MAX_ROWS.
  const edges = [...model.tableSends].sort((a, b) => b.count - a.count
    || a.sender.localeCompare(b.sender) || a.type.localeCompare(b.type));
  if (!edges.length) {
    fill(el, h('p', { class: 'empty' }, `No requests in the last ${fmt.windowLabel(windowMs)}.`));
    return;
  }
  fill(el, table('Requests by sender service and type',
    ['Sender', 'Type', 'Owner', { label: 'Requests', class: 'num' }, { label: 'Errors', class: 'num' },
      { label: 'p50', class: 'num' }, { label: 'p99', class: 'num' }],
    edges.slice(0, MAX_ROWS).map((e) => [
      e.sender,
      { class: 'mono', content: e.type },
      owner(e.type) || badge('crit', 'no owner'),
      num(fmt.int(e.count)),
      errorCell(e),
      num(fmt.latency(e.p50Ms)),
      num(fmt.latency(e.p99Ms)),
    ])), capNote(Math.min(edges.length, MAX_ROWS), edges.length, 'busiest', 'edges'));
}

export default {
  id: 'overview',
  title: 'Overview',
  live: true,
  load: (ctx) => ctx.api.get('graph'),

  mount(root, ctx) {
    let data = null;
    let drawn = '';
    let query = ctx.params.get('q') || '';
    let hideIdle = ctx.params.get('idle') === 'hide';
    let hovered = null;
    let current = null;

    const tiles = h('div', { class: 'tiles' });
    const windowMeta = h('span', { class: 'meta' });
    const search = h('input', {
      type: 'search', value: query, placeholder: 'Filter by service or type',
      'aria-label': 'Filter the graph by service or type',
    });
    const idle = h('input', { type: 'checkbox', checked: hideIdle });
    const graphBox = h('div', { class: 'graph-wrap' });
    const notes = h('div');
    const edgesMeta = h('span', { class: 'meta' });
    const edgesBox = h('div');
    const legend = h('div', { class: 'legend' },
      h('span', null, h('i', { class: 'thin' }), h('i', { class: 'thick' }), 'thickness: requests (log scale)'),
      h('span', null, h('i'), 'errors under 1 %'),
      h('span', null, h('i', { class: 'warn' }), '1 % or more'),
      h('span', null, h('i', { class: 'crit' }), '5 % or more'),
      h('span', null, h('i', { class: 'idle' }), 'owner, no requests'));

    root.append(
      h('h1', { class: 'visually-hidden' }, 'Overview'),
      tiles,
      h('section', { class: 'card', 'aria-label': 'Traffic graph' },
        h('div', { class: 'card-head' }, h('h2', null, 'Traffic'), windowMeta),
        h('div', { class: 'controls' }, search, h('label', null, idle, 'Hide types without requests')),
        graphBox, legend, notes),
      h('section', { class: 'card', 'aria-label': 'Requests by sender and type' },
        h('div', { class: 'card-head' }, h('h2', null, 'Requests by sender and type'), edgesMeta),
        edgesBox));

    // The SVG is rebuilt only when its structure (nodes, their states, edges, width) changes;
    // otherwise a refresh updates widths, levels and labels in place.
    const draw = (force) => {
      if (!data) return;
      const width = graphBox.clientWidth;
      const model = buildModel(data, { query, hideIdle });
      const structure = structureKey(model, width);
      if (!model.left.length && !model.middle.length && !model.right.length) {
        hideTip();
        current = null;
        drawn = structure;
        fill(graphBox, h('p', { class: 'empty' }, data.nodes.length
          ? 'Nothing matches the filter.'
          : 'No services are connected and nothing was sent in the window.'));
      } else if (!force && current && structure === drawn) {
        current.update(model);
      } else {
        drawn = structure;
        const focusedKey = graphBox.contains(document.activeElement) ? document.activeElement.dataset.key : null;
        hideTip();
        current = renderGraph(model, width, data.windowMs);
        fill(graphBox, current.svg);
        const again = focusedKey && current.svg.querySelector(`[data-key="${CSS.escape(focusedKey)}"]`);
        if (again) again.focus({ preventScroll: true });
        else if (hovered) current.highlight(hovered);
      }
      const extra = [];
      if (model.capped) {
        extra.push(h('p', { class: 'note' }, badge('warn', 'not all drawn'),
          ` ${fmt.int(model.capped)} more types hidden: the graph draws the registered and busiest ones and those with problems. Use the filter or the Types page.`));
      }
      if (model.hidden) extra.push(h('p', { class: 'note' }, `${model.hidden} types hidden by the filter.`));
      if (data.evictedEdges) {
        extra.push(h('p', { class: 'note' }, badge('warn', 'edge cap'),
          ` ${fmt.int(data.evictedEdges)} sender/type edges were dropped at the cap of ${fmt.int(data.maxEdges)}; the least recently used go first.`));
      }
      fill(notes, extra);
      renderEdges(edgesBox, model, data.windowMs);
      edgesMeta.textContent = `last ${fmt.windowLabel(data.windowMs)}, ${model.tableSends.length} edges`;
    };

    graphBox.addEventListener('pointerover', (ev) => {
      const g = ev.target.closest?.('.g-node');
      if (g) hovered = g.dataset.key;
    });
    graphBox.addEventListener('pointerleave', () => { hovered = null; });
    search.addEventListener('input', () => {
      query = search.value;
      ctx.setParams({ q: query });
      draw(true);
    });
    idle.addEventListener('change', () => {
      hideIdle = idle.checked;
      ctx.setParams({ idle: hideIdle ? 'hide' : null });
      draw(true);
    });
    let lastWidth = 0;
    const resize = new ResizeObserver(() => {
      const w = graphBox.clientWidth;
      if (Math.abs(w - lastWidth) > 8) {
        lastWidth = w;
        draw(false);
      }
    });
    resize.observe(graphBox);

    return {
      update(graph) {
        data = graph;
        renderTiles(tiles, graph.totals, graph.windowMs);
        windowMeta.textContent = `last ${fmt.windowLabel(graph.windowMs)}, refreshed ${fmt.clock(graph.nowMs)}`;
        // Never replace what the reader is selecting; keep horizontal scroll on narrow screens.
        if (!selectionIn(edgesBox)) keepScroll(root, () => draw(false));
      },
      dispose() {
        resize.disconnect();
        hideTip();
      },
    };
  },
};
