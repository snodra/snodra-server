// The overview graph: three columns, sender services -> types -> owner services, laid out by
// hand (no graph library). Edge thickness follows the request count on a log scale, colour the
// error share (1 % warning, 5 % critical). A service that sends and owns appears in both outer
// columns. Hovering or focusing a node follows its traffic through the graph.

import { fill, h, s } from './dom.js';
import * as fmt from './format.js';

const ROW = 28;
const PITCH = ROW + 8;
const HEAD = 24;
const CHAR = 6.8; // average width of a 12 px system-ui glyph
const MIN_GUTTER = 90;
const MIN_NODE = 150;
const MAX_NODE = 270;
export const MIN_WIDTH = 3 * MIN_NODE + 2 * MIN_GUTTER;

const strip = (id, prefix) => id.slice(prefix.length);

function mean(values) {
  return values.length ? values.reduce((a, b) => a + b, 0) / values.length : Infinity;
}

/**
 * Which nodes and edges the graph shows, in which order. `query` keeps types whose name, owner
 * or senders contain it; `hideIdle` drops types without requests in the window.
 */
export function buildModel(graph, { query = '', hideIdle = false } = {}) {
  const services = new Map();
  const types = new Map();
  for (const n of graph.nodes) (n.kind === 'service' ? services : types).set(n.name, n);
  const sends = graph.edges
    .filter((e) => e.role === 'send')
    .map((e) => ({ ...e, sender: strip(e.from, 'service:'), type: strip(e.to, 'type:') }));
  const handles = graph.edges
    .filter((e) => e.role === 'handle')
    .map((e) => ({ ...e, type: strip(e.from, 'type:'), owner: strip(e.to, 'service:') }));

  const sendsTo = new Map();
  for (const e of sends) {
    if (!sendsTo.has(e.type)) sendsTo.set(e.type, []);
    sendsTo.get(e.type).push(e);
  }
  const q = query.trim().toLowerCase();
  const matches = (name) => name.toLowerCase().includes(q);
  const keepType = (t) => {
    const into = sendsTo.get(t.name) || [];
    if (hideIdle && !into.some((e) => e.count > 0)) return false;
    return !q || matches(t.name) || t.owners.some(matches) || into.some((e) => matches(e.sender));
  };
  const kept = [...types.values()].filter(keepType);
  const keptNames = new Set(kept.map((t) => t.name));
  const vSends = sends.filter((e) => keptNames.has(e.type));
  const vHandles = handles.filter((e) => keptNames.has(e.type));

  // Owners by name; types grouped by owner (types nobody owns last); senders by the mean
  // position of the types they call; then types within a group by the mean of their senders.
  const right = [...new Set(vHandles.map((e) => e.owner))].sort();
  const ownerIndex = new Map(right.map((name, i) => [name, i]));
  const groupOf = (t) => (t.owners.length ? ownerIndex.get(t.owners[0]) ?? right.length : right.length);
  let middle = kept.sort((a, b) => groupOf(a) - groupOf(b) || a.name.localeCompare(b.name));

  const senderNames = new Set(vSends.map((e) => e.sender));
  if (!hideIdle) {
    // Connected services that own nothing and sent nothing in the window (an idle gateway).
    const busy = new Set([...sends.map((e) => e.sender), ...handles.map((e) => e.owner)]);
    for (const svc of services.values()) {
      if (!busy.has(svc.name) && (!q || matches(svc.name))) senderNames.add(svc.name);
    }
  }
  const midIndex = () => new Map(middle.map((t, i) => [t.name, i]));
  let mi = midIndex();
  const bary = new Map([...senderNames].map((name) => [
    name, mean(vSends.filter((e) => e.sender === name).map((e) => mi.get(e.type))),
  ]));
  const left = [...senderNames].sort((a, b) => bary.get(a) - bary.get(b) || a.localeCompare(b));
  const leftIndex = new Map(left.map((name, i) => [name, i]));
  const typeBary = new Map(middle.map((t) => [
    t.name, mean((sendsTo.get(t.name) || []).filter((e) => leftIndex.has(e.sender)).map((e) => leftIndex.get(e.sender))),
  ]));
  middle = middle.sort((a, b) => groupOf(a) - groupOf(b)
    || typeBary.get(a.name) - typeBary.get(b.name) || a.name.localeCompare(b.name));
  mi = midIndex();

  return { services, types, left, middle, right, sends: vSends, handles: vHandles, hidden: types.size - kept.length };
}

// ---- tooltip -----------------------------------------------------------------------------

let tipEl = null;

function tipNode() {
  if (!tipEl) {
    tipEl = h('div', { class: 'tip', role: 'tooltip', id: 'graph-tip', hidden: true });
    document.body.append(tipEl);
  }
  return tipEl;
}

export function showTip(content, x, y) {
  const el = fill(tipNode(), content);
  el.hidden = false;
  const r = el.getBoundingClientRect();
  let left = x + 14;
  let top = y + 14;
  if (left + r.width > window.innerWidth - 8) left = Math.max(8, x - r.width - 14);
  if (top + r.height > window.innerHeight - 8) top = Math.max(8, y - r.height - 14);
  el.style.left = `${left}px`;
  el.style.top = `${top}px`;
}

export function hideTip() {
  if (tipEl) tipEl.hidden = true;
}

function rows(pairs) {
  return h('table', null, h('tbody', null, pairs.filter(Boolean).map(([k, v]) => h('tr', null, h('td', null, k), h('td', { class: 'num' }, v)))));
}

function trafficRows(t) {
  if (!t.count) return [['Requests', '0 in the window']];
  return [
    ['Requests', fmt.int(t.count)],
    ['Errors', `${fmt.int(t.errors)} (${fmt.errorRate(t.errors, t.count)})`],
    ['p50', fmt.latency(t.p50Ms)],
    ['p99', fmt.latency(t.p99Ms)],
  ];
}

// ---- rendering ---------------------------------------------------------------------------

function clip(text, max, fromStart) {
  if (text.length <= max) return text;
  if (max <= 1) return '…';
  return fromStart ? `…${text.slice(text.length - max + 1)}` : `${text.slice(0, max - 1)}…`;
}

const warnMark = (cx, cy) => s('path', { class: 'mark-warn', d: `M${cx} ${cy - 5}L${cx + 5.5} ${cy + 4.5}L${cx - 5.5} ${cy + 4.5}Z` });
const critMark = (cx, cy) => s('path', { class: 'mark-crit', d: `M${cx} ${cy - 5.5}L${cx + 5.5} ${cy}L${cx} ${cy + 5.5}L${cx - 5.5} ${cy}Z` });

/**
 * The graph as an SVG element `width` wide. `windowMs` labels the tooltips. Returns
 * { svg, highlight(key|null) } so the caller can keep a highlight across refreshes.
 */
export function renderGraph(model, width, windowMs) {
  const W = Math.max(MIN_WIDTH, Math.floor(width));
  const nodeW = Math.min(MAX_NODE, (W - 2 * MIN_GUTTER) / 3);
  const gutter = (W - 3 * nodeW) / 2;
  const colX = [0, nodeW + gutter, 2 * (nodeW + gutter)];
  const cols = [model.left, model.middle.map((t) => t.name), model.right];
  const rowsN = Math.max(1, ...cols.map((c) => c.length));
  const H = HEAD + rowsN * PITCH;
  const yOf = (col, i) => HEAD + ((rowsN - cols[col].length) * PITCH) / 2 + i * PITCH;
  const pos = new Map();
  cols.forEach((names, col) => names.forEach((name, i) => pos.set(`${'LMR'[col]}:${name}`, { x: colX[col], y: yOf(col, i) })));
  const windowText = `last ${fmt.windowLabel(windowMs)}`;

  const svg = s('svg', {
    width: W, height: H, viewBox: `0 0 ${W} ${H}`, role: 'group',
    'aria-label': `Traffic graph, ${windowText}: ${model.left.length} senders, ${model.middle.length} types, ${model.right.length} owners`,
  });
  ['Senders', 'Types', 'Owners'].forEach((label, col) => {
    svg.append(s('text', { class: 'g-col', x: colX[col] + 2, y: 12, 'aria-hidden': 'true' }, label));
  });

  // Edges first (under the nodes). Thick ones first so thin ones stay visible on top.
  const all = [
    ...model.sends.map((e) => ({ e, from: `L:${e.sender}`, to: `M:${e.type}`, title: `${e.sender} → ${e.type}` })),
    ...model.handles.map((e) => ({ e, from: `M:${e.type}`, to: `R:${e.owner}`, title: `${e.type} → ${e.owner} (owner)` })),
  ].filter((d) => pos.has(d.from) && pos.has(d.to));
  const max = Math.max(1, ...all.map((d) => d.e.count));
  const rank = { idle: 0, ok: 1, warn: 2, crit: 3 };
  for (const d of all) {
    d.level = fmt.errorLevel(d.e.errors, d.e.count);
    d.width = d.e.count ? 1.5 + (6.5 * Math.log1p(d.e.count)) / Math.log1p(max) : 1;
  }
  all.sort((a, b) => rank[a.level] - rank[b.level] || b.width - a.width);

  const edgesByNode = new Map();
  const link = (key, el) => {
    if (!edgesByNode.has(key)) edgesByNode.set(key, []);
    edgesByNode.get(key).push(el);
  };
  const edgeLayer = s('g', { 'aria-hidden': 'true' });
  for (const d of all) {
    const a = pos.get(d.from);
    const b = pos.get(d.to);
    const x0 = a.x + nodeW;
    const y0 = a.y + ROW / 2;
    const x1 = b.x;
    const y1 = b.y + ROW / 2;
    const dx = (x1 - x0) / 2;
    const path = `M${x0} ${y0}C${x0 + dx} ${y0} ${x1 - dx} ${y1} ${x1} ${y1}`;
    const line = s('path', { class: ['g-edge', d.level], d: path, 'stroke-width': d.width.toFixed(2) });
    const hit = s('path', { class: 'g-hit', d: path, 'stroke-width': Math.max(12, d.width + 6) });
    const content = [h('strong', null, d.title), rows(trafficRows(d.e))];
    hit.addEventListener('pointermove', (ev) => showTip(content, ev.clientX, ev.clientY));
    hit.addEventListener('pointerleave', hideTip);
    const g = s('g', { dataset: { from: d.from, to: d.to } }, line, hit);
    g.edgeData = d;
    link(d.from, g);
    link(d.to, g);
    edgeLayer.append(g);
  }
  svg.append(edgeLayer);

  // Nodes.
  const nodeLayer = s('g');
  const nodeEls = new Map();
  const addNode = (key, label, opts) => {
    const p = pos.get(key);
    const g = s('g', {
      class: ['g-node', opts.level, opts.gone && 'gone'],
      transform: `translate(${p.x} ${p.y})`,
      tabindex: 0, role: 'img', 'aria-label': opts.aria, dataset: { key },
    });
    g.append(s('rect', { class: 'box', width: nodeW, height: ROW, rx: 6 }));
    let textX = 10;
    if (opts.kindLetter) {
      g.append(s('rect', { class: 'kind-box', x: 6, y: 6, width: 18, height: 16, rx: 3 }));
      g.append(s('text', { class: 'kind-letter', x: 15, y: 18, 'text-anchor': 'middle' }, opts.kindLetter));
      textX = 30;
    }
    const markW = opts.mark ? 16 : 0;
    const subW = opts.sub ? opts.sub.length * 6.2 + 10 : 0;
    const maxChars = Math.floor((nodeW - textX - 8 - markW - subW) / CHAR);
    g.append(s('text', { x: textX, y: 18 }, clip(label, maxChars, opts.clipStart)));
    if (opts.sub) g.append(s('text', { class: 'sub', x: nodeW - 8 - markW, y: 18, 'text-anchor': 'end' }, opts.sub));
    if (opts.mark === 'warn') g.append(warnMark(nodeW - 12, ROW / 2));
    if (opts.mark === 'crit') g.append(critMark(nodeW - 12, ROW / 2));
    g.tipContent = opts.tip;
    nodeEls.set(key, g);
    nodeLayer.append(g);
  };

  const sendSummary = (name) => {
    const out = model.sends.filter((e) => e.sender === name);
    const req = out.reduce((n, e) => n + e.count, 0);
    const err = out.reduce((n, e) => n + e.errors, 0);
    return { types: out.length, req, err };
  };
  for (const name of model.left) {
    const svc = model.services.get(name) || { instances: 0, staleInstances: 0 };
    const sent = sendSummary(name);
    const inst = `${svc.instances} inst`;
    addNode(`L:${name}`, name, {
      sub: inst,
      gone: svc.instances === 0,
      mark: svc.staleInstances ? 'warn' : null,
      aria: `Sender ${name}, ${svc.instances} instances, sent ${sent.req} requests to ${sent.types} types, ${windowText}`,
      tip: [h('strong', null, name), rows([
        ['Instances', svc.instances === 0 ? 'none (left)' : `${svc.instances}${svc.staleInstances ? `, ${svc.staleInstances} stale` : ''}`],
        ['Sends', `${sent.types} types`],
        ['Requests', fmt.int(sent.req)],
        ['Errors', `${fmt.int(sent.err)} (${fmt.errorRate(sent.err, sent.req)})`],
      ])],
    });
  }
  for (const t of model.middle) {
    const into = model.sends.filter((e) => e.type === t.name);
    const req = into.reduce((n, e) => n + e.count, 0);
    const err = into.reduce((n, e) => n + e.errors, 0);
    const state = !t.registered ? 'no owner' : t.orphaned ? 'orphaned (owner has no instance)' : 'owned';
    addNode(`M:${t.name}`, t.name, {
      kindLetter: t.typeKind === 'QUERY' ? 'Q' : 'C',
      clipStart: true,
      level: !t.registered ? 'crit' : t.orphaned ? 'warn' : null,
      mark: !t.registered ? 'crit' : t.orphaned ? 'warn' : null,
      aria: `${t.typeKind === 'QUERY' ? 'Query' : 'Command'} ${t.name}, ${state}, ${req} requests from ${into.length} senders, ${windowText}`,
      tip: [h('strong', null, t.name), rows([
        ['Kind', t.typeKind === 'QUERY' ? 'query' : 'command'],
        ['Owner', t.owners.join(', ') || 'none: requests get NO_HANDLER'],
        ['State', state],
        ['Senders', String(into.length)],
        ...trafficRows({ count: req, errors: err, p50Ms: null, p99Ms: null }).slice(0, 2),
      ])],
    });
  }
  for (const name of model.right) {
    const svc = model.services.get(name) || { instances: 0, staleInstances: 0 };
    const owned = model.handles.filter((e) => e.owner === name);
    addNode(`R:${name}`, name, {
      sub: `${svc.instances} inst`,
      level: svc.instances === 0 ? 'warn' : null,
      gone: svc.instances === 0,
      mark: svc.instances === 0 || svc.staleInstances ? 'warn' : null,
      aria: `Owner ${name}, ${svc.instances} instances, owns ${owned.length} types`,
      tip: [h('strong', null, name), rows([
        ['Instances', svc.instances === 0 ? 'none: its types are orphaned' : `${svc.instances}${svc.staleInstances ? `, ${svc.staleInstances} stale` : ''}`],
        ['Owns', `${owned.length} types`],
        ['Requests', fmt.int(owned.reduce((n, e) => n + e.count, 0))],
      ])],
    });
  }
  svg.append(nodeLayer);

  // Highlight: a node, its edges and the flow beyond (sender -> types -> owners and back).
  const related = (key) => {
    const edges = new Set();
    const nodes = new Set([key]);
    const follow = (k, dir) => {
      for (const g of edgesByNode.get(k) || []) {
        const other = dir === 'out' ? g.dataset.to : g.dataset.from;
        if ((dir === 'out' ? g.dataset.from : g.dataset.to) !== k) continue;
        edges.add(g);
        nodes.add(other);
        if (other.startsWith('M:')) follow(other, dir);
      }
    };
    if (!key.startsWith('R:')) follow(key, 'out');
    if (!key.startsWith('L:')) follow(key, 'in');
    return { edges, nodes };
  };
  const highlight = (key) => {
    for (const el of svg.querySelectorAll('.related')) el.classList.remove('related');
    if (!key || !nodeEls.has(key)) {
      svg.classList.remove('focus-mode');
      return;
    }
    const r = related(key);
    svg.classList.add('focus-mode');
    for (const g of r.edges) g.querySelector('.g-edge').classList.add('related');
    for (const k of r.nodes) nodeEls.get(k)?.classList.add('related');
  };
  for (const [key, g] of nodeEls) {
    g.addEventListener('pointerenter', (ev) => { highlight(key); showTip(g.tipContent, ev.clientX, ev.clientY); });
    g.addEventListener('pointermove', (ev) => showTip(g.tipContent, ev.clientX, ev.clientY));
    g.addEventListener('pointerleave', () => { highlight(svg.contains(document.activeElement) ? document.activeElement.dataset?.key : null); hideTip(); });
    g.addEventListener('focus', () => {
      highlight(key);
      const r = g.getBoundingClientRect();
      showTip(g.tipContent, r.right - 10, r.bottom - 6);
    });
    g.addEventListener('blur', () => { highlight(null); hideTip(); });
    g.addEventListener('keydown', (ev) => {
      if (ev.key === 'Escape') { g.blur(); hideTip(); }
    });
  }
  svg.addEventListener('pointerleave', hideTip);
  return { svg, highlight };
}
