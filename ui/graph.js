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

/** Type nodes drawn at most, ranked registered first, then by requests in the window... */
export const MAX_TYPE_NODES = 60;
/** ...plus at most this many more that have a problem (no owner, orphaned, 1 % errors). */
export const MAX_PROBLEM_NODES = 40;

/** Requests and errors of a list of edges, with the list. */
function aggregate(map, key, e) {
  let a = map.get(key);
  if (!a) {
    a = { edges: [], req: 0, err: 0 };
    map.set(key, a);
  }
  a.edges.push(e);
  a.req += e.count;
  a.err += e.errors;
}

const NONE = { edges: [], req: 0, err: 0 };

/**
 * Which nodes and edges the graph shows, in which order. `query` keeps types whose name, owner
 * or senders contain it; `hideIdle` drops types without requests in the window. Per-type,
 * per-sender and per-owner aggregates are built once here (Maps), so rendering is linear in the
 * edges. At most `maxTypes` type nodes (plus `maxProblems` with a problem) are kept: any client
 * can create thousands of types by sending names nobody owns. `tableSends` are the edges of
 * every type the filter keeps, for the table; `sends` and `handles` only those drawn.
 */
export function buildModel(graph, {
  query = '', hideIdle = false, maxTypes = MAX_TYPE_NODES, maxProblems = MAX_PROBLEM_NODES,
} = {}) {
  const services = new Map();
  const types = new Map();
  for (const n of graph.nodes) (n.kind === 'service' ? services : types).set(n.name, n);
  const sends = [];
  const handles = [];
  for (const e of graph.edges) {
    if (e.role === 'send') sends.push({ ...e, sender: strip(e.from, 'service:'), type: strip(e.to, 'type:') });
    else if (e.role === 'handle') handles.push({ ...e, type: strip(e.from, 'type:'), owner: strip(e.to, 'service:') });
  }

  const intoType = new Map();
  const fromSender = new Map();
  const byOwner = new Map();
  const handlesOf = new Map();
  for (const e of sends) {
    aggregate(intoType, e.type, e);
    aggregate(fromSender, e.sender, e);
  }
  for (const e of handles) {
    aggregate(byOwner, e.owner, e);
    aggregate(handlesOf, e.type, e);
  }
  const into = (name) => intoType.get(name) || NONE;

  const q = query.trim().toLowerCase();
  const matches = (name) => name.toLowerCase().includes(q);
  const keepType = (t) => {
    const edges = into(t.name).edges;
    if (hideIdle && !edges.some((e) => e.count > 0)) return false;
    return !q || matches(t.name) || t.owners.some(matches) || edges.some((e) => matches(e.sender));
  };
  const matched = [...types.values()].filter(keepType);

  // The cap: registered types first (services bound how many exist), then by traffic; then
  // the busiest of the remaining types with a problem.
  let kept = matched;
  if (matched.length > maxTypes) {
    const ranked = [...matched].sort((a, b) => (b.registered - a.registered)
      || into(b.name).req - into(a.name).req || a.name.localeCompare(b.name));
    const problem = (t) => {
      const level = fmt.errorLevel(into(t.name).err, into(t.name).req);
      return !t.registered || t.orphaned || level === 'warn' || level === 'crit';
    };
    const rest = ranked.slice(maxTypes).filter(problem)
      .sort((a, b) => into(b.name).req - into(a.name).req || a.name.localeCompare(b.name));
    kept = [...ranked.slice(0, maxTypes), ...rest.slice(0, maxProblems)];
  }
  const matchedNames = new Set(matched.map((t) => t.name));
  const keptNames = new Set(kept.map((t) => t.name));
  const tableSends = sends.filter((e) => matchedNames.has(e.type));
  const vSends = tableSends.filter((e) => keptNames.has(e.type));
  const vHandles = handles.filter((e) => keptNames.has(e.type));

  // Owners by name; types grouped by owner (types nobody owns last); senders by the mean
  // position of the types they call; then types within a group by the mean of their senders.
  const right = [...new Set(vHandles.map((e) => e.owner))].sort();
  const ownerIndex = new Map(right.map((name, i) => [name, i]));
  const groupOf = (t) => (t.owners.length ? ownerIndex.get(t.owners[0]) ?? right.length : right.length);
  let middle = kept.sort((a, b) => groupOf(a) - groupOf(b) || a.name.localeCompare(b.name));

  const vFrom = new Map();
  const vInto = new Map();
  for (const e of vSends) {
    aggregate(vFrom, e.sender, e);
    aggregate(vInto, e.type, e);
  }
  const senderNames = new Set(vFrom.keys());
  if (!hideIdle) {
    // Connected services that own nothing and sent nothing in the window (an idle gateway).
    for (const svc of services.values()) {
      const busy = fromSender.has(svc.name) || byOwner.has(svc.name);
      if (!busy && (!q || matches(svc.name))) senderNames.add(svc.name);
    }
  }
  const midIndex = () => new Map(middle.map((t, i) => [t.name, i]));
  let mi = midIndex();
  const bary = new Map([...senderNames].map((name) => [
    name, mean((vFrom.get(name) || NONE).edges.map((e) => mi.get(e.type))),
  ]));
  const left = [...senderNames].sort((a, b) => bary.get(a) - bary.get(b) || a.localeCompare(b));
  const leftIndex = new Map(left.map((name, i) => [name, i]));
  const typeBary = new Map(middle.map((t) => [
    t.name, mean((vInto.get(t.name) || NONE).edges.filter((e) => leftIndex.has(e.sender)).map((e) => leftIndex.get(e.sender))),
  ]));
  middle = middle.sort((a, b) => groupOf(a) - groupOf(b)
    || typeBary.get(a.name) - typeBary.get(b.name) || a.name.localeCompare(b.name));
  mi = midIndex();

  return {
    services, types, left, middle, right, sends: vSends, handles: vHandles, tableSends,
    hidden: types.size - matched.length,
    capped: matched.length - kept.length,
    sender: (name) => fromSender.get(name) || NONE,
    into,
    owner: (name) => byOwner.get(name) || NONE,
  };
}

/**
 * What the drawn SVG depends on besides counters: the nodes, their states and the edges. While
 * it stays the same, a refresh updates the drawn graph in place (`update`).
 */
export function structureKey(model, width) {
  const svc = (name) => {
    const n = model.services.get(name);
    return n ? [n.instances, n.staleInstances] : null;
  };
  return JSON.stringify([
    Math.floor(width),
    model.left.map((name) => [name, svc(name)]),
    model.middle.map((t) => [t.name, t.typeKind, t.registered, t.orphaned, t.owners]),
    model.right.map((name) => [name, svc(name)]),
    model.sends.map((e) => `${e.from}>${e.to}`),
    model.handles.map((e) => `${e.from}>${e.to}`),
  ]);
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

const RANK = { idle: 0, ok: 1, warn: 2, crit: 3 };

/** The drawn edges of `model` with their level and width (log scale against the busiest). */
function edgeVisuals(model) {
  const all = [
    ...model.sends.map((e) => ({ e, from: `L:${e.sender}`, to: `M:${e.type}`, title: `${e.sender} → ${e.type}` })),
    ...model.handles.map((e) => ({ e, from: `M:${e.type}`, to: `R:${e.owner}`, title: `${e.type} → ${e.owner} (owner)` })),
  ];
  let max = 1;
  for (const d of all) max = Math.max(max, d.e.count);
  for (const d of all) {
    d.key = `${d.from}>${d.to}`;
    d.level = fmt.errorLevel(d.e.errors, d.e.count);
    d.width = d.e.count ? 1.5 + (6.5 * Math.log1p(d.e.count)) / Math.log1p(max) : 1;
  }
  return all;
}

/** Aria label and tooltip of every node, from the model's aggregates. */
function nodeInfo(model, windowText) {
  const out = new Map();
  for (const name of model.left) {
    const svc = model.services.get(name) || { instances: 0, staleInstances: 0 };
    const sent = model.sender(name);
    out.set(`L:${name}`, {
      aria: `Sender ${name}, ${svc.instances} instances, sent ${sent.req} requests to ${sent.edges.length} types, ${windowText}`,
      tip: [h('strong', null, name), rows([
        ['Instances', svc.instances === 0 ? 'none (left)' : `${svc.instances}${svc.staleInstances ? `, ${svc.staleInstances} stale` : ''}`],
        ['Sends', `${sent.edges.length} types`],
        ['Requests', fmt.int(sent.req)],
        ['Errors', `${fmt.int(sent.err)} (${fmt.errorRate(sent.err, sent.req)})`],
      ])],
    });
  }
  for (const t of model.middle) {
    const into = model.into(t.name);
    const state = !t.registered ? 'no owner' : t.orphaned ? 'orphaned (owner has no instance)' : 'owned';
    out.set(`M:${t.name}`, {
      aria: `${t.typeKind === 'QUERY' ? 'Query' : 'Command'} ${t.name}, ${state}, ${into.req} requests from ${into.edges.length} senders, ${windowText}`,
      tip: [h('strong', null, t.name), rows([
        ['Kind', t.typeKind === 'QUERY' ? 'query' : 'command'],
        [t.owners.length > 1 ? 'Owners' : 'Owner', t.owners.join(', ') || 'none: requests get NO_HANDLER'],
        ...(t.shared ? [['Shared', 'yes: every owner answers it']] : []),
        ['State', state],
        ['Senders', String(into.edges.length)],
        ...trafficRows({ count: into.req, errors: into.err, p50Ms: null, p99Ms: null }).slice(0, 2),
      ])],
    });
  }
  for (const name of model.right) {
    const svc = model.services.get(name) || { instances: 0, staleInstances: 0 };
    const owned = model.owner(name);
    out.set(`R:${name}`, {
      aria: `Owner ${name}, ${svc.instances} instances, owns ${owned.edges.length} types`,
      tip: [h('strong', null, name), rows([
        ['Instances', svc.instances === 0 ? 'none: its types are orphaned' : `${svc.instances}${svc.staleInstances ? `, ${svc.staleInstances} stale` : ''}`],
        ['Owns', `${owned.edges.length} types`],
        ['Requests', fmt.int(owned.req)],
      ])],
    });
  }
  return out;
}

/**
 * The graph as an SVG element `width` wide. `windowMs` labels the tooltips. Returns
 * { svg, highlight(key|null), update(model) }: `update` takes a model with the same
 * [`structureKey`] and changes widths, levels, labels and tooltips in place.
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
  const all = edgeVisuals(model).filter((d) => pos.has(d.from) && pos.has(d.to));
  all.sort((a, b) => RANK[a.level] - RANK[b.level] || b.width - a.width);

  const edgesByNode = new Map();
  const edgeEls = new Map();
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
    const g = s('g', { dataset: { from: d.from, to: d.to } }, line, hit);
    g.edgeData = d;
    const tip = () => [h('strong', null, g.edgeData.title), rows(trafficRows(g.edgeData.e))];
    hit.addEventListener('pointermove', (ev) => showTip(tip(), ev.clientX, ev.clientY));
    hit.addEventListener('pointerleave', hideTip);
    edgeEls.set(d.key, { g, line, hit });
    link(d.from, g);
    link(d.to, g);
    edgeLayer.append(g);
  }
  svg.append(edgeLayer);

  // Nodes.
  const info = nodeInfo(model, windowText);
  const nodeLayer = s('g');
  const nodeEls = new Map();
  const addNode = (key, label, opts) => {
    const p = pos.get(key);
    const { aria, tip } = info.get(key);
    const g = s('g', {
      class: ['g-node', opts.level, opts.gone && 'gone'],
      transform: `translate(${p.x} ${p.y})`,
      tabindex: 0, role: 'img', 'aria-label': aria, dataset: { key },
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
    g.tipContent = tip;
    nodeEls.set(key, g);
    nodeLayer.append(g);
  };

  for (const name of model.left) {
    const svc = model.services.get(name) || { instances: 0, staleInstances: 0 };
    addNode(`L:${name}`, name, {
      sub: `${svc.instances} inst`,
      gone: svc.instances === 0,
      mark: svc.staleInstances ? 'warn' : null,
    });
  }
  for (const t of model.middle) {
    addNode(`M:${t.name}`, t.name, {
      kindLetter: t.typeKind === 'QUERY' ? 'Q' : 'C',
      clipStart: true,
      level: !t.registered ? 'crit' : t.orphaned ? 'warn' : null,
      mark: !t.registered ? 'crit' : t.orphaned ? 'warn' : null,
    });
  }
  for (const name of model.right) {
    const svc = model.services.get(name) || { instances: 0, staleInstances: 0 };
    addNode(`R:${name}`, name, {
      sub: `${svc.instances} inst`,
      level: svc.instances === 0 ? 'warn' : null,
      gone: svc.instances === 0,
      mark: svc.instances === 0 || svc.staleInstances ? 'warn' : null,
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
  let highlighted = null;
  const highlight = (key) => {
    for (const el of svg.querySelectorAll('.related')) el.classList.remove('related');
    if (!key || !nodeEls.has(key)) {
      highlighted = null;
      svg.classList.remove('focus-mode');
      return;
    }
    highlighted = key;
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

  // Counters changed, structure did not: widths, levels, labels and tooltips in place.
  const update = (next) => {
    for (const d of edgeVisuals(next)) {
      const el = edgeEls.get(d.key);
      if (!el) continue;
      el.g.edgeData = d;
      el.line.setAttribute('class', `g-edge ${d.level}`);
      el.line.setAttribute('stroke-width', d.width.toFixed(2));
      el.hit.setAttribute('stroke-width', String(Math.max(12, d.width + 6)));
    }
    for (const [key, { aria, tip }] of nodeInfo(next, windowText)) {
      const g = nodeEls.get(key);
      if (!g) continue;
      g.setAttribute('aria-label', aria);
      g.tipContent = tip;
    }
    if (highlighted) highlight(highlighted);
  };
  return { svg, highlight, update };
}
