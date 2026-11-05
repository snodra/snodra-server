// Tiny DOM builders. Every string becomes a text node and every attribute goes through
// setAttribute, so names that clients registered (services, instances, types, tags) can never
// inject markup. No inline style or event-handler attributes: the CSP forbids them.

const SVG_NS = 'http://www.w3.org/2000/svg';
const PROPERTIES = new Set(['value', 'checked', 'hidden', 'disabled', 'selected']);

function apply(el, props) {
  for (const [key, value] of Object.entries(props || {})) {
    if (value === null || value === undefined || value === false) continue;
    if (key === 'on') {
      for (const [event, fn] of Object.entries(value)) el.addEventListener(event, fn);
    } else if (key.startsWith('on') || key === 'style') {
      throw new Error(`attribute ${key} is not allowed; use "on" or a class`);
    } else if (key === 'class') {
      el.setAttribute('class', Array.isArray(value) ? value.filter(Boolean).join(' ') : value);
    } else if (key === 'dataset') {
      Object.assign(el.dataset, value);
    } else if (PROPERTIES.has(key)) {
      el[key] = value;
    } else {
      el.setAttribute(key, value === true ? '' : String(value));
    }
  }
}

function append(el, children) {
  for (const child of children) {
    if (child === null || child === undefined || child === false) continue;
    if (Array.isArray(child)) append(el, child);
    else el.append(child instanceof Node ? child : document.createTextNode(String(child)));
  }
}

/** An HTML element: h('td', { class: 'num' }, 42). */
export function h(tag, props, ...children) {
  const el = document.createElement(tag);
  apply(el, props);
  append(el, children);
  return el;
}

/** An SVG element. */
export function s(tag, props, ...children) {
  const el = document.createElementNS(SVG_NS, tag);
  apply(el, props);
  append(el, children);
  return el;
}

/** Replaces all children of `el`. */
export function fill(el, ...children) {
  el.replaceChildren();
  append(el, children);
  return el;
}

/** A status badge: shape and label, never colour alone. level: good | warn | crit | plain. */
export function badge(level, label, title) {
  return h('span', { class: ['badge', level], title }, label);
}

/**
 * A table with a header row; `rows` are arrays of cells (a Node, a string, or { class, content }).
 * `kind` is an extra table class (for fixed column widths).
 */
export function table(caption, headers, rows, kind) {
  const head = h('tr', null, headers.map((th) => {
    const spec = typeof th === 'string' ? { label: th } : th;
    return h('th', { scope: 'col', class: spec.class, title: spec.title }, spec.label);
  }));
  return h('div', { class: 'table-wrap', role: 'region', 'aria-label': caption, tabindex: 0 },
    h('table', { class: ['data', kind] },
      h('caption', { class: 'visually-hidden' }, caption),
      h('thead', null, head),
      h('tbody', null, rows.map(row))));
}

/** One table row: cells as `table` takes them. */
export function row(cells) {
  return h('tr', null, cells.map((cell) => {
    if (cell && typeof cell === 'object' && !(cell instanceof Node) && !Array.isArray(cell)) {
      return h('td', { class: cell.class, title: cell.title }, cell.content);
    }
    return h('td', null, cell);
  }));
}

/**
 * Rows a table draws at most: thousands of DOM rows per refresh freeze the tab. Filters still
 * search every row; `capNote` says how many are shown.
 */
export const MAX_ROWS = 200;

/** The note under a capped table, or null when every row is shown. */
export function capNote(shown, total, which, noun) {
  if (shown >= total) return null;
  return h('p', { class: 'note' }, `Showing the ${which} ${shown} of ${total} ${noun}; use the filter to narrow.`);
}

/** A numeric cell. */
export function num(content, title) {
  return { class: 'num', content, title };
}

/** True while the reader has a text selection inside `el`. */
export function selectionIn(el) {
  const sel = document.getSelection();
  return Boolean(sel && !sel.isCollapsed && sel.anchorNode && el.contains(sel.anchorNode));
}

/** Runs `render` (which replaces content under `container`) keeping horizontal scroll offsets. */
export function keepScroll(container, render) {
  const key = (el) => el.getAttribute('aria-label') || el.className;
  const saved = new Map();
  for (const el of container.querySelectorAll('.table-wrap, .graph-wrap')) saved.set(key(el), el.scrollLeft);
  const own = container.scrollLeft;
  render();
  container.scrollLeft = own;
  for (const el of container.querySelectorAll('.table-wrap, .graph-wrap')) {
    const left = saved.get(key(el));
    if (left) el.scrollLeft = left;
  }
}
