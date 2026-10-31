// Command and query types: owners, instances, state and traffic in the window.

import { badge, fill, h, keepScroll, num, selectionIn, table } from '../dom.js';
import * as fmt from '../format.js';

function state(t) {
  if (!t.registered) return badge('crit', 'no owner', 'Sent, but no service registered it: requests get NO_HANDLER');
  if (t.orphaned) return badge('warn', `orphaned ${fmt.span(t.orphanedForMs)}`, 'The owner has no live instance; ownership is held for the grace period');
  return badge('good', 'owned');
}

function problem(t) {
  return !t.registered || t.orphaned || fmt.errorLevel(t.traffic.errors, t.traffic.count) === 'warn'
    || fmt.errorLevel(t.traffic.errors, t.traffic.count) === 'crit';
}

function errors(t) {
  const { count, errors: n } = t.traffic;
  if (!count) return num(h('span', { class: 'dim' }, '—'));
  const level = fmt.errorLevel(n, count);
  const text = `${fmt.int(n)} (${fmt.errorRate(n, count)})`;
  return num(level === 'warn' || level === 'crit' ? badge(level, text) : text);
}

export default {
  id: 'types',
  title: 'Types',
  live: true,
  load: (ctx) => ctx.api.get('types'),

  mount(root, ctx) {
    let query = ctx.params.get('q') || '';
    let kind = ctx.params.get('kind') || '';
    let problemsOnly = ctx.params.get('problems') === '1';
    let last = null;

    const search = h('input', {
      type: 'search', value: query, placeholder: 'Filter by type, owner or sender',
      'aria-label': 'Filter types',
    });
    const kindSelect = h('select', { 'aria-label': 'Kind' },
      h('option', { value: '', selected: kind === '' }, 'Commands and queries'),
      h('option', { value: 'COMMAND', selected: kind === 'COMMAND' }, 'Commands'),
      h('option', { value: 'QUERY', selected: kind === 'QUERY' }, 'Queries'));
    const problems = h('input', { type: 'checkbox', checked: problemsOnly });
    const meta = h('span', { class: 'meta' });
    const body = h('div');
    root.append(
      h('div', { class: 'card-head' }, h('h1', null, 'Types'), meta),
      h('div', { class: 'controls' }, search, kindSelect,
        h('label', null, problems, 'Only problems (no owner, orphaned, 1 % errors or more)')),
      h('section', { class: 'card', 'aria-label': 'Types' }, body));

    const render = () => {
      const q = query.trim().toLowerCase();
      const shown = last.types.filter((t) => (!kind || t.kind === kind)
        && (!problemsOnly || problem(t))
        && (!q || [t.type, ...t.owners, ...t.senders, ...t.instances].some((s) => s.toLowerCase().includes(q))));
      meta.textContent = `${shown.length} of ${last.types.length} types, traffic over the last ${fmt.windowLabel(last.windowMs)}`;
      if (!last.types.length) {
        fill(body, h('p', { class: 'empty' }, 'No types registered and none sent in the window.'));
        return;
      }
      if (!shown.length) {
        fill(body, h('p', { class: 'empty' }, 'Nothing matches the filter.'));
        return;
      }
      fill(body, table('Types', [
        'Type', 'Kind', 'Owner',
        { label: 'Instances', class: 'num', title: 'Live instances that registered the type' },
        'State', 'Senders',
        { label: 'Requests', class: 'num' }, { label: 'Errors', class: 'num' },
        { label: 'p50', class: 'num' }, { label: 'p99', class: 'num' },
      ], shown.map((t) => [
        { class: 'mono', content: t.type },
        h('span', { class: 'kind', title: t.kind === 'QUERY' ? 'query' : 'command' }, t.kind === 'QUERY' ? 'Q' : 'C'),
        t.owners.length ? t.owners.join(', ') : { class: 'dim', content: 'none' },
        num(t.registered ? fmt.int(t.liveInstances) : h('span', { class: 'dim' }, '—'), t.instances.join(', ')),
        state(t),
        t.senders.length ? t.senders.join(', ') : { class: 'dim', content: 'none' },
        num(t.traffic.count ? fmt.int(t.traffic.count) : h('span', { class: 'dim' }, '0')),
        errors(t),
        num(fmt.latency(t.traffic.p50Ms)),
        num(fmt.latency(t.traffic.p99Ms)),
      ])));
    };

    const draw = () => {
      if (!last || selectionIn(body)) return;
      keepScroll(body, render);
    };

    search.addEventListener('input', () => {
      query = search.value;
      ctx.setParams({ q: query });
      draw();
    });
    kindSelect.addEventListener('change', () => {
      kind = kindSelect.value;
      ctx.setParams({ kind });
      draw();
    });
    problems.addEventListener('change', () => {
      problemsOnly = problems.checked;
      ctx.setParams({ problems: problemsOnly ? '1' : null });
      draw();
    });

    return {
      update(types) {
        last = types;
        draw();
      },
    };
  },
};
