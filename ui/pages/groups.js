// Consumer groups (ADR 0025, P7): every group with committed offsets on a topic this hub lists,
// with its lag, read from Kafka by the hub, so a group without a live member shows too (it is
// highlighted). A row opens to its partitions; the Processors page links a processor's group here
// (`?open=<group>`). This hub only: every hub reads the same Kafka.

import { badge, fill, h, table } from '../dom.js';
import * as fmt from '../format.js';

const str = (x) => (typeof x === 'string' ? x : '');
const arr = (x) => (Array.isArray(x) ? x : []);
const plural = (n, word) => `${fmt.int(n)} ${word}${n === 1 ? '' : 's'}`;

function browserOf(ctx) {
  const eb = ctx.version && ctx.version.eventBrowser;
  return eb && eb.enabled ? eb : null;
}

/** A Kafka offset or lag; null is unknown. */
function offsetCell(v, unknown) {
  return Number.isFinite(v) ? { class: 'num', content: fmt.int(v) } : { class: 'num dim', content: '—', title: unknown };
}

const PARTITION_HEADERS = [
  { label: 'Partition', class: 'num' },
  { label: 'Committed', class: 'num', title: 'The group\'s committed offset' },
  { label: 'High watermark', class: 'num', title: 'The offset of the next record written to the partition' },
  { label: 'Lag', class: 'num' },
];

export default {
  id: 'consumer-groups',
  title: 'Consumer groups',
  browser: true,
  live: false,
  load: async (ctx) => {
    const eb = browserOf(ctx);
    if (!eb) return { disabled: true };
    const topic = eb.topics.includes(ctx.params.get('topic')) ? ctx.params.get('topic') : eb.topics[0];
    return { eb, answer: await ctx.api.browse(`consumer-groups?topic=${encodeURIComponent(topic)}`) };
  },

  mount(root, ctx) {
    const eb = browserOf(ctx);
    if (!eb) {
      root.append(h('h1', null, 'Consumer groups'),
        h('section', { class: 'card' }, h('p', { class: 'empty' }, 'The event browser is off on this hub: set KRAKEN_HUB_KAFKA_BOOTSTRAP.')));
      return { update() {} };
    }
    const open = new Set((ctx.params.get('open') || '').split(',').filter(Boolean));
    const meta = h('span', { class: 'meta' });
    const body = h('div');
    let last = null;
    const topic = h('select', { 'aria-label': 'Topic', on: { change: () => { ctx.setParams({ topic: topic.value }); ctx.refresh(); } } },
      eb.topics.map((t) => h('option', { value: t, selected: t === (ctx.params.get('topic') || eb.topics[0]) }, t)));
    root.append(
      h('div', { class: 'card-head' }, h('h1', null, 'Consumer groups'), meta),
      h('div', { class: 'controls' }, topic,
        h('button', { type: 'button', on: { click: () => ctx.refresh() } }, 'Refresh')),
      h('section', { class: 'card', 'aria-label': 'Consumer groups' }, body),
      h('p', { class: 'note' }, 'Committed offsets and high watermarks as Kafka reports them now. A group without members is highlighted: nothing consumes, its lag only grows.'));

    const toggle = (id) => {
      if (open.has(id)) open.delete(id);
      else open.add(id);
      ctx.setParams({ open: [...open].join(',') || null });
      render();
    };

    const render = () => {
      const answer = last.answer;
      if (!answer.ok || !answer.data || typeof answer.data !== 'object') {
        const why = answer.data && typeof answer.data.error === 'string' ? answer.data.error : `HTTP ${answer.status}`;
        meta.textContent = '';
        fill(body, h('p', { class: 'error-line' }, `The hub could not list the groups: ${why}`));
        return;
      }
      const v = answer.data;
      const groups = arr(v.groups);
      meta.textContent = `${plural(groups.length, 'group')} on ${str(v.topic)}`;
      if (v.missing) {
        fill(body, h('p', { class: 'empty' }, `Topic ${str(v.topic)} does not exist on the broker.`));
        return;
      }
      if (!groups.length) {
        fill(body, h('p', { class: 'empty' }, `No consumer group has committed offsets on ${str(v.topic)}.`));
        return;
      }
      const headers = ['Group', 'State', { label: 'Members', class: 'num' }, { label: 'Partitions', class: 'num' },
        { label: 'Lag', class: 'num', title: 'Sum of the known lags (high watermark minus committed offset)' }];
      const rows = [];
      groups.forEach((g, n) => {
        const id = str(g.groupId);
        const expanded = open.has(id);
        const idle = !g.members;
        const detailId = `group-detail-${n}`;
        rows.push(h('tr', { class: idle ? 'idle' : null },
          h('td', { class: 'nowrap' }, h('button', {
            type: 'button', class: 'disclose', 'aria-expanded': expanded ? 'true' : 'false', 'aria-controls': detailId,
            title: expanded ? 'Hide the partitions' : 'Show the partitions', on: { click: () => toggle(id) },
          }, id)),
          h('td', { class: 'nowrap' }, str(g.state), idle ? [' ', badge('warn', 'no members', 'No live member: the group only has committed offsets')] : null),
          h('td', { class: 'num' }, fmt.int(g.members)),
          h('td', { class: 'num' }, fmt.int(arr(g.partitions).length)),
          h('td', { class: 'num' }, fmt.int(g.lag))));
        if (expanded) {
          rows.push(h('tr', { class: 'detail', id: detailId }, h('td', { colspan: headers.length },
            table(`Partitions of ${id} on ${str(v.topic)}`, PARTITION_HEADERS, arr(g.partitions).map((p) => [
              { class: 'num', content: String(p.partition) },
              offsetCell(p.committed, 'nothing committed'),
              offsetCell(p.highWatermark, 'unknown'),
              offsetCell(p.lag, 'nothing committed'),
            ]), 'partitions'))));
        }
      });
      fill(body,
        h('div', { class: 'table-wrap', role: 'region', 'aria-label': 'Groups', tabindex: 0 },
          h('table', { class: 'data groups' },
            h('caption', { class: 'visually-hidden' }, `Consumer groups on ${str(v.topic)}, by lag`),
            h('thead', null, h('tr', null, headers.map((th) => {
              const spec = typeof th === 'string' ? { label: th } : th;
              return h('th', { scope: 'col', class: spec.class, title: spec.title }, spec.label);
            }))),
            h('tbody', null, rows))),
        v.truncated ? h('p', { class: 'note' }, 'Not every group is listed: more than 500, or the time budget ran out.') : null);
    };

    return {
      update(data) {
        if (data.disabled) return;
        last = data;
        render();
      },
    };
  },
};
