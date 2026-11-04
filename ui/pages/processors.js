// Event processors (ADR 0022, P6a): one row per (service, processor), merged over every reachable
// hub like the other pages; a row opens to every instance's partitions. Read-only: pause, resume,
// replay and dead-letter retry come with the control plane (P6b).

import { MAX_ROWS, badge, capNote, fill, h, keepScroll, selectionIn, table } from '../dom.js';
import * as fmt from '../format.js';
import { merge } from '../processors.js';

const STATE_BADGES = {
  RUNNING: ['good', 'Partitions assigned and records flowing'],
  FAILING: ['crit', 'A record exhausted its attempts in the last 60 s, or the consumer reports an error'],
  STARTING: ['warn', 'Started; no partition assigned yet, or a rebalance is in progress'],
  PAUSED: ['warn', 'Paused from the control plane'],
  STOPPED: ['plain', 'Stopping or stopped'],
  UNSPECIFIED: ['plain', 'A state this dashboard does not know'],
};

function stateBadge(state, prefix = '') {
  const [level, title] = STATE_BADGES[state] || STATE_BADGES.UNSPECIFIED;
  return badge(level, state, prefix ? `${prefix}: ${title}` : title);
}

const looksLikeProcessors = (d) => d && typeof d === 'object' && typeof d.hubId === 'string'
  && d.hubId !== '' && Array.isArray(d.services);

/** The key of a row in the `open` URL parameter. */
const keyOf = (service, name) => `${encodeURIComponent(service)}/${encodeURIComponent(name)}`;

const plural = (n, word) => `${fmt.int(n)} ${word}${n === 1 ? '' : 's'}`;

/** A Kafka offset or lag; -1 (or null) is unknown. */
function offsetCell(v, unknown = 'unknown') {
  return v === null || v < 0
    ? { class: 'num dim', content: '—', title: unknown }
    : { class: 'num', content: fmt.int(v) };
}

function deadLettersCell(p) {
  if (p.deadLetterMode === 'topic') {
    return { class: 'dim', content: 'to topic', title: `Failed records go to ${p.topic}.dlt (dead-letter mode "topic"); count them with the topic's tooling` };
  }
  if (p.deadLetterMode === 'log') {
    return { class: 'dim', content: 'logged only', title: 'Failed records are logged and skipped (dead-letter mode "log")' };
  }
  if (!p.deadLetters) return { class: 'num dim', content: '0' };
  return { class: 'num', content: badge('crit', plural(p.deadLetters, 'dead letter'), 'Letters of this processor in the service\'s dead_letters collection (sampled every 30 s)') };
}

function lastErrorCell(p, nowMs) {
  if (!p.lastError) return { class: 'dim', content: 'none' };
  const { message, atMs, instanceId } = p.lastError;
  return {
    title: `${message} (instance ${instanceId}${atMs ? `, ${fmt.time(atMs)}` : ''})`,
    content: [message, atMs ? [' ', h('span', { class: 'dim nowrap' }, `${fmt.span(nowMs - atMs)} ago`)] : null],
  };
}

function problem(p) {
  return p.state !== 'RUNNING' || p.deadLetters > 0 || p.failedTotal > 0;
}

function matches(service, p, q) {
  if (!q) return true;
  const hay = [service, p.name, p.groupId, p.topic, ...p.eventTypes, ...p.instances.map((i) => i.instanceId),
    p.lastError ? p.lastError.message : ''];
  return hay.some((s) => s.toLowerCase().includes(q));
}

const HEADERS = [
  'Service', 'Processor', 'Group', 'Topic', 'State',
  { label: 'Instances', class: 'num', title: 'Instances reporting this processor' },
  { label: 'Partitions', class: 'num', title: 'Partitions assigned over its instances' },
  { label: 'Lag', class: 'num', title: 'Sum of the known lags (high watermark minus committed offset)' },
  { label: 'Dead letters', class: 'num' },
  'Last error',
];

const PARTITION_HEADERS = [
  { label: 'Partition', class: 'num' },
  { label: 'Position', class: 'num', title: 'The next offset this instance will process' },
  { label: 'Committed', class: 'num', title: 'The consumer group\'s committed offset' },
  { label: 'High watermark', class: 'num', title: 'The offset of the next record written to the partition' },
  { label: 'Lag', class: 'num' },
];

/** Every instance of an open row: a line about it and its partitions. */
function details(p, data) {
  return p.instances.map((i) => h('div', { class: 'proc-instance' },
    h('p', { class: 'proc-instance-head' },
      h('span', { class: 'mono' }, i.instanceId),
      stateBadge(i.state, i.instanceId),
      h('span', null, `reported ${fmt.span(i.statusAgeMs)} ago, processed ${fmt.int(i.processedTotal)}, failed ${fmt.int(i.failedTotal)}`),
      data.scope === 'all'
        ? h('span', { title: 'Reachable hubs that hold a report of this instance; the freshest is shown' }, `seen by ${i.seenBy.join(', ')}`)
        : null,
      i.groupId !== p.groupId ? badge('warn', `group ${i.groupId}`, 'This instance reports another consumer group than the freshest report') : null),
    i.partitions.length
      ? table(`Partitions of ${p.name} on ${i.instanceId}`, PARTITION_HEADERS, i.partitions.map((x) => [
        { class: 'num', content: String(x.partition) },
        offsetCell(x.position),
        offsetCell(x.committed, 'none committed'),
        offsetCell(x.highWatermark),
        offsetCell(x.lag),
      ]), 'partitions')
      : h('p', { class: 'note' }, 'No partition assigned to this instance.')));
}

export default {
  id: 'processors',
  title: 'Processors',
  live: true,
  load: async (ctx) => {
    const own = await ctx.api.get('processors');
    const views = [{ hubId: own.hubId, data: own }];
    let older = [];
    if (ctx.allHubs) {
      const peers = await ctx.hubs.collect('processors', looksLikeProcessors);
      views.push(...peers.views);
      older = peers.older;
    }
    return {
      ...merge(views),
      scope: ctx.allHubs ? 'all' : 'this',
      nowMs: own.nowMs,
      hubs: views.map((v) => v.hubId),
      older,
    };
  },

  mount(root, ctx) {
    let query = ctx.params.get('q') || '';
    let problemsOnly = ctx.params.get('problems') === '1';
    const open = new Set((ctx.params.get('open') || '').split(',').filter(Boolean));
    let last = null;

    const search = h('input', {
      type: 'search', value: query, placeholder: 'Filter by service, processor, group, topic, event type or instance',
      'aria-label': 'Filter processors',
    });
    const problems = h('input', { type: 'checkbox', checked: problemsOnly });
    const meta = h('span', { class: 'meta' });
    const body = h('div');
    root.append(
      h('div', { class: 'card-head' }, h('h1', null, 'Processors'), meta),
      h('div', { class: 'controls' }, search,
        h('label', null, problems, 'Only problems (not running, failures or dead letters)')),
      h('section', { class: 'card', 'aria-label': 'Event processors' }, body),
      h('p', { class: 'note' }, 'Read-only. Pause, resume, replay and retrying dead letters come with the control plane (phase 6b); the libraries\' admin API retries and discards letters today.'));

    const toggle = (key) => {
      if (open.has(key)) open.delete(key);
      else open.add(key);
      ctx.setParams({ open: [...open].join(',') || null });
      render();
    };

    const draw = () => {
      if (!last || selectionIn(body)) return;
      keepScroll(body, () => render());
    };

    const render = () => {
      const q = query.trim().toLowerCase();
      const all = last.services.flatMap((s) => s.processors.map((p) => ({ service: s.serviceName, p })));
      const merged = last.scope === 'all';
      meta.textContent = `${plural(all.length, 'processor')} in ${plural(last.services.length, 'service')}`
        + (merged ? ` on ${plural(last.hubs.length, 'reachable hub')}` : '')
        + (last.older.length ? `; ${last.older.join(', ')} predate${last.older.length === 1 ? 's' : ''} processor status` : '');
      if (!all.length) {
        fill(body, h('p', { class: 'empty' }, 'No service reports event processors. Instances report them once their kraken-messaging library has event processor status (capability "processors", phase 6a).'));
        return;
      }
      const shown = all.filter(({ service, p }) => matches(service, p, q) && (!problemsOnly || problem(p)));
      if (!shown.length) {
        fill(body, h('p', { class: 'empty' }, 'Nothing matches the filter.'));
        return;
      }
      const rows = [];
      shown.slice(0, MAX_ROWS).forEach(({ service, p }, n) => {
        const key = keyOf(service, p.name);
        const expanded = open.has(key);
        const id = `processor-detail-${n}`;
        const cells = [
          h('td', { class: 'nowrap' }, service),
          h('td', { class: 'nowrap' }, h('button', {
            type: 'button', class: 'disclose', 'aria-expanded': expanded ? 'true' : 'false', 'aria-controls': id,
            title: expanded ? 'Hide the partitions of every instance' : 'Show the partitions of every instance',
            on: { click: () => toggle(key) },
          }, p.name)),
          h('td', { class: 'mono nowrap' }, p.groupId),
          h('td', { class: 'mono nowrap' }, p.topic),
          h('td', null, stateBadge(p.state)),
          h('td', { class: 'num' }, fmt.int(p.instancesReporting)),
          h('td', { class: 'num' }, fmt.int(p.assignedPartitions)),
        ];
        for (const cell of [offsetCell(p.lag, 'No instance knows its lag yet'), deadLettersCell(p), lastErrorCell(p, last.nowMs)]) {
          cells.push(h('td', { class: cell.class, title: cell.title }, cell.content));
        }
        rows.push(h('tr', null, cells));
        if (expanded) {
          rows.push(h('tr', { class: 'detail', id },
            h('td', { colspan: HEADERS.length }, details(p, last))));
        }
      });
      fill(body,
        h('div', { class: 'table-wrap', role: 'region', 'aria-label': 'Processors', tabindex: 0 },
          h('table', { class: 'data processors' },
            h('caption', { class: 'visually-hidden' }, 'Event processors per service'),
            h('thead', null, h('tr', null, HEADERS.map((th) => {
              const spec = typeof th === 'string' ? { label: th } : th;
              return h('th', { scope: 'col', class: spec.class, title: spec.title }, spec.label);
            }))),
            h('tbody', null, rows))),
        capNote(Math.min(shown.length, MAX_ROWS), shown.length, 'first', 'processors'));
    };

    search.addEventListener('input', () => {
      query = search.value;
      ctx.setParams({ q: query });
      draw();
    });
    problems.addEventListener('change', () => {
      problemsOnly = problems.checked;
      ctx.setParams({ problems: problemsOnly ? '1' : null });
      draw();
    });

    return {
      update(data) {
        last = data;
        draw();
      },
    };
  },
};
