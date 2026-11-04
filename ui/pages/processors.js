// Event processors (ADR 0022, P6a): one row per (service, processor), merged over every reachable
// hub like the other pages; a row opens to every instance's partitions. With a write credential
// the page pauses, resumes and resets a processor (ADR 0023, P6b); the call goes to the hub that
// served the page, which reaches every instance.

import * as controls from '../controls.js';
import { MAX_ROWS, badge, capNote, fill, h, keepScroll, selectionIn, table } from '../dom.js';
import * as fmt from '../format.js';
import { merge } from '../processors.js';

const STATE_BADGES = {
  RUNNING: ['good', 'Partitions assigned and records flowing'],
  FAILING: ['crit', 'A record exhausted its attempts in the last 60 s, or the consumer reports an error'],
  STARTING: ['warn', 'Started; no partition assigned yet, or a rebalance is in progress'],
  PAUSED: ['warn', 'Paused from the control plane: in its consumer group, consuming nothing'],
  STOPPED: ['plain', 'Stopping or stopped, or out of its consumer group while a reset runs'],
  UNSPECIFIED: ['plain', 'A state this dashboard does not know'],
};

function stateBadge(state, prefix = '') {
  const [level, title] = STATE_BADGES[state] || STATE_BADGES.UNSPECIFIED;
  return badge(level, state, prefix ? `${prefix}: ${title}` : title);
}

/** The state, then a replaying badge and the reset operation holding the lock (ADR 0023). */
function stateCell(p, prefix = '') {
  return [
    stateBadge(p.state, prefix),
    p.replaying
      ? [' ', badge('warn', 'replaying', 'A reset moved the offsets back: records below each partition\'s replay end are handled again')]
      : null,
    p.operationId
      ? [' ', h('span', { class: 'mono dim nowrap', title: `Reset operation ${p.operationId} holds the processor's lock` }, `op ${p.operationId.slice(0, 8)}`)]
      : null,
  ];
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

function deadLettersCell(service, p) {
  if (p.deadLetterMode === 'topic') {
    return { class: 'dim', content: 'to topic', title: `Failed records go to ${p.topic}.dlt (dead-letter mode "topic"); count them with the topic's tooling` };
  }
  if (p.deadLetterMode === 'log') {
    return { class: 'dim', content: 'logged only', title: 'Failed records are logged and skipped (dead-letter mode "log")' };
  }
  const href = `#/dead-letters?service=${encodeURIComponent(service)}&processor=${encodeURIComponent(p.name)}`;
  if (!p.deadLetters) return { class: 'num dim', content: h('a', { href, title: 'The dead letters of this processor' }, '0') };
  return {
    class: 'num',
    content: h('a', { href, title: 'Letters of this processor in the service\'s dead_letters collection (sampled every 30 s)' },
      badge('crit', plural(p.deadLetters, 'dead letter'))),
  };
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

const ACTIONS = { label: 'Actions', title: 'Pause, resume or reset the processor on every instance (sent to the hub that served this page)' };

const PARTITION_HEADERS = [
  { label: 'Partition', class: 'num' },
  { label: 'Position', class: 'num', title: 'The next offset this instance will process' },
  { label: 'Committed', class: 'num', title: 'The consumer group\'s committed offset' },
  { label: 'High watermark', class: 'num', title: 'The offset of the next record written to the partition' },
  { label: 'Lag', class: 'num' },
];

const REPLAY_HEADER = { label: 'Replay until', class: 'num', title: 'Records below this offset are a replay (handled again)' };

/** The body of a reset to what the dialog chose, or an error message. */
function resetBody(choice, time, offsets) {
  if (choice === 'earliest' || choice === 'latest') return { to: choice };
  if (choice === 'time') {
    const ms = Date.parse(time.value);
    return Number.isFinite(ms) ? { to: { timestampMs: ms } } : 'Choose the time to reset to.';
  }
  const list = [];
  for (const part of offsets.value.split(',').map((x) => x.trim()).filter(Boolean)) {
    const m = part.match(/^(\d+)\s*:\s*(\d+)$/);
    if (!m) return `"${part}" is not partition:offset.`;
    list.push({ partition: Number(m[1]), offset: Number(m[2]) });
  }
  return list.length ? { to: { offsets: list } } : 'List the offsets as partition:offset, separated by commas.';
}

/** The reset dialog: the target and what a reset does; resolves to the request body or null. */
async function askReset(service, p) {
  let choice = 'earliest';
  const radio = (value, label) => h('label', { class: 'choice' },
    h('input', { type: 'radio', name: 'reset-to', value, checked: value === choice, on: { change: () => { choice = value; } } }),
    label);
  const time = h('input', { type: 'datetime-local', step: '1', 'aria-label': 'Reset to this time' });
  const offsets = h('input', { type: 'text', placeholder: '0:120, 1:98', 'aria-label': 'Offsets as partition:offset', spellcheck: 'false' });
  const confirmed = await controls.confirmDialog({
    title: `Reset ${p.name} of ${service}?`,
    lines: [
      `Every instance of ${service} that runs the processor ${p.name} leaves its consumer group until the reset ends; one of them moves the group's offsets.`,
      'A reset to anything but "latest" is a replay: the processor\'s event handlers run again for every record from the target up to where it was (sagas and @DisallowReplay handlers excepted), after its @ResetHandler methods.',
    ],
    extra: h('fieldset', { class: 'reset-target' },
      h('legend', null, 'Reset to'),
      radio('earliest', 'the earliest offsets (replay everything kept)'),
      radio('latest', 'the latest offsets (skip what is waiting, no replay)'),
      h('div', { class: 'choice-row' }, radio('time', 'the first record at or after'), time),
      h('div', { class: 'choice-row' }, radio('offsets', 'these offsets'), offsets)),
    confirm: 'Reset',
  });
  return confirmed ? resetBody(choice, time, offsets) : null;
}

/** A line about the answer of a pause, resume or reset. */
function outcomeText(action, service, name, data) {
  if (action === 'reset') {
    const ranges = data.ranges || [];
    return `Reset ${name} of ${service} (operation ${data.operationId}) on ${data.executedBy}: `
      + (data.replay ? `replaying ${plural(ranges.length, 'partition')}, ${fmt.int(data.markersDeleted)} markers deleted, ${plural(data.resetHandlers, 'reset handler')} run.` : 'no replay.');
  }
  const done = action === 'pause' ? 'Paused' : 'Resumed';
  const skipped = (data.notCapable || []).length;
  return `${done} ${name} of ${service} on ${plural(data.acknowledged, 'instance')}`
    + (data.failed ? `; ${data.failed} did not confirm` : '')
    + (skipped ? `; ${plural(skipped, 'instance')} without processor control kept consuming` : '') + '.';
}

/** Every instance of an open row: a line about it and its partitions. */
function details(p, data) {
  return p.instances.map((i) => h('div', { class: 'proc-instance' },
    h('p', { class: 'proc-instance-head' },
      h('span', { class: 'mono' }, i.instanceId),
      stateCell(i, i.instanceId),
      h('span', null, `reported ${fmt.span(i.statusAgeMs)} ago, processed ${fmt.int(i.processedTotal)}, failed ${fmt.int(i.failedTotal)}`),
      data.scope === 'all'
        ? h('span', { title: 'Reachable hubs that hold a report of this instance; the freshest is shown' }, `seen by ${i.seenBy.join(', ')}`)
        : null,
      i.groupId !== p.groupId ? badge('warn', `group ${i.groupId}`, 'This instance reports another consumer group than the freshest report') : null),
    i.partitions.length
      ? partitionTable(p, i)
      : h('p', { class: 'note' }, 'No partition assigned to this instance.')));
}

function partitionTable(p, i) {
  const replay = i.partitions.some((x) => x.replayUntil > 0);
  return table(`Partitions of ${p.name} on ${i.instanceId}`, replay ? [...PARTITION_HEADERS, REPLAY_HEADER] : PARTITION_HEADERS,
    i.partitions.map((x) => [
      { class: 'num', content: String(x.partition) },
      offsetCell(x.position),
      offsetCell(x.committed, 'none committed'),
      offsetCell(x.highWatermark),
      offsetCell(x.lag),
      replay ? (x.replayUntil > 0 ? { class: 'num', content: fmt.int(x.replayUntil) } : { class: 'num dim', content: '—', title: 'Not replaying' }) : null,
    ].filter((c) => c !== null)), 'partitions');
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
    const status = controls.statusLine();
    let busy = false;
    root.append(
      h('div', { class: 'card-head' }, h('h1', null, 'Processors'), meta),
      h('div', { class: 'controls' }, search,
        h('label', null, problems, 'Only problems (not running, failures or dead letters)')),
      status.el,
      h('section', { class: 'card', 'aria-label': 'Event processors' }, body),
      controls.writeTokenCard(() => draw()));

    /** Pause, resume or reset `p` of `service`, then refreshes the page. */
    const act = async (action, service, p) => {
      if (busy) return;
      let body;
      if (action === 'reset') {
        body = await askReset(service, p);
        if (body === null) return;
        if (typeof body === 'string') {
          status.show(body, true);
          return;
        }
      }
      busy = true;
      status.show(`${action === 'reset' ? 'Resetting' : action === 'pause' ? 'Pausing' : 'Resuming'} ${p.name} of ${service}...`, false);
      try {
        await controls.run(status, 'POST', controls.processorPath(service, p.name, action), body,
          (data) => outcomeText(action, service, p.name, data));
      } finally {
        busy = false;
        ctx.refresh();
      }
    };

    const actionsCell = (service, p) => h('td', { class: 'nowrap actions' },
      p.state === 'PAUSED'
        ? h('button', { type: 'button', on: { click: () => act('resume', service, p) } }, 'Resume')
        : h('button', { type: 'button', on: { click: () => act('pause', service, p) } }, 'Pause'),
      ' ',
      h('button', { type: 'button', on: { click: () => act('reset', service, p) } }, 'Reset…'));

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
      const writable = ctx.api.hasWriteToken();
      const headers = writable ? [...HEADERS, ACTIONS] : HEADERS;
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
          h('td', { class: 'nowrap' }, stateCell(p)),
          h('td', { class: 'num' }, fmt.int(p.instancesReporting)),
          h('td', { class: 'num' }, fmt.int(p.assignedPartitions)),
        ];
        for (const cell of [offsetCell(p.lag, 'No instance knows its lag yet'), deadLettersCell(service, p), lastErrorCell(p, last.nowMs)]) {
          cells.push(h('td', { class: cell.class, title: cell.title }, cell.content));
        }
        if (writable) cells.push(actionsCell(service, p));
        rows.push(h('tr', null, cells));
        if (expanded) {
          rows.push(h('tr', { class: 'detail', id },
            h('td', { colspan: headers.length }, details(p, last))));
        }
      });
      fill(body,
        h('div', { class: 'table-wrap', role: 'region', 'aria-label': 'Processors', tabindex: 0 },
          h('table', { class: 'data processors' },
            h('caption', { class: 'visually-hidden' }, 'Event processors per service'),
            h('thead', null, h('tr', null, headers.map((th) => {
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
