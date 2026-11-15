// Event processors (ADR 0022, P6a): one row per (service, processor), merged over every reachable
// hub like the other pages; a row opens to every instance's segments. With a write credential
// the page pauses, resumes and resets a processor (ADR 0023, P6b); the call goes to the hub that
// served the page, which reaches every instance. A reset is offered only when the processor's
// event store declares the `replay` capability (ADR 0027). While an event browser add-on is
// connected (ADR 0027), the page also lists the subscriptions no live instance reports, with the
// lag the browser reads (kraken.events.Lag), so a stopped service's backlog stays visible.

import * as controls from '../controls.js';
import { MAX_ROWS, badge, capNote, fill, h, keepScroll, selectionIn, table } from '../dom.js';
import * as fmt from '../format.js';
import { REPLAY, can, merge } from '../processors.js';

const STATE_BADGES = {
  RUNNING: ['good', 'Segments assigned and records flowing'],
  FAILING: ['crit', 'A record exhausted its attempts in the last 60 s, or the reader reports an error'],
  STARTING: ['warn', 'Started; no segment assigned yet, or a reassignment is in progress'],
  PAUSED: ['warn', 'Paused from the control plane: in its subscription, consuming nothing'],
  STOPPED: ['plain', 'Stopping or stopped, or out of its subscription while a reset runs'],
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
      ? [' ', badge('warn', 'replaying', 'A reset moved the positions back: records before each segment\'s replay end are handled again')]
      : null,
    p.operationId
      ? [' ', h('span', { class: 'mono dim nowrap', title: `Reset operation ${p.operationId} holds the processor's lock` }, `op ${p.operationId.slice(0, 8)}`)]
      : null,
  ];
}

/** How long the lag of subscriptions without a live instance is reused before it is read again:
 * the page refreshes every second or two, a lag listing is a heavier read of the event store. */
const LAG_EVERY_MS = 15000;

/** The last lag answer of this hub's event browser and when it was read. */
let lagCache = { at: 0, answer: null };

/** The lag listing (`/api/v1/events/lag`, the browser's default source), at most every
 * LAG_EVERY_MS; null when no event browser is installed or the read failed. */
async function lagOf(ctx) {
  if (!(ctx.version && ctx.version.eventBrowser && ctx.version.eventBrowser.installed)) return null;
  if (Date.now() - lagCache.at < LAG_EVERY_MS) return lagCache.answer;
  let answer = null;
  try {
    const r = await ctx.api.browse('events/lag');
    answer = r.ok && r.data && Array.isArray(r.data.subscriptions) ? r.data : null;
  } catch (error) {
    // A 401 already showed the sign-in form; anything else only leaves the card out.
    if (error instanceof ctx.api.AuthError) throw error;
  }
  lagCache = { at: Date.now(), answer };
  return answer;
}

const IDLE_HEADERS = [
  { label: 'Subscription', title: 'The event store\'s name for a processor\'s subscription' },
  { label: 'Source', title: 'What it reads' },
  { label: 'State', title: 'As the event store reports it' },
  { label: 'Segments', class: 'num', title: 'Segments of the source' },
  { label: 'Lag', class: 'num', title: 'Sum of the known lags (records between the committed position and the head)' },
];

/** The subscriptions with a committed position that no live instance reports, by lag. */
function idleCard(lag, live) {
  if (!lag) return null;
  const idle = lag.subscriptions.filter((x) => typeof x.subscription === 'string' && !live.has(x.subscription));
  if (!idle.length) return null;
  return [
    h('h2', null, 'Subscriptions without a live instance'),
    table('Subscriptions without a live instance, by lag', IDLE_HEADERS, idle.slice(0, MAX_ROWS).map((x) => [
      { class: 'mono nowrap', content: x.subscription },
      { class: 'mono nowrap', content: String(lag.source || '') },
      { class: 'nowrap', content: [String(x.state || ''), ' ', badge('warn', 'no live instance', 'No connected instance reports this subscription: nothing consumes, its lag only grows')] },
      { class: 'num', content: fmt.int(Array.isArray(x.segments) ? x.segments.length : 0) },
      lagCell(Number.isFinite(x.lag) ? x.lag : null),
    ]), 'subscriptions'),
    lag.truncated ? h('p', { class: 'note' }, 'Not every subscription is listed: the event browser stopped at its bound.') : null,
    h('p', { class: 'note' }, `Read by the event browser from ${String(lag.source || 'the default source')}, at most every ${LAG_EVERY_MS / 1000} s.`),
  ];
}

const looksLikeProcessors = (d) => d && typeof d === 'object' && typeof d.hubId === 'string'
  && d.hubId !== '' && Array.isArray(d.services);

/** The key of a row in the `open` URL parameter. */
const keyOf = (service, name) => `${encodeURIComponent(service)}/${encodeURIComponent(name)}`;

const plural = (n, word) => `${fmt.int(n)} ${word}${n === 1 ? '' : 's'}`;

/** A lag; -1 (or null) is unknown. */
function lagCell(v, unknown = 'unknown') {
  return v === null || v < 0
    ? { class: 'num dim', content: '—', title: unknown }
    : { class: 'num', content: fmt.int(v) };
}

/** A position: an opaque string only the event store reads, shown as text; empty is unknown. */
function positionCell(v, unknown = 'unknown') {
  return v === ''
    ? { class: 'num dim', content: '—', title: unknown }
    : { class: 'num mono', content: v };
}

function deadLettersCell(service, p) {
  if (p.deadLetterMode === 'event-store') {
    return { class: 'dim', content: 'to the event store', title: `Failed records go to the dead-letter destination of the event store${p.eventStore ? ` (${p.eventStore})` : ''} (dead-letter mode "event-store"); count them with its tooling` };
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
  const hay = [service, p.name, p.subscription, p.source, p.eventStore, ...p.eventTypes, ...p.instances.map((i) => i.instanceId),
    p.lastError ? p.lastError.message : ''];
  return hay.some((s) => s.toLowerCase().includes(q));
}

const HEADERS = [
  'Service', 'Processor',
  { label: 'Subscription', title: 'The event store\'s name for the processor\'s subscription' },
  { label: 'Source', title: 'What the processor reads (a topic, a stream)' },
  { label: 'Event store', title: 'The event store the processor reads, with the capabilities it declares' },
  'State',
  { label: 'Instances', class: 'num', title: 'Instances reporting this processor' },
  { label: 'Segments', class: 'num', title: 'Segments assigned over its instances' },
  { label: 'Lag', class: 'num', title: 'Sum of the known lags (records between the committed position and the head)' },
  { label: 'Dead letters', class: 'num' },
  'Last error',
];

const ACTIONS = { label: 'Actions', title: 'Pause, resume or reset the processor on every instance (sent to the hub that served this page)' };

const SEGMENT_HEADERS = [
  { label: 'Segment', class: 'num' },
  { label: 'Position', class: 'num', title: 'The next position this instance will process' },
  { label: 'Committed', class: 'num', title: 'The subscription\'s committed position' },
  { label: 'Head', class: 'num', title: 'The position of the next record written to the segment' },
  { label: 'Lag', class: 'num' },
];

const REPLAY_HEADER = { label: 'Replay until', class: 'num', title: 'Records before this position are a replay (handled again)' };

/** The body of a reset to what the dialog chose, or an error message. */
function resetBody(choice, time, positions) {
  if (choice === 'earliest' || choice === 'latest') return { to: choice };
  if (choice === 'time') {
    const ms = Date.parse(time.value);
    return Number.isFinite(ms) ? { to: { timestampMs: ms } } : 'Choose the time to reset to.';
  }
  // Positions are opaque strings: one segment per line, split on the first `=` only, so a
  // position may hold `,` or `=`.
  const list = [];
  for (const line of positions.value.split(/\r?\n/).map((x) => x.trim()).filter(Boolean)) {
    const at = line.indexOf('=');
    const segment = at > 0 ? line.slice(0, at).trim() : '';
    const position = at > 0 ? line.slice(at + 1).trim() : '';
    if (!segment || !position) return `"${line}" is not segment=position.`;
    list.push({ segment, position });
  }
  return list.length ? { to: { positions: list } } : 'List the positions, one segment=position per line.';
}

/** The reset dialog: the target and what a reset does; resolves to the request body or null. */
async function askReset(service, p) {
  let choice = 'earliest';
  const radio = (value, label) => h('label', { class: 'choice' },
    h('input', { type: 'radio', name: 'reset-to', value, checked: value === choice, on: { change: () => { choice = value; } } }),
    label);
  const time = h('input', { type: 'datetime-local', step: '1', 'aria-label': 'Reset to this time' });
  const positions = h('textarea', { rows: '3', placeholder: '0=120\n1=98', 'aria-label': 'Positions, one segment=position per line', spellcheck: 'false' });
  const confirmed = await controls.confirmDialog({
    title: `Reset ${p.name} of ${service}?`,
    lines: [
      `Every instance of ${service} that runs the processor ${p.name} leaves its subscription until the reset ends; one of them moves the subscription's committed positions.`,
      'A reset to anything but "latest" is a replay: the processor\'s event handlers run again for every record from the target up to where it was (sagas and @DisallowReplay handlers excepted), after its @ResetHandler methods.',
    ],
    extra: h('fieldset', { class: 'reset-target' },
      h('legend', null, 'Reset to'),
      radio('earliest', 'the earliest positions (replay everything kept)'),
      radio('latest', 'the latest positions (skip what is waiting, no replay)'),
      h('div', { class: 'choice-row' }, radio('time', 'the first record at or after'), time),
      h('div', { class: 'choice-row' }, radio('positions', 'these positions'), positions)),
    confirm: 'Reset',
  });
  return confirmed ? resetBody(choice, time, positions) : null;
}

/** A line about the answer of a pause, resume or reset. */
function outcomeText(action, service, name, data) {
  if (action === 'reset') {
    const ranges = data.ranges || [];
    return `Reset ${name} of ${service} (operation ${data.operationId}) on ${data.executedBy}: `
      + (data.replay ? `replaying ${plural(ranges.length, 'segment')}, ${fmt.int(data.markersDeleted)} markers deleted, ${plural(data.resetHandlers, 'reset handler')} run.` : 'no replay.');
  }
  const done = action === 'pause' ? 'Paused' : 'Resumed';
  return `${done} ${name} of ${service} on ${plural(data.acknowledged, 'instance')}`
    + (data.failed ? `; ${data.failed} did not confirm` : '') + '.';
}

/** Every instance of an open row: a line about it and its segments. */
function details(p, data) {
  return p.instances.map((i) => h('div', { class: 'proc-instance' },
    h('p', { class: 'proc-instance-head' },
      h('span', { class: 'mono' }, i.instanceId),
      stateCell(i, i.instanceId),
      h('span', null, `reported ${fmt.span(i.statusAgeMs)} ago, processed ${fmt.int(i.processedTotal)}, failed ${fmt.int(i.failedTotal)}`),
      data.scope === 'all'
        ? h('span', { title: 'Reachable hubs that hold a report of this instance; the freshest is shown' }, `seen by ${i.seenBy.join(', ')}`)
        : null,
      i.subscription !== p.subscription ? badge('warn', `subscription ${i.subscription}`, 'This instance reports another subscription than the freshest report') : null,
      i.eventStore !== p.eventStore ? badge('warn', `event store ${i.eventStore || 'unknown'}`, 'This instance reports another event store than the freshest report') : null),
    i.segments.length
      ? segmentTable(p, i)
      : h('p', { class: 'note' }, 'No segment assigned to this instance.')));
}

function segmentTable(p, i) {
  const replay = i.segments.some((x) => x.replayUntil !== '');
  return table(`Segments of ${p.name} on ${i.instanceId}`, replay ? [...SEGMENT_HEADERS, REPLAY_HEADER] : SEGMENT_HEADERS,
    i.segments.map((x) => [
      { class: 'num mono', content: x.segment },
      positionCell(x.position),
      positionCell(x.committed, 'none committed'),
      positionCell(x.head),
      lagCell(x.lag),
      replay ? positionCell(x.replayUntil, 'Not replaying') : null,
    ].filter((c) => c !== null)), 'segments');
}

/** The event store and, in its title, the capabilities it declares. */
function eventStoreCell(p) {
  if (!p.eventStore) return { class: 'dim', content: '—', title: 'The instances do not say' };
  const caps = p.capabilities.length ? p.capabilities.join(', ') : 'none';
  return { class: 'mono nowrap', content: p.eventStore, title: `Capabilities: ${caps}` };
}

export default {
  id: 'processors',
  title: 'Processors',
  live: true,
  load: async (ctx) => {
    const [own, lag] = await Promise.all([ctx.api.get('processors'), lagOf(ctx)]);
    const views = [{ hubId: own.hubId, data: own }];
    if (ctx.allHubs) views.push(...(await ctx.hubs.collect('processors', looksLikeProcessors)).views);
    return {
      ...merge(views),
      scope: ctx.allHubs ? 'all' : 'this',
      nowMs: own.nowMs,
      hubs: views.map((v) => v.hubId),
      lag,
    };
  },

  mount(root, ctx) {
    let query = ctx.params.get('q') || '';
    let problemsOnly = ctx.params.get('problems') === '1';
    const open = new Set((ctx.params.get('open') || '').split(',').filter(Boolean));
    let last = null;

    const search = h('input', {
      type: 'search', value: query, placeholder: 'Filter by service, processor, subscription, source, event store, event type or instance',
      'aria-label': 'Filter processors',
    });
    const problems = h('input', { type: 'checkbox', checked: problemsOnly });
    const meta = h('span', { class: 'meta' });
    const body = h('div');
    const idle = h('section', { class: 'card', 'aria-label': 'Subscriptions without a live instance', hidden: true });
    const status = controls.statusLine();
    let busy = false;
    root.append(
      h('div', { class: 'card-head' }, h('h1', null, 'Processors'), meta),
      h('div', { class: 'controls' }, search,
        h('label', null, problems, 'Only problems (not running, failures or dead letters)')),
      status.el,
      h('section', { class: 'card', 'aria-label': 'Event processors' }, body),
      idle,
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

    /** Pause or resume; a reset only when the event store declares `replay`. */
    const actionsCell = (service, p) => h('td', { class: 'nowrap actions' },
      p.state === 'PAUSED'
        ? h('button', { type: 'button', on: { click: () => act('resume', service, p) } }, 'Resume')
        : h('button', { type: 'button', on: { click: () => act('pause', service, p) } }, 'Pause'),
      ' ',
      can(p, REPLAY)
        ? h('button', { type: 'button', on: { click: () => act('reset', service, p) } }, 'Reset…')
        : h('span', { class: 'dim', title: `The event store${p.eventStore ? ` (${p.eventStore})` : ''} does not declare the replay capability` }, 'no reset'));

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
      const card = idleCard(last.lag, new Set(all.map(({ p }) => p.subscription)));
      fill(idle, card);
      idle.hidden = !card;
      const merged = last.scope === 'all';
      meta.textContent = `${plural(all.length, 'processor')} in ${plural(last.services.length, 'service')}`
        + (merged ? ` on ${plural(last.hubs.length, 'reachable hub')}` : '');
      if (!all.length) {
        fill(body, h('p', { class: 'empty' }, 'No service reports event processors: no connected instance runs one.'));
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
            title: expanded ? 'Hide the segments of every instance' : 'Show the segments of every instance',
            on: { click: () => toggle(key) },
          }, p.name)),
          h('td', { class: 'mono nowrap' }, p.subscription),
          h('td', { class: 'mono nowrap' }, p.source),
        ];
        const store = eventStoreCell(p);
        cells.push(h('td', { class: store.class, title: store.title }, store.content));
        cells.push(
          h('td', { class: 'nowrap' }, stateCell(p)),
          h('td', { class: 'num' }, fmt.int(p.instancesReporting)),
          h('td', { class: 'num' }, fmt.int(p.assignedSegments)));
        for (const cell of [lagCell(p.lag, 'No instance knows its lag yet'), deadLettersCell(service, p), lastErrorCell(p, last.nowMs)]) {
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
