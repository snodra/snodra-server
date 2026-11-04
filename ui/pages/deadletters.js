// Dead letters (ADR 0022, ADR 0023 P6b): without a processor chosen, every processor whose letters
// park in its service's dead_letters collection; with one (`?service=&processor=`), its letters as
// one instance lists them through the hub (no payloads), and with a write credential retry, retry
// all and discard. Not live: a list asks an instance, so it is read on demand.

import * as controls from '../controls.js';
import { badge, fill, h, table } from '../dom.js';
import * as fmt from '../format.js';
import { merge } from '../processors.js';

/** Letters one page shows (the hub sends at most 100). */
const LIMIT = 100;

const plural = (n, word) => `${fmt.int(n)} ${word}${n === 1 ? '' : 's'}`;

const looksLikeProcessors = (d) => d && typeof d === 'object' && typeof d.hubId === 'string'
  && d.hubId !== '' && Array.isArray(d.services);

const REASONS = {
  HANDLER_FAILED: ['crit', 'A handler kept failing until its attempts ran out'],
  SEQUENCED: ['warn', 'Parked behind an earlier letter of the same key, so records of one key never overtake each other'],
  INVALID: ['plain', 'The record could not be read (no type, or not a JSON object)'],
};

function reasonBadge(reason) {
  const [level, title] = REASONS[reason] || ['plain', 'A reason this dashboard does not know'];
  return badge(level, reason || 'unknown', title);
}

const str = (x) => (typeof x === 'string' ? x : '');
const num = (x) => (Number.isFinite(x) ? x : 0);

/** The letters of one processor, read from this hub; `{ error, status }` when the hub refused. */
async function letters(ctx, service, processor) {
  try {
    const data = await ctx.api.get(`${controls.processorPath(service, processor, 'dead-letters')}?limit=${LIMIT}`);
    return { data };
  } catch (error) {
    if (error instanceof ctx.api.AuthError) throw error;
    return { error: error.message, status: error.status };
  }
}

/** Every processor with a dead-letter collection, merged over the reachable hubs. */
async function index(ctx) {
  const own = await ctx.api.get('processors');
  const views = [{ hubId: own.hubId, data: own }];
  if (ctx.allHubs) views.push(...(await ctx.hubs.collect('processors', looksLikeProcessors)).views);
  return merge(views).services.flatMap((s) => s.processors
    .filter((p) => p.deadLetterMode === 'collection' || p.deadLetterMode === '')
    .map((p) => ({ service: s.serviceName, p })));
}

const WHY = {
  404: 'No instance of the service reports this processor now.',
  503: 'No instance that runs this processor accepts control: they predate processor control (phase 6b), so their letters can only be read through the library\'s admin API.',
};

/** The letters of the processor the URL names, else the processors that keep dead letters. */
async function load(ctx) {
  const service = ctx.params.get('service');
  const processor = ctx.params.get('processor');
  if (service && processor) return { service, processor, ...(await letters(ctx, service, processor)) };
  return { index: await index(ctx) };
}

export default {
  id: 'dead-letters',
  title: 'Dead letters',
  live: false,
  load,

  mount(root, ctx) {
    const status = controls.statusLine();
    const body = h('div');
    const meta = h('span', { class: 'meta' });
    let last = null;
    let busy = false;
    root.append(
      h('div', { class: 'card-head' }, h('h1', null, 'Dead letters'), meta),
      status.el,
      body,
      controls.writeTokenCard(() => render()));

    const reload = async () => {
      try {
        last = await load(ctx);
        render();
      } catch (error) {
        status.show(`Could not refresh (${error.message}).`, true);
      }
    };

    const act = async (method, rest, describe, ask) => {
      if (busy) return;
      if (ask && !(await ask())) return;
      busy = true;
      try {
        await controls.run(status, method, controls.processorPath(last.service, last.processor, rest), undefined, describe);
      } finally {
        busy = false;
        reload();
      }
    };

    const retry = (l) => act('POST', `dead-letters/${encodeURIComponent(l.id)}/retry`,
      (d) => `Retry of ${l.id}: ${d.result}${d.detail ? ` (${d.detail})` : ''}.`);
    const discard = (l) => act('DELETE', `dead-letters/${encodeURIComponent(l.id)}`,
      () => `Discarded ${l.id}.`,
      () => controls.confirmDialog({
        title: `Discard letter ${l.id}?`,
        lines: [
          `The record at partition ${l.partition}, offset ${l.offset} (${l.eventType || 'no type'}, key ${l.key || 'none'}) of ${last.processor} in ${last.service} is never handled; this cannot be undone.`,
          'Letters of the same key parked behind it are retried next.',
        ],
        confirm: 'Discard',
      }));
    const retryAll = () => act('POST', 'dead-letters/retry-all',
      (d) => `Retry all: ${fmt.int(d.retried)} retried, ${fmt.int(d.failed)} failed again, ${fmt.int(d.blocked)} still blocked, ${fmt.int(d.skipped)} skipped.`);

    const renderIndex = () => {
      meta.textContent = `${plural(last.index.length, 'processor')} with a dead-letter collection`;
      if (!last.index.length) {
        fill(body, h('section', { class: 'card' },
          h('p', { class: 'empty' }, 'No processor keeps dead letters in a collection here (or no service reports event processors).')));
        return;
      }
      fill(body, h('section', { class: 'card', 'aria-label': 'Processors with dead letters' },
        table('Processors with a dead-letter collection', ['Service', 'Processor', { label: 'Dead letters', class: 'num' }, ''],
          last.index.map(({ service, p }) => [
            service,
            p.name,
            p.deadLetters ? { class: 'num', content: badge('crit', plural(p.deadLetters, 'dead letter')) } : { class: 'num dim', content: '0' },
            h('a', { href: `#/dead-letters?service=${encodeURIComponent(service)}&processor=${encodeURIComponent(p.name)}` }, 'Show the letters'),
          ]))));
    };

    const renderLetters = () => {
      const writable = ctx.api.hasWriteToken();
      const head = h('div', { class: 'card-head' },
        h('h2', null, `${last.processor} of ${last.service}`),
        h('a', { href: `#/processors?q=${encodeURIComponent(last.service)}` }, 'Processors'),
        h('button', { type: 'button', on: { click: () => reload() } }, 'Refresh'));
      if (last.error) {
        meta.textContent = '';
        fill(body, h('section', { class: 'card' }, head,
          h('p', { class: 'error-line' }, WHY[last.status] || `The hub could not list the letters (${last.error}).`)));
        return;
      }
      const d = last.data;
      const list = Array.isArray(d.letters) ? d.letters : [];
      const total = num(d.total);
      meta.textContent = `${plural(total, 'letter')}${list.length < total ? `, the oldest ${fmt.int(list.length)} shown` : ''}; listed by instance ${str(d.instanceId)}`;
      if (writable && list.length) {
        head.append(h('button', { type: 'button', on: { click: () => retryAll() } }, 'Retry all'));
      }
      if (!list.length) {
        fill(body, h('section', { class: 'card' }, head, h('p', { class: 'empty' }, 'No dead letters.')));
        return;
      }
      const headers = ['Letter', 'Reason', 'Event type', 'Key', { label: 'Partition', class: 'num' }, { label: 'Offset', class: 'num' },
        { label: 'Attempts', class: 'num' }, 'Error', 'Parked', 'Blocked by'];
      if (writable) headers.push('Actions');
      const rows = list.map((l) => {
        const cells = [
          { class: 'mono', content: str(l.id) },
          reasonBadge(str(l.reason)),
          { class: 'mono', content: str(l.eventType) || '—' },
          { class: 'mono', content: str(l.key) || '—' },
          { class: 'num', content: fmt.int(num(l.partition)) },
          { class: 'num', content: fmt.int(num(l.offset)) },
          { class: 'num', content: fmt.int(num(l.attempts)) },
          { title: str(l.errorMessage), content: [str(l.errorType) ? h('span', { class: 'mono' }, `${str(l.errorType)}: `) : null, str(l.errorMessage) || '—'] },
          { class: 'nowrap', title: l.parkedAtMs ? fmt.time(l.parkedAtMs) : '', content: l.parkedAtMs ? fmt.time(l.parkedAtMs) : '—' },
          { class: 'mono', content: str(l.blockedBy) || '—' },
        ];
        if (writable) {
          cells.push({
            class: 'nowrap actions',
            content: [
              h('button', { type: 'button', on: { click: () => retry(l) } }, 'Retry'), ' ',
              h('button', { type: 'button', on: { click: () => discard(l) } }, 'Discard'),
            ],
          });
        }
        return cells;
      });
      fill(body, h('section', { class: 'card', 'aria-label': 'Dead letters' }, head,
        table(`Dead letters of ${last.processor} in ${last.service}`, headers, rows, 'letters'),
        h('p', { class: 'note' }, 'No payloads or headers leave the instance; the library\'s admin API shows a letter in full.')));
    };

    const render = () => {
      if (!last) return;
      if (last.index) renderIndex();
      else renderLetters();
    };

    return {
      update(data) {
        last = data;
        render();
      },
    };
  },
};
