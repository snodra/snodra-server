// Events (ADR 0025, P7): a read-only browser over the Kafka topics this hub lists
// (KRAKEN_HUB_EVENT_TOPICS). A search runs newest first over every partition and pages with the
// hub's cursor; a record opens to its headers and, where this hub serves payloads to the token the
// tab holds, its payload; the tail prepends new records as they come. This hub only: every hub
// reads the same Kafka. Everything is rendered as text (h / textContent), payloads included.

import { badge, fill, h, row, table } from '../dom.js';
import * as fmt from '../format.js';

/** Rows the tail keeps; older ones fall off the end. */
const MAX_TAIL_ROWS = 500;

const TAIL_CAPTION = 'Records as they arrive, newest first';

/** The time range presets: label and span in ms (null: any time). */
const RANGES = [
  ['15m', 'Last 15 minutes', 15 * 60 * 1000],
  ['1h', 'Last hour', 60 * 60 * 1000],
  ['24h', 'Last 24 hours', 24 * 60 * 60 * 1000],
  ['7d', 'Last 7 days', 7 * 24 * 60 * 60 * 1000],
  ['all', 'Any time', null],
  ['custom', 'Custom', null],
];

/** The text filters: URL parameter, label, placeholder. */
const FIELDS = [
  ['type', 'Type', 'orders.OrderPlaced or orders.*'],
  ['key', 'Key', 'record key, exact'],
  ['source', 'Source', 'service, exact'],
  ['correlationId', 'Correlation id', 'exact'],
  ['userId', 'User id', 'exact'],
  ['eventId', 'Event id', 'exact'],
];

const STOPPED = {
  limit: 'stopped at the page size',
  scanBudget: 'stopped by the scan budget (KRAKEN_HUB_EVENT_SEARCH_MAX_SCANNED)',
  timeBudget: 'stopped by the time budget (KRAKEN_HUB_EVENT_SEARCH_TIMEOUT)',
};

const ENDED = {
  maxDuration: 'it reached its maximum duration (KRAKEN_HUB_EVENT_TAIL_DURATION)',
  slowReader: 'this page fell too far behind and the hub dropped it',
  shutdown: 'the hub is shutting down',
};

const str = (x) => (typeof x === 'string' ? x : '');
const isObject = (x) => x !== null && typeof x === 'object' && !Array.isArray(x);

/** The browser facts of /api/v1/version, or null when the browser is off. */
function browserOf(ctx) {
  const eb = ctx.version && ctx.version.eventBrowser;
  return eb && eb.enabled ? eb : null;
}

/** epoch ms of a datetime-local value, or null. */
function localMs(value) {
  const ms = Date.parse(value);
  return Number.isFinite(ms) ? ms : null;
}

/** The API query of a search from the page's parameters (the time range resolved now). */
function searchQuery(params, eb) {
  const q = new URLSearchParams();
  q.set('topic', params.get('topic') || eb.topics[0]);
  const range = params.get('range') || 'all';
  const preset = RANGES.find((r) => r[0] === range);
  if (preset && preset[2]) q.set('from', String(Date.now() - preset[2]));
  if (range === 'custom') {
    const from = localMs(params.get('from') || '');
    const to = localMs(params.get('to') || '');
    if (from !== null) q.set('from', String(from));
    if (to !== null) q.set('to', String(to));
  }
  for (const [name] of FIELDS) {
    const v = params.get(name);
    if (v) q.set(name, v);
  }
  if (eb.payloads && params.get('q')) q.set('q', params.get('q'));
  return q;
}

/** The API query of a tail: the search's, without the time range. */
function tailQuery(params, eb) {
  const q = searchQuery(params, eb);
  q.delete('from');
  q.delete('to');
  return q;
}

/** `#/events?...` searching `topic` for `name` = `value` only. */
function searchHref(topic, name, value) {
  const p = new URLSearchParams({ topic, [name]: value });
  return `#/events?${p}`;
}

/** The API path of one record. */
const recordPath = (topic, partition, offset) => `events/${encodeURIComponent(topic)}/${partition}/${offset}`;

async function load(ctx) {
  const eb = browserOf(ctx);
  if (!eb) return { disabled: true };
  const query = searchQuery(ctx.params, eb);
  const record = ctx.params.get('record');
  const [search, detail] = await Promise.all([
    ctx.api.browse(`events?${query}`),
    record ? ctx.api.browse(`events/${record}`) : Promise.resolve(null),
  ]);
  return { eb, query: query.toString(), search, detail };
}

/** A summary's cells for the results and the tail. */
function cells(r, open) {
  const headers = isObject(r.headers) ? r.headers : {};
  const type = str(headers.type);
  const corr = str(headers.correlationId);
  return [
    { class: 'nowrap', content: Number.isFinite(r.timestamp) ? fmt.time(r.timestamp) : '—' },
    { class: 'mono', content: type ? h('a', { href: searchHref(r.topic, 'type', type), title: 'Search by this type' }, type) : '—' },
    { class: 'mono', content: r.key === null || r.key === undefined ? h('span', { class: 'dim' }, 'none') : [str(r.key), r.keyTruncated ? '…' : ''] },
    {
      class: 'num nowrap',
      content: h('button', { type: 'button', class: 'linkish', title: 'Show this record', on: { click: () => open(r) } },
        `${r.partition} / ${r.offset}`),
    },
    { class: 'mono', content: str(headers.source) || '—' },
    { class: 'mono', content: corr ? h('a', { href: searchHref(r.topic, 'correlationId', corr), title: 'Search by this correlation id' }, corr) : '—' },
  ];
}

const HEADERS = ['Time', 'Type', 'Key', { label: 'Partition / offset', class: 'num' }, 'Source', 'Correlation id'];

/** The payload of a record detail, as text. */
function payloadView(d) {
  if (isObject(d.payload) && typeof d.payload.hidden === 'string') {
    return h('p', { class: 'note' }, badge('plain', 'payload hidden', d.payload.hidden), ' ', str(d.payload.reason));
  }
  const truncated = d.payloadTruncated ? h('p', { class: 'note' }, 'Cut at 256 KiB.') : null;
  if (typeof d.payloadBase64 === 'string') {
    return [h('p', { class: 'note' }, 'The payload is not UTF-8; base64:'), h('pre', { class: 'payload' }, d.payloadBase64), truncated];
  }
  if (typeof d.payload !== 'string') return h('p', { class: 'note' }, 'No payload (a tombstone).');
  let text = d.payload;
  try {
    text = JSON.stringify(JSON.parse(d.payload), null, 2);
  } catch {
    // Not JSON: shown as it is.
  }
  return [h('pre', { class: 'payload', tabindex: 0, 'aria-label': 'Payload' }, text), truncated];
}

export default {
  id: 'events',
  title: 'Events',
  browser: true,
  live: false,
  load,

  mount(root, ctx) {
    const meta = h('span', { class: 'meta' });
    const results = h('div');
    const detail = h('div');
    const tailBody = h('div');
    const tailStatus = h('p', { class: 'note', role: 'status' });
    let last = null;
    let rows = [];
    let tail = null;
    // Tail records not drawn yet, oldest first; drawn once per animation frame, not while paused.
    let pending = [];
    let frame = 0;
    let tailRows = null;
    let paused = false;

    const eb = browserOf(ctx);
    if (!eb) {
      root.append(h('h1', null, 'Events'),
        h('section', { class: 'card' }, h('p', { class: 'empty' }, 'The event browser is off on this hub: set KRAKEN_HUB_KAFKA_BOOTSTRAP.')));
      return { update() {} };
    }

    // The form: every field writes the URL, Search runs it.
    const topic = h('select', { 'aria-label': 'Topic' },
      eb.topics.map((t) => h('option', { value: t, selected: t === (ctx.params.get('topic') || eb.topics[0]) }, t)));
    const range = h('select', { 'aria-label': 'Time range' },
      RANGES.map(([id, label]) => h('option', { value: id, selected: id === (ctx.params.get('range') || 'all') }, label)));
    const from = h('input', { type: 'datetime-local', step: '1', 'aria-label': 'From', value: ctx.params.get('from') || '' });
    const to = h('input', { type: 'datetime-local', step: '1', 'aria-label': 'To', value: ctx.params.get('to') || '' });
    const custom = h('span', { class: 'choice-row', hidden: range.value !== 'custom' }, 'from', from, 'to', to);
    range.addEventListener('change', () => { custom.hidden = range.value !== 'custom'; });
    const inputs = FIELDS.map(([name, label, placeholder]) => [name, h('input', {
      type: 'search', 'aria-label': label, placeholder: `${label}: ${placeholder}`, value: ctx.params.get(name) || '', spellcheck: 'false',
    })]);
    const payload = eb.payloads
      ? h('input', { type: 'search', 'aria-label': 'Payload text', placeholder: 'Payload text (exact, case-sensitive)', value: ctx.params.get('q') || '', spellcheck: 'false' })
      : null;
    const form = h('form', { class: 'controls event-form', role: 'search' },
      topic, range, custom, inputs.map(([, el]) => el), payload,
      h('button', { type: 'submit' }, 'Search'));
    form.addEventListener('submit', (event) => {
      event.preventDefault();
      const values = { topic: topic.value, range: range.value, record: null };
      values.from = range.value === 'custom' ? from.value : null;
      values.to = range.value === 'custom' ? to.value : null;
      for (const [name, el] of inputs) values[name] = el.value.trim();
      if (payload) values.q = payload.value;
      ctx.setParams(values);
      fill(detail);
      ctx.refresh();
      if (tail) startTail();
    });

    const tailButton = h('button', { type: 'button', 'aria-pressed': 'false' }, 'Tail');
    const pause = h('input', { type: 'checkbox' });
    root.append(
      h('div', { class: 'card-head' }, h('h1', null, 'Events'), meta),
      form,
      payload ? null : h('p', { class: 'note' }, 'Payloads are not served to this page (KRAKEN_HUB_EVENT_BROWSER_PAYLOADS and a dashboard or write token): records show their headers only.'),
      detail,
      h('section', { class: 'card', 'aria-label': 'Live tail' },
        h('div', { class: 'card-head' }, h('h2', null, 'Tail'), tailButton, h('label', null, pause, 'Pause')),
        tailStatus, tailBody),
      h('section', { class: 'card', 'aria-label': 'Search results' }, results));

    const openRecord = async (r) => {
      const path = recordPath(r.topic, r.partition, r.offset);
      ctx.setParams({ record: path.slice('events/'.length) });
      fill(detail, h('section', { class: 'card' }, h('p', { class: 'empty' }, 'Loading the record…')));
      try {
        renderDetail(await ctx.api.browse(path));
      } catch (error) {
        // A 401 already showed the sign-in form.
        if (error instanceof ctx.api.AuthError) return;
        fill(detail, h('section', { class: 'card' }, h('p', { class: 'error-line' }, `Could not read the record (${error.message}).`)));
      }
    };

    const renderDetail = (answer) => {
      if (!answer) {
        fill(detail);
        return;
      }
      const close = h('button', { type: 'button', on: { click: () => { ctx.setParams({ record: null }); fill(detail); } } }, 'Close');
      if (!answer.ok || !isObject(answer.data)) {
        const why = answer.data && typeof answer.data.error === 'string' ? answer.data.error : `HTTP ${answer.status}`;
        fill(detail, h('section', { class: 'card' }, h('div', { class: 'card-head' }, h('h2', null, 'Record'), close),
          h('p', { class: 'error-line' }, why)));
        return;
      }
      const d = answer.data;
      const fact = (k, v) => [h('dt', null, k), h('dd', null, v)];
      const all = Array.isArray(d.allHeaders) ? d.allHeaders : [];
      fill(detail, h('section', { class: 'card', 'aria-label': 'Record' },
        h('div', { class: 'card-head' }, h('h2', null, `${str(d.topic)} ${d.partition} / ${d.offset}`), close),
        h('dl', { class: 'facts' },
          fact('Time', Number.isFinite(d.timestamp) ? fmt.time(d.timestamp) : '—'),
          fact('Key', d.key === null || d.key === undefined ? 'none' : h('span', { class: 'mono' }, str(d.key))),
          fact('Size', `${fmt.int(d.size)} bytes`)),
        table('Headers', ['Header', 'Value'], all.map((x) => [
          { class: 'mono', content: str(x.name) },
          {
            class: 'mono',
            content: typeof x.valueHex === 'string'
              ? [h('span', { class: 'dim' }, 'hex '), x.valueHex]
              : (x.value === null ? h('span', { class: 'dim' }, 'null') : str(x.value)),
          },
        ]), 'headers'),
        d.headersOmitted ? h('p', { class: 'note' }, `${fmt.int(d.headersOmitted)} more headers not shown.`) : null,
        h('h3', null, 'Payload'),
        payloadView(d)));
    };

    const statsText = (s) => {
      if (s.missing) return `Topic ${s.topic} does not exist on the broker.`;
      const scanned = `Scanned ${fmt.int(s.scanned)} records`;
      if (s.exhausted) return `${scanned}; every partition searched.`;
      return `${scanned}, ${STOPPED[s.stoppedBy] || 'stopped'}.`;
    };

    const renderResults = (s, more) => {
      const table1 = rows.length
        ? table('Events found, newest first', HEADERS, rows.map((r) => cells(r, openRecord)), 'events')
        : h('p', { class: 'empty' }, s.missing ? 'Nothing to search.' : 'No record matches.');
      const loadMore = s.cursor
        ? h('button', { type: 'button', on: { click: () => more(s.cursor) } }, 'Load more')
        : null;
      meta.textContent = `${fmt.int(rows.length)} found`;
      fill(results, h('p', { class: 'note' }, statsText(s)), table1, loadMore);
    };

    const show = (answer, query) => {
      if (!answer.ok || !isObject(answer.data)) {
        const why = answer.data && typeof answer.data.error === 'string' ? answer.data.error : `HTTP ${answer.status}`;
        meta.textContent = '';
        fill(results, h('p', { class: 'error-line' }, `The hub refused the search: ${why}`));
        return;
      }
      const s = answer.data;
      rows = rows.concat(Array.isArray(s.results) ? s.results : []);
      const more = async (cursor) => {
        try {
          show(await ctx.api.browse(`events?${query}&cursor=${encodeURIComponent(cursor)}`), query);
        } catch (error) {
          // A 401 already showed the sign-in form.
        if (error instanceof ctx.api.AuthError) return;
          fill(results, h('p', { class: 'error-line' }, `Could not load more (${error.message}).`));
        }
      };
      renderResults(s, more);
    };

    // ---- tail ----

    // Prepends the records that came since the last frame (only the newest that can stay) and
    // drops the rows beyond MAX_TAIL_ROWS: a busy topic costs one small change per frame.
    const drawTail = () => {
      frame = 0;
      const fresh = pending.slice(-MAX_TAIL_ROWS).reverse();
      pending = [];
      if (!fresh.length) return;
      if (!tailRows) {
        const wrap = table(TAIL_CAPTION, HEADERS, [], 'events');
        tailRows = wrap.querySelector('tbody');
        fill(tailBody, wrap);
      }
      tailRows.prepend(...fresh.map((r) => row(cells(r, openRecord))));
      while (tailRows.rows.length > MAX_TAIL_ROWS) tailRows.lastElementChild.remove();
    };

    const queueTail = (record) => {
      pending.push(record);
      // Only the newest MAX_TAIL_ROWS can be shown: let the rest go, a batch at a time.
      if (pending.length >= 2 * MAX_TAIL_ROWS) pending = pending.slice(-MAX_TAIL_ROWS);
      if (!paused && !frame) frame = requestAnimationFrame(drawTail);
    };

    const stopTail = (why) => {
      if (tail) tail.abort();
      tail = null;
      ctx.setParams({ tail: null });
      tailButton.textContent = 'Tail';
      tailButton.setAttribute('aria-pressed', 'false');
      if (why) tailStatus.textContent = why;
    };

    const startTail = () => {
      if (tail) tail.abort();
      const controller = new AbortController();
      tail = controller;
      ctx.setParams({ tail: 'on' });
      tailButton.textContent = 'Stop tail';
      tailButton.setAttribute('aria-pressed', 'true');
      tailStatus.textContent = 'Starting the tail…';
      const ended = (text) => {
        if (tail === controller) stopTail(text);
      };
      ctx.api.readTail(tailQuery(ctx.params, eb).toString(), {
        signal: controller.signal,
        onOpen: () => {},
        onRefused: (status, data) => ended(`The hub refused the tail: ${data && typeof data.error === 'string' ? data.error : `HTTP ${status}`}`),
        onEvent: (name, data) => {
          let value;
          try {
            value = JSON.parse(data);
          } catch {
            return;
          }
          if (name === 'start') {
            tailStatus.textContent = `Tailing ${str(value.topic)} (${fmt.int(value.partitions)} partitions) from now until ${fmt.clock(value.endsAtMs)}.`;
          } else if (name === 'record' && isObject(value)) {
            queueTail(value);
          } else if (name === 'end') {
            ended(`The tail ended: ${ENDED[value.reason] || str(value.reason)}.`);
          }
        },
      }).then(() => ended(tailStatus.textContent), (error) => {
        if (controller.signal.aborted) return;
        if (error instanceof ctx.api.AuthError) return;
        ended(`The tail stopped (${error.message}).`);
      });
    };

    tailButton.addEventListener('click', () => {
      if (tail) stopTail('Tail stopped.');
      else startTail();
    });
    pause.addEventListener('change', () => {
      paused = pause.checked;
      if (!paused && pending.length && !frame) frame = requestAnimationFrame(drawTail);
    });

    // A link with tail=on opens the page tailing.
    if (ctx.params.get('tail') === 'on') startTail();

    return {
      update(data) {
        if (data.disabled) return;
        last = data;
        rows = [];
        show(last.search, last.query);
        renderDetail(last.detail);
      },
      dispose() {
        if (tail) tail.abort();
        tail = null;
        if (frame) cancelAnimationFrame(frame);
        frame = 0;
      },
    };
  },
};
