// Services and their live instances.

import { badge, fill, h, keepScroll, num, selectionIn, table } from '../dom.js';
import * as fmt from '../format.js';

function typeLinks(label, names) {
  if (!names.length) return null;
  return [h('strong', null, `${label} `), names.map((n) => h('a', { class: 'chip', href: `#/types?q=${encodeURIComponent(n)}` }, n)), ' '];
}

function instanceRows(service, nowMs) {
  return service.instances.map((i) => [
    { class: 'mono nowrap', content: [i.instanceId, i.stale ? [' ', badge('warn', 'stale', 'Heartbeat older than a third of the eviction timeout; still routed')] : null] },
    i.clientVersion ? { class: 'mono nowrap', content: i.clientVersion } : { class: 'dim', content: 'none (v1)', title: 'v1 clients send no client version' },
    i.protocolVersion >= 2 ? `v${i.protocolVersion}` : { class: 'dim', content: 'v1' },
    i.flowControlled
      ? num(i.permitsAvailable === 0 ? badge('warn', '0', 'No permit left: new requests wait in the queue') : fmt.int(i.permitsAvailable))
      : num(h('span', { class: 'dim' }, 'unlimited'), 'Not flow-controlled'),
    num(i.queued ? fmt.int(i.queued) : h('span', { class: 'dim' }, '0')),
    num(`${fmt.int(i.inFlightAsHandler)} / ${fmt.int(i.inFlightAsSender)}`),
    num(fmt.int(i.loadFactor)),
    Object.keys(i.tags).length
      ? Object.entries(i.tags).map(([k, v]) => h('span', { class: 'chip' }, `${k}=${v}`))
      : { class: 'dim', content: 'none' },
    num(`${fmt.span(i.lastHeartbeatAgeMs)} ago`),
    num(fmt.span(nowMs - i.connectedSinceMs), fmt.time(i.connectedSinceMs)),
  ]);
}

const HEADERS = [
  'Instance', 'Client', 'Protocol',
  { label: 'Permits', class: 'num', title: 'Permits left for a flow-controlled instance' },
  { label: 'Queued', class: 'num', title: 'Requests waiting in the hub for this instance' },
  { label: 'In flight', class: 'num', title: 'Handling / sent and not answered yet' },
  { label: 'Load factor', class: 'num' },
  'Tags',
  { label: 'Heartbeat', class: 'num' },
  { label: 'Connected', class: 'num' },
];

function matches(service, q) {
  if (!q) return true;
  const hay = [
    service.serviceName,
    ...service.instances.flatMap((i) => [
      i.instanceId, i.clientVersion, ...i.commandTypes, ...i.queryTypes,
      ...Object.entries(i.tags).map(([k, v]) => `${k}=${v}`),
    ]),
  ];
  return hay.some((s) => s.toLowerCase().includes(q));
}

export default {
  id: 'services',
  title: 'Services',
  live: true,
  load: (ctx) => ctx.api.get('topology'),

  mount(root, ctx) {
    let query = ctx.params.get('q') || '';
    let last = null;
    const search = h('input', {
      type: 'search', value: query, placeholder: 'Filter by service, instance, tag or type',
      'aria-label': 'Filter services',
    });
    const meta = h('span', { class: 'meta' });
    const list = h('div');
    root.append(
      h('div', { class: 'card-head' }, h('h1', null, 'Services'), meta),
      h('div', { class: 'controls' }, search),
      list);

    const draw = () => {
      // Never replace what the reader is selecting (an instance id to copy, say).
      if (!last || selectionIn(list)) return;
      keepScroll(list, () => render());
    };

    const render = () => {
      const q = query.trim().toLowerCase();
      const shown = last.services.filter((s) => matches(s, q));
      meta.textContent = `${shown.length} of ${last.services.length} services, ${last.totals.instances} instances`;
      if (!last.services.length) {
        fill(list, h('p', { class: 'empty' }, 'No services connected.'));
        return;
      }
      if (!shown.length) {
        fill(list, h('p', { class: 'empty' }, 'Nothing matches the filter.'));
        return;
      }
      fill(list, shown.map((svc) => {
        const stale = svc.instances.filter((i) => i.stale).length;
        const commands = [...new Set(svc.instances.flatMap((i) => i.commandTypes))].sort();
        const queries = [...new Set(svc.instances.flatMap((i) => i.queryTypes))].sort();
        const handles = commands.length + queries.length;
        return h('section', { class: 'card', 'aria-label': `Service ${svc.serviceName}` },
          h('div', { class: 'card-head' },
            h('h2', null, svc.serviceName),
            h('span', { class: 'meta' }, `${svc.instances.length} instance${svc.instances.length === 1 ? '' : 's'}, handles ${handles} type${handles === 1 ? '' : 's'}`),
            stale ? badge('warn', `${stale} stale`) : null),
          handles
            ? h('p', { class: 'types-line' }, typeLinks('Commands', commands), typeLinks('Queries', queries))
            : h('p', { class: 'types-line' }, 'Sender only: registered no types.'),
          table(`Instances of ${svc.serviceName}`, HEADERS, instanceRows(svc, last.nowMs), 'instances'));
      }));
    };

    search.addEventListener('input', () => {
      query = search.value;
      ctx.setParams({ q: query });
      draw();
    });

    return {
      update(topology) {
        last = topology;
        draw();
      },
    };
  },
};
