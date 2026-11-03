// Services and their live instances; with peer hubs (ADR 0016) optionally merged over every
// reachable hub, keyed by instance id, with the hubs that list each instance and disagreements.

import { badge, fill, h, keepScroll, num, selectionIn, table } from '../dom.js';
import * as fmt from '../format.js';
import { STATUS_STALE_MS } from '../hubs.js';

function typeLinks(label, names) {
  if (!names.length) return null;
  return [h('strong', null, `${label} `), names.map((n) => h('a', { class: 'chip', href: `#/types?q=${encodeURIComponent(n)}` }, n)), ' '];
}

/** The hubs an instance reported (InstanceStatus), flagged when `hubId` is not among them or
 * the report is older than STATUS_STALE_MS. */
function hubsCell(i, hubId) {
  if (!(i.statusAgeMs >= 0)) {
    return { class: 'dim', content: 'not reported', title: 'The client sends no InstanceStatus (single-hub or older client)' };
  }
  const ids = Array.isArray(i.connectedHubIds) ? i.connectedHubIds : [];
  const missing = !ids.includes(hubId);
  const old = i.statusAgeMs > STATUS_STALE_MS;
  return {
    title: `reported ${fmt.span(i.statusAgeMs)} ago`,
    content: [
      ids.map((id) => h('span', { class: ['chip', 'mono', id === hubId ? 'self' : null] }, id)),
      missing ? badge('warn', `${hubId} missing`, 'The instance does not list this hub among the hubs it is connected to') : null,
      old ? badge('warn', `${fmt.span(i.statusAgeMs)} old`, 'Clients report every 5 s; this report is older than 15 s') : null,
    ],
  };
}

function instanceRows(service, data) {
  const merged = data.scope === 'all';
  return service.instances.map((i) => {
    const row = [
      { class: 'mono nowrap', content: [
        i.instanceId,
        i.auth ? [' ', i.authenticatedService
          ? badge('good', i.authKind === 'jwt' ? 'authenticated (jwt)' : i.authKind === 'certificate' ? 'authenticated (certificate)' : 'authenticated',
            i.authKind === 'jwt'
              ? `The JWT is for service ${i.authenticatedService}; it expires ${new Date(i.expiresAt).toISOString()} unless the client refreshes it`
              : i.authKind === 'certificate'
                ? `The client certificate ${i.certFingerprint} is for service ${i.authenticatedService}; the session ends ${new Date(i.expiresAt).toISOString()} when the certificate does, and the client reconnects with a renewed one`
                : `The token is granted service ${i.authenticatedService}`)
          : badge('warn', i.auth, 'No valid token at registration: enforce mode would refuse this instance')] : null,
        i.stale ? [' ', badge('warn', 'stale', 'Heartbeat older than a third of the eviction timeout; still routed')] : null,
        merged ? (i.flags || []).map((f) => [' ', badge('crit', f.text, 'The reachable hubs disagree about this instance')]) : null,
      ] },
      i.clientVersion ? { class: 'mono nowrap', content: i.clientVersion, title: i.clientVersion } : { class: 'dim', content: 'none (v1)', title: 'v1 clients send no client version' },
      i.protocolVersion >= 2 ? `v${i.protocolVersion}` : { class: 'dim', content: 'v1' },
      i.flowControlled
        ? num(i.permitsAvailable === 0 ? badge('warn', '0', 'No permit left: new requests wait in the queue') : fmt.int(i.permitsAvailable))
        : num(h('span', { class: 'dim' }, 'unlimited'), 'Not flow-controlled'),
      num(i.queued ? fmt.int(i.queued) : h('span', { class: 'dim' }, '0')),
      num(`${fmt.int(i.inFlightAsHandler)} / ${fmt.int(i.inFlightAsSender)}`),
      num(fmt.int(i.loadFactor)),
      Object.keys(i.tags || {}).length
        ? Object.entries(i.tags).map(([k, v]) => h('span', { class: 'chip' }, `${k}=${v}`))
        : { class: 'dim', content: 'none' },
      num(`${fmt.span(i.lastHeartbeatAgeMs)} ago`),
      num(fmt.span(data.nowMs - i.connectedSinceMs), fmt.time(i.connectedSinceMs)),
      hubsCell(i, merged ? i.viewHubId : data.hubId),
    ];
    if (merged) {
      row.push({
        title: i.viewHubId === data.hubId ? null : `Shown as ${i.viewHubId} sees it`,
        content: data.hubs.map((hub) => (i.seenBy.includes(hub)
          ? h('span', { class: ['chip', 'mono', hub === data.hubId ? 'self' : null] }, hub)
          : badge('crit', `not on ${hub}`))),
      });
    }
    return row;
  });
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
  { label: 'Hubs', title: 'Hubs the instance reports being connected to (InstanceStatus)' },
];

const MERGED_HEADERS = [...HEADERS, { label: 'Seen by', title: 'Reachable hubs that list this instance' }];

function matches(service, q) {
  if (!q) return true;
  const hay = [
    service.serviceName,
    ...service.instances.flatMap((i) => [
      i.instanceId, i.clientVersion, ...i.commandTypes, ...i.queryTypes,
      ...Object.entries(i.tags || {}).map(([k, v]) => `${k}=${v}`),
      ...(i.connectedHubIds || []), ...(i.flags || []).map((f) => f.text),
    ]),
  ];
  return hay.some((s) => s.toLowerCase().includes(q));
}

export default {
  id: 'services',
  title: 'Services',
  live: true,
  load: async (ctx) => {
    const topology = await ctx.api.get('topology');
    return ctx.allHubs ? ctx.hubs.merged(topology) : topology;
  },

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
      const merged = last.scope === 'all';
      meta.textContent = `${shown.length} of ${last.services.length} services, ${last.totals.instances} instances`
        + (merged ? ` on ${last.hubs.length} reachable hub${last.hubs.length === 1 ? '' : 's'}` : '');
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
        const disagreeing = merged ? svc.instances.filter((i) => i.flags.length).length : 0;
        const commands = [...new Set(svc.instances.flatMap((i) => i.commandTypes))].sort();
        const queries = [...new Set(svc.instances.flatMap((i) => i.queryTypes))].sort();
        const handles = commands.length + queries.length;
        return h('section', { class: 'card', 'aria-label': `Service ${svc.serviceName}` },
          h('div', { class: 'card-head' },
            h('h2', null, svc.serviceName),
            h('span', { class: 'meta' }, `${svc.instances.length} instance${svc.instances.length === 1 ? '' : 's'}, handles ${handles} type${handles === 1 ? '' : 's'}`),
            stale ? badge('warn', `${stale} stale`) : null,
            disagreeing ? badge('crit', `hubs disagree on ${disagreeing}`) : null),
          handles
            ? h('p', { class: 'types-line' }, typeLinks('Commands', commands), typeLinks('Queries', queries))
            : h('p', { class: 'types-line' }, 'Sender only: registered no types.'),
          table(`Instances of ${svc.serviceName}`, merged ? MERGED_HEADERS : HEADERS, instanceRows(svc, last),
            merged ? 'instances merged' : 'instances'));
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
