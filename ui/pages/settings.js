// The hub's identity and effective configuration (read-only).

import { badge, fill, h, table } from '../dom.js';
import * as fmt from '../format.js';

function value(v, secret) {
  if (secret) return { class: 'dim', content: 'hidden' };
  if (v === null || v === undefined) return { class: 'dim', content: 'generated' };
  if (v === '') return { class: 'dim', content: 'none' };
  return { class: 'mono', content: v };
}

export default {
  id: 'settings',
  title: 'Settings',
  live: false,
  load: async (ctx) => {
    const [settings, version] = await Promise.all([ctx.api.get('settings'), ctx.api.get('version')]);
    return { settings: settings.settings, version };
  },

  mount(root) {
    const hub = h('dl', { class: 'facts' });
    const body = h('div');
    root.append(
      h('h1', null, 'Settings'),
      h('section', { class: 'card', 'aria-label': 'Hub' }, h('div', { class: 'card-head' }, h('h2', null, 'Hub')), hub),
      h('section', { class: 'card', 'aria-label': 'Configuration' },
        h('div', { class: 'card-head' }, h('h2', null, 'Configuration'),
          h('span', { class: 'meta' }, 'Environment variables as the hub applied them. Read-only: change them in the deployment and restart.')),
        body,
        h('p', { class: 'note' }, 'Secret settings (the dashboard tokens) are listed by name only; their values never leave the hub. ACL tokens are not settings: the ACL file holds their digests.')));

    return {
      update({ settings, version: v }) {
        const fact = (k, val) => [h('dt', null, k), h('dd', null, val)];
        fill(hub,
          fact('Hub id', h('span', { class: 'mono' }, v.hubId)),
          fact('Version', v.version),
          fact('Protocol', `v${v.protocolVersion}${v.capabilities.length ? `, capabilities: ${v.capabilities.join(', ')}` : ''}`),
          fact('Started', `${fmt.time(v.startedAtMs)} (up ${fmt.span(v.nowMs - v.startedAtMs)})`),
          fact('Dashboard build', h('span', { class: 'mono' }, v.buildId)),
          fact('Grafana', v.grafanaUrl ? h('a', { href: v.grafanaUrl, target: '_blank', rel: 'noopener noreferrer' }, v.grafanaUrl) : h('span', { class: 'dim' }, 'not configured (KRAKEN_HUB_GRAFANA_URL)')));
        fill(body, table('Configuration', ['Variable', 'Value', 'Default', ''], settings.map((s) => {
          const changed = !s.secret && s.default !== null && s.value !== s.default;
          return [
            { class: 'mono', content: [s.name, h('span', { class: 'desc' }, s.description)] },
            value(s.value, s.secret),
            value(s.default, s.secret),
            changed ? badge('plain', 'changed', 'Differs from the default') : '',
          ];
        })));
      },
    };
  },
};
