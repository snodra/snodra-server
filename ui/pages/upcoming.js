// Pages that later roadmap phases fill. Each is a nav entry and a short note today.

import { h } from '../dom.js';

/** A placeholder page for a feature that ships in roadmap phase `phase`. */
export function upcoming({ id, title, phase, text }) {
  return {
    id,
    title,
    phase,
    live: false,
    load: null,
    mount(root) {
      root.append(
        h('h1', null, title),
        h('section', { class: 'card' },
          h('p', null, h('strong', null, `Available from phase ${phase}.`)),
          h('p', { class: 'note' }, text)));
      return { update() {} };
    },
  };
}
