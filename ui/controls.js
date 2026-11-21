// The processor controls of the dashboard: the write-token field, the confirm dialog
// and the line that reports a control call. The write token is typed into a password field, kept
// in sessionStorage by api.js and sent as a header to this hub only; it never goes into a URL.
// With peer hubs a control call still goes to this hub: every instance is connected to every hub
//, so this hub reaches them all.

import * as api from './api.js';
import { fill, h } from './dom.js';

/** The help line under the write-token field. */
const HELP = 'Pause, resume, reset and dead-letter actions need the write token (SNODRA_SERVER_DASHBOARD_WRITE_TOKEN) or an ACL credential with control = true. '
  + 'It is kept in this tab only and sent as an Authorization header to this hub, never in a URL. '
  + 'With several hubs, a control goes to the hub that served this page: every instance is connected to every hub, so it reaches them all.';

/**
 * The write-token card: a password field while no write credential is held, else a note and a
 * button that forgets it. `onChange()` runs after either.
 */
export function writeTokenCard(onChange) {
  const card = h('section', { class: 'card write-token', 'aria-label': 'Controls' });
  const render = () => {
    if (api.hasWriteToken()) {
      fill(card,
        h('div', { class: 'card-head' }, h('h2', null, 'Controls'),
          h('span', { class: 'meta' }, 'enabled for this tab')),
        h('p', { class: 'note' }, HELP),
        h('button', { type: 'button', on: { click: () => { api.clearWriteToken(); render(); onChange(); } } },
          'Forget the write token'));
      return;
    }
    const input = h('input', {
      id: 'write-token', name: 'write-token', type: 'password', autocomplete: 'off', spellcheck: 'false', required: true,
    });
    const form = h('form', { class: 'login inline' },
      h('label', { for: 'write-token' }, 'Write token'),
      input,
      h('button', { type: 'submit' }, 'Enable controls'));
    form.addEventListener('submit', (event) => {
      event.preventDefault();
      api.setWriteToken(input.value);
      render();
      onChange();
    });
    fill(card,
      h('div', { class: 'card-head' }, h('h2', null, 'Controls'), h('span', { class: 'meta' }, 'read-only')),
      form,
      h('p', { class: 'note' }, HELP));
  };
  render();
  return card;
}

/**
 * Asks before an action: a modal dialog with `title`, the `lines` of text and the `extra` nodes
 * (inputs), and the buttons Cancel and `confirm`. Resolves to true when confirmed.
 */
export function confirmDialog({ title, lines, extra = null, confirm }) {
  return new Promise((resolve) => {
    const dialog = h('dialog', { class: 'confirm', 'aria-label': title });
    const done = (value) => {
      dialog.close();
      dialog.remove();
      resolve(value);
    };
    dialog.addEventListener('cancel', () => done(false));
    fill(dialog,
      h('form', { method: 'dialog' },
        h('h2', null, title),
        lines.map((line) => h('p', null, line)),
        extra,
        h('div', { class: 'dialog-buttons' },
          h('button', { type: 'button', on: { click: () => done(false) } }, 'Cancel'),
          h('button', { type: 'button', class: 'danger', on: { click: () => done(true) } }, confirm))));
    document.body.append(dialog);
    dialog.showModal();
  });
}

/** One instance's answer (`{ result, code, message, instanceId }`) as text, or null. */
function answerText(a) {
  if (!a || typeof a !== 'object' || typeof a.result !== 'string') return null;
  const what = typeof a.code === 'string' && a.code ? `refused ${a.code}` : a.result;
  const message = typeof a.message === 'string' && a.message ? `: ${a.message}` : '';
  const by = typeof a.instanceId === 'string' && a.instanceId ? ` (instance ${a.instanceId})` : '';
  return `${what}${message}${by}`;
}

/**
 * The reason a control call failed, from the hub's JSON or the status: the hub's `error` text
 * (with the instance's refusal code when it names one), the answer that ended a reset (`error`
 * as an object), or the first instance of a pause or resume that did not confirm.
 */
function reasonOf(data) {
  if (!data || typeof data !== 'object') return null;
  if (typeof data.error === 'string') {
    return typeof data.code === 'string' && data.code ? `${data.error} (${data.code})` : data.error;
  }
  const ended = answerText(data.error);
  if (ended) return ended;
  const instances = Array.isArray(data.instances) ? data.instances : [];
  const failed = instances.find((i) => i && i.result !== 'ok');
  return failed ? answerText(failed) : null;
}

/** The reason a control call failed, as the status line shows it. */
export function failure(result) {
  const reason = reasonOf(result.data);
  if (result.status === 401) return 'The hub did not accept the write token.';
  if (result.status === 403) return reason || 'This credential may not control processors.';
  return `HTTP ${result.status}${reason ? `: ${reason}` : ''}`;
}

/** A status line a page fills with the outcome of its latest control call. */
export function statusLine() {
  const el = h('p', { class: 'control-status', role: 'status', hidden: true });
  return {
    el,
    show(text, failed) {
      el.textContent = text;
      el.classList.toggle('failed', Boolean(failed));
      el.hidden = false;
    },
  };
}

/** Calls the control `path` (under /api/v1/) and reports it on `status`; resolves to the result,
 * or null when the hub could not be reached. */
export async function run(status, method, path, body, describe) {
  try {
    const result = await api.control(method, path, body);
    if (result.ok) status.show(describe(result.data), false);
    else status.show(failure(result), true);
    return result;
  } catch (error) {
    status.show(`Could not reach the hub (${error.message}).`, true);
    return null;
  }
}

/** The API path of `processor` of `service`, plus `rest`. */
export function processorPath(service, processor, rest) {
  return `services/${encodeURIComponent(service)}/processors/${encodeURIComponent(processor)}/${rest}`;
}
