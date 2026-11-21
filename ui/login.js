// The sign-in form of a hub that has a dashboard token (SNODRA_SERVER_DASHBOARD_TOKEN).
// The token is typed into a password field, kept in sessionStorage by api.js and sent as a header;
// the form never puts it in a URL.

import { fill, h } from './dom.js';

/**
 * Replaces the content of `root` with the form. `submit(token)` resolves to an error message
 * (the token was rejected, the hub did not answer) or to null when the token works.
 */
export function showLogin(root, submit) {
  const message = h('p', { class: 'error-line', role: 'alert', hidden: true });
  const input = h('input', {
    id: 'login-token',
    name: 'token',
    type: 'password',
    autocomplete: 'off',
    spellcheck: 'false',
    required: true,
  });
  const form = h('form', { class: 'login' },
    h('label', { for: 'login-token' }, 'Dashboard token'),
    input,
    h('button', { type: 'submit' }, 'Sign in'),
    message);
  form.addEventListener('submit', async (event) => {
    event.preventDefault();
    const failure = await submit(input.value);
    if (failure) {
      message.textContent = failure;
      message.hidden = false;
      input.select();
    }
  });
  fill(root, h('section', { class: 'card login-card', 'aria-label': 'Sign in' },
    h('div', { class: 'card-head' }, h('h1', null, 'Sign in')),
    h('p', { class: 'note' },
      'This hub requires a dashboard token (SNODRA_SERVER_DASHBOARD_TOKEN). It is kept in this tab only and sent as an Authorization header.'),
    form));
  input.focus();
}
