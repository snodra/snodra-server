// Live updates: the hub's server-sent `summary` events, with polling as the fallback while the
// stream is unavailable (old proxies, too many open streams, a restarting hub).

import { get, streamUrl } from './api.js';

const POLL_MS = 2000;
const STREAM_RETRY_MS = 15000;

/** A summary like the stream's, from a topology snapshot (the polling fallback). */
function fromTopology(t) {
  const stale = t.services.flatMap((s) => s.instances).filter((i) => i.stale).length;
  return { hubId: t.hubId, nowMs: t.nowMs, totals: t.totals, staleInstances: stale, revision: null, buildId: null };
}

/**
 * Calls `onSummary(summary)` for every update and `onState(state)` with
 * live | polling | offline whenever the connection state changes.
 */
export function startLive({ onSummary, onState }) {
  let source = null;
  let pollTimer = null;
  let state = null;

  const setState = (next) => {
    if (next !== state) {
      state = next;
      onState(next);
    }
  };

  const poll = async () => {
    let summary;
    try {
      summary = fromTopology(await get('topology'));
    } catch {
      if (pollTimer !== null) setState('offline');
      return;
    }
    // The stream may have come back while the request was out.
    if (pollTimer === null) return;
    setState('polling');
    onSummary(summary);
  };

  const startPolling = () => {
    if (pollTimer !== null) return;
    poll();
    pollTimer = setInterval(poll, POLL_MS);
  };

  const stopPolling = () => {
    clearInterval(pollTimer);
    pollTimer = null;
  };

  const connect = () => {
    if (typeof EventSource !== 'function') {
      startPolling();
      return;
    }
    source = new EventSource(streamUrl());
    source.addEventListener('open', () => {
      stopPolling();
      setState('live');
    });
    source.addEventListener('summary', (event) => {
      let summary;
      try {
        summary = JSON.parse(event.data);
      } catch {
        return;
      }
      stopPolling();
      setState('live');
      onSummary(summary);
    });
    source.addEventListener('error', () => {
      // CONNECTING: the browser retries by itself; poll meanwhile. CLOSED (for example a 503
      // when too many streams are open): poll, and try the stream again later.
      startPolling();
      if (source.readyState === EventSource.CLOSED) {
        source = null;
        setTimeout(connect, STREAM_RETRY_MS);
      }
    });
  };

  connect();
}
