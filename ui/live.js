// Live updates: the hub's server-sent `summary` events, with polling as the fallback while the
// stream is unavailable (old proxies, too many open streams, a restarting hub). The stream is
// read with fetch (see readStream in api.js) so the dashboard token stays in a header.

import { AuthError, canStream, get, readStream } from './api.js';

const POLL_MS = 2000;
const STREAM_RETRY_MS = 15000;
const STREAM_RECONNECT_MS = 3000;

/** A summary like the stream's, from a topology snapshot (the polling fallback). */
function fromTopology(t) {
  const stale = t.services.flatMap((s) => s.instances).filter((i) => i.stale).length;
  return { hubId: t.hubId, nowMs: t.nowMs, totals: t.totals, staleInstances: stale, revision: null, buildId: null };
}

/**
 * Calls `onSummary(summary)` for every update and `onState(state)` with
 * live | polling | offline whenever the connection state changes. Returns `{ restart }`, which
 * starts over (after the reader entered a dashboard token, or to leave the polling fallback).
 */
export function startLive({ onSummary, onState }) {
  let controller = null;
  let retryTimer = null;
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

  const retryIn = (ms) => {
    clearTimeout(retryTimer);
    retryTimer = setTimeout(connect, ms);
  };

  async function connect() {
    if (!canStream()) {
      startPolling();
      return;
    }
    controller = new AbortController();
    const mine = controller;
    try {
      await readStream({
        signal: mine.signal,
        onOpen: () => {
          stopPolling();
          setState('live');
        },
        onEvent: (name, data) => {
          if (name !== 'summary') return;
          let summary;
          try {
            summary = JSON.parse(data);
          } catch {
            return;
          }
          stopPolling();
          setState('live');
          onSummary(summary);
        },
      });
      // The hub ended the stream (it restarts, or a proxy cut it): poll, then reconnect.
      startPolling();
      retryIn(STREAM_RECONNECT_MS);
    } catch (error) {
      if (mine.signal.aborted) return;
      // 401: the page asks for a token and restarts the stream; no polling would work either.
      if (error instanceof AuthError) return;
      // A 503 (too many open streams) or a network error: poll, and try the stream again later.
      startPolling();
      retryIn(STREAM_RETRY_MS);
    }
  }

  connect();

  return {
    restart() {
      controller?.abort();
      clearTimeout(retryTimer);
      stopPolling();
      connect();
    },
  };
}
