// Number, duration and time formatting for the dashboard.

const integer = new Intl.NumberFormat('en');
const compact = new Intl.NumberFormat('en', { notation: 'compact', maximumFractionDigits: 1 });

/** 1,284 */
export function int(n) {
  return integer.format(n ?? 0);
}

/** 1,284 below 10,000, then 12.9K. */
export function count(n) {
  return (n ?? 0) < 10000 ? integer.format(n ?? 0) : compact.format(n);
}

/** A latency in ms: 0.42 ms, 4.2 ms, 42 ms, 1.2 s; null is a dash. */
export function latency(ms) {
  if (ms === null || ms === undefined) return '—';
  if (ms < 1) return `${ms.toFixed(2)} ms`;
  if (ms < 10) return `${ms.toFixed(1)} ms`;
  if (ms < 1000) return `${Math.round(ms)} ms`;
  return `${(ms / 1000).toFixed(ms < 10000 ? 2 : 1)} s`;
}

/** A span of time: 850 ms, 12 s, 4 m 10 s, 3 h 5 m, 2 d 3 h. */
export function span(ms) {
  const v = Math.max(0, ms ?? 0);
  if (v < 1000) return `${Math.round(v)} ms`;
  const s = Math.floor(v / 1000);
  if (s < 60) return `${s} s`;
  if (s < 3600) return `${Math.floor(s / 60)} m ${s % 60} s`;
  if (s < 86400) return `${Math.floor(s / 3600)} h ${Math.floor((s % 3600) / 60)} m`;
  return `${Math.floor(s / 86400)} d ${Math.floor((s % 86400) / 3600)} h`;
}

/** A window length: "5 min", "90 s", "1 h". */
export function windowLabel(ms) {
  const s = Math.round((ms ?? 0) / 1000);
  if (s % 3600 === 0 && s >= 3600) return `${s / 3600} h`;
  if (s % 60 === 0 && s >= 60) return `${s / 60} min`;
  return `${s} s`;
}

/** Error share: "0 %", "<0.1 %", "2.4 %", "100 %". */
export function errorRate(errors, total) {
  if (!total) return '—';
  if (!errors) return '0 %';
  const pct = (errors / total) * 100;
  if (pct < 0.1) return '<0.1 %';
  return `${pct < 10 ? pct.toFixed(1) : Math.round(pct)} %`;
}

/** Error level of an edge or type: idle | ok | warn (1 % or more) | crit (5 % or more). */
export function errorLevel(errors, total) {
  if (!total) return 'idle';
  const share = errors / total;
  if (share >= 0.05) return 'crit';
  if (share >= 0.01) return 'warn';
  return 'ok';
}

const timeFormat = new Intl.DateTimeFormat(undefined, {
  year: 'numeric', month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', second: '2-digit',
});
const clockFormat = new Intl.DateTimeFormat(undefined, { hour: '2-digit', minute: '2-digit', second: '2-digit' });

/** A wall-clock time with the date, in the viewer's time zone. */
export function time(epochMs) {
  return timeFormat.format(new Date(epochMs));
}

/** Hours, minutes and seconds only. */
export function clock(epochMs) {
  return clockFormat.format(new Date(epochMs));
}
