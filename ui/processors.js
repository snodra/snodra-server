// Event processors (ADR 0022): the merge of /api/v1/processors over the reachable hubs and the
// per-processor aggregate, with the rules of the hub's processors.rs. The page always
// re-aggregates from the per-instance reports, so one hub and many hubs take the same path. An
// instance seen by several hubs counts once: its freshest report (lowest statusAgeMs) wins.
// Everything a hub sends is data: it is checked for shape and rendered as text by the page.

const arr = (x) => (Array.isArray(x) ? x : []);
const str = (x) => (typeof x === 'string' ? x : '');
const count = (x) => (Number.isFinite(x) && x > 0 ? x : 0);
const offset = (x) => (Number.isFinite(x) && x >= -1 ? x : -1);

/** Worse states rank higher; a name this page does not know is UNSPECIFIED. */
const SEVERITY = { FAILING: 5, UNSPECIFIED: 4, STARTING: 3, PAUSED: 2, STOPPED: 1, RUNNING: 0 };

/** The state name as shown: one of SEVERITY's keys. */
export function stateOf(name) {
  return Object.prototype.hasOwnProperty.call(SEVERITY, name) ? name : 'UNSPECIFIED';
}

/** Freshest report first, then by instance id. */
function fresher(a, b) {
  return (a.statusAgeMs - b.statusAgeMs) || (a.instanceId < b.instanceId ? -1 : a.instanceId > b.instanceId ? 1 : 0);
}

/** One instance's report of a processor, normalised (null when it is not one). */
function instanceOf(i) {
  if (!i || typeof i !== 'object' || typeof i.instanceId !== 'string' || i.instanceId === '') return null;
  return {
    instanceId: i.instanceId,
    statusAgeMs: Number.isFinite(i.statusAgeMs) && i.statusAgeMs >= 0 ? i.statusAgeMs : Number.MAX_SAFE_INTEGER,
    state: stateOf(i.state),
    groupId: str(i.groupId),
    topic: str(i.topic),
    deadLetterMode: str(i.deadLetterMode),
    processedTotal: count(i.processedTotal),
    failedTotal: count(i.failedTotal),
    deadLetters: count(i.deadLetters),
    lastError: str(i.lastError),
    lastErrorAtMs: count(i.lastErrorAtMs),
    eventTypes: arr(i.eventTypes).filter((t) => typeof t === 'string'),
    partitions: arr(i.partitions)
      .filter((p) => p && Number.isInteger(p.partition) && p.partition >= 0)
      .map((p) => ({
        partition: p.partition,
        position: offset(p.position),
        committed: offset(p.committed),
        highWatermark: offset(p.highWatermark),
        lag: offset(p.lag),
      }))
      .sort((a, b) => a.partition - b.partition),
    seenBy: arr(i.seenBy),
  };
}

/** One processor over its instances (at least one), as processors.rs `aggregate`. */
export function aggregate(name, instances) {
  const list = [...instances].sort((a, b) => (a.instanceId < b.instanceId ? -1 : a.instanceId > b.instanceId ? 1 : 0));
  const byFreshness = [...list].sort(fresher);
  const freshest = byFreshness[0];
  let state = 'RUNNING';
  for (const i of list) if (SEVERITY[i.state] > SEVERITY[state]) state = i.state;
  // Each partition as the freshest instance that reports it has it.
  const partitions = new Map();
  for (const i of byFreshness) for (const p of i.partitions) if (!partitions.has(p.partition)) partitions.set(p.partition, p);
  const known = [...partitions.values()].map((p) => p.lag).filter((l) => l >= 0);
  let lastError = null;
  for (const i of list) {
    if (!i.lastError) continue;
    if (!lastError || i.lastErrorAtMs > lastError.atMs) {
      lastError = { message: i.lastError, atMs: i.lastErrorAtMs, instanceId: i.instanceId };
    }
  }
  return {
    name,
    groupId: freshest.groupId,
    topic: freshest.topic,
    deadLetterMode: freshest.deadLetterMode,
    state,
    instancesReporting: list.length,
    assignedPartitions: partitions.size,
    lag: known.length ? known.reduce((a, l) => a + l, 0) : null,
    deadLetters: Math.max(0, ...list.map((i) => i.deadLetters)),
    processedTotal: list.reduce((a, i) => a + i.processedTotal, 0),
    failedTotal: list.reduce((a, i) => a + i.failedTotal, 0),
    eventTypes: [...new Set(list.flatMap((i) => i.eventTypes))].sort(),
    lastError,
    instances: list,
  };
}

/**
 * Merges the /api/v1/processors answers of several hubs (`[{ hubId, data }]`, this hub first):
 * per (service, processor, instance) the freshest report wins and `seenBy` lists the hubs that
 * hold one. Returns `{ services: [{ serviceName, deadLetters, processors }] }`, sorted by name.
 */
export function merge(views) {
  const services = new Map();
  for (const { hubId, data } of views) {
    for (const s of arr(data && data.services)) {
      const serviceName = str(s && s.serviceName);
      let byName = services.get(serviceName);
      if (!byName) services.set(serviceName, byName = new Map());
      for (const p of arr(s.processors)) {
        const name = str(p && p.name);
        if (!name) continue;
        let byInstance = byName.get(name);
        if (!byInstance) byName.set(name, byInstance = new Map());
        for (const raw of arr(p.instances)) {
          const i = instanceOf(raw);
          if (!i) continue;
          const held = byInstance.get(i.instanceId);
          const seenBy = held ? [...held.seenBy, hubId] : [hubId];
          byInstance.set(i.instanceId, { ...(held && fresher(held, i) <= 0 ? held : i), seenBy });
        }
      }
    }
  }
  return {
    services: [...services]
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([serviceName, byName]) => {
        const processors = [...byName]
          .filter(([, byInstance]) => byInstance.size)
          .sort(([a], [b]) => a.localeCompare(b))
          .map(([name, byInstance]) => aggregate(name, [...byInstance.values()]));
        return { serviceName, deadLetters: processors.reduce((a, p) => a + p.deadLetters, 0), processors };
      })
      .filter((s) => s.processors.length),
  };
}

/**
 * The processors of one service from its topology instances (the Services page): how many
 * distinct processors, and the dead letters (per processor the highest count an instance
 * reported, summed).
 */
export function serviceSummary(instances) {
  const byName = new Map();
  for (const i of arr(instances)) {
    for (const p of arr(i && i.processors)) {
      const name = str(p && p.name);
      if (!name) continue;
      byName.set(name, Math.max(byName.get(name) || 0, count(p.deadLetters)));
    }
  }
  return { processors: byName.size, deadLetters: [...byName.values()].reduce((a, n) => a + n, 0) };
}
