// Multi-hub view (ADR 0016). Hubs never talk to each other: the page reads every peer listed in
// KRAKEN_HUB_PEERS itself (CORS) and merges their topologies here, keyed by instance id, flagging
// where the reachable hubs disagree. An unreachable peer is shown as unreachable, never as a
// disagreement. Everything a peer sends is data: it is checked for shape and rendered as text.

import { getFrom } from './api.js';

/** Clients report InstanceStatus every 5 s; a report older than this is flagged. */
export const STATUS_STALE_MS = 15000;

/** KRAKEN_HUB_PEERS allows at most this many. */
const MAX_PEERS = 16;

let own = null;
let origins = [];
/** One entry per peer URL: { origin, state, hubId, error }; state is unknown | self |
 * reachable | unreachable | duplicate (another URL of a hub already listed). */
let peers = [];
const listeners = new Set();

const arr = (x) => (Array.isArray(x) ? x : []);
const str = (x) => (typeof x === 'string' ? x : '');

function notify() {
  const view = snapshot();
  for (const fn of listeners) fn(view);
}

/** Takes this hub's id and peer list from /api/v1/version. */
export function configure(version) {
  own = str(version.hubId);
  const list = arr(version.peers).filter((p) => typeof p === 'string').slice(0, MAX_PEERS);
  if (list.join(',') !== origins.join(',')) {
    origins = list;
    peers = list.map((origin) => ({ origin, state: 'unknown', hubId: null, error: null }));
  }
  notify();
}

/** True when this hub lists peers; without them the page shows this hub only, as before. */
export function enabled() {
  return origins.length > 0;
}

export function ownHubId() {
  return own;
}

/** Calls `fn(peers)` whenever a peer's state changes. */
export function onChange(fn) {
  listeners.add(fn);
}

export function snapshot() {
  return peers.map((p) => ({ ...p }));
}

function looksLikeTopology(t) {
  return t && typeof t === 'object' && typeof t.hubId === 'string' && t.hubId !== ''
    && Array.isArray(t.services) && Array.isArray(t.types);
}

/**
 * Reads /api/v1/<name> from every peer and updates each peer's state. Returns `views`, one
 * `{ hubId, data }` per reachable peer other than this hub (each answer must pass `valid` and
 * name its hub in `hubId`).
 */
async function fetchPeers(name, valid) {
  const results = await Promise.all(peers.map(async (peer) => {
    try {
      return { peer, data: await getFrom(peer.origin, name) };
    } catch (error) {
      return { peer, error };
    }
  }));
  const seen = new Set([own]);
  const views = [];
  for (const { peer, data, error } of results) {
    if (error || !valid(data)) {
      peer.state = 'unreachable';
      peer.error = error ? error.message : `the answer is not a hub's ${name}`;
      continue;
    }
    peer.hubId = data.hubId;
    peer.error = null;
    if (data.hubId === own) {
      peer.state = 'self';
    } else if (seen.has(data.hubId)) {
      peer.state = 'duplicate';
    } else {
      seen.add(data.hubId);
      peer.state = 'reachable';
      views.push({ hubId: data.hubId, data });
    }
  }
  notify();
  return { views };
}

/** Refreshes the peers' states (for the header) without merging. */
export async function check() {
  if (enabled()) await fetchPeers('topology', looksLikeTopology);
}

/** This hub's topology merged with every reachable peer's. */
export async function merged(ownTopology) {
  const { views } = await fetchPeers('topology', looksLikeTopology);
  return merge(ownTopology, views.map(({ hubId, data }) => ({ hubId, topology: data })), snapshot());
}

/**
 * /api/v1/<name> of every reachable peer other than this hub, for a page that merges a view
 * other than the topology (the Processors page, ADR 0022): `{ views: [{ hubId, data }] }`.
 */
export async function collect(name, valid) {
  if (!enabled()) return { views: [] };
  return fetchPeers(name, valid);
}

const typesKey = (i) => JSON.stringify([[...arr(i.commandTypes)].sort(), [...arr(i.queryTypes)].sort()]);

/**
 * The merged view: services with their instances (each with `seenBy`, the hubs that list it,
 * `viewHubId`, the hub whose copy is shown, and `flags`), totals, and the disagreements:
 * - missing: an instance listed by some reachable hubs and not by others;
 * - types: an instance whose registered command or query types differ between hubs;
 * - owner: a type owned by different services on different hubs;
 * - connected: an instance whose reported connected hub ids omit a reachable hub.
 */
export function merge(ownTopology, remote, peerStates) {
  const views = [{ hubId: ownTopology.hubId, topology: ownTopology }, ...remote];
  const hubIds = views.map((v) => v.hubId);
  const instances = new Map();
  const serviceNames = new Set();
  const typeNames = new Set();
  const owners = new Map();
  for (const v of views) {
    for (const s of arr(v.topology.services)) {
      const service = str(s.serviceName);
      serviceNames.add(service);
      for (const i of arr(s.instances)) {
        if (!i || typeof i.instanceId !== 'string') continue;
        let entry = instances.get(i.instanceId);
        if (!entry) instances.set(i.instanceId, entry = { id: i.instanceId, byHub: new Map() });
        entry.byHub.set(v.hubId, { service, instance: i });
      }
    }
    for (const t of arr(v.topology.types)) {
      const type = str(t?.type);
      typeNames.add(type);
      let byOwner = owners.get(type);
      if (!byOwner) owners.set(type, byOwner = new Map());
      // A shared query type (ADR 0017) is owned by the whole set.
      const owner = arr(t.ownerServices).map(str).join(' + ');
      if (!byOwner.has(owner)) byOwner.set(owner, []);
      byOwner.get(owner).push(v.hubId);
    }
  }

  const disagreements = [];
  const byService = new Map();
  const ids = [...instances.keys()].sort();
  for (const id of ids) {
    const entry = instances.get(id);
    const seenBy = hubIds.filter((hub) => entry.byHub.has(hub));
    const flags = [];
    const missing = hubIds.filter((hub) => !entry.byHub.has(hub));
    if (missing.length) flags.push({ kind: 'missing', text: `not registered on ${missing.join(', ')}` });
    const signatures = new Map();
    for (const [hub, { instance }] of entry.byHub) {
      const key = typesKey(instance);
      if (!signatures.has(key)) signatures.set(key, []);
      signatures.get(key).push(hub);
    }
    if (signatures.size > 1) {
      flags.push({ kind: 'types', text: `registers different types on ${[...signatures.values()].map((hs) => hs.join(' and ')).join(' than on ')}` });
    }
    const notConnected = new Set();
    for (const { instance } of entry.byHub.values()) {
      if (!(instance.statusAgeMs >= 0)) continue;
      const reported = arr(instance.connectedHubIds);
      for (const hub of hubIds) if (!reported.includes(hub)) notConnected.add(hub);
    }
    if (notConnected.size) flags.push({ kind: 'connected', text: `reports no connection to ${[...notConnected].join(', ')}` });

    const viewHubId = entry.byHub.has(hubIds[0]) ? hubIds[0] : seenBy[0];
    const { service, instance } = entry.byHub.get(viewHubId);
    for (const flag of flags) disagreements.push({ ...flag, subject: `instance ${id} (${service})` });
    if (!byService.has(service)) byService.set(service, []);
    byService.get(service).push({ ...instance, seenBy, viewHubId, flags });
  }
  for (const [type, byOwner] of [...owners].sort(([a], [b]) => a.localeCompare(b))) {
    if (byOwner.size < 2) continue;
    disagreements.push({
      kind: 'owner',
      subject: `type ${type}`,
      text: `owned by ${[...byOwner].map(([owner, hubs]) => `${owner} on ${hubs.join(', ')}`).join('; ')}`,
    });
  }

  return {
    scope: 'all',
    hubId: ownTopology.hubId,
    nowMs: ownTopology.nowMs,
    hubs: hubIds,
    peers: peerStates,
    services: [...byService].sort(([a], [b]) => a.localeCompare(b))
      .map(([serviceName, list]) => ({ serviceName, instances: list })),
    totals: {
      hubs: hubIds.length,
      unreachable: peerStates.filter((p) => p.state === 'unreachable').length,
      services: serviceNames.size,
      instances: instances.size,
      types: typeNames.size,
    },
    disagreements,
  };
}

/** Labels of the disagreement kinds. */
export const KIND_LABELS = {
  missing: 'Missing on a hub',
  types: 'Types differ',
  owner: 'Owner differs',
  connected: 'Hub not connected',
};
