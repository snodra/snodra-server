#!/usr/bin/env bash
# Fails when the vendored hub.proto drifted from the umbrella's kraken/protocol/hub.proto (the source of truth).
#
# The umbrella copy is $KRAKEN_PROTO_SOURCE, else ../kraken/protocol/hub.proto next to this repo. When it does not
# exist the check is skipped (exit 0), unless KRAKEN_PROTO_SYNC_REQUIRED=true (CI jobs that clone the umbrella).
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
copy="$root/proto/hub.proto"
source="${KRAKEN_PROTO_SOURCE:-$root/../kraken/protocol/hub.proto}"

sha256() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | cut -d' ' -f1; else shasum -a 256 "$1" | cut -d' ' -f1; fi
}

if [ ! -f "$source" ]; then
  if [ "${KRAKEN_PROTO_SYNC_REQUIRED:-false}" = "true" ]; then
    echo "proto sync: umbrella proto not found at $source" >&2
    exit 1
  fi
  echo "proto sync: umbrella proto not found at $source, skipping"
  exit 0
fi

want="$(sha256 "$source")"
have="$(sha256 "$copy")"
if [ "$want" != "$have" ]; then
  echo "proto sync: proto/hub.proto (sha256 $have) drifted from $source (sha256 $want)" >&2
  echo "proto sync: run: cp $source $copy" >&2
  exit 1
fi
echo "proto sync: proto/hub.proto matches $source (sha256 $want)"
