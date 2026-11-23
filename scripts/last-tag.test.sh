#!/bin/sh
# Tests for last-tag.sh: sh scripts/last-tag.test.sh
set -u
cd "$(dirname "$0")"
failed=0

# check NAME EXPECTED TAGS...
check() {
  name=$1 want=$2
  shift 2
  got=$(printf '%s\n' "$@" | sh ./last-tag.sh 2>&1) || got="exit $?: $got"
  if [ "$got" = "$want" ]; then
    echo "ok   $name"
  else
    echo "FAIL $name: want '$want', got '$got'"
    failed=1
  fi
}

check "the only tag"                  v0.5.4  v0.5.4
check "the highest, not the last"     v0.10.0 v0.10.0 v0.9.9 v0.2.0
check "numbers, not text"             v0.5.10 v0.5.9 v0.5.10 v0.5.2
check "major first"                   v1.0.0  v0.99.99 v1.0.0
check "pre-releases are ignored"      v0.7.0  v0.7.0 v0.8.0-rc1 v0.8.0-beta.1
check "malformed tags are ignored"    v0.5.4  v0.5.4 v0.6 v0.08.1 v0.6.0.1 vv0.7.0 0.9.0 latest
check "no tags"                       ""
check "no release tag"                ""      v0.8.0-rc1 nightly

exit $failed
