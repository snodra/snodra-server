#!/bin/sh
# The highest release tag among the tag names on standard input, or nothing.
#
#   git tag --merged HEAD --list 'v*' | scripts/last-tag.sh
#
# A release tag is strictly vN.N.N without leading zeros; anything else (v0.8.0-rc1, v1.2, v0.08.1)
# is ignored. "Highest" is by version, not by date or by distance from HEAD.
set -eu

grep -Ex 'v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)' |
  sed 's/^v//' |
  sort -t. -k1,1n -k2,2n -k3,3n |
  sed -n '$s/^/v/p'
