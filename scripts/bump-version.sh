#!/bin/sh
# Writes a version into every file of this repository that carries it. Run from the repository root:
#
#   scripts/bump-version.sh 0.6.0
#
# Fails, changing nothing further, when a file no longer has the line it expects, so a moved
# version is caught by the tests (scripts/bump-version.test.sh) and not by a release.
set -eu

crate=snodra-server
image=snodra/snodra-server

version=${1:-}
case $version in
  '' | *[!0-9.]* ) echo "usage: $0 X.Y.Z" >&2; exit 2 ;;
esac

# rewrite FILE AWK-PROGRAM: the program sets done=1 when it replaced the version.
rewrite() {
  if awk -v v="$version" -v crate="$crate" -v image="$image" "$2"' END { exit !done }' "$1" >"$1.tmp"; then
    mv "$1.tmp" "$1"
  else
    rm -f "$1.tmp"
    echo "bump-version: no version to replace in $1" >&2
    exit 1
  fi
}

# The [package] version (the binary and the dashboard report it).
rewrite Cargo.toml '
  /^\[/ { pkg = ($0 == "[package]") }
  pkg && !done && /^version = "/ { $0 = "version = \"" v "\""; done = 1 }
  { print }'

# The lock file entry of this crate (the release builds with --locked).
rewrite Cargo.lock '
  prev == "name = \"" crate "\"" && /^version = "/ { $0 = "version = \"" v "\""; done = 1 }
  { print; prev = $0 }'

# The image tag in the README example.
rewrite README.md '
  { if (gsub(image ":[0-9]+\\.[0-9]+\\.[0-9]+", image ":" v)) done = 1; print }'
