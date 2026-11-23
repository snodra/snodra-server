#!/bin/sh
# Tests for bump-version.sh against copies of this repository's files: sh scripts/bump-version.test.sh
set -u
root=$(cd "$(dirname "$0")/.." && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
failed=0

fail() {
  echo "FAIL $*"
  failed=1
}

mkdir "$work/scripts"
cp "$root/Cargo.toml" "$root/Cargo.lock" "$root/README.md" "$work/"
cp "$root/scripts/bump-version.sh" "$work/scripts/"
cd "$work" || exit 1

if sh scripts/bump-version.sh 9.8.7; then echo "ok   it runs"; else fail "it runs"; fi

pkg=$(awk '/^\[/ { p = ($0 == "[package]") } p && /^version = / { print; exit }' Cargo.toml)
[ "$pkg" = 'version = "9.8.7"' ] && echo "ok   Cargo.toml" || fail "Cargo.toml: $pkg"

lock=$(awk 'prev == "name = \"snodra-server\"" { print; exit } { prev = $0 }' Cargo.lock)
[ "$lock" = 'version = "9.8.7"' ] && echo "ok   Cargo.lock" || fail "Cargo.lock: $lock"

grep -q 'snodra/snodra-server:9\.8\.7' README.md && echo "ok   README.md" || fail "README.md"

# Only those lines change: one in each file.
for f in Cargo.toml Cargo.lock README.md; do
  n=$(diff "$root/$f" "$f" | grep -c '^>')
  [ "$n" -eq 1 ] && echo "ok   one line in $f" || fail "$n lines changed in $f"
done

# A file without the expected line fails the bump.
printf '[package]\nname = "x"\n' >Cargo.toml
if sh scripts/bump-version.sh 9.8.8 2>/dev/null; then fail "a missing version fails"; else echo "ok   a missing version fails"; fi

if sh scripts/bump-version.sh 1.x 2>/dev/null; then fail "a malformed version fails"; else echo "ok   a malformed version fails"; fi

exit $failed
