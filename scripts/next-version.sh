#!/bin/sh
# The next version from the Conventional Commit subjects since the last release.
#
#   git log --format=%s v0.5.4..HEAD | scripts/next-version.sh v0.5.4
#
# $1 is the last tag (v0.5.4 or 0.5.4; empty means no release yet, 0.0.0). Standard input is one
# subject per line; a line starting "BREAKING CHANGE:" stands for a commit body carrying that footer.
# Prints the next version without the "v", or nothing when no commit is a release reason:
#   feat                       -> next minor
#   fix, perf                  -> next patch
#   anything else              -> no release (docs, test, chore, ci, refactor, build, style, release)
#   "!" or BREAKING CHANGE     -> next major, except below 1.0.0, where it is the next minor:
#                                 1.0.0 is declared by hand, never computed.
set -eu

die() {
  echo "next-version: $*" >&2
  exit 2
}

last=${1:-}
last=${last#v}
[ -n "$last" ] || last=0.0.0
# Strictly N.N.N without leading zeros: a pre-release or 0.08.1 is not one of our versions.
case $last in
  *[!0-9.]* ) die "not a version: '$1'" ;;
esac
grep -Eqx '(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)' <<EOF || die "not a version: '$1'"
$last
EOF
IFS=. read -r major minor patch <<EOF
$last
EOF

subjects=$(cat)
has() {
  grep -Eq "$1" <<EOF
$subjects
EOF
}

scope='(\([^)]*\))?'
if has "^[A-Za-z]+$scope!:|^BREAKING[ -]CHANGE:"; then
  if [ "$major" -eq 0 ]; then bump=minor; else bump=major; fi
elif has "^[Ff][Ee][Aa][Tt]$scope:"; then
  bump=minor
elif has "^([Ff][Ii][Xx]|[Pp][Ee][Rr][Ff])$scope:"; then
  bump=patch
else
  exit 0
fi

case $bump in
  major) next="$((major + 1)).0.0" ;;
  minor) next="$major.$((minor + 1)).0" ;;
  patch) next="$major.$minor.$((patch + 1))" ;;
esac

if [ "$major" -eq 0 ] && [ "${next%%.*}" -ne 0 ]; then
  die "refusing to compute $next from $last: 1.0.0 is declared by hand"
fi
echo "$next"
