#!/bin/sh
# Tests for next-version.sh: sh scripts/next-version.test.sh
set -u
cd "$(dirname "$0")"
failed=0

# check NAME LAST-TAG EXPECTED SUBJECTS...
check() {
  name=$1 tag=$2 want=$3
  shift 3
  got=$(printf '%s\n' "$@" | sh ./next-version.sh "$tag" 2>&1) || got="exit $?: $got"
  if [ "$got" = "$want" ]; then
    echo "ok   $name"
  else
    echo "FAIL $name: want '$want', got '$got'"
    failed=1
  fi
}

check "feat is a minor"              v0.5.4 0.6.0  "fix: a" "feat: b" "docs: c"
check "two feats are one minor"      v0.5.4 0.6.0  "feat: a" "feat: b"
check "a scoped feat"                v0.5.4 0.6.0  "feat(cli): a"
check "fix is a patch"               v0.5.4 0.5.5  "fix: a" "chore: b"
check "a scoped fix"                 v0.5.4 0.5.5  "fix(release): a"
check "perf is a patch"              v0.5.4 0.5.5  "perf: a"
check "the type is case-blind"       v0.5.4 0.6.0  "Feat: a"
check "chores only: no release"      v0.5.4 ""     "docs: a" "test: b" "chore: c" "ci: d" "refactor: e" "build: f" "style: g"
check "release commits: no release"  v0.5.4 ""     "release: 0.5.4"
check "no commits: no release"       v0.5.4 ""
check "not a conventional subject"   v0.5.4 ""     "Merge branch 'x'" "fixes the thing" "feature: x"
check "a type must open the subject" v0.5.4 ""     "docs: the feat: prefix"
check "! below 1.0 is a minor"       v0.5.4 0.6.0  "refactor!: a"
check "scoped ! below 1.0 is a minor" v0.5.4 0.6.0 "fix(api)!: a"
check "BREAKING CHANGE below 1.0"    v0.5.4 0.6.0  "chore: a" "BREAKING CHANGE: b"
check "BREAKING-CHANGE below 1.0"    v0.9.9 0.10.0 "BREAKING-CHANGE: b"
check "the tag without a v"          0.5.4  0.5.5  "fix: a"
check "no tag yet"                   ""     0.1.0  "feat: a"
check "no tag yet, a fix"            ""     0.0.1  "fix: a"
check "after 1.0, ! is a major"      v1.2.3 2.0.0  "feat!: a"
check "after 1.0, feat is a minor"   v1.2.3 1.3.0  "feat: a"
check "a malformed tag fails"        v0.5   "exit 2: next-version: not a version: 'v0.5'" "feat: a"
check "a tag with a suffix fails"    v0.5.4-rc1 "exit 2: next-version: not a version: 'v0.5.4-rc1'" "feat: a"
check "a fourth number fails"        v0.5.4.1 "exit 2: next-version: not a version: 'v0.5.4.1'" "feat: a"
check "a leading zero fails"         v0.08.9 "exit 2: next-version: not a version: 'v0.08.9'" "feat: a"
check "a leading zero in major fails" v00.1.0 "exit 2: next-version: not a version: 'v00.1.0'" "fix: a"
check "an empty part fails"          v0..1  "exit 2: next-version: not a version: 'v0..1'" "fix: a"
check "a double v fails"             vv0.5.4 "exit 2: next-version: not a version: 'vv0.5.4'" "fix: a"
check "zeros are numbers"            v0.10.0 0.11.0 "feat: a"
check "a revert is not a reason"     v0.5.4 ""     'Revert "feat: a"' "revert: feat: a"
check "any word with ! is breaking"  v0.5.4 0.6.0  "wip!: a"

exit $failed
