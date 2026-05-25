#!/usr/bin/env bash
# Snapshot every `cfg(target_os = ...)` block in the launch-facing
# tree and diff against the checked-in baseline. New blocks force
# review-time recognition so platform-specific code doesn't drift in
# silently, the same review heuristic that caught the /dev/shm
# hardcoding in `wombatkv-bench` on 2026-05-23.
#
# Usage:
#   scripts/cfg_target_os_audit.sh                # check only
#   scripts/cfg_target_os_audit.sh --update       # regenerate baseline
#
# Baseline format (tests/cfg_target_os_baseline.txt), one line per
# block, sorted:
#   <relative-path>:<line> <cfg-content>
#
# Each new entry the PR introduces is a deliberate choice the reviewer
# should confirm has matching test coverage under the same cfg gate.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BASELINE="${REPO_ROOT}/tests/cfg_target_os_baseline.txt"
UPDATE=0
if [ "${1:-}" = "--update" ]; then
  UPDATE=1
fi

ACTUAL="$(mktemp)"
trap 'rm -f "${ACTUAL}"' EXIT

cd "${REPO_ROOT}"
# Scan the launch-facing tree (workspace crates + ds4 adapter). The
# grep extracts file:line + the cfg fragment so adding/moving a block
# is visible in the diff.
grep -rnE 'cfg\([^)]*target_os' \
  --include='*.rs' \
  --exclude-dir=target \
  --exclude-dir=__temp__ \
  --exclude-dir=bench_data \
  crates/ 2>/dev/null \
  | awk -F: '{
      file = $1
      line = $2
      $1 = ""; $2 = ""
      content = $0
      sub(/^[ \t]+/, "", content)
      sub(/^\/\/[ \t]*/, "", content)
      if (match(content, /cfg\([^)]*target_os[^)]*\)/)) {
        cfg_frag = substr(content, RSTART, RLENGTH)
      } else {
        cfg_frag = content
      }
      printf "%s:%s %s\n", file, line, cfg_frag
    }' \
  | sort > "${ACTUAL}"

if [ "${UPDATE}" -eq 1 ]; then
  mkdir -p "$(dirname "${BASELINE}")"
  cp "${ACTUAL}" "${BASELINE}"
  echo "baseline updated: ${BASELINE} ($(wc -l < "${BASELINE}" | tr -d ' ') cfg(target_os) blocks)"
  exit 0
fi

if [ ! -f "${BASELINE}" ]; then
  echo "FAIL: ${BASELINE} not found; run 'scripts/cfg_target_os_audit.sh --update' to seed it"
  exit 1
fi

if diff -u "${BASELINE}" "${ACTUAL}"; then
  echo "OK: cfg(target_os) baseline matches ($(wc -l < "${BASELINE}" | tr -d ' ') blocks)"
  exit 0
else
  echo ""
  echo "FAIL: cfg(target_os) baseline drift (diff above)."
  echo ""
  echo "  Every cfg(target_os) block in the diff above represents new"
  echo "  platform-specific code. Before accepting, confirm:"
  echo "    1. There is a matching test under the same cfg gate, OR"
  echo "    2. The platform-specific behavior is verified via DST"
  echo "       (e.g. PlatformShmHidden / PlatformIoUringJitter /"
  echo "       PlatformKqueueOnly fault classes)."
  echo "  Then run 'scripts/cfg_target_os_audit.sh --update' and commit"
  echo "  the baseline so reviewers see the explicit jump."
  exit 1
fi
