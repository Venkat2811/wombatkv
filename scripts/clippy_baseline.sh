#!/usr/bin/env bash
# Diff the current clippy warning surface against the checked-in
# baseline so toolchain drift (rustc/clippy bumping pedantic lints
# from warn to deny, or introducing entirely new lints) is visible at
# PR time rather than silently breaking CI after an unrelated change.
#
# Usage:
#   scripts/clippy_baseline.sh                       # check only
#   scripts/clippy_baseline.sh --update              # regenerate the baseline
#
# Output format (tests/clippy_baseline.txt), one line per lint, sorted:
#   clippy::lint_name <count>
#
# The check FAILS when the baseline diff is non-empty. Resolve by
# either fixing the new warnings or running `--update` after review
# (commit the baseline change so the diff is auditable in git).
#
# Caveat: counts can differ between Mac and Linux for `#[cfg(target_os)]`
# code paths. The committed baseline is captured on macOS (the primary
# dev machine). Linux CI runs `make ci` (the tight gate); the baseline
# is a developer-side drift detector, not a CI hard gate.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BASELINE="${REPO_ROOT}/tests/clippy_baseline.txt"
UPDATE=0
if [ "${1:-}" = "--update" ]; then
  UPDATE=1
fi

ACTUAL="$(mktemp)"
trap 'rm -f "${ACTUAL}"' EXIT

# Use the same pedantic surface workspace.lints.clippy enables. The
# Makefile `lint` target is the tight gate (-D correctness suspicious
# perf); this baseline tracks the WIDER pedantic surface to catch
# stylistic drift.
#
# Extraction uses `--message-format=json` + `jq` to pull the canonical
# lint code from each compiler message, stable across runs vs.
# greedy-regex over human-readable text (which counted group names
# AND was sensitive to cargo build-cache state).
cd "${REPO_ROOT}"
ulimit -n 65536 2>/dev/null || true
cargo clippy --workspace --all-targets --release --message-format=json \
  -- -W clippy::pedantic 2>/dev/null \
  | jq -r 'select(.reason == "compiler-message") | .message.code.code // empty' \
  | grep '^clippy::' \
  | sort \
  | uniq -c \
  | awk '{printf "%s %d\n", $2, $1}' \
  | sort > "${ACTUAL}"

if [ "${UPDATE}" -eq 1 ]; then
  mkdir -p "$(dirname "${BASELINE}")"
  cp "${ACTUAL}" "${BASELINE}"
  echo "baseline updated: ${BASELINE} ($(wc -l < "${BASELINE}" | tr -d ' ') lint kinds)"
  exit 0
fi

if [ ! -f "${BASELINE}" ]; then
  echo "FAIL: ${BASELINE} not found; run 'scripts/clippy_baseline.sh --update' to seed it"
  exit 1
fi

if diff -u "${BASELINE}" "${ACTUAL}"; then
  echo "OK: clippy baseline matches ($(wc -l < "${BASELINE}" | tr -d ' ') lint kinds)"
  exit 0
else
  echo ""
  echo "FAIL: clippy warning surface drift (diff above)."
  echo "  - If the new warnings are real, fix them and re-run this script."
  echo "  - If the diff is from a deliberate rustc/clippy bump that you intend"
  echo "    to absorb, run 'scripts/clippy_baseline.sh --update' and commit"
  echo "    the baseline change so the next reviewer sees the explicit jump."
  exit 1
fi
