#!/usr/bin/env bash
# dst-sweep.sh: DST Stage 5: deterministic sweep over seeds × classes.
#
# For each (seed, class) tuple, invokes `wombatkv-dst-runner` to derive
# a fault plan and writes it to a per-tuple JSON file. Reports the
# aggregate event count + per-class plan-event distribution so the
# operator can see which scenarios got the heavier mixes.
#
# This is the planning half of DST. Stage 3.5 (next session) will
# extend the runner to actually spawn a child puffer with the plan
# loaded, drive a sequence of KV ops, compare against the
# WombatKvOracle, and emit a pass/fail report. At that point this
# script becomes the entry point for "find a bug overnight":
#
#   ./dst-sweep.sh --seeds 1-1000 --out /tmp/dst-run-2026-05-18/
#
# CI integration (Phase 5 P10 follow-up) replaces the local out-dir
# with the GitHub Actions cache + sets `--seeds $(date +%s%N)` so
# every CI run picks a fresh random base.
#
# Usage:
#   ./dst-sweep.sh                           # default 1..10 seeds, all classes
#   ./dst-sweep.sh --seeds 50-100            # custom range (inclusive)
#   ./dst-sweep.sh --out /path/to/plans      # custom output dir
#   ./dst-sweep.sh --classes "transient-s3 corrupt-block"   # subset
#
# Exit code: 0 on success, 1 on first failure (plans that fail to
# write surface immediately).

set -eu -o pipefail

# ----- arg parsing -----
SEEDS="1-10"
OUT_DIR="/tmp/wombatkv-dst-sweep"
CLASSES="transient-s3 corrupt-block partial-chain concurrent-save daemon-restart sidecar-drift foyer-eviction transport-drop transport-partial-read transport-slow-write wire-envelope-corruption old-sidecar-v3 old-block-v1 slate-db-write-failure slate-db-manifest-corruption multi-engine-prefix-conflict resource-fd-exhaustion resource-memory-pressure resource-disk-full platform-shm-hidden platform-io-uring-jitter platform-kqueue-only"
RUNNER_BIN=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --seeds)    SEEDS="$2"; shift 2;;
        --out)      OUT_DIR="$2"; shift 2;;
        --classes)  CLASSES="$2"; shift 2;;
        --runner)   RUNNER_BIN="$2"; shift 2;;
        -h|--help)
            sed -n '2,30p' "$0"
            exit 0
            ;;
        *)
            echo "unknown arg: $1" >&2
            exit 2
            ;;
    esac
done

# ----- locate the runner binary -----
if [[ -z "${RUNNER_BIN}" ]]; then
    # Prefer release build if present, otherwise debug.
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
    if [[ -x "${REPO_ROOT}/target/release/wombatkv-dst-runner" ]]; then
        RUNNER_BIN="${REPO_ROOT}/target/release/wombatkv-dst-runner"
    elif [[ -x "${REPO_ROOT}/target/debug/wombatkv-dst-runner" ]]; then
        RUNNER_BIN="${REPO_ROOT}/target/debug/wombatkv-dst-runner"
    else
        echo "FATAL: wombatkv-dst-runner not built. Run:" >&2
        echo "  cargo build -p wombatkv-dst --bin wombatkv-dst-runner" >&2
        exit 1
    fi
fi

# ----- expand seed range -----
if [[ "${SEEDS}" =~ ^([0-9]+)-([0-9]+)$ ]]; then
    SEED_LO="${BASH_REMATCH[1]}"
    SEED_HI="${BASH_REMATCH[2]}"
else
    echo "FATAL: --seeds must be in 'LO-HI' form, got '${SEEDS}'" >&2
    exit 2
fi
if (( SEED_HI < SEED_LO )); then
    echo "FATAL: seed range HI (${SEED_HI}) < LO (${SEED_LO})" >&2
    exit 2
fi

# ----- run the sweep -----
mkdir -p "${OUT_DIR}"
summary_file="${OUT_DIR}/SWEEP_SUMMARY.txt"
: > "${summary_file}"

n_total=0
n_ok=0
n_fail=0
declare -A events_per_class
for class in ${CLASSES}; do
    events_per_class[${class}]=0
done

start_time=$(date +%s)
echo "dst-sweep: seeds=${SEED_LO}..${SEED_HI} classes='${CLASSES}' out=${OUT_DIR}" \
    | tee -a "${summary_file}"

for seed in $(seq "${SEED_LO}" "${SEED_HI}"); do
    for class in ${CLASSES}; do
        n_total=$((n_total + 1))
        plan_file="${OUT_DIR}/${seed}-${class}.json"
        if out=$("${RUNNER_BIN}" --seed "${seed}" --class "${class}" --plan-file "${plan_file}" 2>&1); then
            n_ok=$((n_ok + 1))
            # Parse `events=N` token out of the runner's summary line.
            events=$(echo "${out}" | grep -oE 'events=[0-9]+' | head -1 | cut -d= -f2)
            if [[ -n "${events}" ]]; then
                events_per_class[${class}]=$(( events_per_class[${class}] + events ))
            fi
        else
            n_fail=$((n_fail + 1))
            echo "FAIL seed=${seed} class=${class}: ${out}" | tee -a "${summary_file}"
        fi
    done
done
end_time=$(date +%s)
elapsed=$((end_time - start_time))

echo "" | tee -a "${summary_file}"
echo "===== dst-sweep summary =====" | tee -a "${summary_file}"
echo "total runs:  ${n_total}" | tee -a "${summary_file}"
echo "ok:          ${n_ok}" | tee -a "${summary_file}"
echo "failed:      ${n_fail}" | tee -a "${summary_file}"
echo "elapsed:     ${elapsed}s" | tee -a "${summary_file}"
echo "" | tee -a "${summary_file}"
echo "events generated per class:" | tee -a "${summary_file}"
for class in ${CLASSES}; do
    echo "  ${class}: ${events_per_class[${class}]}" | tee -a "${summary_file}"
done

if (( n_fail > 0 )); then
    exit 1
fi
