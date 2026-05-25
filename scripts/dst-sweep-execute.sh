#!/usr/bin/env bash
# dst-sweep-execute.sh: DST Stage 3.5 sweep: REAL daemon + plan execution.
#
# Successor to dst-sweep.sh, which only generated plans. This script
# drives end-to-end seeded scenarios via the parent runner's
# --spawn-child orchestration (BTreeMap stand-in for fast classes) OR
# the wombatkv-dst-driver bin (real daemon + real WombatKVKvStore for
# put-side fault classes that need the production-code dst_plan path).
#
# Usage:
#   ./dst-sweep-execute.sh                   # 50 seeds × 16 classes via --spawn-child
#   ./dst-sweep-execute.sh --real            # use wombatkv-dst-driver (real daemon)
#   ./dst-sweep-execute.sh --seeds 1-200     # custom seed range
#   ./dst-sweep-execute.sh --classes "transient-s3 corrupt-block"
#
# Pre-reqs for --real:
#   - native MinIO on 127.0.0.1:9200 with bucket wombatkv-dst-sweep
#   - wombatkv-daemon built with --features dst
#
# Output: per-(seed, class) report JSON under /tmp/dst-sweep-execute/
# plus a one-line aggregate summary at the end.

set -uo pipefail

SEEDS="${SEEDS:-1-50}"
OUT_DIR="${OUT_DIR:-/tmp/dst-sweep-execute}"
MODE="${MODE:-spawn-child}"  # "spawn-child" (default, fast) or "real" (slow, full)
CLASSES="${CLASSES:-transient-s3 corrupt-block partial-chain concurrent-save daemon-restart sidecar-drift foyer-eviction transport-drop transport-partial-read transport-slow-write wire-envelope-corruption old-sidecar-v3 old-block-v1 slate-db-write-failure slate-db-manifest-corruption multi-engine-prefix-conflict}"
RUNNER_BIN=""
DRIVER_BIN=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --seeds)    SEEDS="$2"; shift 2;;
        --out)      OUT_DIR="$2"; shift 2;;
        --classes)  CLASSES="$2"; shift 2;;
        --real)     MODE="real"; shift;;
        --runner)   RUNNER_BIN="$2"; shift 2;;
        --driver)   DRIVER_BIN="$2"; shift 2;;
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

if [[ -z "$RUNNER_BIN" ]]; then
    RUNNER_BIN="$(pwd)/target/release/wombatkv-dst-runner"
fi
if [[ -z "$DRIVER_BIN" ]]; then
    DRIVER_BIN="$(pwd)/target/release/wombatkv-dst-driver"
fi
DAEMON_BIN="${DAEMON_BIN:-$(pwd)/target/release/wombatkv-daemon}"

case "$MODE" in
    spawn-child)
        if [[ ! -x "$RUNNER_BIN" ]]; then
            echo "spawn-child mode needs $RUNNER_BIN, build with: cargo build --release -p wombatkv-dst" >&2
            exit 2
        fi
        ;;
    real)
        if [[ ! -x "$DRIVER_BIN" ]]; then
            echo "real mode needs $DRIVER_BIN, build with: cargo build --release -p wombatkv-bench --bin wombatkv-dst-driver --features dst" >&2
            exit 2
        fi
        ;;
    *)
        echo "unknown mode: $MODE (use 'spawn-child' or 'real')" >&2
        exit 2
        ;;
esac

mkdir -p "$OUT_DIR"

# Parse seed range "A-B" or "A".
if [[ "$SEEDS" =~ ^([0-9]+)-([0-9]+)$ ]]; then
    SEED_LO="${BASH_REMATCH[1]}"
    SEED_HI="${BASH_REMATCH[2]}"
else
    SEED_LO="$SEEDS"
    SEED_HI="$SEEDS"
fi

echo "dst-sweep-execute: mode=$MODE seeds=$SEED_LO..$SEED_HI classes=\"$CLASSES\" out=$OUT_DIR"
START_TS=$(date +%s)

TOTAL=0
PASS=0
FAIL=0
DIVERGENT_REPORT=""

for seed in $(seq "$SEED_LO" "$SEED_HI"); do
    for class in $CLASSES; do
        TOTAL=$((TOTAL + 1))
        PLAN="$OUT_DIR/${seed}-${class}.plan.json"
        REPORT="$OUT_DIR/${seed}-${class}.report.json"

        case "$MODE" in
            spawn-child)
                if "$RUNNER_BIN" --seed "$seed" --class "$class" \
                        --plan-file "$PLAN" --spawn-child \
                        --child-report-file "$REPORT" \
                        > /dev/null 2>&1; then
                    PASS=$((PASS + 1))
                else
                    FAIL=$((FAIL + 1))
                    DIVERGENT_REPORT+="  seed=$seed class=$class report=$REPORT"$'\n'
                fi
                ;;
            real)
                # Use a per-run daemon prefix so concurrent runs don't
                # collide on SHM names. Truncated seed-class hash keeps
                # the prefix under the macOS 14-char budget.
                PREFIX="d$(printf '%s' "${seed}${class}" | shasum -a 1 | cut -c1-8)"
                if "$DRIVER_BIN" --seed "$seed" --class "$class" \
                        --plan-file "$PLAN" \
                        --daemon-bin "$DAEMON_BIN" \
                        --daemon-prefix "$PREFIX" \
                        > "$REPORT" 2>&1; then
                    PASS=$((PASS + 1))
                else
                    FAIL=$((FAIL + 1))
                    DIVERGENT_REPORT+="  seed=$seed class=$class report=$REPORT"$'\n'
                fi
                ;;
        esac
    done
done

END_TS=$(date +%s)
ELAPSED=$((END_TS - START_TS))
echo
echo "===== dst-sweep-execute summary ====="
echo "mode:        $MODE"
echo "total runs:  $TOTAL"
echo "pass:        $PASS"
echo "fail:        $FAIL"
echo "elapsed:     ${ELAPSED}s"
if [[ -n "$DIVERGENT_REPORT" ]]; then
    echo
    echo "FAILURES (first 20):"
    echo "$DIVERGENT_REPORT" | head -20
fi

# Exit code reflects sweep verdict.
if [[ "$FAIL" -gt 0 ]]; then
    exit 1
fi
exit 0
