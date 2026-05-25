# Benchmarks

Detailed cross-campaign benchmark notes for WombatKV 0.1.0-alpha.

The top-level [`README.md`](./README.md) carries the slim highlights.
This document carries the depth: per-campaign methodology, full
per-row matrices, cross-campaign reproducibility, and the honest gaps.

## Three campaigns

| Directory | When | What it measures | Trials |
|---|---|---|---|
| [`artifacts/2026-05_alpha-dev-runs/`](./artifacts/2026-05_alpha-dev-runs/) | May 13-22, multi-date | Alpha development integration runs, ds4 bench harness | n=5 to n=25 depending on row |
| [`artifacts/2026-05-24_deployment-mode-matrix/`](./artifacts/2026-05-24_deployment-mode-matrix/) | 2026-05-24, single day | 6 modes × 3 restart policies, methodical sweep + partial-prefix + scenarios | n=5 (canonical exact), n=2 (partial-prefix, scenarios), n=8 (ShareGPT) |
| [`artifacts/2026-05-24_public-corpus-replay/`](./artifacts/2026-05-24_public-corpus-replay/) | 2026-05-24 evening | ShareGPT round-robin + Gutenberg multi-round, per-restore + per-save stage timings, raw TCP transport | n=1 (workloads), n=8 (transport) |

**No mixing.** Every speedup is reported with the campaign's
methodology + caveats. Cross-campaign reproducibility is discussed
in §3 below.

## 1. Where WombatKV wins

### 1.1 Exact restart with local state wiped (canonical)

The headline regime. Process restarts, kvdisk wiped, prompt sent again.
ds4 must re-prefill from token 0; WombatKV restores blocks from S3
and skips ~all of the prefill cost.

| Source | Mode | Native TTFT | Wombat TTFT | Speedup | n |
|---|---|---:|---:|---:|---:|
| 2026-05-24 deployment-mode-matrix | embedded_local | 6987 ms | 78 ms | **89.7×** | 5 |
| 2026-05-24 deployment-mode-matrix | embedded_remote (LAN MinIO) | 6987 ms | 82 ms | **85.1×** | 5 |
| 2026-05-24 deployment-mode-matrix | daemon_shm | 6987 ms | 192 ms | 36.4× | 5 |
| 2026-05-24 deployment-mode-matrix | daemon_tcp_local | 6987 ms | 149 ms | 46.9× | 5 |
| 2026-05-24 deployment-mode-matrix | daemon_http_local | 6987 ms | 203 ms | 34.4× | 5 |
| 2026-05-17 alpha-dev-runs (1.7k tok) | embedded | 7929 ms | 108 ms | **73.1×** | 5 |
| 2026-05-18 alpha-dev-runs (1.3k tok, tuned) | embedded | 7868 ms | 75 ms | **104.8×** | 5 |
| 2026-05-22 alpha-dev-runs (1.3k tok, native MinIO) | embedded-tuned | 8045 ms | 82 ms | **97.7×** | 5 |

Two campaigns, two harnesses, three different prompt sizes → the
70–105× regime reproduces. The differences come from prompt corpus +
tuning + storage path, not from methodology divergence.

### 1.2 Public-corpus generalization

8 distinct ShareGPT prompts (each used once), same restart-wiped pattern:

| Source | Mode | Native TTFT | Wombat TTFT | Speedup | n |
|---|---|---:|---:|---:|---:|
| 2026-05-24 deployment-mode-matrix | embedded_local | 5330 ms | 78 ms | **68.2×** | 8 |
| 2026-05-24 deployment-mode-matrix | embedded_remote | 5330 ms | 95 ms | **56.2×** | 8 |

The win is not a single-prompt artifact.

### 1.3 Partial-prefix reuse (the strongest non-restart claim)

Shared 10000-char prefix, different suffix sizes. WombatKV beats native
ds4 **even when ds4's kvdisk is preserved**:

| Restart | Suffix | Native TTFT | Wombat TTFT | Speedup | n |
|---|---:|---:|---:|---:|---:|
| preserved | 256 | 5651 ms | 1813 ms | **3.12×** | 2 |
| preserved | 2048 | 8511 ms | 3475 ms | **2.45×** | 2 |
| preserved | 8192 | 63158 ms | 14197 ms | **4.45×** | 2 |
| wiped | 256 | 15024 ms | 1993 ms | **7.54×** | 2 |
| wiped | 2048 | 19841 ms | 6098 ms | **3.25×** | 2 |
| wiped | 8192 | 66610 ms | 15045 ms | **4.43×** | 2 |

Source: 2026-05-24 deployment-mode-matrix, expanded sweep.
embedded_local headline rows shown; daemon modes also tested
(daemon_tcp_local wiped suffix=256 → 2.48× WIN, daemon_shm
wiped suffix=256 → 1.99× WIN, daemon_shm preserved → 0.40× LOSS).
Full 30-row table in `partial_prefix_sweep.csv`.

### 1.4 Real multi-turn long-document QA

Gutenberg multi-round (~12k char first turn, 3 turns per conversation,
6 conversations):

| Source | Mode | Native TTFT | Wombat TTFT | Speedup | turn1 speedup |
|---|---|---:|---:|---:|---:|
| 2026-05-24 public-corpus-replay | embedded_local | 40842 ms | 29426 ms | **1.39×** | **2.04× on turn-1** |

First real-workload WIN that isn't exact-restart. embedded mode
amortizes the long shared document across the conversation's turns.

### 1.5 Cross-conversation prefix-share (alpha-dev period)

5 conversations × 5 turns × ~9.7k-token shared doc, restart between
every turn:

| Source | Mode | Native TTFT | Wombat TTFT | Speedup | n |
|---|---|---:|---:|---:|---:|
| 2026-05-17 alpha-dev-runs | embedded | 110 535 ms | 1883 ms | **58.7×** | 25 turns |

Per-turn breakdown lives in `2026-05_alpha-dev-runs/multi_conv_5x5.csv`.

## 2. Where WombatKV does less

### 2.1 Same-host live-churn (the strongest LOSS regimes)

WombatKV is the wrong tool when ds4's own state machine is doing
what it should:

| Scenario | Mode | Native | Wombat | Speedup | n |
|---|---|---:|---:|---:|---:|
| Same machine, ds4 kvdisk preserved, exact same prompt | embedded_local | 24 ms | 80 ms | **0.30× LOSS** | 5 |
| Same process, no restart | embedded_local | 81 ms | 328 ms | **0.25× LOSS** | 3 |
| Same process, no restart | daemon_shm | 81 ms | 5932 ms | **0.014× LOSS** | 3 |
| Same process, no restart | daemon_tcp_local | 81 ms | 5742 ms | **0.014× LOSS** | 3 |
| Same process, no restart | daemon_http_local | 81 ms | 6168 ms | **0.013× LOSS** | 3 |

Source: 2026-05-24 deployment-mode-matrix. Daemon catastrophic when
there is nothing for the daemon to do.

### 2.2 Multi-agent interleaved single-server

pi_review (5 concurrent agents on one ds4-server):

| Variant | Mode | Native | Wombat | Speedup |
|---|---|---:|---:|---:|
| restart_preserved | c2_embedded | 23932 ms | 36671 ms | **0.65× LOSS** |
| restart_preserved | c3_daemon | 23932 ms | 107379 ms | **0.22× LOSS** |
| restart_wiped | c2_embedded | 23239 ms | 32173 ms | **0.72× LOSS** |
| restart_wiped | c3_daemon | 23239 ms | 81939 ms | **0.28× LOSS** |

Source: 2026-05-24 deployment-mode-matrix, n=2 each. The alpha-dev-runs
`headlines.jsonl` row claiming "pi-review xrestart 110×" describes a
**different** workload (prepopulated bucket xrestart variant from
2026-05-16 showcase). The as-shipped `scripts/scenarios/pi_review.py`
returns the LOSS numbers above.

### 2.3 Conversation switch round-robin

5 users × 5 turns, round-robin on one server:

| Mode | Switch TTFT p50 | Speedup |
|---|---:|---:|
| c1_native | 2825 ms | 1.00 (baseline) |
| c2_embedded | 21643 ms | **0.13× LOSS** |
| c3_daemon | 27061 ms | **0.10× LOSS** |

Source: 2026-05-24 deployment-mode-matrix, n=2 (after the percentile-calc
fix; numbers are much harsher than the pre-fix pass).
ds4's session-switch in RAM is fast; WombatKV adds save+load on
each switch and the cost accumulates over the 5×5 round-robin.

### 2.4 Multi-turn real ShareGPT (the daemon save-tax)

ShareGPT round-robin across 8 conversations:

| Mode | TTFT p50 | Speedup vs native | Wall time |
|---|---:|---:|---:|
| native | 2196 ms | 1.00 | 76 s |
| native_cold | 2007 ms | 1.09× | 85 s |
| embedded_local | 2250 ms | **~parity** | 131 s (1.7× slower wall) |
| daemon_shm | 5422 ms | **0.40× LOSS** | 344 s |
| daemon_tcp_local | 5274 ms | **0.42× LOSS** | 329 s |
| daemon_http_local | 4772 ms | **0.46× LOSS** | 391 s |

Source: 2026-05-24 public-corpus-replay, n=1. Embedded is at parity,
daemon is 2-5× slower than native on real chat.

### 2.5 Cross-host LAN over WiFi

| Source | Mode | Speedup |
|---|---|---:|
| 2026-05-24 deployment-mode-matrix | daemon_tcp_remote | 1.31× |
| 2026-05-24 deployment-mode-matrix | daemon_http_remote | 1.29× |
| 2026-05-18 alpha-dev-runs path B 5-trial | daemon-TCP | 1.6× |
| 2026-05-18 alpha-dev-runs path A 3-trial | embedded over LAN | 4.5× |

Bandwidth-bound on WiFi. Cross-host with the daemon path is barely
above parity.

## 3. Cross-campaign reproducibility

| Claim | alpha-dev-runs | deployment-mode-matrix | public-corpus-replay |
|---|---|---|---|
| Exact-restart wiped at ~1.5k tok | 73.1× (May 17, 1.7k tok), 104.8× (May 18, 1.3k tuned) | **89.7×** (5k char canonical) | not retested |
| Cross-conversation 5×5 ~9.7k tok | **58.7×** | not retested | not retested |
| Bigger context (4.8k tok) cross-restart | 51.8× (May 22 post-refactor) | not retested | not retested |
| Real ShareGPT multi-turn (8 convs) | not in this campaign | 68.2× exact replay (n=8) | **~parity embedded, 2-5× LOSS daemon (multi-turn n=1)** |
| Gutenberg multi-round QA | not in this campaign | not in this campaign | **1.39× WIN embedded (n=1)** |
| Partial-prefix preserved kvdisk | not in this campaign | **2.45-4.45× WIN** (embedded) | not yet |
| pi-review 5 agents | "110×" (May 16, prepopulated bucket xrestart variant) | **0.65-0.72× LOSS** (as-shipped script) | not retested |
| conversation_switch round-robin | not in this campaign | **0.13× LOSS** (after percentile-calc fix) | not rerun in extended pass |

**Reproducibility notes**:
- Exact-restart 70-105× reproduces across two independent harnesses and three prompt sizes. Variance comes from tuning, prompt corpus, storage (Docker MinIO vs native MinIO), methodology stays warmup-primed n=5.
- pi-review "110×" claim from alpha-dev-runs and "0.65× LOSS" from deployment-mode-matrix are NOT the same workload. The first measures per-agent TTFT after a prepopulated-bucket xrestart; the second runs the as-shipped pi_review.py with no prepopulation, save+load dominates. Both numbers are real; the difference is workload definition.
- Cross-host LAN compresses the headline 70-105× to 1.6-6.2× because ~23 MiB of KV state on WiFi is bandwidth-bound.

## 4. The daemon tax, root-cause diagnosed

The single most important diagnostic finding from 2026-05-24:

**Per-restore stage breakdown (sharegpt_round_robin warm hits)**:

| Mode | lookup ms | get_ms | sidecar ms | entry_to_exit ms |
|---|---:|---:|---:|---:|
| embedded_local | 0.0 | 34.4 | 5.4 | **75** |
| daemon_tcp_local | 0.8 | 39.2 | 32.2 | **75** |
| daemon_http_local | 0.7 | 52.9 | 63.4 | **139** |
| daemon_shm | 26.8 | 115.9 | 27.6 | **216** |

**Per-save stage breakdown** (the killer):

| Mode | save entry_to_exit p50 (ms) | vs embedded |
|---|---:|---:|
| embedded_local | 323 | 1.0× |
| daemon_tcp_local | 2146 | **6.65× slower** |
| daemon_shm | 2315 | **7.18× slower** |
| daemon_http_local | 2581 | **8.00× slower** |

For workloads that save state on every turn (real chat), the daemon
save-path overhead dominates and drives the 2-5× LOSS in §2.4.

For remote-daemon restore, the equivalent diagnostic from the
deployment-mode-matrix campaign: **embedded_remote spends ~10 ms in
block fetch, daemon_tcp/http_remote spend 3.0-3.6 seconds at the same
stage**.
The remote-daemon weakness is daemon-side orchestration, not the
network. The raw `wombatkv-tcp-multi-load-bench` shows the wire
itself is much faster:

| Label | Throughput | PUT p50 | GET p50 |
|---|---:|---:|---:|
| TCP loopback | 2361 ops/s | 5.3 ms | 0.12 ms |
| TCP LAN | 493 ops/s | 17.6 ms | 9.3 ms |

LAN GET is 79× slower than loopback GET on raw payload, but the
daemon path adds another 100-300× on top, that's the orchestration
gap to close.

## 5. Methodology caveats

For the full 10-point caveat list with rationale, see
[`artifacts/2026-05-24_deployment-mode-matrix/methodology_caveats.md`](./artifacts/2026-05-24_deployment-mode-matrix/methodology_caveats.md).
Highlights:

Common to all three campaigns:
- TTFT-bound, not full E2E. Total latency is noisier than TTFT because decode + visible-output length vary.
- Native baseline uses `--kv-disk-dir` + `--kv-cache-min-tokens 256`. This is the honest comparison against ds4's real production checkpoint path. `native_cold` rows (where present) disable kvdisk entirely as the additional reference line.
- Streamed OpenAI-compatible chat completions at `temperature=0`. Coherence verified via assistant_content + reasoning_content capture + SHA-256.

Specific to 2026-05-24 public-corpus-replay:
- n=1 trials. Treat as directional, not converged.
- Gutenberg daemon modes were intentionally cut after the ShareGPT daemon LOSS pattern was clear; only native + native_cold + embedded_local for Gutenberg.
- Expanded partial-prefix sweep with daemon modes was still running when this snapshot was taken; results will land in a follow-up artifact dir.
- conversation_switch p95 field is harness-bugged (passes `0.95` instead of `95` to `lib.percentile()`); use `switch_ttfts_median` only. the campaign's rerun with the fix never completed.

Specific to 2026-05-24 deployment-mode-matrix:
- Partial-prefix and scenarios are n=2 each, directional.
- `same_process` cell is two requests with the same prompt in one server, not an uninterrupted live continuation.
- Remote daemon was tested only on `canonical_long_prompt`. ShareGPT remote-daemon rows do not exist.

Specific to 2026-05_alpha-dev-runs:
- Multi-date, multi-prompt-corpus, multi-tuning. Per-row provenance in `headlines.jsonl`.
- The 73.1× and 110× numbers come from different scenarios than what the 2026-05-24 campaign measured; see §3 reproducibility notes for which apples-to-apples cells exist.

## 6. Honest gaps

For the forward-looking analysis (missing cells, remote-daemon
improvement hypotheses, suite hardening playbook drawing on LMCache /
Mooncake / SGLang / llm-d lineage), see
[`artifacts/2026-05-24_deployment-mode-matrix/gaps_and_next_steps.md`](./artifacts/2026-05-24_deployment-mode-matrix/gaps_and_next_steps.md).

Not run, acknowledged:
- Real cloud S3 (AWS, R2, GCS). All rows use MinIO. Expect production S3 GET latency (30-100 ms) to compress the loopback headline by ~3× (from ~89× toward ~25-35×).
- End-to-end ds4 cross-restart on Linux. Transport-side validated on the Linux test host; full ds4 path is post-alpha.
- Daemon modes for partial-prefix sweep (still running as of the snapshot).
- Daemon modes for Gutenberg multi-round (intentionally cut).
- ShareGPT remote-daemon rows.
- conversation_switch rerun with the p95 fix.
- Concurrent multi-client cell-B (1P-1C SHM is single-producer by construction).
- CUDA / NVIDIA, architecture is engine-neutral, only Metal benched.

## 7. Bottom line

WombatKV wins where state needs to **move or be shared**:
- Cross-restart (89.7× canonical; 70-105× regime reproduced across campaigns).
- Cross-conversation prefix-share (58.7× alpha-dev; 1.39× Gutenberg multi-round).
- Cross-engine cross-machine over LAN (4.5-6.2× embedded; varies by prompt size).
- Partial-prefix reuse with kvdisk preserved (2.45-4.45×, the strongest non-restart claim).

WombatKV does less where the engine's own state machine is the right tool:
- Same process, same prompt, no restart (engine cache services it).
- Same machine, ds4 kvdisk intact, exact same prompt (local round-trip wins).
- Live multi-agent / session-switch on one server (daemon save-tax dominates).

The verdict map: **embedded_local is the consistently strong WombatKV mode**.
**daemon modes pay a 6-8× save-path tax** that's fine for save-once / restore-many
workloads but bad for save-every-turn churn. **Remote daemon is the active
problem area**, wire is fast, orchestration is not.
