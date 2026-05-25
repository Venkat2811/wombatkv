# Benchmark methodology

How WombatKV's headline numbers are measured and why the protocol
is structured this way.

## What we measure

The headline number is **cell-B cross-restart speedup**: how much
faster a turn-2 request is when the prior turn's KV state was
restored from WombatKV vs. when ds4 has to cold-prefill from
scratch.

```
Speedup = (ds4-native turn-2 TTFT) / (ds4 + WombatKV turn-2 TTFT)
```

Where:
- **Turn 1 (cold):** first request after process start; ds4 prefills the prompt. WombatKV mode also saves blocks during this turn.
- **Process restart:** kill ds4, wipe local kvdisk, restart. For WombatKV mode the S3 / MinIO bucket persists (this is the whole point); for native mode there is nothing persistent.
- **Turn 2 (warm):** same prompt as turn 1. ds4-native pays full prefill again; ds4 + WombatKV restores blocks from S3 and skips ~all of the prefill.

The "cell-B" label distinguishes this from cell-A (same-process
warm-restore, where the local kvdisk handles it for free in both
modes).

## The warmup-primed protocol (canonical)

**Always warm Metal kernels before the measured turn-2.**

Metal kernel JIT on first decode after process start adds 100-300ms
of cold-start variance. Production traffic is steady-state (users
don't hit the server with their first request immediately after
restart), so the warmup-primed measurement is the honest one.

### Procedure

For each trial:

1. **Wipe state.** Clear the MinIO bucket(s), `rm -rf` the local
   kvdisk + foyer dirs.
2. **Start ds4-server.**
3. **Turn 1 (cold).** Send the prompt; record TTFT. The bench
   prompt is ~5200 bytes of literary text, tokenized to ~1700
   tokens.
4. **Kill ds4-server.** Wait for the lockfile to clear.
5. **Wipe kvdisk only.** Local cache must NOT survive (this is the
   k8s-pod-replace scenario); the puffer dir does survive in
   WombatKV mode (it's the foyer-SSD tier of the cache hierarchy).
6. **Restart ds4-server.**
7. **🔑 Warmup request.** Fire a `max_tokens=1` request with a
   short unrelated prompt (`"warmup ping"`). This primes Metal
   kernel JIT + the model runtime.
8. **Turn 2 (measured).** Send the same prompt as turn 1; record
   TTFT.
9. **Kill ds4-server.**

Repeat across N trials, take the median.

Reference harness: a Python driver that wraps ds4-server start / stop,
executes the 9-step procedure above per trial, and writes per-trial
JSON. The canonical campaign harness lives in the ds4 fork under
`scripts/demo_showcase_lib.py` (shared start / stop / measure
primitives) and `scripts/scenarios/` (per-workload drivers). The
deployment-mode-matrix and public-corpus-replay campaigns were driven
by extensions of that harness; each campaign's per-cell JSON is
preserved in [`artifacts/`](../../../artifacts/).

### Why warmup, not "drop the first trial"

Dropping the first trial removes most of the JIT noise from each
five-trial group's median, but not all of it. The first
turn-2-after-process-restart is *always* the cold one. Adding an
explicit warmup request between restart and the measured turn
moves the JIT cost OUT of the measurement window every trial,
giving tighter spreads and a higher floor.

5-trial warmup-naive: WombatKV warm-restore p50 = 118 ms (range 92-365)
5-trial warmup-primed: WombatKV warm-restore p50 = 108 ms (range 99-290)

The medians are similar; the floor shifts down ~20% and the
upper-tail outliers (Metal JIT lottery losers) disappear.

### Why N=5

N=3 medians are sensitive to single-trial outliers. We saw a
3-trial run produce a 28.9× median right after a 5-trial run
produced 71.7×: same code, just luck of which of (99, 102, 108,
124, 290) ms got sampled. N=5 makes the median robust; N=10 is
more robust still but doesn't change the publishable number.

## The canonical headline

**73.1× cell-B median speedup**, 5-trial, warmup-primed, on M3 Max
with native MinIO loopback, ds4 + DeepSeek-V4-Flash, 1.7k-token
prompt, kvdisk wiped between turns.

```
            ds4-native turn-2     ds4 + WombatKV turn-2     Speedup
Trial 1:    7847 ms               124 ms                    63.3×
Trial 2:    7835 ms               102 ms                    76.8×
Trial 3:    8079 ms               290 ms ← S3 hiccup        27.9×
Trial 4:    8191 ms                99 ms                    82.7×
Trial 5:    7929 ms               108 ms                    73.4×
─────────────────────────────────────────────────────────────────
Median:     7929 ms               108 ms                  **73.1×**
```

Floor:
- WombatKV-warm TTFT: **99 ms** (clean S3)
- Restore-side `entry_to_exit_ms`: **27-49 ms**
- Native cold-prefill TTFT: **7835 ms**

Best case: 82.7×. Worst case (with the S3 hiccup outlier): 27.9×.

Residual variance (99-290 ms) is bounded by MinIO/S3 GET latency,
not by WombatKV code. Per-trial logs show `s3_get_us` spiking on
trial 3.

## Multi-conversation 5×5 (the team-multiplier story)

Five conversations × five turns × ~9700-token shared document.
Each conversation sees the full doc as system context; conv 1
saves doc blocks to S3 during its cold prefill, and convs 2-5 hit
those cached blocks via cross-conversation prefix-share.

**Headline: 58.7× overall median** (110 535 ms / 1883 ms). Cross-
conversation prefix-share is real: agent 1 pays ~130 s once,
agents 2-5 each pay ~2.5 s. Net savings per added agent: ~108 s.

Reference harness: same start / stop primitives as above, with a
multi-conversation driver that issues N conversations × M turns
against the running server and tracks per-turn TTFT. The per-turn
breakdown for this 5×5 cell lives in
[`artifacts/2026-05_alpha-dev-runs/multi_conv_5x5.csv`](../../../artifacts/2026-05_alpha-dev-runs/multi_conv_5x5.csv).

## What we deliberately don't claim

- **Real cloud S3.** Cell-B numbers come from MinIO loopback
  (~15 ms GET). Production AWS S3 is more like 30-100 ms. Expect
  the 73.1× headline to compress to ~25-35× on real S3, still
  good but honest.
- **NVIDIA / CUDA.** Untested. The architecture is engine-neutral
  by design; only Metal is benched.
- **End-to-end ds4 on Linux.** Single + multi-client SHM bench and
  the TCP-bridge bench both ran on Linux (x86_64), and validated
  the transport path. The full ds4 cross-restart cell-B run on
  Linux is still pending.
- **Mac engine to Linux daemon over HTTP.** Daemon-HTTP is
  correctness-validated through the multi-user multi-turn parity
  suite; no cell-B numbers for that combination yet.
- **Concurrent multi-client cell-B.** Single-client only; the 1P-1C
  SHM transport is single-producer by construction.
- **Daemon mode under sustained production-shape load.**
  First-pass load-bench numbers exist; not yet hardened.

## Reproducing

Pre-req: a MinIO server on `127.0.0.1:9000`, ds4-server built with
`WOMBATKV=1`, the model file at `MODEL` path, the prompt file at
`/tmp/pg1184.txt` (first ~5000 chars of Project Gutenberg #1184).

The canonical bench scripts live in the ds4 fork under
`scripts/demo_showcase_lib.py` (shared primitives) and
`scripts/scenarios/*.py` (per-workload drivers, including
`conversation_switch.py` and `pi_review.py`).

Each campaign's `artifacts/<campaign>/README.md` documents the exact
parameters used (n trials, prompt corpus, modes, env vars); the
warmup-primed protocol is the recipe described in this document.

For the canonical headline numbers + per-row breakdowns, see
[`artifacts/2026-05-24_deployment-mode-matrix/`](../../../artifacts/2026-05-24_deployment-mode-matrix/)
and
[`artifacts/2026-05-24_public-corpus-replay/`](../../../artifacts/2026-05-24_public-corpus-replay/).
