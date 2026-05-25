# Deterministic Simulation Testing (DST)

WombatKV's `wombatkv-dst` crate carries the BUGGIFY / Antithesis-style
DST primitives ported into the workspace from earlier vector-search
and SHM-IPC prototypes. This document explains the layout, the runtime
contract, and how to wire new chaos sites.

For the underlying motivation see TigerBeetle's
[VOPR](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/internals/vopr.md),
SlateDB's `slatedb-dst`, and FoundationDB's original
[BUGGIFY paper](https://apple.github.io/foundationdb/testing.html).

## What's in the crate

```text
crates/wombatkv-dst/src/
├── lib.rs                 # re-exports + 5-stage roadmap doc
├── dst_assertions.rs      # assert_always / assert_sometimes /
│                          # assert_reachable / assert_unreachable
├── dst_buggify.rs         # env-gated chaos at prod call sites
├── dst_rng.rs             # XorShift64 + phase()-keyed seed cascade
├── fault.rs               # WombatKvFaultEvent + schedule_for_class
├── oracle.rs              # in-memory shadow of the KV surface
└── bin/wombatkv_dst_runner.rs  # seed → plan generator binary
```

Also:

```text
scripts/dst-sweep.sh       # seed × class sweep harness
```

## The 5-stage roadmap

| Stage | What | Status |
|---|---|---|
| **0** | Primitives in `wombatkv-dst` crate; build standalone | ✅ shipped |
| **1** | Wire `dst_buggify!()` + `assert_*` into prod crates behind `dst` feature | ✅ first wave (`embed.rs`); more sites follow |
| **2** | `schedule_for_class(seed, class) -> FaultPlan` deterministic generator | ✅ shipped |
| **3** | `wombatkv-dst-runner` binary writes plan JSON; sweep loops over seeds × classes | ✅ shipped |
| **3.5** | Runner spawns child puffer, loads plan via `DST_FAULT_PLAN_FILE`, drives ops, reports | ⏳ next session |
| **4** | In-memory `WombatKvOracle` for state verification | ✅ shipped |
| **5** | Sweep script that runs N×M seed combos | ✅ shipped (`scripts/dst-sweep.sh`) |
| **6** | CI integration (GitHub Actions seeded sweep on every PR) | ⏳ deferred |

## Building with DST enabled

DST primitives are zero-cost when not enabled: `#[cfg(feature = "dst")]`
gates every call site. Production builds compile away all chaos
points and assertions.

```sh
# Default (no DST):
cargo build -p wombatkv-node

# DST enabled:
cargo build -p wombatkv-node --features dst

# Run lib tests with DST features:
cargo test -p wombatkv-node --features dst --lib
```

## Running a single seeded scenario

```sh
cargo build -p wombatkv-dst --bin wombatkv-dst-runner --release

./target/release/wombatkv-dst-runner \
    --seed 42 \
    --class corrupt-block \
    --plan-file /tmp/plan.json \
    --print-plan
```

Output:

```text
wombatkv-dst-runner seed=42 class=CorruptBlockBytes events=1 plan=/tmp/plan.json
```

The same `--seed` + `--class` always produces the byte-identical
plan JSON. Reproduces failures from the seed alone, no need to
preserve the plan file across runs.

## Running a sweep

```sh
./scripts/dst-sweep.sh --seeds 1-100 --out /tmp/dst-overnight
```

Generates 100 seeds × 7 classes = 700 plans, writes each to
`/tmp/dst-overnight/<seed>-<class>.json`, and emits a
`SWEEP_SUMMARY.txt` with per-class event distribution. Exits 1 on
any plan-write failure.

## The 7 WombatKV failure classes

Each is documented in `fault.rs`:

1. **TransientS3Failure**: S3 returns ServiceUnavail / Throttling
   mid-GET. Foyer / chain-rehydration must retry or surface a clean
   error; no panic.
2. **CorruptBlockBytes**: S3 returns bytes with bad blake3 chain
   hash. Block-load path must reject; warm-restore must NOT install
   corrupt KV state.
3. **PartialChainSave**, process crashes after some blocks PUT but
   before chain-head PUT. Next bootstrap must NOT see the partial
   chain.
4. **ConcurrentSameKeySave**, two PUTs to the same
   content-addressed key. Idempotent dedup must hold; no torn state.
5. **DaemonRestartMidLookup**. Daemon restarts during a lookup.
   Embedded client must retry against the new daemon or surface
   `DaemonUnavailable` cleanly.
6. **SidecarDriftAfterChain**, raw_tail sidecar PUT succeeds but
   chain-head PUT fails. Next bootstrap must ignore the orphan
   sidecar.
7. **FoyerEvictionMidGet**: LRU evicts a block mid-lease. Lease's
   Arc must keep bytes alive past eviction (use-after-free safety).

Adding a class: extend `WombatKvFailureClass` in `fault.rs`, add a
match arm in `schedule_for_class`, add a `ClassArg` variant in
`bin/wombatkv_dst_runner.rs`.

## Wiring a new chaos site

In `wombatkv-node` or `wombatkv-daemon`, anywhere a fault could
matter:

```rust
// Chaos point, buggify can return early Err to simulate the
// failure class. Inert in non-dst builds.
#[cfg(feature = "dst")]
if wombatkv_dst::dst_buggify!() {
    return Err(MyError::DstInjectedFault);
}
```

Or an invariant:

```rust
// Invariant, must always hold; runner reports violations.
#[cfg(feature = "dst")]
wombatkv_dst::assert_always(
    condition,
    "invariant name",
    "details for the report",
);
```

Or a coverage gate:

```rust
// Sticks once any seeded run hits the branch.
#[cfg(feature = "dst")]
wombatkv_dst::assert_sometimes(
    is_rare_branch,
    "rare branch was exercised",
    "DST coverage gate",
);
```

## Runtime control

The chaos rate is controlled by env vars (see `dst_buggify.rs`):

| Env var | Default | What |
|---|---|---|
| `DST_BUGGIFY` | unset = off | `1` / `true` / `yes` to enable |
| `DST_BUGGIFY_SEED` | `1` | master seed for the buggify roll |
| `DST_BUGGIFY_ACTIVATION_PERCENT` | `25` | % of call sites that "activate" (per-seed) |
| `DST_BUGGIFY_FIRE_PERCENT` | `25` | % of activated calls that fire (per-call) |

Net fire rate: `activation% × fire% / 100`. With defaults: ~6.25%
of `dst_buggify!()` calls inject chaos.

## The oracle

`oracle::WombatKvOracle` is an in-memory reference model of the KV
surface. The runner (Stage 3.5) drives every op against both the
oracle and the real puffer, then asks
`Verdict::check_get(&oracle, ns, key, observed)` whether they
agree. Any `Verdict::Divergence` is a bug.

Properties currently enforced:
- Read-your-writes
- Namespace isolation
- Failed PUT does NOT commit

Out of scope (Stage 4.5 follow-up):
- Cache-tier hints (foyer-RAM vs SSD vs S3)
- Concurrent same-key races
- Compression / chain-hash semantics

## Why DST instead of (or in addition to) regular tests

Regular tests verify the cases you thought to write. DST verifies
the cases you didn't, over thousands of seeded runs with
deterministic fault injection, divergences from the oracle
surface even when no human knew to expect them.

WombatKV's current test count is 336 unit tests across the
workspace (`cargo test --workspace --lib`). These cover the happy
paths plus catalogued failure modes (`fault_harness.rs` in
wombatkv-node). DST extends that to the failure modes nobody has
thought of yet.

The runner+oracle wiring (Stage 3.5) is what closes the loop. Once
that lands and the sweep is running on real seeds against a real
puffer, a "found 3 bugs at seeds 142, 487, 1023" run is the
typical output. Each becomes a regression test once the bug is
fixed.
