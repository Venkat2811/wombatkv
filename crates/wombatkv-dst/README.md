# wombatkv-dst

Deterministic simulation testing (DST) primitives for WombatKV.
Seeded fault-injection harness that catches recovery bugs across
20 failure classes, every plan is reproducible by `(seed, class)`,
so a CI failure replays bit-identically locally.

## Modules

| module | what |
|---|---|
| `dst_rng` | seeded XorShift64 + phased streams for deterministic chaos generation |
| `dst_buggify` | `dst_buggify!()` macro, probabilistic chaos trigger gated on `dst` feature + seeded RNG |
| `dst_plan` | plan-aware fault dispatch: consults a loaded `FaultPlan` and fires the scheduled event for the current op |
| `dst_assertions` | `assert_always` / `sometimes` macros for invariant checking |
| `fault` | fault event taxonomy (`WombatKvFaultEvent`, `S3ErrorKind`) + failure-class scheduler (`schedule_for_class`) |
| `oracle` | in-memory reference KV, observation-vs-oracle verdict (`Verdict::Match` / `Divergence`) |

Forbids `unsafe_code`.

## Failure classes

Storage / concurrency / restart (7):
- `TransientS3Failure`
- `CorruptBlockBytes`
- `PartialChainSave`
- `ConcurrentSameKeySave`
- `DaemonRestartMidLookup`
- `SidecarDriftAfterChain`
- `FoyerEvictionMidGet`

Transport-layer (3, RFC 0018 Phase 6):
- `TransportConnectionDropMidRPC`
- `TransportPartialReadOnHeader`
- `TransportSlowWrite`

Wire / storage / format / version recovery (5):
- `WireEnvelopeCorruption`
- `OldSidecarV3InBucket`
- `OldBlockV1InBucket`
- `SlateDbWriteFailure`
- `SlateDbManifestCorruption`

Multi-tenant + daemon-startup recovery (2,):
- `MultiEnginePrefixConflict`
- `DaemonTpcOnlyNoShmClient` *(covered today by the
  `daemon_tpc_tcp_only_fail_loud` integration test; will become a
  live DST scenario when the runner-child gains real-daemon-binary
  driving)*

## Stages

| stage | shipped | what |
|---|---|---|
| 0 | ✓ | primitives: RNG, buggify, assertions |
| 1 | ✓ | buggify call sites in `wombatkv-node` + `wombatkv-daemon` (SHM dispatch + TCP/HTTP TPC dispatch) |
| 2 | ✓ | fault model + failure-class taxonomy |
| 3 | ✓ | `wombatkv-dst-runner` binary, derives plans from `(seed, class)`, writes JSON |
| 3.5 | ✓ | cross-process child runner (`wombatkv-dst-runner-child`) loads plan via env, drives 20-op oracle-checked sequence, emits structured `ChildReport` for parent diff |
| 4 | ✓ | oracle observation-vs-truth verdict; integrated into the child runner |
| 5 | ✓ | sweep script (`scripts/dst-sweep.sh`): N seeds × all classes for CI |

## Build + run

```sh
# Lib tests (deterministic schedule generation):
cargo test -p wombatkv-dst --release

# Build the runner + cross-process child:
cargo build --release -p wombatkv-dst \
  --bin wombatkv-dst-runner \
  --bin wombatkv-dst-runner-child

# Single-plan run (parent generates + spawns child + reads report):
./target/release/wombatkv-dst-runner \
  --seed 42 \
  --class transient_s3 \
  --plan-file /tmp/plan-42.json \
  --spawn-child \
  --child-bin ./target/release/wombatkv-dst-runner-child

# Sweep: N seeds × all classes (deterministic, same seed → same plan):
./scripts/dst-sweep.sh --seeds 1-50
```

## How a fault class becomes an event

```text
(seed=42, class=TransientS3Failure)
    │
    ▼
schedule_for_class(seed, class)            ← fault.rs:335
    │
    ▼
FaultPlan {
  seed: 42,
  class: TransientS3Failure,
  events: [
    ScheduledFault {
      trigger: AfterS3Get { n: 7 },        ← deterministic via seeded RNG
      event: S3GetFailure {
        error_kind: ServiceUnavail,
      },
    },
  ],
}
    │
    ▼  serialized to JSON, loaded via DST_FAULT_PLAN_FILE env
    ▼
Child runner drives 20 KV ops; at op 7 a get returns ServiceUnavail.
Oracle records: get(k7) → MISS at op 7 (expected, due to suppression).
Real store records: get(k7) → MISS at op 7 (matches oracle).
Verdict: PASS, the puffer's retry path absorbed the transient.
```

## RFC pointers

- RFC 0011, alpha cleanup checklist + DST roadmap origin
- RFC 0018 §13, wire-envelope chaos classes
