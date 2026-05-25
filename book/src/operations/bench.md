# Bench + operator-utility binaries

Every binary in `crates/wombatkv-bench/` exists to measure or
demonstrate one specific capability of the substrate. They are
separate from the production daemon (`crates/wombatkv-daemon/`,
which ships only `wombatkv-daemon`) and the C-ABI library
(`crates/wombatkv-cabi/`, which has no binaries).

When you want to answer a specific question about WombatKV, reach
for the bench that was built for it.

| binary | when to reach for it |
|---|---|
| `wombatkv-puffer-bench` | "What is the end-to-end cell-B story for an in-process engine?", runs the canonical 4-stage workload: cold stash → warm get → cold get → restart-then-warm. This IS the WombatKV pitch as a runnable demo. |
| `wombatkv-puffer-kv` | "Did this key end up in the puffer cache?", interactive CLI: `roundtrip` / `put` / `get` / `stats` / `clear`. Documents (and demos) the **subprocess-state-isolation footgun**: `puffer-kv put` followed by a separate `puffer-kv get` invocation will miss because the puffer rebuilds its index per-process. |
| `wombatkv-embedded-vs-daemon-bench` | "What does daemon mode cost me vs in-process?", spawns the daemon as a subprocess, runs the same PUT/GET workload over both backends, prints a side-by-side latency table per payload size. |
| `wombatkv-shm-bench` | "What's the SHM transport doing for me?", end-to-end SHM perf: ping ping-pong (control-plane RTT, no payload), 4 KiB + 256 KiB put+get workloads, EXISTS round-trips. Reports p10..p99.99 + ops/s + MB/s per stage. Use to compare against UDS or a naive transport. |
| `wombatkv-arena-bench` | "What does the future-zero-copy GET data plane cost?", standalone arena write + read measurement; the lower bound on the zero-copy GET path the daemon will inherit when arena reads land on the wire. |
| `wombatkv-load-bench` | "How does a sustained single-client load look against the daemon?", sequential PUT → GET workload through one daemon prefix, with latency percentiles + throughput. RFC 0011 P10 validation tool. |
| `wombatkv-multi-load-bench` | "What's the multi-client contention story?", spawns N clients × N prefixes against ONE daemon with a shared foyer + S3 backend. Aggregates per-client tails so contention shows up by comparing against the single-client baseline. |
| `wombatkv-tcp-smoke` | "Did my cross-machine setup work at all?", pings + put/get round-trips between a client and a remote daemon. The first sanity check after deploying the daemon to a new host. |
| `wombatkv-tcp-multi-load-bench` | "Does the compio + SO_REUSEPORT N-shard architecture beat std::net under load?": TCP companion to `wombatkv-multi-load-bench`. The win shows up at N ≥ 4 concurrent clients. |
| `wombatkv-dst-driver` (feature: `dst`) | "Does the substrate survive my chosen fault schedule?", deterministic-simulation runner. Drives daemon code paths with seeded RNG + buggify chaos sites. Build with `cargo build --features dst --bin wombatkv-dst-driver`. |

## Build all bins

```bash
cargo build -p wombatkv-bench --release
# DST driver needs the feature flag:
cargo build -p wombatkv-bench --features dst --bin wombatkv-dst-driver --release
```

Built binaries land at `target/release/<bin-name>`.

## Quick reference, the bin you reach for in each common situation

| situation | bin |
|---|---|
| First time I'm running WombatKV | `wombatkv-puffer-bench` |
| Just deployed the daemon to another host | `wombatkv-tcp-smoke` |
| I need a single-line "does the cache work" check | `wombatkv-puffer-kv roundtrip <key>` |
| Customer asks "what does the daemon cost?" | `wombatkv-embedded-vs-daemon-bench` |
| Customer hits cliff at high concurrency | `wombatkv-multi-load-bench` (SHM) or `wombatkv-tcp-multi-load-bench` (TCP) |
| Need to characterize raw SHM transport | `wombatkv-shm-bench` |
| Running a regression for a release | `wombatkv-load-bench` + the relevant `*-multi-*` for the transport you ship |

## Why this is a separate crate

Bench + utility binaries used to be scattered across `wombatkv-daemon/src/bin/` (8 of them) and `wombatkv-node/src/bin/` (2 of them) alongside the production binaries. That meant:

- An OSS reader opening `crates/wombatkv-daemon/` saw 9 binaries with no clear signal that 1 of them (`wombatkv-daemon`) is the production daemon and 8 are scaffolding.
- Bench-only dependencies (clap, anyhow) leaked into the production library's build graph.
- Cross-cutting bench utilities had to pick one host crate even when they exercised multiple.

Moving them to `crates/wombatkv-bench/` makes the production-vs-scaffolding boundary visible at the crate level. The new crate ships zero library code, just the bins, and is excluded from any future "publish to crates.io" subset (`publish = false`).

## Branding note

The hybrid RAM + on-disk cache that the bench binaries exercise is
called the **puffer cache** in operator-facing surfaces (env vars,
binary names, log lines, default paths). It is internally backed by
[foyer-rs](https://crates.io/crates/foyer), and the Rust-API types
(`FoyerHybridCache`, `foyer_cache::config_from_env`) keep the
`foyer` name to make the dep relationship obvious to contributors
reading the code. **External-facing** strings always say "puffer".
