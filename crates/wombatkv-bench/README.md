# wombatkv-bench

Bench + operator-utility binaries for WombatKV. **Not a library** -
every binary stands alone, depends on the production crates via
path-deps, and exists to measure or demonstrate one capability of
the substrate.

## Binaries

Built with `cargo build -p wombatkv-bench --release`. Common
operator + perf utilities:

- `wombatkv-shm-bench`: SHM 1P-1C ring throughput
- `wombatkv-load-bench`, embedded `WombatKVKvStore` put/get throughput
- `wombatkv-tcp-smoke`, first-light cross-host TCP RTT probe
- `wombatkv-tcp-multi-load-bench`, multi-client TCP load
- `wombatkv-dst-driver`, convenience driver for DST runs (built
  with `--features dst`)

Each binary's `--help` documents its specific knobs.

## Why a separate crate?

Bench-only deps (latency histograms, plotting, etc.) shouldn't
contaminate the production crates' compile times. The production
crates (`wombatkv-node`, `wombatkv-daemon`, etc.) stay lean; this
crate carries the experimentation surface.

## Test

```sh
# wombatkv-bench has no lib tests by design, bins only.
cargo build -p wombatkv-bench --release
```
