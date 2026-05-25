# Contributing

## Repo layout

- `crates/wombatkv-*`, the 9 production crates (see
  [Crate organization](./reference/crates.md))
- `book/src/`, this mdbook's source
- `book/src/*.md`, long-form spec docs (rendered as mdbook chapters via
  `{{#include}}`)
- `scripts/`, bench drivers, DST sweep harness, lint policy checks
- `tests/`, integration tests not co-located in a crate

## Pre-push gate (workspace cleanup + runtime changes)

Before any cleanup or runtime change to wombatkv, run:

```sh
# 1. Lib tests
cargo test --workspace --lib --release

# 2. Daemon lib tests (includes wire-corruption rejection coverage)
cargo test -p wombatkv-daemon --lib --release

# 3. Formatting
cargo fmt --all -- --check

# 4. Clippy (zero errors; pedantic warnings allowed)
cargo clippy --workspace --all-targets --release

# 5. DST sweep (N seeds × 17 classes)
./scripts/dst-sweep.sh --seeds 1-50

# 6. mode_smoke for every transport whose code path changed
#    (typical: daemon-tcp, daemon-http, daemon-shm)
python3 scripts/mode_smoke.py daemon-shm
python3 scripts/mode_smoke.py daemon-tcp
python3 scripts/mode_smoke.py daemon-http

# 7. multi_user_multiturn for the changed transports
python3 scripts/scenarios/multi_user_multiturn.py --mode all

# 8. If the daemon binary / SHM listener / runtime_tpc / MAX_OPEN_RETRIES
#    constant or env override changed:
cargo test --release -p wombatkv-daemon \
    --test daemon_tpc_tcp_only_fail_loud -- --test-threads=1

# 9. If C ABI / sidecar / block-v2 touched: run the ds4 C tests
make -C ../ds4 test
```

The full gate runs in ~5-10 minutes; the env override / fail-loud
test alone takes ~90s.

## Env-var naming conventions

See the [Environment variables](./operations/env.md) chapter for the
6-rule convention codified in `book/src/operations/env.md`. tl;dr:

- Every WombatKV env var starts with `WMBT_KV_`.
- Three reserved category sub-prefixes: `WMBT_KV_TEST_*`,
  `WMBT_KV_DST_*`, `WMBT_KV_BENCH_*`.
- Other sub-prefixes (S3, PUFFER, DAEMON_*, TCP, HTTP, SLATEDB) are
  optional functional groupings.
- Time units in name (`_MS` / `_SECS`); bytes (`_BYTES`); counts no suffix.
- Default-on boolean knobs that DISABLE end in `_DISABLE`.

## Building the book locally

```sh
# Install mdbook if needed:
cargo install mdbook

# Build + serve:
mdbook serve
# → http://localhost:3000

# Static build:
mdbook build
# → book/book/
```

## Reporting issues

GitHub issues:
[https://github.com/Venkat2811/wombatkv/issues](https://github.com/Venkat2811/wombatkv/issues)

For security issues, please email the maintainer rather than filing a
public issue.
