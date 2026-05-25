# Contributing to WombatKV

WombatKV is an object-storage-native KV cache system for LLM inference.
This repository publishes a small number of `wombatkv-*` crates plus an
internal DST + bench surface that exercises them.

Everything else in the workspace exists to test, benchmark, or demonstrate
the published crates.

## TL;DR

```sh
git clone https://github.com/Venkat2811/wombatkv
cd wombatkv
cargo build --workspace
cargo test  --workspace --lib --release
cargo fmt   --all -- --check
cargo clippy --workspace --all-targets --release
cargo doc   --workspace --no-deps --release
```

If those gates pass, the branch is usually in good shape for review.

## Workspace layout

```text
crates/
├── wombatkv-core/     # Publishable: foundational types + traits (no internal deps).
├── wombatkv-format/   # Publishable: WAL + segment binary codecs.
├── wombatkv-store/    # Publishable: S3 backend + WAL durability.
├── wombatkv-radix/    # Publishable: in-memory + SlateDB-backed metadata index.
├── wombatkv-node/     # Publishable: foyer hybrid cache + embedded KV.
├── wombatkv-daemon/   # Publishable: SHM / TCP / HTTP daemon + clients.
├── wombatkv-cabi/     # Publishable: C ABI cdylib (libwombatkv.{so,dylib}).
├── wombatkv-dst/      # Internal: deterministic-simulation harness, 20 fault classes.
└── wombatkv-bench/    # Internal: perf benchmark binaries.

docs/                  # Long-form spec docs (rendered into mdbook chapters).
docs/src/              # mdbook source (SUMMARY.md + chapter files).
```

The internal crates (`wombatkv-dst`, `wombatkv-bench`) live in this workspace
because that's where they exercise their target crates, but they are not
published.

## Filing issues

- **Bugs**: include the workspace commit SHA, platform details, `rustc --version`,
  and the smallest reproduction you can manage.
- **Performance regressions**: include before/after numbers and the exact harness
  command that produced them.
- **Feature requests**: lead with the use case and why the current public surface
  is insufficient.

## Sending a PR

Standard GitHub flow: fork, branch, PR.

### Before opening a PR

Run the validation gates above.

If your change touches DST code under `crates/wombatkv-dst/` or fault hooks in
`wombatkv-node` / `wombatkv-daemon`, also run:

```sh
./scripts/dst-sweep.sh --seeds 1-50
```

If your change touches the C ABI in `wombatkv-cabi` or any code path engines
call into through it, also run the cross-mode smoke against an integrated
engine (today: ds4):

```sh
# From the ds4 fork repo:
python3 scripts/mode_smoke.py all
```

If your change touches the daemon binary's main loop, SHM listener,
`runtime_tpc`, or the `MAX_OPEN_RETRIES` env override, also run the
fail-loud integration test (~90s):

```sh
cargo test --release -p wombatkv-daemon \
    --test daemon_tpc_tcp_only_fail_loud -- --test-threads=1
```

### Commit message style

- Conventional commits format: `feat(daemon): …`, `cleanup(env): …`,
  `docs(readme): …`, `ci: …`, `test(dst): …`, `release: 0.1.0-alpha.X`.
- Keep the subject short and concrete.
- For non-trivial commits, include a short verification block in the body.

### Code conventions

- Workspace lints are the source of truth. Do not add `#[allow(...)]` unless
  the lint itself is wrong for the case.
- Every `unsafe { ... }` block needs a `// SAFETY:` comment that states the
  invariant.
- Public items in the published `wombatkv-*` crates need rustdoc.
- Format with `cargo fmt --all`.
- Avoid `unwrap()` and `expect()` in non-test code unless the panic documents
  an invariant a maintainer would check before changing the code.

### What to expect from review

The review bar is straightforward:

- clear public API boundaries
- explicit invariants
- real verification, not assumed verification
- no hidden performance regressions
- alignment with the [naming convention](book/src/operations/env.md) for any new env vars

If a PR is out of scope, the preferred outcome is an explicit no with a
concrete reason, not an ambiguous stall.

## RFCs

Use an RFC for substantive design changes:

- new public types
- new feature flags
- wire-format changes (the RFC 0018 universal envelope is the current baseline)
- semantic changes to existing public behavior
- new env vars that would violate the [6-rule convention](book/src/operations/env.md)

New RFCs should stay lightweight and concrete: the problem, the proposed
change, the rejected alternatives, and the migration plan if any.

## Local development tips

- `cargo build -p wombatkv-cabi --release` produces `target/release/libwombatkv.{so,dylib}`
  that engines link against. The C header is hand-maintained at
  `crates/wombatkv-cabi/include/wombatkv.h`.
- `cargo build -p wombatkv-daemon --release --bin wombatkv-daemon` produces
  the daemon binary.
- `mdbook serve` (from repo root, requires `cargo install mdbook`) renders
  the book locally at http://localhost:3000.
- `scripts/dst-sweep.sh --seeds 1-50` runs the DST sweep harness.
- `scripts/clippy_baseline.sh` (or `make audit-clippy`) diffs the
  current clippy warning surface against `tests/clippy_baseline.txt`
 , surfaces toolchain drift (new pedantic lints promoted by a rustc
  bump) at PR time. Use `--update` after deliberate review to absorb
  a new baseline.
- `scripts/cfg_target_os_audit.sh` (or `make audit-platform`) snapshots
  every `cfg(target_os = ...)` block and diffs against
  `tests/cfg_target_os_baseline.txt`. New blocks are deliberate
  decisions that need matching test coverage under the same cfg gate
  (or a DST `PlatformShmHidden` / `PlatformIoUringJitter` /
  `PlatformKqueueOnly` scenario). Same `--update` idiom after review.

### Linux contributors: bump fd limit

WombatKV's integration tests exercise foyer's hybrid cache, which opens
many files concurrently. Linux's default `ulimit -n` of 1024 is below
the threshold and tests fail with `Too many open files (os error 24)`.

`make ci` and `make test` set `ulimit -n 65536` automatically.

If you invoke `cargo test` directly, bump the limit first:

```sh
ulimit -n 65536
cargo test --workspace
```

For a persistent bump across shells, add to `/etc/security/limits.conf`
(see `man limits.conf`). macOS contributors don't need to do anything;
the default per-process limit on recent macOS is well above what the
suite needs.

## License

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in WombatKV by you shall be licensed under the
[Apache License, Version 2.0](LICENSE), without any additional terms or
conditions.

## Code of conduct

Be specific, assume good faith, and argue from technical substance.

## Where to ask if you're stuck

- GitHub Issues for bugs, features, and performance work
- GitHub Discussions for design questions and usage questions
- See [SUPPORT.md](SUPPORT.md) for the full triage map
