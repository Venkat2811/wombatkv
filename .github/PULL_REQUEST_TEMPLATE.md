## Summary

One or two sentences on what this PR changes and why.

## Crates affected

- [ ] wombatkv-core
- [ ] wombatkv-format
- [ ] wombatkv-store
- [ ] wombatkv-radix
- [ ] wombatkv-node
- [ ] wombatkv-daemon
- [ ] wombatkv-cabi
- [ ] wombatkv-dst (internal)
- [ ] wombatkv-bench (internal)
- [ ] docs / CI / workspace tooling only

## Validation

- [ ] `cargo build --workspace`
- [ ] `cargo test --workspace --lib --release`
- [ ] `cargo clippy --workspace --all-targets --release`
- [ ] `cargo fmt --all -- --check`
- [ ] `cargo doc --workspace --no-deps --release`
- [ ] CHANGELOG.md updated (if user-visible change)
- [ ] Rustdoc updated (if public API changed)
- [ ] If DST code touched: `./scripts/dst-sweep.sh --seeds 1-50` passes
- [ ] If daemon binary / SHM listener / runtime_tpc touched: `cargo test
      --release -p wombatkv-daemon --test daemon_tpc_tcp_only_fail_loud
      -- --test-threads=1` passes (~90s)
- [ ] If C ABI / cabi surface touched: ds4 reference integration's
      `python3 scripts/mode_smoke.py all` passes

## Breaking change?

Yes / No. If yes, describe the migration path.

Note: during the alpha window, breaking changes can land without back-compat
shims. After the OSS tag, breaking changes follow standard semver.
