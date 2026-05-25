# WombatKV, minimal Makefile.

.PHONY: fmt lint test ci store-minio-integration

fmt:
	cargo fmt --all --check

lint:
	cargo clippy --workspace --all-targets -- \
	  -D clippy::correctness -D clippy::suspicious -D clippy::perf

test:
	@# Linux's default fd limit (1024) is below what foyer's hybrid cache
	@# opens during the integration tests. Bump if we can, then run.
	@ulimit -n 65536 2>/dev/null || true; cargo test --workspace

ci: fmt lint test

# Developer-side audits, not gated in CI (line-number sensitive), but
# useful when intentionally adding platform-specific code or absorbing
# a toolchain bump.
audit-platform:
	scripts/cfg_target_os_audit.sh

audit-clippy:
	scripts/clippy_baseline.sh

# Live MinIO integration drill. Requires a reachable S3-compatible
# endpoint; see `.github/workflows/store-minio-integration.yml`.
#
# Live tests live in wombatkv-{node,cabi,daemon} crates' tests/ dirs
#, not wombatkv-store's (no test/* files in that crate). Each test
# probes the configured WMBT_KV_S3_ENDPOINT and either round-trips
# real S3 traffic or spawns a wombatkv-daemon child that does.
store-minio-integration:
	@ulimit -n 65536 2>/dev/null; cargo test --release \
	  -p wombatkv-node    --test embed_live_minio \
	  -p wombatkv-cabi    --test cabi_blocks_remote \
	  -p wombatkv-cabi    --test cabi_blocks \
	  -p wombatkv-cabi    --test sidecar_roundtrip \
	  -p wombatkv-cabi    --test cabi_adversarial_roundtrip \
	  -p wombatkv-daemon  --test client_liveness \
	  -p wombatkv-daemon  --test multi_tenant_reuse \
	  -p wombatkv-daemon  --test daemon_tpc_tcp_only_fail_loud
