# Changelog

All notable changes to the publishable crates in this workspace are
documented here.

During the early OSS release window, the published `wombatkv-*` crates move
in lockstep at the same version. That may change once their public surfaces
stabilize further.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and the versions follow [Semantic Versioning](https://semver.org/) with
explicit pre-release tags such as `-alpha.N`, `-beta.N`, and `-rc.N`.

## [0.1.0-alpha.pre1.0] - 2026-05-25

Initial public launch of the `wombatkv-*` crates on crates.io.

The published crates form an object-storage-native KV cache system for LLM inference: prefix-share blocks that survive across processes, machines, and pod restarts. The integration reference is the [ds4](https://github.com/Venkat2811/ds4) fork; validation covers Mac M3 with native MinIO and cross-host Mac to Linux over LAN. Headline benchmark numbers and per-row breakdowns live in [BENCHMARKS.md](BENCHMARKS.md).

### Features included in this release

#### Core (`wombatkv-core` + `wombatkv-format` + `wombatkv-store`)

- [x] BLAKE3 content-addressed blocks; sha256-derived 24-hex model fingerprint.
- [x] S3 / MinIO backend with idempotent PUT, conditional CAS via If-Match,
      explicit retry budgets, and a default-dev-credentials guardrail
      (`WMBT_KV_LOCAL_DEV=1`).
- [x] WAL durability with read-after-write semantics (RFC 0004); per-format
      explicit version markers (RFC 0002).

#### Metadata index (`wombatkv-radix`)

- [x] L0 in-memory `BlockHash → BlockMeta` index.
- [x] L1 SlateDB-backed metadata index that persists across restarts
      (RFC 0008 §5; unconditional open in daemon + cabi).
- [x] Per-block `LayoutTag` field for multi-engine futures
      (`ds4-v1`, `sglang-page-v1` reserved markers).

#### Embedded KV (`wombatkv-node`)

- [x] foyer hybrid (RAM + SSD) cache with zero-copy borrowed reads.
- [x] Block-prefix lookup + parallel batched GET / PUT.
- [x] Background prefetch worker (`WMBT_KV_PREFETCH_INTERVAL_MS`,
      `WMBT_KV_PREFETCH_TOP_K`).
- [x] Per-namespace LRU eviction (RFC 0009), opt-in via
      `WMBT_KV_NAMESPACE_MAX_BYTES`.
- [x] Optional block-storage compression (`WMBT_KV_BLOCK_COMPRESS=zstd`,
      level 1-22).

#### Daemon (`wombatkv-daemon`)

- [x] Three transports: SHM (myelon disruptor ring, same-host), TCP
      (length-prefixed rkyv, cross-host), HTTP/1.1 (rkyv-in-POST,
      proxy-friendly).
- [x] Universal 16-byte wire envelope (RFC 0018 Phase 1): magic + version +
      CRC32C + body length. Engine clients reject malformed envelopes
      cleanly; no orphan-decode panics.
- [x] compio per-shard runtime for TCP + HTTP listeners
      (`WMBT_KV_TCP_TPC_THREADS` / `WMBT_KV_HTTP_TPC_THREADS`, default 2).
      io_uring on Linux; kqueue on macOS.
- [x] Arena allocator for zero-copy large frames
      (`WMBT_KV_DAEMON_SHM_ARENA_PATH` / `_BYTES`).
- [x] RFC 0011 lifecycle hardening: client-liveness heartbeat (default
      500 ms write rate, 3000 ms stale threshold, 250 ms check period),
      fail-loud SHM attach (bounded `MAX_OPEN_RETRIES`, RFC 0011 P10),
      graceful shutdown drain.

#### C ABI (`wombatkv-cabi`)

- [x] Single `libwombatkv.{so,dylib}` cdylib; hand-written
      `crates/wombatkv-cabi/include/wombatkv.h` header.
- [x] `wmbt_kv_init_from_env()` resolves backend from env at handle-open:
      embedded → SHM → TCP → HTTP priority order.
- [x] Block-prefix hot path: `wmbt_kv_lookup_block_prefix` /
      `_get_kv_blocks_borrowed` / `_put_kv_blocks` / `_release_borrow`.
- [x] Sidecar raw_tail: `wmbt_kv_put_raw_tail` / `_get_raw_tail_borrowed`.
- [x] Hardware-dispatched primitives: `wmbt_kv_crc32c_append` (Castagnoli
      via the `crc32c` Rust crate, runtime dispatch to x86_64 SSE4.2,
      ARMv8.1 CRC32, or software-table fallback), `wmbt_kv_blake3_64hex`.
- [x] ds4 reference integration documented in
      [`ds4/WOMBATKV.md`](https://github.com/Venkat2811/ds4/blob/main/WOMBATKV.md).

#### Env var naming convention

- [x] All env vars prefixed `WMBT_KV_*`.
- [x] Three reserved category sub-prefixes: `WMBT_KV_TEST_*`,
      `WMBT_KV_DST_*`, `WMBT_KV_BENCH_*`.
- [x] Functional sub-prefixes (`S3_`, `PUFFER_`, `DAEMON_*`, `TCP_`, `HTTP_`,
      `SLATEDB_`) optional when the component has clustered knobs.
- [x] Time units in name (`_MS` / `_SECS`); bytes (`_BYTES`); booleans
      default-off + opt-in by setting; default-on knobs that DISABLE end in
      `_DISABLE`.
- See `book/src/operations/env.md` for the full 78-var inventory.

#### Deterministic simulation testing (DST)

- [x] 20 failure classes covering storage, concurrency, restart, transport
      chaos, wire-envelope corruption, version-recovery, multi-tenant
      conflicts, and daemon-startup configuration mismatch (the
      `DaemonTpcOnlyNoShmClient` class catches the `--tpc` + TCP-only
      misconfiguration).
- [x] Cross-process child runner (`wombatkv-dst-runner-child`) loads
      `FaultPlan` via env, drives a 20-op oracle-checked sequence, emits
      structured `ChildReport` for parent diff.
- [x] Sweep harness (`scripts/dst-sweep.sh`): N seeds × all classes for CI.

### Not in this release (deferred)

- [ ] Python connector (`wombatkv-py`) for vLLM / SGLang. The daemon's wire
      protocol (rkyv over TCP / HTTP) is language-agnostic; Python engines
      will use it directly without going through `wombatkv-cabi`.
- [ ] Paged-attention block layout (`sglang-page-v1` `LayoutTag` is reserved
      but no production callers; the chain-shaped block API serves ds4 +
      llama.cpp + Ollama only).
- [ ] Helm chart / production deployment story.
- [ ] Prometheus / OpenTelemetry metrics export from the daemon (hooks exist
      via the `metrics` crate facade; full exporter wiring is deferred to a follow-up release).
- [ ] Hugepages-backed SHM segments; NUMA-aware placement.

### Acknowledgements

- **[antirez/ds4](https://github.com/antirez/ds4)** by Salvatore Sanfilippo -
  the inference engine WombatKV's first integration plugs into. The ds4
  fork in `Venkat2811/ds4` carries the `#ifdef DS4_WOMBATKV` hooks.
- **[`myelon`](https://github.com/Venkat2811/myelon)** by Venkat Raman -
  the ultra-low-latency cross-process transport layer behind the daemon's
  SHM ring. `wombatkv-daemon` builds on `myelon`'s disruptor-mp + compio
  primitives.
- **[SlateDB](https://github.com/slatedb/slatedb)**, the L1 metadata-index
  backing the radix index.
- **[foyer](https://github.com/foyer-rs/foyer)**, the RAM + SSD hybrid
  cache backing the read path.
- Codex (OpenAI) and Claude Code (Anthropic) shared the agentic-engineering
  load across the development cycle. See the README's "Built with" section.

[0.1.0-alpha.pre1.0]: https://github.com/Venkat2811/wombatkv/releases/tag/v0.1.0-alpha.pre1.0
