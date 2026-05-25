# Environment variables

Reference for every `WMBT_KV_*` (and a few `AWS_*`) env var WombatKV
reads at runtime. Four tiers by who should ever set them:

- **Tier 1, Quickstart:** the small set you need to bring up each mode. Set these.
- **Tier 2, Operator:** per-deployment knobs (cache sizing, multi-tenant namespacing, daemon connection details). For production.
- **Tier 3, Advanced:** performance tuning, retry budgets, transport details. Touch only with a specific workload and a benchmark to justify the change.
- **Tier 4, Internal:** test, DST, bench, observability. Not part of the user-facing surface; do not set in production.

Anything not listed here is dead code or a doc-comment fragment.
Open an issue if you find one missed.

---

## Env var naming convention

All WombatKV env vars follow these rules. New env vars MUST conform.

1. **Every WombatKV env var starts with `WMBT_KV_`.** No exceptions.
   (System / framework envs that WombatKV merely reads: `HOME`,
   `HOSTNAME`, `AWS_*`, `MYELON_*`, are documented but not under
   the WombatKV namespace.)

2. **Three reserved category sub-prefixes are required for those
   categories:**
   - `WMBT_KV_TEST_*`, test-only env vars (skip-tests gates, fixture
     selectors). Not part of the production surface.
   - `WMBT_KV_DST_*`. Deterministic Simulation Testing harness knobs.
   - `WMBT_KV_BENCH_*`, benchmark-binary knobs.

3. **Other sub-prefixes are optional functional groupings** for
   components with a coherent cluster of knobs. Today these include
   `S3_*`, `PUFFER_*`, `BLOCK_COMPRESS*`, `DAEMON_*` /
   `DAEMON_SHM_*`, `TCP_*`, `HTTP_*`, `SLATEDB_*`, `PREFETCH_*`,
   `EMBEDDED_*`, `REMOTE_*`. Use these when the component has
   multiple knobs; don't invent a sub-prefix for a singleton.

4. **Singletons stay at root.** `WMBT_KV_BUCKET`, `WMBT_KV_NAMESPACE`,
   `WMBT_KV_FINGERPRINT24`, `WMBT_KV_TIMING`, `WMBT_KV_TIER_EVENTS`,
   `WMBT_KV_QUIET_BANNER`, `WMBT_KV_LOCAL_DEV` are cross-cutting or
   one-off knobs that don't belong to any functional cluster.

5. **Units in the name.** If the value carries a time unit, append
   `_MS` or `_SECS`. Bytes: `_BYTES`. Counts: no suffix.

6. **Booleans: opt-in by setting.** Most boolean env vars are
   default-off and accept `1` / `true` / `on` / `yes` to enable.
   When a feature is default-on and the env var DISABLES it, the
   env name must end in `_DISABLE` (see
   `WMBT_KV_DAEMON_SHM_HEARTBEAT_DISABLE`).

---

## Mode trigger and minimum env per mode

WombatKV has four end-to-end modes. The mode is selected by which
env vars are set at handle-open time.

| mode | trigger (ds4) | trigger (other engines via C ABI) |
|---|---|---|
| **1. Native** (no WombatKV) | (unset `DS4_WOMBATKV_*`) | (do not load `libwombatkv.dylib`) |
| **2. Embedded** (in-process foyer + S3) | `DS4_WOMBATKV_ENABLE=1` | `wmbt_kv_init_from_env()` with `WMBT_KV_S3_*` set and `WMBT_KV_REMOTE_PREFIX` / `WMBT_KV_TCP_ADDR` / `WMBT_KV_HTTP_ADDR` unset |
| **3. Daemon SHM** (same-host) | `DS4_WOMBATKV_ENABLE=1` + `WMBT_KV_REMOTE_PREFIX=<name>` | `WMBT_KV_REMOTE_PREFIX=<name>` |
| **4. Daemon TCP** (cross-host) | `DS4_WOMBATKV_DAEMON_TCP=<host:port>` (preferred shortcut) | `wmbt_kv_open_tcp("<host:port>")` or `WMBT_KV_TCP_ADDR=<host:port>` |
| **5. Daemon HTTP** (cross-host, load-balancer / proxy friendly) | `DS4_WOMBATKV_DAEMON_HTTP=<host:port>` (preferred shortcut) | `wmbt_kv_open_http("<host:port>")` or `WMBT_KV_HTTP_ADDR=<host:port>` |

**Minimum env per mode (ds4-side):**

```bash
# Mode 1, native (no env needed)
./ds4-server --model ... --port 8000

# Mode 2, embedded with MinIO loopback
DS4_WOMBATKV_ENABLE=1 \
WMBT_KV_S3_ENDPOINT=http://127.0.0.1:9000 \
AWS_ACCESS_KEY_ID=minioadmin \
AWS_SECRET_ACCESS_KEY=minioadmin \
WMBT_KV_BUCKET=ds4-kv \
  ./ds4-server --model ... --port 8000

# Mode 3, daemon SHM (two terminals, same host)
# Terminal A, daemon:
WMBT_KV_S3_ENDPOINT=http://127.0.0.1:9000 \
AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
WMBT_KV_BUCKET=ds4-kv \
  wombatkv-daemon --prefix ds4daemon

# Terminal B, engine:
DS4_WOMBATKV_ENABLE=1 \
WMBT_KV_REMOTE_PREFIX=ds4daemon \
  ./ds4-server --model ... --port 8000

# Mode 4, daemon TCP (cross-host)
# Daemon on host B (Linux box):
WMBT_KV_S3_ENDPOINT=http://127.0.0.1:9000 \
AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
WMBT_KV_BUCKET=ds4-kv \
  wombatkv-daemon --tcp 0.0.0.0:7878

# Engine on host A (Mac):
DS4_WOMBATKV_DAEMON_TCP=192.168.x.x:7878 \
  ./ds4-server --model ... --port 8000
```

Engine-side env-var counts (excluding the S3 / bucket vars that
move to the daemon side in modes 3 + 4):

| mode | engine-side env vars |
|---|---|
| 1, native | 0 |
| 2, embedded | 5 (enable + endpoint + 2 creds + bucket) |
| 3, daemon SHM | 2 (enable + remote-prefix) |
| 4, daemon TCP | 1 (`DS4_WOMBATKV_DAEMON_TCP`) |

---

## Tier 1. Quickstart

Set these to bring up modes 2-4. Defaults exist for everything else.

| env var | type | meaning |
|---|---|---|
| `DS4_WOMBATKV_ENABLE` | `0`/`1` | ds4: load `libwombatkv.dylib`. Set to `1` for modes 2, 3. |
| `DS4_WOMBATKV_DAEMON_TCP` | `host:port` | ds4: shortcut to mode 4 (calls `wmbt_kv_open_tcp` directly). |
| `WMBT_KV_S3_ENDPOINT` | URL | S3-compatible endpoint. Alias: `AWS_ENDPOINT_URL_S3`. Required for modes 2 + daemon-side of 3/4. |
| `WMBT_KV_S3_ACCESS_KEY` | string | S3 access key. Alias: `AWS_ACCESS_KEY_ID`. |
| `WMBT_KV_S3_SECRET_KEY` | string | S3 secret key. Alias: `AWS_SECRET_ACCESS_KEY`. |
| `WMBT_KV_BUCKET` | string | S3 bucket name. Without this, the substrate derives a name with a warning, set it explicitly in any non-throwaway setup. |
| `WMBT_KV_NAMESPACE` | string | Multi-tenant namespace key. Default `default`. Set per engine instance to keep tenants isolated. |
| `WMBT_KV_REMOTE_PREFIX` | string | Mode 3 trigger. Connect to a `wombatkv-daemon --prefix <name>` running on the same host. |
| `WMBT_KV_TCP_ADDR` | `host:port` | Mode 4 trigger (low-level; `DS4_WOMBATKV_DAEMON_TCP` is the ds4-side shortcut). |

`WMBT_KV_LOCAL_DEV=1`, opt-in if your endpoint isn't `127.0.0.1` AND
you're using the default `minioadmin/minioadmin` credentials. The
production-safety check refuses default creds on non-loopback
endpoints unless this is set.

---

## Tier 2. Operator

Per-deployment knobs. Defaults are correct for >95% of cases; touch
these to right-size the cache or wire up multi-tenant routing.

### Cache sizing (puffer / foyer)

| env var | type | meaning |
|---|---|---|
| `WMBT_KV_PUFFER_DIR` | path | Directory for foyer's on-disk cache. Default `~/.wombatkv/puffer` (auto-created). |
| `WMBT_KV_PUFFER_RAM_BYTES` | bytes | RAM-tier cache budget. Default scaled from system memory. |
| `WMBT_KV_PUFFER_DISK_BYTES` | bytes | SSD-tier cache budget. Default scaled from available disk. |
| `WMBT_KV_NAMESPACE_MAX_BYTES` | bytes | Enable per-namespace LRU eviction. Default unset = no eviction. |
| `WMBT_KV_DAEMON_EVICTION_INTERVAL_SECS` | int | LRU eviction worker tick. Default 30. Renamed from `WMBT_KV_EVICTION_INTERVAL_SECS` (daemon-only knob now under the `DAEMON_*` group). |

### S3 placement

| env var | type | meaning |
|---|---|---|
| `WMBT_KV_S3_PREFIX` | string | Key-prefix within the bucket. Default `kv/cabi`. Use to share one bucket across multiple WombatKV deployments. |
| `WMBT_KV_S3_REGION` | string | S3 region. Alias: `AWS_REGION` / `AWS_DEFAULT_REGION`. Default `us-east-1`. |
| `WMBT_KV_SLATEDB_PATH` | path | Where SlateDB stores its L1 metadata index. Default under `WMBT_KV_PUFFER_DIR`. |

### Daemon transport: SHM (mode 3)

| env var | type | meaning |
|---|---|---|
| `WMBT_KV_DAEMON_SHM_PREFIX` | string | Server-side prefix. Must match the `--prefix` flag on the daemon and the `WMBT_KV_REMOTE_PREFIX` on the client. |
| `WMBT_KV_DAEMON_SHM_DEPTH` | int | Disruptor ring depth. Default 16. Increase to absorb burstier traffic. |
| `WMBT_KV_DAEMON_SHM_HEARTBEAT_DISABLE` | `0`/`1` | Disable the client-liveness heartbeat (default on, heartbeat detects dead SHM clients). Set to `1`/`true`/`on`/`yes`/`enable`/`enabled` to disable. Renamed from `WMBT_KV_DAEMON_SHM_HEARTBEAT` (which had inverted semantics, env name didn't match what setting it did). |
| `WMBT_KV_DAEMON_SHM_HEARTBEAT_DIR` | path | Where heartbeat files live. Default under `/tmp`. Override in containers. |

### Daemon transport: TCP (mode 4)

| env var | type | meaning |
|---|---|---|
| `WMBT_KV_TCP` | `addr1[,addr2,...]` | Convenience equivalent of `--tcp <addr>` for launch scripts. |
| `WMBT_KV_TCP_DISPATCH_WORKERS` | int | Sync-worker pool size behind the compio bridge. Default 8. |
| `WMBT_KV_REMOTE_CALL_TIMEOUT_MS` | int | Per-RPC timeout on the client side. Default sensible for LAN. |

### Daemon transport: HTTP (mode 5, Same `WireRequest` / `WireResponse` rkyv envelope as the TCP transport,
wrapped in HTTP/1.1 POSTs to `/wmbt/v1/rpc`. Use when the link between
engine and daemon goes through an HTTP-aware load balancer or reverse
proxy that won't pass raw rkyv-over-TCP cleanly.

| env var | type | meaning |
|---|---|---|
| `WMBT_KV_HTTP_ADDR` | `host:port` | Mode 5 trigger on the client side (low-level; `DS4_WOMBATKV_DAEMON_HTTP` is the ds4-side shortcut). |
| `WMBT_KV_HTTP` | `addr1[,addr2,...]` | Daemon-side: convenience equivalent of `--http <addr>` for launch scripts. |
| `WMBT_KV_HTTP_DISPATCH_WORKERS` | int | Sync-worker pool size behind the compio bridge. Default 8. |
| `WMBT_KV_HTTP_TPC_THREADS` | int | Per-shard compio runtime thread count. Default **2** (compio is the only runtime). Set ≥3 for more SO_REUSEPORT shards on high-concurrency clients. See "Daemon HTTP, compio runtime" below. |

The HTTP wire (in the current envelope) carries the RFC 0018 universal envelope:
`[magic 'WMBT' 4][version u32 LE][crc32c u32 LE][len u32 LE][rkyv body]`.
Same envelope as the TCP wire, single shared `envelope` module.

---

## Tier 3. Advanced

Performance tuning. Don't set without a specific benchmark and a
hypothesis. Defaults are tuned for the cell-B
headline (104.8× / 75ms warm restore on Mac M3 Max + native MinIO).

### Block & prefetch

| env var | type | default | meaning |
|---|---|---|---|
| `WMBT_KV_BLOCK_TOKENS` | int | 128 | Token-alignment block size. Must match across save + restore. |
| `WMBT_KV_PREFETCH_INTERVAL_MS` | int (ms) | 30000 | Prefetch worker tick. Set to `0` to disable. Renamed from `WMBT_KV_PREFETCH` (missing unit suffix; the value is always milliseconds). |
| `WMBT_KV_PREFETCH_TOP_K` | int | 64 | Blocks per prefetch cycle. Should cover typical chain length. |

### Compression

One layer, two env vars (algo + level):

| env var | type | default | scope |
|---|---|---|---|
| `WMBT_KV_BLOCK_COMPRESS` | `zstd \| lz4 \| off` | `zstd` | Block-storage compression at the object-store boundary. Default-on so wire bytes shrink ~3-4× on typical KV payloads. |
| `WMBT_KV_BLOCK_COMPRESS_LEVEL` | int 1-22 | 3 | zstd level for block compression. |
| `WMBT_KV_EMBEDDED_ASYNC_S3` | `0`/`1` | `1` (on) | Detach S3 PUT from the put-kv return path so the engine can resume immediately after foyer absorbs the bytes. |

### S3 fine-tuning

| env var | type | default | meaning |
|---|---|---|---|
| `WMBT_KV_S3_GET_RETRIES` | int | 3 | Retry budget for transient S3 GETs. |
| `WMBT_KV_S3_RETRY_BACKOFF_MS` | int | 100 | Initial backoff between retries. |
| `WMBT_KV_S3_RUNTIME_THREADS` | int | derived | Tokio runtime size for S3 calls. |
| `WMBT_KV_S3_PREWARM` | int | 0 | Parallelism for opt-in cold-start S3 prewarm. |

### Daemon SHM, heartbeat tuning

| env var | type | default | meaning |
|---|---|---|---|
| `WMBT_KV_DAEMON_SHM_HEARTBEAT_INTERVAL_MS` | int | **500** | Client write rate. (Source: `DEFAULT_HEARTBEAT_INTERVAL_MS` in `lifecycle.rs:22`.) |
| `WMBT_KV_DAEMON_SHM_HEARTBEAT_STALE_MS` | int | **3000** | Daemon's threshold to declare a client dead. Default = 6× interval (well above the ≥3× lower bound). (`DEFAULT_HEARTBEAT_STALE_MS` in `lifecycle.rs:23`.) |
| `WMBT_KV_DAEMON_SHM_HEARTBEAT_CHECK_MS` | int | **250** | Daemon scan period. Default = ½ interval, so a dead client is detected within ~`stale_ms + check_ms` after its last heartbeat. (`DEFAULT_HEARTBEAT_CHECK_MS` in `lifecycle.rs:24`.) |

### Daemon SHM, arena (large-payload zero-copy)

| env var | type | meaning |
|---|---|---|
| `WMBT_KV_DAEMON_SHM_ARENA_PATH` | path | Backing file for the SHM arena. Setting this enables the arena. |
| `WMBT_KV_DAEMON_SHM_ARENA_BYTES` | bytes | Explicit arena size. |
| `WMBT_KV_DAEMON_SHM_ARENA_MIN_BYTES` | bytes | Floor for auto-sized arena. Default 1 MiB. |
| `WMBT_KV_DAEMON_SHM_ASYNC_PUT` | `0`/`1` | Daemon-side async S3 PUT (the daemon-mode equivalent of `WMBT_KV_EMBEDDED_ASYNC_S3`). |
| `WMBT_KV_DAEMON_SHM_CALL_TIMEOUT_MS` | int | Per-RPC timeout on the daemon side. |
| `WMBT_KV_DAEMON_SHM_DAEMON_BIN` | path | Daemon binary path (used by some smoke tests to auto-spawn). |

### Foyer cache, internals

| env var | type | meaning |
|---|---|---|
| `WMBT_KV_PUFFER_BLOCK_SIZE_BYTES` | bytes | foyer-internal block size. |
| `WMBT_KV_PUFFER_BACKEND` | `block` / `file` / `hybrid` | foyer backend selection. |
| `WMBT_KV_PUFFER_BUFFER_POOL_BYTES` | bytes | foyer-internal buffer pool. |
| `WMBT_KV_PUFFER_IOURING` | `0`/`1` | Linux io_uring on/off. |

### Daemon TCP, compio runtime

| env var | type | meaning |
|---|---|---|
| `WMBT_KV_TCP_TPC_THREADS` | int | Per-shard compio runtime thread count. Default **2** (compio is the only runtime). SO_REUSEPORT shards accept across N compio threads, decoupled from dispatch worker pool via flume (`WMBT_KV_TCP_DISPATCH_WORKERS`). Bump to 4-8 for >16 concurrent clients per daemon. |

### Daemon HTTP, compio runtime

Mirror of the TCP TPC plumbing:

| env var | type | meaning |
|---|---|---|
| `WMBT_KV_HTTP_TPC_THREADS` | int | Per-shard compio runtime thread count for the HTTP listener. Default 2. Same trade-offs as `WMBT_KV_TCP_TPC_THREADS`. |

### `WMBT_KV_DAEMON_SHUTDOWN_DRAIN_TIMEOUT_SECS`

Overrides the daemon's graceful-shutdown async-PUT drain budget
(`SHUTDOWN_DRAIN_TIMEOUT`, default 10 sec, `wombatkv-daemon.rs:103`).
On SIGTERM/SIGINT the daemon waits up to this many seconds for
in-flight S3 PUTs to finish before exiting. Values <1 are ignored.

- **CI**: dial down to `1-2` sec for faster test teardown / restart.
- **Slow-S3 ops**: dial up to `30+` sec if the S3 backend has long
  PUT tail latencies (cross-region, congested AZ, etc.) so writes
  aren't truncated on graceful restart.

### `WMBT_KV_DAEMON_SHM_ATTACH_TIMEOUT_SECS`

Overrides the SHM segment-attach deadline (`ATTACH_TIMEOUT`,
default 30 sec, `wombatkv-daemon/src/lib.rs:121`). Used by both
the daemon's SHM-listener attach loop and `RemoteKvStoreClient`'s
attach path. Values <1 are ignored.

- **Slow Docker startup**: dial up to `60-120` sec on hosts where
  cold-start daemon takes longer than 30s to expose SHM segments
  (typical on first run after image pull).
- **CI fail-fast**: dial down to `5` sec to surface attach hangs
  faster than the production budget.

### `WMBT_KV_DAEMON_MAX_OPEN_RETRIES`

Overrides the SHM TPC shard's `MAX_OPEN_RETRIES` constant
(`wombatkv-daemon.rs:833`). Default 20; values <1 are ignored.
Each outer retry sleeps 250 ms; inner disruptor-mp attach retries
inside each outer attempt can take 100–1000 ms. The default
fail-loud window is ~5 seconds outer + several minutes worst-case
total wall time.

Use case:
- **CI regression tests**: `daemon_tpc_tcp_only_fail_loud.rs`
  sets this to `2` so the fail-loud `shard_error` event surfaces
  within <90s instead of ~10 min, while still exercising the same
  code path.
- **Production debugging**, operators investigating a SHM
  attach-stall can dial this down to get a faster failure message,
  rather than waiting through the default budget.

Don't set this <5 in production, transient resource contention on
busy hosts can need >2 retries to settle.

### `--tpc` flag, when to use it caveat)

The daemon CLI's `--tpc` flag flips the **SHM listener** to per-shard
compio runtime (on Linux: io_uring). The TCP and HTTP listeners
**already use compio per-shard unconditionally** (defaults
`WMBT_KV_TCP_TPC_THREADS=2`, `WMBT_KV_HTTP_TPC_THREADS=2`), they do
NOT need `--tpc`.

**Use `--tpc` only when SHM clients are co-located on the daemon host.**
The canonical case: loopback embedded-mode integration where the engine
binary runs on the same machine as the daemon and uses the SHM
transport. There, `--tpc` gives the SHM data plane the same per-shard
io_uring runtime that TCP/HTTP get by default.

**Don't use `--tpc` on TCP-only or HTTP-only cross-host deployments**
(daemon on one host, engine on another, all traffic over TCP/HTTP).
The SHM shard will try to attach to `wk<prefix>r` / `wk<prefix>s`
segments that never get created (no local engine to create them);
after the SHM-attach retries exhaust, the daemon exits. Regression
coverage:
`crates/wombatkv-daemon/tests/daemon_tpc_tcp_only_fail_loud.rs`
plus the DST class `DaemonTpcOnlyNoShmClient`.

### Misc

| env var | type | meaning |
|---|---|---|
| `WMBT_KV_FINGERPRINT24` | 48-hex chars | Active-model digest for prefetch affinity scoring. Zero-filled when unset. |
| `WMBT_KV_QUIET_BANNER` | `0`/`1` | Suppress the once-per-process alpha banner. |
| `WMBT_KV_PREFETCH_DRY_RUN` | `0`/`1` | Prefetch worker logs candidates without materializing them. Useful for tuning `WMBT_KV_PREFETCH_TOP_K` against a workload without paying the actual GET cost. |

---

## Defaults audit, what to flip for production (deferred, not done)

These are env-gated knobs whose current defaults are **defensible for
alpha** but worth reconsidering before the OSS 1.0 cut. None of these
are currently flipped, listed here so the decision isn't lost.

| env var | current default | argument for flipping | risk |
|---|---|---|---|
| `WMBT_KV_TCP_TPC_THREADS` | `2` (compio) | FLIPPED 1: std::net path deleted, compio always on; bump for >16 concurrent clients | larger N wakes more compio threads on tiny deployments |
| `WMBT_KV_HTTP_TPC_THREADS` | `2` (compio) | FLIPPED 1: same as TCP | same |
| `WMBT_KV_BLOCK_COMPRESS` | unset (off) | zstd cuts S3 bytes ~3-5× for typical KV; transparent at read | wire change, old uncompressed S3 objects + new compressed ones must coexist (envelope already handles this, but bucket-level mix complicates audits) |
| `WMBT_KV_NAMESPACE_MAX_BYTES` | unset (unbounded) | production deployments leak S3 forever without a cap | flips behavior under existing deployments, they'd see eviction |
| `WMBT_KV_PUFFER_IOURING` | `0` (off) | Linux io_uring is faster for the foyer disk tier | needs Linux + recent kernel; default-on must include a runtime check |

When we make any of these defaults flip, it's a single-commit change
+ ENV.md update + CHANGELOG entry, not deserving of its own RFC.

## Tier 4. Internal

Test, DST, bench, and observability instrumentation. NOT part of the
user-facing surface.

### Test gates

| env var | meaning |
|---|---|
| `WMBT_KV_TEST_EMBED_LIVE` | Set to `1` to enable the live MinIO integration test in `crates/wombatkv-node/tests/embed_live_minio.rs`. Off by default so unit-test runs stay deterministic. |

### DST. Deterministic Simulation Testing

| env var | meaning |
|---|---|
| `WMBT_KV_DST_BUGGIFY` | Set to `1` to activate the buggify chaos sites in production code paths. Off in normal builds. |
| `WMBT_KV_DST_BUGGIFY_SEED` | RNG seed for the buggify activation/fire dice. |
| `WMBT_KV_DST_BUGGIFY_ACTIVATION_PERCENT` | What fraction of buggify sites are "active" for this seed. |
| `WMBT_KV_DST_BUGGIFY_FIRE_PERCENT` | When active, what fraction of calls actually fire. |

### Bench harnesses

| env var | meaning |
|---|---|
| `WMBT_KV_BENCH_NAMESPACE` | Namespace override for the synthetic bench binaries. |
| `WMBT_KV_BENCH_OPS` | Op count for the synthetic bench. |
| `WMBT_KV_BENCH_PAYLOAD_KIB` | Payload size for the synthetic bench. |
| `WMBT_KV_BENCH_S3_PREFIX` | S3 prefix for the synthetic bench. |

### Observability

| env var | meaning |
|---|---|
| `WMBT_KV_TIMING` | Set to `1` to emit `[MyelonInstr]` per-call timing JSON lines for every key store operation. Off by default; bench-only. |
| `WMBT_KV_TIER_EVENTS` | Set to `1` to emit per-load tier-attribution events (which cache tier served the hit). |
| `WMBT_KV_METRICS_MAX_SAMPLES` | Cap on in-memory metrics-aggregator sample count. |

### Daemon-SHM trace

| env var | meaning |
|---|---|
| `WMBT_KV_DAEMON_SHM_TRACE_DAEMON` | Trace daemon-side ring-state transitions. |
| `WMBT_KV_DAEMON_SHM_TRACE_HB` | Trace heartbeat write/read events. |
| `WMBT_KV_DAEMON_SHM_TRACE_UNLINK` | Trace SHM segment cleanup. |

---

## Naming conventions

`WMBT_KV_*` sub-prefixes used in this doc:

| prefix | scope |
|---|---|
| (none) | user-facing (Tiers 1-3) |
| `WMBT_KV_S3_*` | S3 storage layer |
| `WMBT_KV_PUFFER_*` | foyer cache layer |
| `WMBT_KV_DAEMON_SHM_*` | daemon SHM transport |
| `WMBT_KV_TCP_*` | daemon TCP transport |
| `WMBT_KV_PREFETCH_*` | block prefetch worker |
| `WMBT_KV_BLOCK_*` | block-prefix API (the only KV-payload path) |
| `WMBT_KV_TEST_*` | test-only (Tier 4) |
| `WMBT_KV_DST_*` | deterministic simulation testing (Tier 4) |
| `WMBT_KV_BENCH_*` | bench harness internal (Tier 4) |
