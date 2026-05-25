#![deny(unsafe_code)]
//! `WombatKV` puffer daemon: SHM transport.
//!
//! Listens on one or more pairs of myelon SHM rings (req + resp) and
//! serves the same put/get/restore/ping/stats/clear operations as the
//! UDS daemon. All ring pairs share a single `WombatKVKvStore`
//! (foyer + S3) so multiple co-located inference engines see the same
//! cache, that is the daemon multi-tenancy story.
//!
//! Usage:
//!   # Single engine (back-compat):
//!   wombatkv-daemon [--prefix <name>]
//!
//!   # Multiple engines on one host, all sharing one foyer:
//!   wombatkv-daemon --prefix engineA --prefix engineB --prefix engineC
//!
//! The first `--prefix` flag wins over `WMBT_KV_DAEMON_SHM_PREFIX`. If the
//! flag is repeated, ALL prefixes are served. With zero `--prefix` flags
//! the daemon falls back to `WMBT_KV_DAEMON_SHM_PREFIX` or `default`.
//!
//! Required env: `WMBT_KV_S3`_*, optional `WMBT_KV_PUFFER`_*.

use std::collections::HashMap;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use bytes::Bytes;
use myelon::transport::ReassemblyBuffer;
use wombatkv_daemon::{
    cleanup_prefix_segments, constants::SHUTDOWN_DRAIN_TIMEOUT, decode_get_kv_blocks_batch_req,
    decode_key_batch, decode_lookup_block_prefix_req, decode_put_kv_blocks_batch_req,
    encode_bytes_batch, encode_get_kv_blocks_batch_resp, encode_key_batch,
    encode_lookup_block_prefix_resp, encode_put_kv_blocks_batch_resp, fits_one_frame,
    op as op_codes, runtime_tpc, segment_names, status, ArenaWriter, GetKvBlocksBatchResp,
    HeartbeatMonitor, LookupBlockPrefixResp, PutKvBlocksBatchResp, WireRequest, WireResponse,
    DEFAULT_ARENA_BYTES, FRAME_DATA_BYTES,
};
use wombatkv_node::embed::{EmbedConfig, GetOutcome, HitTier, WombatKVKvStore};
use wombatkv_node::embed_metrics::metrics;
use wombatkv_node::foyer_cache::FoyerCacheConfig;
use wombatkv_radix::{BlockMeta, MetadataIndex, SlateDbMetadataIndex};
use wombatkv_store::wal_store::{S3ObjectStore, S3ObjectStoreConfig};

/// Object-key namespace for content-addressed block payloads. Must
/// match `wombatkv-cabi::ffi::BLOCK_KEY_PREFIX`, both sides of the
/// daemon transport derive object-store keys from this prefix.
const BLOCK_KEY_PREFIX: &str = "wombatkv/v1/block/b3=";

/// Stack budget for each per-engine SHM worker thread.
const SHM_THREAD_STACK_BYTES: usize = 128 * 1024 * 1024;
const LARGE_MANIFEST_MAGIC: &str = "WMBT_KV_SHM_LARGE_V1";
const LARGE_CHUNK_KEY_PREFIX: &str = "__wmbt_kv_shm_large";
const ARENA_PATH_ENV: &str = "WMBT_KV_DAEMON_SHM_ARENA_PATH";
const ARENA_BYTES_ENV: &str = "WMBT_KV_DAEMON_SHM_ARENA_BYTES";
const ARENA_MIN_BYTES_ENV: &str = "WMBT_KV_DAEMON_SHM_ARENA_MIN_BYTES";
const DEFAULT_ARENA_MIN_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, Copy)]
struct ArenaSlab {
    offset: u64,
    len: u32,
    tier: u8,
}

struct ArenaShared {
    writer: ArenaWriter,
    slabs: HashMap<String, ArenaSlab>,
    min_bytes: usize,
}

type SharedArena = Arc<Mutex<ArenaShared>>;

#[derive(Debug, Clone)]
struct LargeManifest {
    id: String,
    total_len: usize,
    chunk_count: usize,
}

/// Process-wide graceful-shutdown flag. Set by the signal handler thread
/// on SIGTERM/SIGINT (and SIGHUP on Unix). The per-prefix `serve_prefix`
/// worker polls this between request batches so it can break out of its
/// inner loop cleanly, in-flight requests are dispatched and ACK'd
/// before exit, the SHM segments are unlinked, and main joins on each
/// worker before draining async-PUT threads and closing the `SlateDB`
/// index.
///
/// Implemented as an `OnceLock` so we can take a `&'static AtomicBool`
/// reference into the signal-handling thread without `Arc` ceremony.
fn shutdown_flag() -> &'static AtomicBool {
    static FLAG: OnceLock<AtomicBool> = OnceLock::new();
    FLAG.get_or_init(|| AtomicBool::new(false))
}

/// Resolves the effective async-PUT shutdown-drain timeout. Reads
/// `WMBT_KV_DAEMON_SHUTDOWN_DRAIN_TIMEOUT_SECS` env (alpha.14+) for an
/// override, falling back to the
/// [`constants::SHUTDOWN_DRAIN_TIMEOUT`] default (10 s, covers ~5
/// sequential large S3 PUTs at 2 s each, generous for a shutdown
/// path). Each background S3 PUT is gated by `async_put_lock()`
/// (one heavy I/O at a time); past the budget we log "drain timeout"
/// and exit anyway so SIGKILL escalation is unnecessary.
///
/// Override use cases:
///   - CI: dial down to 1-2 sec for faster test teardown.
///   - Slow-S3 ops: dial up to 30+ sec if the S3 backend has long
///     PUT tail latencies (cross-region, congested AZ).
/// Values <1 are ignored (default kept).
fn effective_shutdown_drain_timeout() -> Duration {
    std::env::var("WMBT_KV_DAEMON_SHUTDOWN_DRAIN_TIMEOUT_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|n| *n >= 1)
        .map_or(SHUTDOWN_DRAIN_TIMEOUT, Duration::from_secs)
}

/// Tracks the number of in-flight async-PUT worker threads. Bumped by
/// the daemon's PUT handler before spawn and decremented by the worker
/// closure on exit. The graceful shutdown path polls this to wait for
/// the queue to fully drain before closing the `SlateDB` index.
fn async_put_inflight() -> &'static std::sync::atomic::AtomicUsize {
    static COUNT: OnceLock<std::sync::atomic::AtomicUsize> = OnceLock::new();
    COUNT.get_or_init(|| std::sync::atomic::AtomicUsize::new(0))
}

/// Install a SIGTERM/SIGINT/SIGHUP handler that flips the
/// [`shutdown_flag`] atomic. On Unix we use `sigwait` on a dedicated
/// thread, async-signal-safe by design (the wait thread is the only
/// signal recipient, the main and worker threads ignore the signal). On
/// non-Unix (we don't support Windows but the cfg keeps the build
/// clean) this is a no-op.
///
/// We do NOT call any async-signal-unsafe code from the wait thread -
/// only `AtomicBool::store` is invoked, and the JSON event is emitted
/// AFTER `sigwait` returns (i.e. outside the OS signal-handler context),
/// which is the documented-safe pattern.
fn install_shutdown_signal_handlers() {
    #[cfg(unix)]
    {
        // Spawn the signal-wait thread up front so the main thread and
        // workers can mask SIGTERM/SIGINT/SIGHUP via the process-wide
        // default mask. We don't mask explicitly because std-spawned
        // threads inherit the process mask, and `sigwait` requires the
        // signal be blocked in the calling thread. The simplest portable
        // posture: block these signals on this thread before sigwait.
        thread::Builder::new()
            .name("wombatkv-shutdown-signal".to_string())
            .spawn(move || {
                // SAFETY: sigemptyset / sigaddset / pthread_sigmask /
                // sigwait are all async-signal-safe libc calls that
                // operate on caller-owned stack data. We do not touch
                // any shared mutable state from inside the signal
                // delivery, only `AtomicBool::store` (lock-free, safe).
                #[allow(unsafe_code)]
                unsafe {
                    let mut set: libc::sigset_t = std::mem::zeroed();
                    libc::sigemptyset(&raw mut set);
                    libc::sigaddset(&raw mut set, libc::SIGTERM);
                    libc::sigaddset(&raw mut set, libc::SIGINT);
                    libc::sigaddset(&raw mut set, libc::SIGHUP);
                    // Block these on this thread so sigwait can collect
                    // them. Other threads inherit whatever the parent
                    // had; on a fresh daemon process no thread has a
                    // custom handler, so the kernel queues to this one.
                    let _ = libc::pthread_sigmask(libc::SIG_BLOCK, &raw const set, std::ptr::null_mut());
                    let mut sig: i32 = 0;
                    loop {
                        let rc = libc::sigwait(&raw const set, &raw mut sig);
                        if rc != 0 {
                            // EINTR or similar, retry. We never want
                            // to exit this loop without a signal.
                            continue;
                        }
                        break;
                    }
                    eprintln!(
                        "{{\"scope\":\"wombatkv_shm_daemon\",\"event\":\"shutdown_signal\",\"signal\":{sig}}}"
                    );
                    shutdown_flag().store(true, Ordering::SeqCst);
                }
            })
            .expect("spawn shutdown-signal thread");
    }
    #[cfg(not(unix))]
    {
        // No graceful-shutdown story on non-Unix. The daemon binary is
        // not built for Windows in the embedded surface, so this branch is
        // unreachable in practice; left here so future ports don't
        // silently drop the contract.
    }
}

/// Drain in-flight async-PUT worker threads (best-effort, bounded).
///
/// The PUT handler spawns one detached `std::thread` per ACK'd async
/// write; those threads run `set_background_qos_self()` then serialize
/// on `async_put_lock()` to push their payload to S3. We can't `join`
/// them (no handles kept) so the drain counts down [`async_put_inflight`]
/// and stops when it reaches zero or [`SHUTDOWN_DRAIN_TIMEOUT`] elapses.
///
/// Returns `true` when fully drained, `false` on timeout. Either way
/// callers should proceed to close the `SlateDB` index, letting the
/// process exit without the close means stale rows in `SlateDB`.
fn drain_async_puts() -> bool {
    let start = Instant::now();
    let counter = async_put_inflight();
    let budget = effective_shutdown_drain_timeout();
    loop {
        let n = counter.load(Ordering::SeqCst);
        if n == 0 {
            eprintln!(
                "{{\"scope\":\"wombatkv_shm_daemon\",\"event\":\"async_put_drained\",\"elapsed_ms\":{}}}",
                start.elapsed().as_millis()
            );
            return true;
        }
        if start.elapsed() >= budget {
            eprintln!(
                "{{\"scope\":\"wombatkv_shm_daemon\",\"event\":\"async_put_drain_timeout\",\
                 \"inflight\":{n},\"elapsed_ms\":{},\"budget_ms\":{}}}",
                start.elapsed().as_millis(),
                budget.as_millis()
            );
            return false;
        }
        thread::sleep(Duration::from_millis(25));
    }
}

/// Best-effort `SlateDB` close at shutdown. We don't own the only `Arc`
/// reference (workers hold clones via `slatedb_index.clone()`), so a
/// successful close depends on all worker threads having dropped their
/// clones already, which is the case after the per-prefix workers have
/// joined in the main shutdown path. If `Arc::try_unwrap` fails we log
/// and continue; `Drop` will still spin down the runtime cleanly.
fn close_slatedb_best_effort(idx: Arc<SlateDbMetadataIndex>) {
    match Arc::try_unwrap(idx) {
        Ok(owned) => {
            let started = Instant::now();
            match owned.close() {
                Ok(()) => eprintln!(
                    "{{\"scope\":\"wombatkv_shm_daemon\",\"event\":\"slatedb_closed\",\
                     \"elapsed_ms\":{}}}",
                    started.elapsed().as_millis()
                ),
                Err(err) => eprintln!(
                    "{{\"scope\":\"wombatkv_shm_daemon\",\"event\":\"slatedb_close_failed\",\
                     \"err\":\"{err}\"}}"
                ),
            }
        }
        Err(_arc) => {
            eprintln!(
                "{{\"scope\":\"wombatkv_shm_daemon\",\"event\":\"slatedb_close_skipped\",\
                 \"reason\":\"outstanding_arc_clones\"}}"
            );
        }
    }
}

/// Emit the 0.1.0-alpha banner once per daemon process unless the
/// user has suppressed it with `WMBT_KV_QUIET_BANNER=1`. Mirrors the
/// embedded-mode policy in `wombatkv-cabi`.
static BANNER_EMITTED: OnceLock<()> = OnceLock::new();

fn emit_alpha_banner(cfg: &wombatkv_daemon::DaemonConfig) {
    if BANNER_EMITTED.set(()).is_err() {
        return;
    }
    if cfg.quiet_banner {
        return;
    }
    eprintln!(
        "WombatKV 0.1.0-alpha: validated on macOS M3/M4 + native MinIO. \
         Linux io_uring + cloud-S3 are beta milestones."
    );
}

/// Emit one-line stderr warnings for any experimental 0.1.0-alpha
/// capabilities the user has explicitly opted into.
///
/// Mirrors `wombatkv-cabi::ffi::emit_experimental_warnings` so daemon
/// users see the same disclaimers as embedded-mode users.
static EXPERIMENTAL_WARNINGS_EMITTED: OnceLock<()> = OnceLock::new();

fn emit_experimental_warnings(cfg: &wombatkv_daemon::DaemonConfig) {
    if EXPERIMENTAL_WARNINGS_EMITTED.set(()).is_err() {
        return;
    }
    let lru_on = cfg.namespace_max_bytes.is_some_and(|n| n > 0);
    if lru_on {
        eprintln!(
            "WombatKV: LRU eviction is experimental in 0.1.0-alpha; \
             please report behavior at https://github.com/Venkat2811/wombatkv/issues"
        );
    }
}

fn main() -> ExitCode {
    // Install the SIGTERM/SIGINT/SIGHUP handler before any other work so
    // a signal delivered while the daemon is still opening foyer/S3
    // still flips the shutdown flag. Doing this first also avoids a
    // race where serve_prefix attaches to SHM rings before the wait
    // thread has masked signals on its own thread of control.
    install_shutdown_signal_handlers();

    // Single source of truth for all WMBT_KV_* env vars the daemon
    // consumes, see crates/wombatkv-daemon/src/config.rs. CLI args
    // (parsed below) layer ON TOP of the env-derived defaults.
    let mut cfg = wombatkv_daemon::DaemonConfig::from_env();

    // Mirror the embedded-mode banner + experimental-warnings policy
    // so users running `wombatkv-daemon` directly still see the alpha
    // disclaimer plus any non-headline feature they've opted into.
    emit_alpha_banner(&cfg);
    emit_experimental_warnings(&cfg);

    // (DST Stage 3.5) If a fault plan is supplied via DST_FAULT_PLAN_FILE,
    // load it + install in the global dst_plan state so production code
    // paths (embed::put_kv etc.) consult plan-aware buggify under
    // cfg(feature = "dst"). Inert when env unset OR when the binary was
    // built without --features dst.
    #[cfg(feature = "dst")]
    if let Ok(path) = std::env::var("DST_FAULT_PLAN_FILE") {
        match std::fs::read_to_string(&path) {
            Ok(json) => match serde_json::from_str::<wombatkv_dst::FaultPlan>(&json) {
                Ok(plan) => {
                    eprintln!(
                        "wombatkv-daemon[dst]: loaded plan from {} ({} events, seed={})",
                        path,
                        plan.events.len(),
                        plan.seed,
                    );
                    wombatkv_dst::dst_plan::set_plan(plan);
                }
                Err(e) => {
                    eprintln!(
                        "wombatkv-daemon[dst]: failed to parse plan {path}: {e} \
                         (continuing without plan-aware buggify)"
                    );
                }
            },
            Err(e) => {
                eprintln!(
                    "wombatkv-daemon[dst]: failed to read DST_FAULT_PLAN_FILE={path}: {e} \
                     (continuing without plan-aware buggify)"
                );
            }
        }
    }

    let mut tpc_flag = false;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--prefix" => {
                let Some(v) = args.next() else {
                    eprintln!("--prefix requires a value");
                    return ExitCode::FAILURE;
                };
                cfg.shm_prefixes.push(v);
            }
            "--tpc" => {
                // Opt-in: switch to per-shard compio runtime (RFC 0007
                // §10 P4 scaffolding). See `runtime_tpc.rs` for the
                // macOS-vs-Linux story.
                tpc_flag = true;
            }
            "--tcp" => {
                // Cross-machine: serve length-prefixed rkyv frames over
                // a TCP listener at <addr>. May be repeated to bind
                // multiple addresses (e.g. localhost + LAN ip). See
                // `tcp_transport.rs` for the wire format. The TCP path
                // shares the same WombatKVKvStore as the SHM prefixes,
                // so cross-process AND cross-machine traffic land in
                // one foyer + S3 backing store.
                let Some(v) = args.next() else {
                    eprintln!("--tcp requires an address (e.g. 0.0.0.0:7878)");
                    return ExitCode::FAILURE;
                };
                cfg.tcp_addrs.push(v);
            }
            "--http" => {
                // Cross-machine (HTTP/1.1 + rkyv): serves the same
                // WireRequest / WireResponse rkyv envelope as --tcp,
                // wrapped in HTTP/1.1 POSTs to /wmbt/v1/rpc. Useful
                // when the link is behind an HTTP-aware load balancer,
                // proxy, or middlebox that won't pass raw rkyv-over-TCP.
                // Repeatable; shares one foyer + S3 backend with --tcp
                // and --prefix.
                let Some(v) = args.next() else {
                    eprintln!("--http requires an address (e.g. 0.0.0.0:7879)");
                    return ExitCode::FAILURE;
                };
                cfg.http_addrs.push(v);
            }
            "-h" | "--help" => {
                eprintln!(
                    "usage: wombatkv-daemon [--prefix <name>]... [--tcp <addr>]... [--http <addr>]... [--tpc]\n\
                     Pass --prefix multiple times to serve N engines from one\n\
                     daemon process, all engines share one foyer + S3 store.\n\
                     Pass --tcp <addr> (e.g. 0.0.0.0:7878) to serve length-\n\
                     prefixed rkyv frames over TCP for cross-machine clients.\n\
                     Pass --http <addr> (e.g. 0.0.0.0:7879) to serve the same\n\
                     rkyv envelope wrapped in HTTP/1.1 POSTs (load-balancer\n\
                     friendly).\n\
                     --tpc switches the SHM path to the per-shard compio runtime.\n\
                          NOTE: --tpc requires SHM clients co-located on the daemon\n\
                          host. On TCP-only / HTTP-only cross-host deployments (no\n\
                          local engine), --tpc will fail-loud after the SHM TPC\n\
                          shard's attach retries exhaust (RFC 0011 P10). For cross-\n\
                          host, leave --tpc off, the TCP and HTTP listeners always\n\
                          use compio per-shard runtime regardless.\n\
                     env: WMBT_KV_S3_*, WMBT_KV_PUFFER_*, WMBT_KV_DAEMON_SHM_PREFIX, \
                          WMBT_KV_DAEMON_SHM_DEPTH, WMBT_KV_TCP, WMBT_KV_HTTP"
                );
                return ExitCode::SUCCESS;
            }
            other => {
                eprintln!("unknown arg: {other}");
                return ExitCode::FAILURE;
            }
        }
    }

    // env-gated TCP/HTTP addresses are already merged into `cfg` by
    // DaemonConfig::from_env() at the top of main; CLI flags above
    // appended to the same vecs. Same for shm_prefixes from
    // WMBT_KV_DAEMON_SHM_PREFIX.

    // Per-shard compio TPC for the SHM data plane: --tpc CLI flag opt-in.
    // No env-gate; the legacy WMBT_KV_TPC env-gate was deleted with the
    // cleanup-2 pass.
    let tpc_enabled = tpc_flag;

    if cfg.shm_prefixes.is_empty() && cfg.tcp_addrs.is_empty() && cfg.http_addrs.is_empty() {
        // No transports requested at all → fall back to the historical
        // "default" SHM prefix so legacy launches stay working. When
        // --tcp / --http is given we DON'T inject a SHM default, the
        // operator explicitly asked for those transports only.
        cfg.shm_prefixes.push("default".into());
    }

    // Fail loud at startup if any prefix would produce SHM segment names
    // that exceed the macOS portable budget, better than a cryptic
    // ENAMETOOLONG mid-mmap when the first client tries to connect.
    for prefix in &cfg.shm_prefixes {
        if let Err(err) = wombatkv_daemon::validate_segment_name_budget(prefix) {
            eprintln!("{err}");
            return ExitCode::from(2);
        }
    }

    // Local owned shorthands so the remainder of main reads naturally.
    // Vec<String>s are tiny; clone cost is negligible vs the upcoming
    // store init + SHM segment creates.
    let prefixes: Vec<String> = cfg.shm_prefixes.clone();
    let tcp_addrs: Vec<String> = cfg.tcp_addrs.clone();
    let http_addrs: Vec<String> = cfg.http_addrs.clone();
    let depth = cfg.shm_depth;
    // Captured by per-listener spawn closures (each thread gets its
    // own usize; cheap Copy semantics, no need to plumb &cfg through
    // 'static closures).
    let cfg_tcp_tpc = cfg.tcp_tpc_threads;
    let cfg_tcp_dispatch = cfg.tcp_dispatch_workers;
    let cfg_http_tpc = cfg.http_tpc_threads;
    let cfg_http_dispatch = cfg.http_dispatch_workers;

    let store = match build_store(&cfg) {
        Ok(s) => Arc::new(s),
        Err(err) => {
            eprintln!("wombatkv-daemon: failed to open store: {err}");
            return ExitCode::FAILURE;
        }
    };

    // L1 SlateDB metadata index (RFC 0008 §5): UNCONDITIONAL.
    //
    // SlateDB is the production metadata index. The daemon always:
    //   1. Opens (or creates) the on-disk SlateDB at the configured root.
    //   2. Bootstraps the in-memory `store.metadata_index()` from it so
    //      LOOKUP_BLOCK_PREFIX sees the previous process's writes.
    //   3. Performs synchronous write-through on PUT_KV_BLOCKS_BATCH so
    //      subsequent restarts can rehydrate the index.
    // Mirrors the embedded-mode path in `wombatkv-cabi::ffi::Handle::from_env`.
    let slatedb_index = match open_slatedb_index_from_env(&cfg) {
        Ok(idx) => idx,
        Err(err) => {
            eprintln!(
                "wombatkv-daemon: SlateDB open failed (continuing without persistence): {err}"
            );
            None
        }
    };
    if let Some(idx) = slatedb_index.as_ref() {
        match store.bootstrap_from_slatedb(idx.as_ref()) {
            Ok(n) => {
                eprintln!("wombatkv-daemon[slatedb]: hydrated {n} blocks into metadata index");
            }
            Err(err) => {
                eprintln!(
                    "wombatkv-daemon[slatedb]: bootstrap_from_slatedb failed \
                     (continuing): {err}"
                );
            }
        }
    }

    // LRU eviction worker (RFC 0009 §4). Opt-in via:
    //   WMBT_KV_NAMESPACE_MAX_BYTES=<N>      per-namespace cap (0 = off)
    //   WMBT_KV_EVICTION_INTERVAL_SECS=<N>   default 30
    // Daemon eviction uses the `WMBT_KV_NAMESPACE` env to scope the
    // worker; this is consistent with how the SlateDB index above is
    // scoped, so a single-namespace daemon deployment evicts cleanly.
    // The worker is bound to the handle (`_eviction_worker`); dropping
    // the handle at shutdown joins the thread.
    let _eviction_worker = spawn_daemon_eviction_worker(&store, slatedb_index.clone(), &cfg);

    let arena = match build_arena() {
        Ok(arena) => arena,
        Err(err) => {
            eprintln!("wombatkv-daemon: failed to open arena: {err}");
            return ExitCode::FAILURE;
        }
    };

    println!(
        "{{\"scope\":\"wombatkv_shm_daemon\",\"event\":\"starting\",\"prefixes\":{:?},\"depth\":{depth},\"arena_enabled\":{},\"slatedb_enabled\":{},\"tpc\":{tpc_enabled},\"tcp_addrs\":{:?},\"http_addrs\":{:?}}}",
        prefixes,
        arena.is_some(),
        slatedb_index.is_some(),
        tcp_addrs,
        http_addrs,
    );

    // Spawn TCP listeners (one std::thread per --tcp addr). Each TCP
    // connection thread calls dispatch(...) just like the SHM side.
    for addr in &tcp_addrs {
        let addr = addr.clone();
        let store_clone = Arc::clone(&store);
        let arena_clone = arena.clone();
        let slatedb_clone = slatedb_index.clone();
        let _ = thread::Builder::new()
            .name(format!("wombatkv-tcp-listen-{addr}"))
            .spawn(move || {
                let dispatch_cb = move |_conn_id: u64, req: WireRequest| -> WireResponse {
                    dispatch(
                        &store_clone,
                        arena_clone.as_ref(),
                        slatedb_clone.as_ref(),
                        req.id,
                        req,
                    )
                };
                // alpha.11+1 sprawl cleanup: the std::net + thread-per-
                // conn fallback was deleted. ONE runtime per transport:
                // compio bridge. Default WMBT_KV_TCP_TPC_THREADS=2 so
                // SO_REUSEPORT engages out of the box. Set to 1 for
                // single-thread compio (no SO_REUSEPORT, lowest mem
                // footprint); set to N>2 for higher concurrency.
                //
                // The bridge worker pool sized by
                // `WMBT_KV_TCP_DISPATCH_WORKERS` (default 8), compio
                // shards do framing only, sync workers do blocking
                // dispatch.
                // Values resolved once at startup via DaemonConfig::from_env.
                let tpc = cfg_tcp_tpc;
                let dispatch_workers = cfg_tcp_dispatch;
                let result = match addr.parse::<std::net::SocketAddr>() {
                    Ok(sa) => wombatkv_daemon::tcp_transport::serve_tcp_compio_bridge(
                        sa,
                        tpc,
                        dispatch_workers,
                        dispatch_cb,
                    ),
                    Err(e) => {
                        eprintln!(
                            r#"{{"scope":"wombatkv_tcp_daemon","event":"compio_addr_parse_failed","addr":"{addr}","error":"{e}"}}"#
                        );
                        return;
                    }
                };
                if let Err(e) = result {
                    eprintln!(
                        r#"{{"scope":"wombatkv_tcp_daemon","event":"listener_exit","addr":"{addr}","error":"{e}"}}"#
                    );
                }
            });
    }

    // Spawn HTTP listeners (one std::thread per --http addr). Same
    // dispatch closure shape as TCP, both flow into one foyer + S3
    // backend so HTTP/TCP/SHM are interchangeable transports over the
    // same data plane.
    //
    // TPC path: `WMBT_KV_HTTP_TPC_THREADS=N` (mirror of the TCP env)
    // switches to N-shard SO_REUSEPORT compio accept + decoupled
    // dispatch worker pool (sized by `WMBT_KV_HTTP_DISPATCH_WORKERS`,
    // default 8). Default 0 = std::net + thread-per-conn fallback.
    for addr in &http_addrs {
        let addr = addr.clone();
        let store_clone = Arc::clone(&store);
        let arena_clone = arena.clone();
        let slatedb_clone = slatedb_index.clone();
        let _ = thread::Builder::new()
            .name(format!("wombatkv-http-listen-{addr}"))
            .spawn(move || {
                let dispatch_cb = move |_conn_id: u64, req: WireRequest| -> WireResponse {
                    dispatch(
                        &store_clone,
                        arena_clone.as_ref(),
                        slatedb_clone.as_ref(),
                        req.id,
                        req,
                    )
                };
                // Values resolved once at startup via DaemonConfig::from_env.
                let tpc = cfg_http_tpc;
                let dispatch_workers = cfg_http_dispatch;
                let result = match addr.parse::<std::net::SocketAddr>() {
                    Ok(sa) => wombatkv_daemon::http_transport::serve_http_compio_bridge(
                        sa,
                        tpc,
                        dispatch_workers,
                        dispatch_cb,
                    ),
                    Err(e) => {
                        eprintln!(
                            r#"{{"scope":"wombatkv_http_daemon","event":"compio_addr_parse_failed","addr":"{addr}","error":"{e}"}}"#
                        );
                        return;
                    }
                };
                if let Err(e) = result {
                    eprintln!(
                        r#"{{"scope":"wombatkv_http_daemon","event":"listener_exit","addr":"{addr}","error":"{e}"}}"#
                    );
                }
            });
    }

    // If ONLY remote-transport addresses were given (no --prefix), park
    // the main thread so the daemon stays alive serving them. Park-and-
    // shutdown is signal-driven (see register_shutdown_signal_handler).
    if prefixes.is_empty() && (!tcp_addrs.is_empty() || !http_addrs.is_empty()) {
        println!(
            r#"{{"scope":"wombatkv_remote_daemon","event":"ready_remote_only","tcp_addrs":{tcp_addrs:?},"http_addrs":{http_addrs:?}}}"#
        );
        while !shutdown_flag().load(Ordering::SeqCst) {
            thread::sleep(Duration::from_millis(250));
        }
        return ExitCode::SUCCESS;
    }

    if tpc_enabled {
        run_tpc(prefixes, depth, store, arena, slatedb_index)
    } else {
        run_legacy_per_prefix(prefixes, depth, store, arena, slatedb_index)
    }
}

/// Existing path: one `std::thread` per prefix, sync `serve_prefix`.
/// Production-default until TPC is benchmarked on Linux.
fn run_legacy_per_prefix(
    prefixes: Vec<String>,
    depth: usize,
    store: Arc<WombatKVKvStore<S3ObjectStore>>,
    arena: Option<SharedArena>,
    slatedb_index: Option<Arc<SlateDbMetadataIndex>>,
) -> ExitCode {
    // Spawn one worker thread per prefix. Each worker owns its own ring
    // pair endpoints; all share the same Arc<WombatKVKvStore>, which
    // is what gives us cross-engine reuse via the shared foyer.
    let mut handles = Vec::with_capacity(prefixes.len());
    for prefix in prefixes {
        let store = store.clone();
        let arena = arena.clone();
        let slatedb_index = slatedb_index.clone();
        let prefix_for_name = prefix.clone();
        let join = thread::Builder::new()
            .name(format!("wombatkv-shm-loop-{prefix_for_name}"))
            .stack_size(SHM_THREAD_STACK_BYTES)
            .spawn(move || serve_prefix(store, prefix, depth, arena, slatedb_index))
            .map_err(|err| {
                eprintln!("wombatkv-daemon: spawn worker for {prefix_for_name}: {err}");
                err
            });
        match join {
            Ok(h) => handles.push((prefix_for_name, h)),
            Err(_) => return ExitCode::FAILURE,
        }
    }

    // Wait for all workers. If any returns an error, log it but keep
    // the others running, a transient attach failure for one engine
    // shouldn't take down the whole daemon. The process exits when ALL
    // workers exit.
    let mut any_failure = false;
    for (prefix, handle) in handles {
        match handle.join() {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                eprintln!("wombatkv-daemon[{prefix}]: worker error: {err}");
                any_failure = true;
            }
            Err(_) => {
                eprintln!("wombatkv-daemon[{prefix}]: worker panicked");
                any_failure = true;
            }
        }
    }
    run_shutdown_drain(slatedb_index);
    if any_failure {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

/// Run the post-worker-join shutdown drain: wait for in-flight async-PUT
/// background threads to finish, then close the `SlateDB` index. Called by
/// both [`run_legacy_per_prefix`] and [`run_tpc`] after their workers
/// have all returned (regardless of whether they returned via the
/// shutdown-flag path or because their client disconnected normally).
///
/// The drain runs against the process-wide `async_put_inflight()`
/// counter, which is incremented inside the PUT handler before each
/// spawn and decremented by the spawned thread on exit. The `SlateDB`
/// close is best-effort: if another part of the process still holds an
/// `Arc<SlateDbMetadataIndex>` clone we log and continue, since `Drop`
/// still spins down the runtime cleanly.
fn run_shutdown_drain(slatedb_index: Option<Arc<SlateDbMetadataIndex>>) {
    let drained = drain_async_puts();
    if !drained {
        // Async PUTs that didn't drain inside the budget will be
        // truncated on process exit. We've already logged the drain
        // timeout; nothing else to do here.
    }
    if let Some(idx) = slatedb_index {
        close_slatedb_best_effort(idx);
    }
}

/// Opt-in TPC path: one OS thread per shard, each driving its own
/// `compio::runtime::Runtime`. Shard count = number of configured
/// prefixes (1:1 mapping for the skeleton; a future landing
/// will introduce `SHARD_COUNT` > `PREFIX_COUNT` with cross-shard
/// mailbox routing).
///
/// On macOS this currently delivers correctness (kqueue reactor +
/// per-thread isolation) but NOT the `io_uring` perf wins. The shape
/// commit is what matters here. See `runtime_tpc.rs` for details.
fn run_tpc(
    prefixes: Vec<String>,
    depth: usize,
    store: Arc<WombatKVKvStore<S3ObjectStore>>,
    arena: Option<SharedArena>,
    slatedb_index: Option<Arc<SlateDbMetadataIndex>>,
) -> ExitCode {
    let num_shards = prefixes.len().max(1);
    println!(
        "{{\"scope\":\"wombatkv_shm_daemon\",\"event\":\"tpc_mode\",\"num_shards\":{num_shards},\"prefixes\":{prefixes:?}}}"
    );

    // Snapshot the prefix list into a shared `Arc<[String]>` so each
    // shard closure can pick out its own prefix by `shard_id`.
    let prefixes_arc: Arc<Vec<String>> = Arc::new(prefixes);
    let store_arc = store.clone();
    let arena_arc = arena.clone();
    let slatedb_arc = slatedb_index.clone();

    let shard_fn: Arc<runtime_tpc::ShardFn> = Arc::new(move |shard_id: usize| {
        let prefix = prefixes_arc[shard_id].clone();
        let store = store_arc.clone();
        let arena = arena_arc.clone();
        let slatedb_index = slatedb_arc.clone();
        Box::pin(async move {
            // SKELETON: the existing `serve_prefix` body is a sync
            // loop that parks on `disruptor_mp` consumer wait. We
            // wrap that here in `compio::runtime::spawn_blocking` so
            // the per-shard runtime owns its thread but the heavy
            // work doesn't yet need an async ring driver, that's
            // the daemon-mode deeper refactor (RFC 0007 §10 P5).
            //
            // The shape that matters today:
            //   - one OS thread per shard
            //   - one compio runtime per thread (kqueue on mac,
            //     io_uring on Linux)
            //   - thread is CPU-pinned on Linux (no-op on macOS)
            //   - shard future cannot migrate across threads
            //
            // When the ring is converted to a `compio::io`-backed
            // reactor primitive, the body of `serve_prefix` becomes
            // truly async and `spawn_blocking` disappears.
            let _ = compio::runtime::spawn_blocking(move || {
                if let Err(err) = serve_prefix(store, prefix, depth, arena, slatedb_index) {
                    eprintln!(
                        "{{\"scope\":\"wombatkv_shm_daemon\",\"event\":\"shard_error\",\"shard_id\":{shard_id},\"err\":{err:?}}}"
                    );
                }
            })
            .await;
        }) as runtime_tpc::ShardFuture
    });

    let handles = runtime_tpc::spawn_per_shard(num_shards, shard_fn);
    let mut any_failure = false;
    for (idx, handle) in handles.into_iter().enumerate() {
        if let Err(err) = handle.join() {
            eprintln!("wombatkv-daemon[shard-{idx}]: tpc shard thread panicked: {err:?}");
            any_failure = true;
        }
    }
    // After all shard threads have joined, the only `Arc<ShardFn>`
    // clones that ever existed (one per shard, plus the original we
    // moved into `spawn_per_shard`) are all dropped, which in turn
    // drops the move-closure's captured clones of `store_arc /
    // arena_arc / slatedb_arc`. The locally-bound `store_arc` and
    // friends were themselves consumed into the closure, so there's
    // nothing more to drop manually here, only `slatedb_index` (the
    // function-parameter Arc) remains, which is exactly what
    // `run_shutdown_drain` consumes for the SlateDB close path.
    run_shutdown_drain(slatedb_index);
    if any_failure {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

fn serve_prefix(
    store: Arc<WombatKVKvStore<S3ObjectStore>>,
    prefix: String,
    depth: usize,
    arena: Option<SharedArena>,
    slatedb_index: Option<Arc<SlateDbMetadataIndex>>,
) -> Result<(), String> {
    // Fail-loud after this many open_retry passes, beyond ~20 attempts
    // (~5 seconds outer; inner disruptor-mp attach retries can stretch
    // total wall time to several minutes) the retry-loop is masking a
    // real failure mode (SHM name budget, stale segment lingering across
    // crashes, permissions, or --tpc + TCP-only-no-SHM-client config
    // mismatch). Quiet retry-forever made these mysteries; surface the
    // error with the captured failure string so operators can act on it.
    // Matches the same fail-loud spirit as the daemon's own bucket /
    // namespace validation at startup.
    //
    // `WMBT_KV_DAEMON_MAX_OPEN_RETRIES` env override (alpha.14+) lets
    // tests + ops dial this down. CI's regression test for the
    // --tpc-on-TCP-only config mismatch sets it to 2 so the fail-loud
    // exit fires in <10 seconds instead of ~10 minutes.
    let max_open_retries: u32 = std::env::var("WMBT_KV_DAEMON_MAX_OPEN_RETRIES")
        .ok()
        .and_then(|s| s.parse::<u32>().ok())
        .filter(|n| *n >= 1)
        .unwrap_or(20);
    let mut open_retries: u32 = 0;
    loop {
        if shutdown_flag().load(Ordering::SeqCst) {
            println!(
                "{{\"scope\":\"wombatkv_shm_daemon\",\"event\":\"worker_exit_shutdown\",\"prefix\":\"{prefix}\"}}"
            );
            cleanup_prefix_segments(&prefix);
            return Ok(());
        }
        let (req_seg, resp_seg) = segment_names(&prefix);
        println!(
            "{{\"scope\":\"wombatkv_shm_daemon\",\"event\":\"opening\",\"prefix\":\"{prefix}\",\"req_seg\":\"{req_seg}\",\"resp_seg\":\"{resp_seg}\",\"depth\":{depth}}}"
        );

        let (mut req_consumer, mut resp_producer) = match wombatkv_daemon::open_daemon(
            &req_seg, &resp_seg, depth,
        ) {
            Ok(v) => {
                open_retries = 0;
                v
            }
            Err(e) => {
                open_retries += 1;
                println!(
                        "{{\"scope\":\"wombatkv_shm_daemon\",\"event\":\"open_retry\",\"prefix\":\"{prefix}\",\"attempt\":{open_retries},\"max\":{max_open_retries},\"error\":\"{}\"}}",
                        e.to_string().replace('"', "'").replace('\\', "/"),
                    );
                if open_retries >= max_open_retries {
                    return Err(format!(
                            "open_daemon for prefix '{prefix}' failed {max_open_retries} times; last error: {e}. \
                             Likely causes: SHM name budget exceeded (macOS POSIX-SHM is 31 chars including null; the disruptor-mp `_producer_seq` suffix adds 13 chars, so the daemon prefix is capped at 14 chars on macOS, see `validate_segment_name_budget` for the exact math); \
                             stale segments lingering after a prior crash (the daemon runs cleanup_prefix_segments at startup; if that's still not enough, set `WMBT_KV_DAEMON_SHM_TRACE_UNLINK=1` to log every shm_unlink call); \
                             insufficient permissions on the SHM namespace. The earlier retry-forever behavior is replaced by this fail-loud surface (see RFC 0011 P10)."
                        ));
                }
                thread::sleep(Duration::from_millis(250));
                continue;
            }
        };
        if !resp_producer
            .discover_consumer_id(wombatkv_daemon::RESP_CONSUMER_ID, Duration::from_secs(30))
        {
            println!(
                "{{\"scope\":\"wombatkv_shm_daemon\",\"event\":\"resp_attach_retry\",\"prefix\":\"{prefix}\"}}"
            );
            thread::sleep(Duration::from_millis(250));
            continue;
        }
        println!(
            "{{\"scope\":\"wombatkv_shm_daemon\",\"event\":\"ready\",\"prefix\":\"{prefix}\"}}"
        );

        let mut heartbeat = HeartbeatMonitor::attach(&prefix);
        if heartbeat.is_some() {
            println!(
                "{{\"scope\":\"wombatkv_shm_daemon\",\"event\":\"heartbeat_attached\",\"prefix\":\"{prefix}\"}}"
            );
        }
        let mut reassembly_buf = ReassemblyBuffer::new(FRAME_DATA_BYTES);
        let mut close_after_response = false;
        // Trace flag is read directly here rather than threaded from
        // DaemonConfig because this site is inside a 4-deep call/spawn
        // chain (per-prefix worker → serve_prefix → inner loop / op
        // handler); reading the env once per loop iter is a no-op
        // perf cost and dramatically simpler than plumbing a bool.
        let trace = std::env::var("WMBT_KV_DAEMON_SHM_TRACE_DAEMON").is_ok();
        loop {
            let delivered =
                req_consumer.process_available::<WireRequest, _>(&mut reassembly_buf, |_, req| {
                    let close = req.op == op_codes::CLOSE;
                    let op_name = op_codes::name(req.op);
                    let req_id = req.id;
                    let payload_len = req.payload.len();
                    let key_for_log = req.key.clone();
                    // DST chaos site (daemon request handler): buggify
                    // can sleep mid-dispatch to provoke the client-side
                    // timeout / retry path. Inert in non-dst builds.
                    #[cfg(feature = "dst")]
                    if wombatkv_dst::dst_buggify!() {
                        std::thread::sleep(std::time::Duration::from_millis(50));
                    }
                    if trace {
                        eprintln!(
                            "[daemon-trace] dispatch START id={req_id} op={op_name} key={key_for_log:?} payload={payload_len}"
                        );
                    }
                    let dispatch_t0 = Instant::now();
                    let resp = dispatch(&store, arena.as_ref(), slatedb_index.as_ref(), req.id, req);
                    let dispatch_us = dispatch_t0.elapsed().as_micros();
                    if trace {
                        eprintln!(
                            "[daemon-trace] dispatch DONE  id={req_id} op={op_name} status={} took={dispatch_us}us",
                            resp.status
                        );
                    }
                    let publish_t0 = Instant::now();
                    match resp_producer.publish(&resp, 1) {
                        Ok(()) => {
                            if trace {
                                eprintln!(
                                    "[daemon-trace] publish  OK   id={req_id} op={op_name} took={}us",
                                    publish_t0.elapsed().as_micros()
                                );
                            }
                        }
                        Err(err) => {
                            eprintln!(
                                "wombatkv-daemon[{prefix}]: publish error id={req_id} op={op_name} took={}us: {err}",
                                publish_t0.elapsed().as_micros()
                            );
                        }
                    }
                    if close {
                        close_after_response = true;
                    }
                });

            if close_after_response {
                println!(
                    "{{\"scope\":\"wombatkv_shm_daemon\",\"event\":\"client_closed\",\"prefix\":\"{prefix}\"}}"
                );
                cleanup_prefix_segments(&prefix);
                break;
            }
            if shutdown_flag().load(Ordering::SeqCst) {
                // SIGTERM/SIGINT arrived between request batches. The
                // current `process_available` call drained whatever was
                // already on the request ring (in-flight requests get
                // dispatched and ACK'd above), so it's safe to break
                // out now. We DON'T accept new requests after this
                // point, but a client that publishes between the
                // process_available return and this poll will see its
                // request remain on the ring until daemon restart, by
                // design (graceful shutdown stops accepting new work).
                println!(
                    "{{\"scope\":\"wombatkv_shm_daemon\",\"event\":\"client_loop_exit_shutdown\",\"prefix\":\"{prefix}\"}}"
                );
                cleanup_prefix_segments(&prefix);
                return Ok(());
            }
            if delivered == 0 {
                if let Some(monitor) = heartbeat.as_mut() {
                    if let Some(reason) = monitor.poll_reopen_reason() {
                        let message = reason.message();
                        println!(
                            "{{\"scope\":\"wombatkv_shm_daemon\",\"event\":\"client_stale_reopen\",\"prefix\":\"{prefix}\",\"reason\":{message:?}}}"
                        );
                        if reason.requires_segment_cleanup() {
                            cleanup_prefix_segments(&prefix);
                        }
                        break;
                    }
                }
                thread::sleep(Duration::from_micros(100));
            }
        }
    }
}

fn build_store(
    daemon_cfg: &wombatkv_daemon::DaemonConfig,
) -> Result<WombatKVKvStore<S3ObjectStore>, String> {
    let s3_cfg = S3ObjectStoreConfig::from_env().map_err(|err| format!("{err:?}"))?;
    let s3 = S3ObjectStore::new(s3_cfg).map_err(|err| format!("{err:?}"))?;
    s3.ensure_bucket().map_err(|err| format!("{err:?}"))?;

    let mut foyer = FoyerCacheConfig::default();
    foyer.ssd_dir = daemon_cfg.puffer_dir.clone();
    if let Some(parsed) = daemon_cfg.puffer_ram_bytes {
        foyer.ram_bytes = parsed;
    }
    if let Some(parsed) = daemon_cfg.puffer_disk_bytes {
        foyer.ssd_bytes = parsed;
    }
    if let Some(parsed) = daemon_cfg.puffer_block_size_bytes {
        foyer.block_size = parsed as usize;
    }
    foyer.iouring = false;
    let cfg = EmbedConfig {
        s3_prefix: daemon_cfg.s3_prefix.clone(),
        foyer,
        write_through_s3: true,
        compression: wombatkv_node::compression::BlockCompressionConfig::from_env(),
    };
    WombatKVKvStore::new(cfg, s3).map_err(|err| format!("{err}"))
}

/// Open (or create) the L1 SlateDB-backed metadata index for the
/// daemon, production-default, always opens. Mirrors the embedded-mode
/// path in `wombatkv-cabi::ffi::Handle::from_env`:
///
///   - `WMBT_KV_SLATEDB_PATH=<dir>` selects the local-fs root; default
///     `<WMBT_KV_PUFFER_DIR>/slatedb` (or `/tmp/wombatkv-puffer-shm-foyer/slatedb`
///     when `WMBT_KV_PUFFER_DIR` is unset, matching `build_store`).
///   - `MYELON_NODE_ID=<id>` selects the per-process node directory;
///     defaults to `HOSTNAME` / `COMPUTERNAME` or `"default-node"`.
///   - `WMBT_KV_NAMESPACE=<ns>` selects the tenant key prefix; defaults
///     to `"default"`.
///
/// A failure here returns `Err`; the caller logs and continues without
/// the L1 index so the daemon still serves traffic (matches the
/// embedded path's "continue on bootstrap failure" stance).
fn open_slatedb_index_from_env(
    daemon_cfg: &wombatkv_daemon::DaemonConfig,
) -> Result<Option<Arc<SlateDbMetadataIndex>>, String> {
    let root = &daemon_cfg.slatedb_path;
    // MYELON_NODE_ID is myelon's env (not WMBT_KV_*) so it stays read
    // directly here, see config.rs "What's NOT here" doc-section.
    let node_id = std::env::var("MYELON_NODE_ID")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(daemon_hostname_or_default);
    let namespace = &daemon_cfg.namespace;
    eprintln!(
        "wombatkv-daemon[slatedb]: opening at {root:?} node_id={node_id:?} namespace={namespace:?}"
    );
    let idx = SlateDbMetadataIndex::open_local(root, &node_id, namespace)
        .map_err(|err| format!("SlateDbMetadataIndex::open_local({root:?}): {err}"))?;
    Ok(Some(Arc::new(idx)))
}

/// Best-effort hostname resolution for the `SlateDB` node-id default.
/// Mirrors `wombatkv-cabi`'s helper without dragging in a `hostname`
/// crate dep: `SlateDB` only treats this as an opaque path component.
fn daemon_hostname_or_default() -> String {
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "default-node".to_string())
}

/// Build + spawn the LRU eviction worker. Returns `None` when
/// `WMBT_KV_NAMESPACE_MAX_BYTES` is unset/0 (the default-off state).
fn spawn_daemon_eviction_worker(
    store: &Arc<WombatKVKvStore<S3ObjectStore>>,
    slatedb_index: Option<Arc<SlateDbMetadataIndex>>,
    daemon_cfg: &wombatkv_daemon::DaemonConfig,
) -> Option<wombatkv_node::lru::LruEvictionWorker> {
    let cfg = wombatkv_node::lru::LruConfig::from_env(&daemon_cfg.namespace)?;
    eprintln!(
        "wombatkv-daemon[lru]: starting eviction worker \
         (namespace_max_bytes={}, interval_secs={}, namespace={:?}, \
         slatedb_attached={})",
        cfg.namespace_max_bytes,
        cfg.interval.as_secs(),
        cfg.namespace,
        slatedb_index.is_some()
    );
    Some(store.start_eviction_worker(cfg, slatedb_index))
}

fn build_arena() -> Result<Option<SharedArena>, String> {
    let Ok(path) = std::env::var(ARENA_PATH_ENV) else {
        return Ok(None);
    };
    if path.is_empty() {
        return Ok(None);
    }

    let size = std::env::var(ARENA_BYTES_ENV)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(DEFAULT_ARENA_BYTES);
    let min_bytes = std::env::var(ARENA_MIN_BYTES_ENV)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(DEFAULT_ARENA_MIN_BYTES);
    let writer = ArenaWriter::create(&PathBuf::from(&path), size)
        .map_err(|err| format!("ArenaWriter::create(path={path:?}, size={size}): {err}"))?;
    println!(
        "{{\"scope\":\"wombatkv_shm_daemon\",\"event\":\"arena_ready\",\"path\":{path:?},\"size\":{size},\"min_bytes\":{min_bytes}}}"
    );
    Ok(Some(Arc::new(Mutex::new(ArenaShared { writer, slabs: HashMap::new(), min_bytes }))))
}

/// When set, the daemon ACKs PUT requests as soon as foyer has buffered the
/// bytes, and runs the slow object-store write in a background thread. The
/// hypothesis is that ds4's decode regresses while the daemon is mid-S3-PUT
/// because both contend for unified-memory bandwidth on macOS, async-PUT
/// returns the daemon worker to idle as fast as possible.
///
/// Trade-off: a subsequent GET racing the background S3 write will hit foyer
/// (which we wrote synchronously inside `store.put_kv`, before spawning the
/// async S3 path) but will not yet find the object in S3. For a co-located
/// single-client deployment this is fine; for multi-client / cross-host
/// shapes it's not safe.
const ASYNC_PUT_ENV: &str = "WMBT_KV_DAEMON_SHM_ASYNC_PUT";

fn async_put_enabled() -> bool {
    matches!(std::env::var(ASYNC_PUT_ENV).ok().as_deref(), Some("1" | "true" | "yes" | "on"))
}

/// Serialize async-PUT spawned threads so at most one is doing
/// heavy I/O (foyer admission + S3 PUT) at any given moment.
///
/// On its own, `QoS_BACKGROUND` alone reduces the cell-E contention
/// but doesn't fully match cell D's perf, macOS `QoS` is a *hint*,
/// not a hard pin, so the scheduler sometimes still co-runs daemon
/// threads on P-cores with Metal kernels. With N concurrent async
/// PUTs, the chance of a P-core collision grows linearly. Serializing
/// to exactly one in-flight S3 PUT eliminates the burst-collision
/// without changing ACK semantics: ds4 still gets its OK back
/// immediately and moves on; only the *daemon-side* work is queued.
///
/// Off-Mac this is also a sensible default (no harm).
fn async_put_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

/// Lower the calling thread's macOS `QoS` to background.
///
/// In `WMBT_KV_DAEMON_SHM_ASYNC_PUT=1` mode the daemon spawns OS threads to run
/// the heavy `put_kv` (foyer admission + S3 PUT) without blocking ds4.
/// On macOS those threads inherit `QOS_CLASS_USER_INITIATED` from the
/// daemon process, which is the same scheduling tier as Metal command
/// submission, they fight ds4's decode kernels for unified-memory
/// bandwidth, and we measured ds4's first-50-token decode rate collapsing
/// from ~17 t/s to ~2 t/s during overlapping daemon I/O.
///
/// Setting `QOS_CLASS_BACKGROUND` (0x09) tells the scheduler to run this
/// thread on E-cores and yield to higher-priority work, eliminating the
/// contention. Off-Mac this is a no-op.
#[allow(unsafe_code)]
fn set_background_qos_self() {
    #[cfg(target_os = "macos")]
    {
        // qos_class_t values from <sys/qos.h>:
        //   QOS_CLASS_USER_INTERACTIVE = 0x21
        //   QOS_CLASS_USER_INITIATED   = 0x19  (process default for foreground apps)
        //   QOS_CLASS_DEFAULT          = 0x15
        //   QOS_CLASS_UTILITY          = 0x11
        //   QOS_CLASS_BACKGROUND       = 0x09
        const QOS_CLASS_BACKGROUND: u32 = 0x09;
        extern "C" {
            fn pthread_set_qos_class_self_np(qc: u32, rel: i32) -> i32;
        }
        // SAFETY: standard libc call, no preconditions, returns int status.
        // We don't propagate the status, failure just leaves the thread at
        // its original QoS, which is the prior (broken) behavior.
        let _ = unsafe { pthread_set_qos_class_self_np(QOS_CLASS_BACKGROUND, 0) };
    }
}

fn dispatch(
    store: &Arc<WombatKVKvStore<S3ObjectStore>>,
    arena: Option<&SharedArena>,
    slatedb_index: Option<&Arc<SlateDbMetadataIndex>>,
    id: u64,
    req: WireRequest,
) -> WireResponse {
    // (DST Stage 3.5) advance the plan-aware buggify op counter
    // BEFORE handling the op so fault scheduled for "AfterKvOp { n: counter+1 }"
    // fires inside this op's production-code paths (embed::put_kv etc.).
    #[cfg(feature = "dst")]
    wombatkv_dst::dst_plan::advance_op();

    match req.op {
        op_codes::PING => WireResponse {
            id,
            status: status::OK,
            op: op_codes::PING,
            payload: Vec::new(),
            message: String::new(),
        },
        op_codes::PUT => {
            let bytes = Bytes::from(req.payload);
            // Trace flag is read directly here rather than threaded from
            // DaemonConfig because this site is inside a 4-deep call/spawn
            // chain (per-prefix worker → serve_prefix → inner loop / op
            // handler); reading the env once per loop iter is a no-op
            // perf cost and dramatically simpler than plumbing a bool.
            let trace = std::env::var("WMBT_KV_DAEMON_SHM_TRACE_DAEMON").is_ok();
            if async_put_enabled() {
                let store_clone = store.clone();
                let arena_owned = arena.cloned();
                let ns = req.namespace.clone();
                let key = req.key.clone();
                let bytes_clone = bytes.clone();
                let payload_len = bytes_clone.len();
                if trace {
                    eprintln!(
                        "[daemon-trace] async-put SPAWN-attempt id={id} key={key:?} size={payload_len}"
                    );
                }
                // Off the worker thread: the slow part of put_kv is the
                // synchronous object_store.put_object call. We pay foyer-RAM
                // insert as well (cheap, sub-ms) inside this spawned task,
                // which means subsequent GETs race the background put, but
                // foyer is updated within microseconds of the spawn so the
                // realistic window is "next request, not next instruction."
                let key_for_log = key.clone();
                let ns_for_log = ns.clone();
                let spawn_t0 = Instant::now();
                // Bump the in-flight counter BEFORE spawning. The
                // shutdown drain path polls this; we want it to see
                // "1 inflight" the instant we commit to spawning. The
                // worker decrements on exit. If spawn fails, we
                // decrement on the error path below so the counter
                // doesn't leak.
                async_put_inflight().fetch_add(1, Ordering::SeqCst);
                let spawn_result = std::thread::Builder::new()
                    .name(format!("wombatkv-shm-async-put-{id}"))
                    .spawn(move || {
                        // Decrement the in-flight counter even on panic
                        // by stashing the guard in a Drop type. The
                        // shutdown drain otherwise polls forever on a
                        // worker that crashed mid-PUT.
                        struct InflightGuard;
                        impl Drop for InflightGuard {
                            fn drop(&mut self) {
                                async_put_inflight().fetch_sub(1, Ordering::SeqCst);
                            }
                        }
                        let _inflight_guard = InflightGuard;
                        // Lower this thread's QoS to background on macOS so
                        // foyer admission + S3 PUT don't compete with ds4's
                        // Metal compute on unified-memory hardware. See
                        // set_background_qos_self() for the mechanism.
                        set_background_qos_self();
                        // Serialize: at most one async-PUT thread runs the
                        // heavy I/O at a time. With unbounded concurrent
                        // PUTs the QoS hint alone is insufficient, the
                        // scheduler still co-runs threads on P-cores
                        // occasionally. Serializing eliminates the
                        // burst-contention without changing the ACK
                        // semantics ds4 sees. See async_put_lock().
                        let _put_guard = async_put_lock().lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                        let thread_t0 = Instant::now();
                        if trace {
                            eprintln!(
                                "[daemon-trace] async-put THREAD START id={id} key={key:?} size={payload_len}"
                            );
                        }
                        let put_t0 = Instant::now();
                        let put_result = store_clone.put_kv(&ns, &key, bytes_clone.clone());
                        let put_ms = put_t0.elapsed().as_millis();
                        match put_result {
                            Ok(()) => {
                                if trace {
                                    eprintln!(
                                        "[daemon-trace] async-put STORE  OK id={id} key={key:?} took={put_ms}ms"
                                    );
                                }
                            }
                            Err(err) => {
                                eprintln!(
                                    "[daemon-trace] async-put STORE FAIL id={id} key={key:?} took={put_ms}ms err={err}"
                                );
                                return;
                            }
                        }
                        if let Some(arena) = arena_owned {
                            let mat_t0 = Instant::now();
                            match materialize_large_payload(
                                &store_clone,
                                &arena,
                                &ns,
                                &key,
                                &bytes_clone,
                                1,
                            ) {
                                Ok(_) => {
                                    if trace {
                                        eprintln!(
                                            "[daemon-trace] async-put ARENA OK id={id} key={key:?} took={}ms",
                                            mat_t0.elapsed().as_millis()
                                        );
                                    }
                                }
                                Err(err) => {
                                    eprintln!(
                                        "[daemon-trace] async-put ARENA FAIL id={id} key={key:?} err={err}"
                                    );
                                }
                            }
                        }
                        if trace {
                            eprintln!(
                                "[daemon-trace] async-put THREAD DONE id={id} key={key:?} total={}ms",
                                thread_t0.elapsed().as_millis()
                            );
                        }
                    });
                let spawn_ms = spawn_t0.elapsed().as_millis();
                match spawn_result {
                    Ok(_handle) => {
                        if trace {
                            eprintln!(
                                "[daemon-trace] async-put SPAWN  OK id={id} key={key_for_log:?} took={spawn_ms}ms"
                            );
                        }
                        return WireResponse {
                            id,
                            status: status::OK,
                            op: op_codes::PUT,
                            payload: Vec::new(),
                            message: String::new(),
                        };
                    }
                    Err(err) => {
                        // Spawn failed, undo the fetch_add we did
                        // pre-spawn (no worker thread will ever
                        // decrement it). Then log loud and fall
                        // through to the sync path so the client still
                        // gets a real ACK (vs the prior code which
                        // panicked the daemon worker via .expect()).
                        async_put_inflight().fetch_sub(1, Ordering::SeqCst);
                        eprintln!(
                            "[daemon-trace] async-put SPAWN FAIL id={id} key={key_for_log:?} ns={ns_for_log:?} took={spawn_ms}ms err={err}; falling back to sync"
                        );
                    }
                }
            }
            if trace {
                eprintln!(
                    "[daemon-trace] sync-put START id={id} key={:?} size={}",
                    req.key,
                    bytes.len()
                );
            }
            let sync_t0 = Instant::now();
            let put_result = store.put_kv(&req.namespace, &req.key, bytes.clone());
            let sync_ms = sync_t0.elapsed().as_millis();
            match put_result {
                Ok(()) => {
                    if trace {
                        eprintln!(
                            "[daemon-trace] sync-put STORE  OK id={id} key={:?} took={sync_ms}ms",
                            req.key
                        );
                    }
                    if let Some(arena) = arena {
                        let mat_t0 = Instant::now();
                        match materialize_large_payload(
                            store,
                            arena,
                            &req.namespace,
                            &req.key,
                            &bytes,
                            1,
                        ) {
                            Ok(_) => {
                                if trace {
                                    eprintln!(
                                        "[daemon-trace] sync-put ARENA OK id={id} key={:?} took={}ms",
                                        req.key,
                                        mat_t0.elapsed().as_millis()
                                    );
                                }
                            }
                            Err(err) => {
                                eprintln!(
                                    "wombatkv-daemon: arena materialize after PUT failed for {}/{}: {err}",
                                    req.namespace, req.key
                                );
                            }
                        }
                    }
                    WireResponse {
                        id,
                        status: status::OK,
                        op: op_codes::PUT,
                        payload: Vec::new(),
                        message: String::new(),
                    }
                }
                Err(err) => {
                    eprintln!(
                        "[daemon-trace] sync-put STORE FAIL id={id} key={:?} took={sync_ms}ms err={err}",
                        req.key
                    );
                    err_response(id, op_codes::PUT, format!("put: {err}"))
                }
            }
        }
        op_codes::GET => {
            if let Some(slab) =
                arena.and_then(|arena| lookup_arena_slab(arena, &req.namespace, &req.key))
            {
                return arena_response(id, op_codes::GET, slab);
            }
            match store.get_kv(&req.namespace, &req.key) {
                Ok(GetOutcome::Hit { tier, payload }) => {
                    let tier_byte = match tier {
                        HitTier::Foyer => 1u8,
                        HitTier::ObjectStore => 2u8,
                    };
                    if let Some(arena) = arena {
                        match materialize_large_payload(
                            store,
                            arena,
                            &req.namespace,
                            &req.key,
                            &payload,
                            tier_byte,
                        ) {
                            Ok(Some(slab)) => return arena_response(id, op_codes::GET, slab),
                            Ok(None) => {}
                            Err(err) => eprintln!(
                                "wombatkv-daemon: arena materialize after GET failed for {}/{}: {err}",
                                req.namespace, req.key
                            ),
                        }
                    }
                    WireResponse {
                        id,
                        status: status::OK,
                        op: op_codes::GET,
                        payload: payload.to_vec(),
                        message: format!("tier:{tier_byte}"),
                    }
                }
                Ok(GetOutcome::Miss) => WireResponse {
                    id,
                    status: status::MISS,
                    op: op_codes::GET,
                    payload: Vec::new(),
                    message: String::new(),
                },
                Err(err) => err_response(id, op_codes::GET, format!("get: {err}")),
            }
        }
        op_codes::GET_MANY => {
            let keys = match decode_key_batch(&req.payload) {
                Ok(keys) => keys,
                Err(err) => return err_response(id, op_codes::GET_MANY, err.to_string()),
            };
            let mut items = Vec::with_capacity(keys.len());
            let mut tier_byte = 1u8;
            for key in keys {
                match store.get_kv(&req.namespace, &key) {
                    Ok(GetOutcome::Hit { tier, payload }) => {
                        if matches!(tier, HitTier::ObjectStore) {
                            tier_byte = 2;
                        }
                        match resolve_large_payload(store, &req.namespace, payload) {
                            Ok(payload) => items.push(payload),
                            Err(err) => {
                                return err_response(
                                    id,
                                    op_codes::GET_MANY,
                                    format!("get_many large payload: {err}"),
                                );
                            }
                        }
                    }
                    Ok(GetOutcome::Miss) => {
                        return WireResponse {
                            id,
                            status: status::MISS,
                            op: op_codes::GET_MANY,
                            payload: Vec::new(),
                            message: key,
                        };
                    }
                    Err(err) => {
                        return err_response(id, op_codes::GET_MANY, format!("get_many: {err}"));
                    }
                }
            }

            let batch = encode_bytes_batch(&items);
            if let Some(arena) = arena {
                match write_arena_payload(arena, &batch, tier_byte) {
                    Ok(Some(slab)) => return arena_response(id, op_codes::GET_MANY, slab),
                    Ok(None) => {}
                    Err(err) => eprintln!("wombatkv-daemon: arena GET_MANY failed: {err}"),
                }
            }

            if fits_one_frame(batch.len()) {
                WireResponse {
                    id,
                    status: status::OK,
                    op: op_codes::GET_MANY,
                    payload: batch,
                    message: format!("tier:{tier_byte}"),
                }
            } else {
                WireResponse {
                    id,
                    status: status::TOO_LARGE,
                    op: op_codes::GET_MANY,
                    payload: Vec::new(),
                    message: format!(
                        "get_many response {} bytes requires {} or smaller batch",
                        batch.len(),
                        ARENA_PATH_ENV
                    ),
                }
            }
        }
        op_codes::EXISTS => match store.exists_kv(&req.namespace, &req.key) {
            Ok(true) => WireResponse {
                id,
                status: status::OK,
                op: op_codes::EXISTS,
                payload: Vec::new(),
                message: String::new(),
            },
            Ok(false) => WireResponse {
                id,
                status: status::MISS,
                op: op_codes::EXISTS,
                payload: Vec::new(),
                message: String::new(),
            },
            Err(err) => err_response(id, op_codes::EXISTS, format!("exists: {err}")),
        },
        op_codes::LIST => match store.list_kv_keys(&req.namespace) {
            Ok(keys) => WireResponse {
                id,
                status: status::OK,
                op: op_codes::LIST,
                payload: encode_key_batch(&keys),
                message: keys.len().to_string(),
            },
            Err(err) => err_response(id, op_codes::LIST, format!("list: {err}")),
        },
        op_codes::STATS => WireResponse {
            id,
            status: status::OK,
            op: op_codes::STATS,
            payload: Vec::new(),
            message: metrics().to_json_lines(),
        },
        op_codes::CLEAR => {
            store.clear_foyer();
            if let Some(arena) = arena {
                match arena.lock() {
                    Ok(mut arena) => arena.slabs.clear(),
                    Err(err) => eprintln!("wombatkv-daemon: arena CLEAR lock poisoned: {err}"),
                }
            }
            WireResponse {
                id,
                status: status::OK,
                op: op_codes::CLEAR,
                payload: Vec::new(),
                message: String::new(),
            }
        }
        op_codes::RESTORE => match store.restore_from_s3(&req.namespace) {
            Ok(count) => WireResponse {
                id,
                status: status::OK,
                op: op_codes::RESTORE,
                payload: Vec::new(),
                message: format!("{count}"),
            },
            Err(err) => err_response(id, op_codes::RESTORE, format!("restore: {err}")),
        },
        op_codes::CLOSE => WireResponse {
            id,
            status: status::OK,
            op: op_codes::CLOSE,
            payload: Vec::new(),
            message: String::new(),
        },
        op_codes::LOOKUP_BLOCK_PREFIX => dispatch_lookup_block_prefix(store, id, &req),
        op_codes::GET_KV_BLOCKS_BATCH => dispatch_get_kv_blocks_batch(store, id, &req),
        op_codes::PUT_KV_BLOCKS_BATCH => {
            dispatch_put_kv_blocks_batch(store, slatedb_index, id, &req)
        }
        other => err_response(id, other, format!("unknown op {other}")),
    }
}

/// Daemon-side handler for [`op_codes::LOOKUP_BLOCK_PREFIX`]. Decodes the
/// length-prefix request payload, queries the in-process metadata index,
/// encodes the length-prefix response payload. The wire-level `status` field is `OK` even
/// when no hashes match, a leading miss is a normal data answer, not an
/// error. Malformed hex inputs return `status::ERROR`.
fn dispatch_lookup_block_prefix(
    store: &Arc<WombatKVKvStore<S3ObjectStore>>,
    id: u64,
    req: &WireRequest,
) -> WireResponse {
    let lookup_req = match decode_lookup_block_prefix_req(&req.payload) {
        Ok(r) => r,
        Err(err) => {
            return err_response(
                id,
                op_codes::LOOKUP_BLOCK_PREFIX,
                format!("decode lookup_block_prefix req: {err}"),
            );
        }
    };
    let hashes = match parse_block_hash_list(&lookup_req.block_hashes_hex) {
        Ok(h) => h,
        Err(err) => {
            let resp = LookupBlockPrefixResp { matched_count: 0, error: Some(err.clone()) };
            return encode_block_resp_or_err(
                id,
                op_codes::LOOKUP_BLOCK_PREFIX,
                status::ERROR,
                err,
                encode_lookup_block_prefix_resp(&resp).map_err(|e| e.to_string()),
            );
        }
    };
    let index = store.metadata_index();
    let matched = index.longest_prefix(&hashes);
    let resp = LookupBlockPrefixResp {
        matched_count: u32::try_from(matched).unwrap_or(u32::MAX),
        error: None,
    };
    encode_block_resp_or_err(
        id,
        op_codes::LOOKUP_BLOCK_PREFIX,
        status::OK,
        String::new(),
        encode_lookup_block_prefix_resp(&resp).map_err(|e| e.to_string()),
    )
}

/// Daemon-side handler for [`op_codes::GET_KV_BLOCKS_BATCH`]. Decodes the
/// hash list, parallel-fetches each block from the `WombatKVKvStore` via
/// the existing per-key `get_kv` path, encodes the length-prefix response. Miss
/// semantics are carried in the response payload (`payloads: None`) so
/// the wire status stays OK for the common all-hit and any-miss cases.
fn dispatch_get_kv_blocks_batch(
    store: &Arc<WombatKVKvStore<S3ObjectStore>>,
    id: u64,
    req: &WireRequest,
) -> WireResponse {
    let get_req = match decode_get_kv_blocks_batch_req(&req.payload) {
        Ok(r) => r,
        Err(err) => {
            return err_response(
                id,
                op_codes::GET_KV_BLOCKS_BATCH,
                format!("decode get_kv_blocks_batch req: {err}"),
            );
        }
    };
    // Validate hex up front. We don't actually need the parsed bytes
    // here (we go straight to `wombatkv/v1/block/b3=<hex>` keys), just an
    // up-front rejection of malformed input.
    if let Err(err) = parse_block_hash_list(&get_req.block_hashes_hex) {
        let resp = GetKvBlocksBatchResp { payloads: None, error: Some(err.clone()) };
        return encode_block_resp_or_err(
            id,
            op_codes::GET_KV_BLOCKS_BATCH,
            status::ERROR,
            err,
            encode_get_kv_blocks_batch_resp(&resp).map_err(|e| e.to_string()),
        );
    }

    // Parallel fanout, all-or-nothing. Matches the cabi
    // `Backend::Embedded::get_many_kv_batch` shape: any miss => `None`,
    // any backend error short-circuits.
    use std::sync::atomic::{AtomicBool, Ordering};
    let store_ref = store.as_ref();
    let namespace = &get_req.namespace;
    let saw_miss = AtomicBool::new(false);
    let results: Vec<Result<Option<Bytes>, String>> = std::thread::scope(|s| {
        let handles: Vec<_> = get_req
            .block_hashes_hex
            .iter()
            .map(|hex| {
                s.spawn(|| {
                    if saw_miss.load(Ordering::Relaxed) {
                        return Ok(None);
                    }
                    let key = block_key_from_hex(hex);
                    match store_ref.get_kv(namespace, &key) {
                        Ok(GetOutcome::Hit { payload, .. }) => Ok(Some(payload)),
                        Ok(GetOutcome::Miss) => {
                            saw_miss.store(true, Ordering::Relaxed);
                            Ok(None)
                        }
                        Err(err) => Err(format!("get_kv {key}: {err}")),
                    }
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| {
                h.join().unwrap_or_else(|_| Err("get_kv_blocks_batch thread panic".to_string()))
            })
            .collect()
    });

    let mut items: Vec<Vec<u8>> = Vec::with_capacity(get_req.block_hashes_hex.len());
    for r in results {
        match r {
            Ok(Some(payload)) => items.push(payload.to_vec()),
            Ok(None) => {
                let resp = GetKvBlocksBatchResp { payloads: None, error: None };
                return encode_block_resp_or_err(
                    id,
                    op_codes::GET_KV_BLOCKS_BATCH,
                    status::OK,
                    String::new(),
                    encode_get_kv_blocks_batch_resp(&resp).map_err(|e| e.to_string()),
                );
            }
            Err(err) => {
                let resp = GetKvBlocksBatchResp { payloads: None, error: Some(err.clone()) };
                return encode_block_resp_or_err(
                    id,
                    op_codes::GET_KV_BLOCKS_BATCH,
                    status::ERROR,
                    err,
                    encode_get_kv_blocks_batch_resp(&resp).map_err(|e| e.to_string()),
                );
            }
        }
    }
    let resp = GetKvBlocksBatchResp { payloads: Some(items), error: None };
    encode_block_resp_or_err(
        id,
        op_codes::GET_KV_BLOCKS_BATCH,
        status::OK,
        String::new(),
        encode_get_kv_blocks_batch_resp(&resp).map_err(|e| e.to_string()),
    )
}

/// Daemon-side handler for [`op_codes::PUT_KV_BLOCKS_BATCH`]. Decodes the
/// length-prefix request, parallel-puts each block via `store.put_kv`, then -
/// only after ALL puts succeed, updates the daemon's metadata index so
/// a subsequent `LOOKUP_BLOCK_PREFIX` sees the new entries. On partial
/// failure the index is left unchanged for the failed batch.
fn dispatch_put_kv_blocks_batch(
    store: &Arc<WombatKVKvStore<S3ObjectStore>>,
    slatedb_index: Option<&Arc<SlateDbMetadataIndex>>,
    id: u64,
    req: &WireRequest,
) -> WireResponse {
    let put_req = match decode_put_kv_blocks_batch_req(&req.payload) {
        Ok(r) => r,
        Err(err) => {
            return err_response(
                id,
                op_codes::PUT_KV_BLOCKS_BATCH,
                format!("decode put_kv_blocks_batch req: {err}"),
            );
        }
    };
    if put_req.block_hashes_hex.len() != put_req.payloads.len() {
        let msg = format!(
            "put_kv_blocks_batch length mismatch: {} hashes vs {} payloads",
            put_req.block_hashes_hex.len(),
            put_req.payloads.len()
        );
        let resp = PutKvBlocksBatchResp { total_bytes: 0, error: Some(msg.clone()) };
        return encode_block_resp_or_err(
            id,
            op_codes::PUT_KV_BLOCKS_BATCH,
            status::ERROR,
            msg,
            encode_put_kv_blocks_batch_resp(&resp).map_err(|e| e.to_string()),
        );
    }
    let hashes = match parse_block_hash_list(&put_req.block_hashes_hex) {
        Ok(h) => h,
        Err(err) => {
            let resp = PutKvBlocksBatchResp { total_bytes: 0, error: Some(err.clone()) };
            return encode_block_resp_or_err(
                id,
                op_codes::PUT_KV_BLOCKS_BATCH,
                status::ERROR,
                err,
                encode_put_kv_blocks_batch_resp(&resp).map_err(|e| e.to_string()),
            );
        }
    };

    // Parallel put. Mirrors Backend::Embedded::put_kv_blocks's
    // thread::scope fanout: K threads = K simultaneous S3 PUTs against
    // the same backend.
    let store_ref = store.as_ref();
    let namespace = &put_req.namespace;
    let keys: Vec<String> =
        put_req.block_hashes_hex.iter().map(|hex| block_key_from_hex(hex)).collect();
    let results: Vec<Result<(), String>> = std::thread::scope(|s| {
        let handles: Vec<_> = keys
            .iter()
            .zip(put_req.payloads.iter())
            .map(|(key, payload)| {
                s.spawn(move || {
                    let bytes = Bytes::copy_from_slice(payload);
                    store_ref
                        .put_kv(namespace, key, bytes)
                        .map_err(|err| format!("put_kv {key}: {err}"))
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| {
                h.join().unwrap_or_else(|_| Err("put_kv_blocks_batch thread panic".to_string()))
            })
            .collect()
    });
    for r in results {
        if let Err(err) = r {
            let resp = PutKvBlocksBatchResp { total_bytes: 0, error: Some(err.clone()) };
            return encode_block_resp_or_err(
                id,
                op_codes::PUT_KV_BLOCKS_BATCH,
                status::ERROR,
                err,
                encode_put_kv_blocks_batch_resp(&resp).map_err(|e| e.to_string()),
            );
        }
    }

    // All puts succeeded, now stamp the metadata index. The C ABI
    // `wmbt_kv_put_kv_blocks` documents that the index is updated for
    // every successful PUT, so a subsequent `wmbt_kv_lookup_block_prefix`
    // sees the new presence. We mirror that here: insert each hash as
    // a root entry (zero parent/seq, the C ABI does not yet carry
    // chain wiring; matches `Backend::Embedded::put_kv_blocks`).
    //
    // SlateDB-backed index is always-on as of v0.1.0-alpha.2 (the
    // earlier `WMBT_KV_BOOTSTRAP_SLATEDB` env-gate was removed in
    // commit eb84c3c). Perform synchronous write-through alongside the
    // in-memory insert so the metadata survives a daemon restart. The
    // L1 SlateDB writer flushes after every insert (RFC 0008 §7 strict
    // durability), keeping the daemon-mode story aligned with the
    // embedded path's `bootstrap_from_slatedb` rehydrate.
    let index = store.metadata_index();
    let mut total: u64 = 0;
    for (hash, payload) in hashes.iter().zip(put_req.payloads.iter()) {
        let meta = BlockMeta::new_root(payload.len() as u64, [0u8; 24], [0u8; 16]);
        index.insert(*hash, meta);
        if let Some(slate) = slatedb_index {
            slate.insert(*hash, meta);
        }
        total = total.saturating_add(payload.len() as u64);
    }
    let resp = PutKvBlocksBatchResp { total_bytes: total, error: None };
    encode_block_resp_or_err(
        id,
        op_codes::PUT_KV_BLOCKS_BATCH,
        status::OK,
        String::new(),
        encode_put_kv_blocks_batch_resp(&resp).map_err(|e| e.to_string()),
    )
}

/// Compose the standalone content-addressed key for a block hash, mirroring
/// `wombatkv-cabi`'s `block_key_for_hash`. The input is a 64-char lower-hex
/// blake3 hash; the output is `wombatkv/v1/block/b3=<hex>`. Producers and
/// consumers MUST agree on this scheme (it is part of the C ABI contract).
fn block_key_from_hex(hex: &str) -> String {
    let mut s = String::with_capacity(BLOCK_KEY_PREFIX.len() + hex.len());
    s.push_str(BLOCK_KEY_PREFIX);
    s.push_str(hex);
    s
}

/// Validate and parse a list of 64-char lower-hex blake3 hashes into
/// `BlockHash` (`[u8; 32]`). Returns the first parse error encountered.
fn parse_block_hash_list(hex_list: &[String]) -> Result<Vec<[u8; 32]>, String> {
    let mut out = Vec::with_capacity(hex_list.len());
    for (i, hex) in hex_list.iter().enumerate() {
        out.push(parse_block_hash_hex(hex).map_err(|err| format!("hash[{i}]: {err}"))?);
    }
    Ok(out)
}

fn parse_block_hash_hex(hex: &str) -> Result<[u8; 32], String> {
    let bytes = hex.as_bytes();
    if bytes.len() != 64 {
        return Err(format!("block hash hex must be 64 chars; got {} for {hex:?}", bytes.len()));
    }
    let mut out = [0u8; 32];
    for i in 0..32 {
        let hi = decode_hex_nibble(bytes[2 * i])
            .ok_or_else(|| format!("bad hex char at pos {} in {hex:?}", 2 * i))?;
        let lo = decode_hex_nibble(bytes[2 * i + 1])
            .ok_or_else(|| format!("bad hex char at pos {} in {hex:?}", 2 * i + 1))?;
        out[i] = (hi << 4) | lo;
    }
    Ok(out)
}

fn decode_hex_nibble(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

/// Build a `WireResponse` from a length-prefix-encoded payload result.
/// If encode fails (vanishingly rare, only OOM-style or u32-length
/// overflow), fall back to `err_response` so the daemon never panics on
/// a bad encode. The caller passes the intended `status` + `message`
/// for the success path; on encode failure we override to `status::ERROR`.
fn encode_block_resp_or_err(
    id: u64,
    op: u8,
    success_status: u8,
    success_message: String,
    encoded: Result<Vec<u8>, String>,
) -> WireResponse {
    match encoded {
        Ok(payload) => {
            WireResponse { id, status: success_status, op, payload, message: success_message }
        }
        Err(err) => err_response(id, op, format!("block-payload encode: {err}")),
    }
}

fn err_response(id: u64, op: u8, message: String) -> WireResponse {
    WireResponse { id, status: status::ERROR, op, payload: Vec::new(), message }
}

fn arena_response(id: u64, op: u8, slab: ArenaSlab) -> WireResponse {
    WireResponse {
        id,
        status: status::OK,
        op,
        payload: Vec::new(),
        message: format!("tier:{};arena:{}:{}", slab.tier, slab.offset, slab.len),
    }
}

fn arena_key(namespace: &str, key: &str) -> String {
    format!("{namespace}\0{key}")
}

fn lookup_arena_slab(arena: &SharedArena, namespace: &str, key: &str) -> Option<ArenaSlab> {
    let Ok(arena) = arena.lock() else {
        return None;
    };
    arena.slabs.get(&arena_key(namespace, key)).copied()
}

fn remember_arena_slab(
    arena: &SharedArena,
    namespace: &str,
    key: &str,
    bytes: &[u8],
    tier: u8,
) -> Result<Option<ArenaSlab>, String> {
    let slab = write_arena_payload(arena, bytes, tier)?;
    let Some(slab) = slab else {
        return Ok(None);
    };
    let mut arena = arena.lock().map_err(|err| format!("arena lock poisoned: {err}"))?;
    arena.slabs.insert(arena_key(namespace, key), slab);
    Ok(Some(slab))
}

fn write_arena_payload(
    arena: &SharedArena,
    bytes: &[u8],
    tier: u8,
) -> Result<Option<ArenaSlab>, String> {
    let mut arena = arena.lock().map_err(|err| format!("arena lock poisoned: {err}"))?;
    if bytes.len() < arena.min_bytes {
        return Ok(None);
    }
    let prev_epoch = arena.writer.wrap_epoch();
    let (offset, len) =
        arena.writer.write_payload(bytes).map_err(|err| format!("arena write_payload: {err}"))?;
    if arena.writer.wrap_epoch() != prev_epoch {
        arena.slabs.clear();
    }
    Ok(Some(ArenaSlab { offset, len, tier }))
}

fn materialize_large_payload(
    store: &WombatKVKvStore<S3ObjectStore>,
    arena: &SharedArena,
    namespace: &str,
    key: &str,
    payload: &[u8],
    tier: u8,
) -> Result<Option<ArenaSlab>, String> {
    let Some(manifest) = parse_large_manifest(payload) else {
        return remember_arena_slab(arena, namespace, key, payload, tier);
    };
    if manifest.total_len < {
        let arena = arena.lock().map_err(|err| format!("arena lock poisoned: {err}"))?;
        arena.min_bytes
    } {
        return Ok(None);
    }

    let out = load_large_payload(store, namespace, &manifest)?;
    remember_arena_slab(arena, namespace, key, &out, tier)
}

fn resolve_large_payload(
    store: &WombatKVKvStore<S3ObjectStore>,
    namespace: &str,
    payload: Bytes,
) -> Result<Bytes, String> {
    let Some(manifest) = parse_large_manifest(&payload) else {
        return Ok(payload);
    };
    load_large_payload(store, namespace, &manifest)
}

fn load_large_payload(
    store: &WombatKVKvStore<S3ObjectStore>,
    namespace: &str,
    manifest: &LargeManifest,
) -> Result<Bytes, String> {
    let mut out = Vec::with_capacity(manifest.total_len);
    for idx in 0..manifest.chunk_count {
        let chunk_key = large_chunk_key(&manifest.id, idx);
        match store
            .get_kv(namespace, &chunk_key)
            .map_err(|err| format!("get chunk {idx}: {err}"))?
        {
            GetOutcome::Hit { payload, .. } => out.extend_from_slice(&payload),
            GetOutcome::Miss => {
                return Err(format!("missing large-payload chunk {idx} for {}", manifest.id));
            }
        }
    }
    if out.len() != manifest.total_len {
        return Err(format!(
            "large-payload length mismatch: expected {} got {}",
            manifest.total_len,
            out.len()
        ));
    }
    Ok(Bytes::from(out))
}

fn large_chunk_key(id: &str, idx: usize) -> String {
    format!("{LARGE_CHUNK_KEY_PREFIX}/{id}/{idx:08}")
}

fn parse_large_manifest(payload: &[u8]) -> Option<LargeManifest> {
    let text = std::str::from_utf8(payload).ok()?;
    let mut lines = text.lines();
    if lines.next()? != LARGE_MANIFEST_MAGIC {
        return None;
    }
    let id = lines.next()?.to_string();
    let total_len = lines.next()?.parse().ok()?;
    let chunk_count = lines.next()?.parse().ok()?;
    Some(LargeManifest { id, total_len, chunk_count })
}
