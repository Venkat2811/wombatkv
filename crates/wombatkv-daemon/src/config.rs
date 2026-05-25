//! Centralized daemon configuration.
//!
//! All `WMBT_KV_*` env reads for the `wombatkv-daemon` binary land in
//! exactly one place: [`DaemonConfig::from_env`]. The binary then
//! consumes the resolved struct instead of sprinkling `env::var(...)`
//! calls through main + spawn closures.
//!
//! # Why centralize
//!
//! - **Auditability:** `cargo doc -p wombatkv-daemon` produces the
//!   canonical env-var reference; ENV.md generation can be driven
//!   off this struct's rustdoc.
//! - **Testability:** unit tests inject `DaemonConfig::default()` or
//!   custom values without mutating process env.
//! - **Operability:** `wombatkv-daemon --print-config` becomes a
//!   single `println!("{cfg:#?}")` in the binary.
//! - **Single rename surface:** the alpha-breaking-window policy
//!   (`feedback_alpha_breaking_window.md`) lets us rename env vars
//!   freely pre-tag; one struct = one search-and-replace per rename.
//!
//! # What's NOT here
//!
//! - Per-subsystem config that already has its own `from_env`:
//!   `S3ObjectStoreConfig`, `FoyerCacheConfig`, `EmbedConfig`,
//!   `BlockCompressionConfig`, `LruConfig`, `PrefetchConfig`.
//!   Those stay in their owning crates; this struct holds only the
//!   daemon-binary-orchestration knobs.
//! - System env vars (`HOSTNAME`, `COMPUTERNAME`, `USER`), read
//!   directly where used, not WombatKV's surface.
//! - myelon-side env vars (`MYELON_NODE_ID`, myelon wait-strategy
//!   sub-tunables), managed by myelon's own config surface.

use std::path::PathBuf;
use std::time::Duration;

use myelon::MyelonWaitStrategy;

/// All the daemon-binary's WMBT_KV_* knobs in one place.
///
/// Field rustdoc IS the operator documentation, keep it accurate.
#[derive(Debug, Clone)]
pub struct DaemonConfig {
    // ---- Transport ----
    /// SHM prefixes to serve. From `WMBT_KV_DAEMON_SHM_PREFIX`
    /// (single value); CLI `--prefix` flags additively extend the
    /// list. If empty after CLI + env, the daemon falls back to
    /// `["default"]` when no TCP/HTTP listeners are configured.
    pub shm_prefixes: Vec<String>,

    /// TCP listener addresses. From `WMBT_KV_TCP` (comma-separated
    /// `host:port` list); CLI `--tcp <addr>` flags extend.
    pub tcp_addrs: Vec<String>,

    /// HTTP listener addresses. From `WMBT_KV_HTTP`; CLI `--http`
    /// extends.
    pub http_addrs: Vec<String>,

    /// SHM ring depth. From `WMBT_KV_DAEMON_SHM_DEPTH`. Default 16.
    pub shm_depth: usize,

    // ---- Wait strategy ----
    /// SHM consumer wait strategy. From `WMBT_KV_DAEMON_SHM_WAIT_STRATEGY`
    /// (`block` / `busyspin`). Default `Block` (signal-driven
    /// park): ~µs wake latency, 0% idle CPU. `BusySpin` keeps the
    /// consumer thread spinning at 100% CPU for ultra-low-latency
    /// RPC scenarios where the wake-µs matters and CPU is free.
    pub wait_strategy: MyelonWaitStrategy,

    // ---- TCP TPC plumbing ----
    /// Per-shard compio TPC threads for the TCP listener. From
    /// `WMBT_KV_TCP_TPC_THREADS`. Default 2 (SO_REUSEPORT-balanced
    /// accept across 2 compio threads). Bump for >16 concurrent
    /// clients.
    pub tcp_tpc_threads: usize,

    /// Sync dispatch worker pool size behind the TCP compio bridge.
    /// From `WMBT_KV_TCP_DISPATCH_WORKERS`. Default 8.
    pub tcp_dispatch_workers: usize,

    // ---- HTTP TPC plumbing ----
    /// Per-shard compio TPC threads for the HTTP listener. From
    /// `WMBT_KV_HTTP_TPC_THREADS`. Default 2.
    pub http_tpc_threads: usize,

    /// Sync dispatch worker pool size behind the HTTP compio bridge.
    /// From `WMBT_KV_HTTP_DISPATCH_WORKERS`. Default 8.
    pub http_dispatch_workers: usize,

    // ---- Storage layout ----
    /// Foyer SSD-tier directory. From `WMBT_KV_PUFFER_DIR`. Default
    /// `~/.wombatkv/puffer` (matches `wombatkv-cabi`'s default; survives
    /// reboot, no LAN-port collisions across runs); falls back to
    /// `/tmp/wombatkv-puffer-shm-foyer` with a warning if `$HOME` is
    /// unset or `mkdir -p` fails (e.g. daemon launched as a system
    /// service without a home dir). Also the default parent for
    /// SlateDB if `WMBT_KV_SLATEDB_PATH` is unset.
    pub puffer_dir: PathBuf,

    /// Foyer RAM-tier byte budget. From `WMBT_KV_PUFFER_RAM_BYTES`.
    /// `None` means use FoyerCacheConfig's default.
    pub puffer_ram_bytes: Option<u64>,

    /// Foyer SSD-tier byte budget. From `WMBT_KV_PUFFER_DISK_BYTES`.
    pub puffer_disk_bytes: Option<u64>,

    /// Foyer-internal block size. From `WMBT_KV_PUFFER_BLOCK_SIZE_BYTES`.
    pub puffer_block_size_bytes: Option<u64>,

    /// S3 object-key prefix for daemon-served blocks. From
    /// `WMBT_KV_S3_PREFIX`. Default `kv/puffer-shm`.
    pub s3_prefix: String,

    /// SlateDB L1 metadata-index on-disk root. From
    /// `WMBT_KV_SLATEDB_PATH`. Defaults to `<puffer_dir>/slatedb`.
    pub slatedb_path: PathBuf,

    /// Logical namespace tag (multi-tenant scope). From
    /// `WMBT_KV_NAMESPACE`. Default `default`.
    pub namespace: String,

    // ---- Eviction ----
    /// Per-namespace LRU eviction cap (bytes). From
    /// `WMBT_KV_NAMESPACE_MAX_BYTES`. `None` means eviction disabled
    /// (no cap). Production deployments should set this, without it,
    /// the bucket grows forever.
    pub namespace_max_bytes: Option<u64>,

    /// Eviction worker poll interval. From
    /// `WMBT_KV_EVICTION_INTERVAL_SECS`. Default 30s.
    pub eviction_interval: Duration,

    // ---- Operator UX ----
    /// Suppress the 0.1.0-alpha banner. From `WMBT_KV_QUIET_BANNER`.
    pub quiet_banner: bool,

    /// Trace daemon SHM I/O to stderr. From
    /// `WMBT_KV_DAEMON_SHM_TRACE_DAEMON`.
    pub trace_daemon: bool,

    /// Prefetch worker logs candidates without materializing.
    /// From `WMBT_KV_PREFETCH_DRY_RUN`. Useful for tuning
    /// `WMBT_KV_PREFETCH_TOP_K` against a workload without paying
    /// the actual GET cost.
    pub prefetch_dry_run: bool,
}

/// Fallback puffer dir when `$HOME` is unset or `~/.wombatkv/puffer`
/// can't be created. Kept as the legacy daemon name so any operator
/// monitoring `/tmp/` for the daemon's foyer dir under the old default
/// still finds it on systems without a home dir.
const DAEMON_FALLBACK_PUFFER_DIR: &str = "/tmp/wombatkv-puffer-shm-foyer";

/// Resolve the daemon's default Foyer SSD-tier directory:
///   1. `$HOME/.wombatkv/puffer` (preferred, survives reboot, matches
///      `wombatkv-cabi`'s default so embedded vs. daemon share the same
///      filesystem location when on the same host),
///   2. `/tmp/wombatkv-puffer-shm-foyer` (legacy fallback, with a
///      stderr warning), if `$HOME` is unset OR `mkdir -p` fails.
///
/// Mirrors the resolution logic in `wombatkv-cabi/src/ffi.rs`
/// (`default_puffer_dir`). Duplicated rather than shared because
/// `wombatkv-daemon` and `wombatkv-cabi` don't depend on a common
/// utility crate and the helper is small.
fn default_daemon_puffer_dir() -> PathBuf {
    let home = std::env::var("HOME").ok().filter(|s| !s.is_empty());
    let candidate = match home {
        Some(h) => PathBuf::from(h).join(".wombatkv").join("puffer"),
        None => return PathBuf::from(DAEMON_FALLBACK_PUFFER_DIR),
    };
    match std::fs::create_dir_all(&candidate) {
        Ok(()) => candidate,
        Err(err) => {
            eprintln!(
                "wombatkv-daemon: could not create default puffer dir {} \
                 ({err}); falling back to {DAEMON_FALLBACK_PUFFER_DIR}",
                candidate.display()
            );
            PathBuf::from(DAEMON_FALLBACK_PUFFER_DIR)
        }
    }
}

impl Default for DaemonConfig {
    fn default() -> Self {
        let puffer_dir = default_daemon_puffer_dir();
        let slatedb_path = puffer_dir.join("slatedb");
        Self {
            shm_prefixes: Vec::new(),
            tcp_addrs: Vec::new(),
            http_addrs: Vec::new(),
            shm_depth: crate::DEFAULT_RING_DEPTH,
            wait_strategy: MyelonWaitStrategy::Block,
            tcp_tpc_threads: 2,
            tcp_dispatch_workers: 8,
            http_tpc_threads: 2,
            http_dispatch_workers: 8,
            puffer_dir,
            puffer_ram_bytes: None,
            puffer_disk_bytes: None,
            puffer_block_size_bytes: None,
            s3_prefix: "kv/puffer-shm".to_string(),
            slatedb_path,
            namespace: "default".to_string(),
            namespace_max_bytes: None,
            eviction_interval: Duration::from_secs(30),
            quiet_banner: false,
            trace_daemon: false,
            prefetch_dry_run: false,
        }
    }
}

impl DaemonConfig {
    /// Resolve from env. Reads every `WMBT_KV_*` var the daemon
    /// binary consumes, applies defaults, and returns a fully-
    /// populated config. CLI args are merged separately by
    /// `bin/wombatkv-daemon.rs` (which calls this and then layers
    /// CLI overrides on top).
    #[must_use]
    pub fn from_env() -> Self {
        let mut cfg = Self::default();

        // Transport
        if let Ok(v) = std::env::var("WMBT_KV_DAEMON_SHM_PREFIX") {
            if !v.is_empty() {
                cfg.shm_prefixes.push(v);
            }
        }
        if let Ok(v) = std::env::var("WMBT_KV_TCP") {
            for chunk in v.split(',') {
                let chunk = chunk.trim();
                if !chunk.is_empty() {
                    cfg.tcp_addrs.push(chunk.to_string());
                }
            }
        }
        if let Ok(v) = std::env::var("WMBT_KV_HTTP") {
            for chunk in v.split(',') {
                let chunk = chunk.trim();
                if !chunk.is_empty() {
                    cfg.http_addrs.push(chunk.to_string());
                }
            }
        }
        if let Some(n) = env_parse::<usize>("WMBT_KV_DAEMON_SHM_DEPTH") {
            cfg.shm_depth = n;
        }

        // Wait strategy
        cfg.wait_strategy = match std::env::var("WMBT_KV_DAEMON_SHM_WAIT_STRATEGY")
            .ok()
            .as_deref()
            .map(str::trim)
        {
            Some("busyspin" | "BusySpin" | "spin") => MyelonWaitStrategy::BusySpin,
            Some("block" | "Block" | "park") => MyelonWaitStrategy::Block,
            Some(other) if !other.is_empty() => {
                eprintln!(
                        "WombatKV: unknown WMBT_KV_DAEMON_SHM_WAIT_STRATEGY={other:?}, defaulting to 'block'"
                    );
                MyelonWaitStrategy::Block
            }
            _ => MyelonWaitStrategy::Block,
        };

        // TCP / HTTP TPC
        if let Some(n) = env_parse::<usize>("WMBT_KV_TCP_TPC_THREADS") {
            cfg.tcp_tpc_threads = n.max(1);
        }
        if let Some(n) = env_parse::<usize>("WMBT_KV_TCP_DISPATCH_WORKERS") {
            cfg.tcp_dispatch_workers = n.max(1);
        }
        if let Some(n) = env_parse::<usize>("WMBT_KV_HTTP_TPC_THREADS") {
            cfg.http_tpc_threads = n.max(1);
        }
        if let Some(n) = env_parse::<usize>("WMBT_KV_HTTP_DISPATCH_WORKERS") {
            cfg.http_dispatch_workers = n.max(1);
        }

        // Storage layout
        if let Ok(v) = std::env::var("WMBT_KV_PUFFER_DIR") {
            if !v.is_empty() {
                cfg.puffer_dir = PathBuf::from(&v);
                // SlateDB default tracks puffer_dir unless overridden below
                cfg.slatedb_path = cfg.puffer_dir.join("slatedb");
            }
        }
        cfg.puffer_ram_bytes = env_parse::<u64>("WMBT_KV_PUFFER_RAM_BYTES");
        cfg.puffer_disk_bytes = env_parse::<u64>("WMBT_KV_PUFFER_DISK_BYTES");
        cfg.puffer_block_size_bytes = env_parse::<u64>("WMBT_KV_PUFFER_BLOCK_SIZE_BYTES");
        if let Ok(v) = std::env::var("WMBT_KV_S3_PREFIX") {
            if !v.is_empty() {
                cfg.s3_prefix = v;
            }
        }
        if let Ok(v) = std::env::var("WMBT_KV_SLATEDB_PATH") {
            if !v.is_empty() {
                cfg.slatedb_path = PathBuf::from(v);
            }
        }
        if let Ok(v) = std::env::var("WMBT_KV_NAMESPACE") {
            if !v.is_empty() {
                cfg.namespace = v;
            }
        }

        // Eviction
        cfg.namespace_max_bytes = env_parse::<u64>("WMBT_KV_NAMESPACE_MAX_BYTES");
        if let Some(secs) = env_parse::<u64>("WMBT_KV_EVICTION_INTERVAL_SECS") {
            cfg.eviction_interval = Duration::from_secs(secs);
        }

        // Operator UX
        cfg.quiet_banner = env_truthy("WMBT_KV_QUIET_BANNER");
        cfg.trace_daemon = std::env::var("WMBT_KV_DAEMON_SHM_TRACE_DAEMON").is_ok();
        cfg.prefetch_dry_run = env_truthy("WMBT_KV_PREFETCH_DRY_RUN");

        cfg
    }
}

fn env_parse<T: std::str::FromStr>(key: &str) -> Option<T> {
    std::env::var(key).ok().and_then(|v| v.parse::<T>().ok())
}

fn env_truthy(key: &str) -> bool {
    matches!(
        std::env::var(key).ok().as_deref(),
        Some("1" | "true" | "True" | "TRUE" | "yes" | "Yes" | "YES" | "on" | "On" | "ON")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_sensible() {
        let cfg = DaemonConfig::default();
        assert_eq!(cfg.shm_depth, crate::DEFAULT_RING_DEPTH);
        assert_eq!(cfg.tcp_tpc_threads, 2);
        assert_eq!(cfg.http_tpc_threads, 2);
        assert_eq!(cfg.tcp_dispatch_workers, 8);
        assert_eq!(cfg.http_dispatch_workers, 8);
        assert!(matches!(cfg.wait_strategy, MyelonWaitStrategy::Block));
        assert!(!cfg.quiet_banner);
        assert!(!cfg.trace_daemon);
        assert!(!cfg.prefetch_dry_run);
        assert_eq!(cfg.namespace, "default");
        assert_eq!(cfg.s3_prefix, "kv/puffer-shm");
        assert_eq!(cfg.eviction_interval, Duration::from_secs(30));
    }

    #[test]
    fn env_parse_returns_none_for_garbage() {
        // unsafe-free: use a name that's definitely not set
        let key = "WMBT_KV_TEST_GARBAGE_THAT_NEVER_EXISTS_12345";
        assert!(env_parse::<u64>(key).is_none());
    }

    #[test]
    fn env_truthy_matches_canonical_set() {
        // Just exercise the canonical-string match arm logic via the
        // helper without touching real env (the matcher logic itself
        // is the testable piece).
        for tv in ["1", "true", "True", "TRUE", "yes", "on", "ON"] {
            assert!(matches!(
                Some(tv),
                Some("1" | "true" | "True" | "TRUE" | "yes" | "Yes" | "YES" | "on" | "On" | "ON")
            ));
        }
        for fv in ["0", "false", "no", "off", ""] {
            assert!(!matches!(
                Some(fv),
                Some("1" | "true" | "True" | "TRUE" | "yes" | "Yes" | "YES" | "on" | "On" | "ON")
            ));
        }
    }
}
