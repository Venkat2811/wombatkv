//! Centralized cabi (embedded + remote-client) configuration.
//!
//! Mirror of `wombatkv-daemon::config::DaemonConfig`. All `WMBT_KV_*`
//! env vars the cabi consumes land in one rustdoc-annotated struct
//! with [`CabiConfig::from_env`] doing the resolution.
//!
//! # What lives here
//!
//! Every env var the cabi reads at handle-init time. This is the
//! operator-facing surface for `wmbt_kv_init` / embedded-mode setup,
//! remote-client connection (TCP / HTTP), foyer / slatedb roots, and
//! S3 prefixing.
//!
//! # What lives elsewhere
//!
//! - `WMBT_KV_TIMING` (runtime perf-emit flag): read per-call inside
//!   the timing macro since it's a hot flag that operators may toggle
//!   at runtime via env reload (not yet implemented but the read site
//!   has to stay flexible).
//! - `WMBT_KV_FINGERPRINT24`: per-call argument, not handle-level config.
//! - `WMBT_KV_QUIET_BANNER` + `WMBT_KV_NAMESPACE_MAX_BYTES` (banner +
//!   experimental warnings): one-shot startup checks, read directly.
//! - `MYELON_NODE_ID`, `HOSTNAME`, `COMPUTERNAME`, `HOME`: system /
//!   myelon-namespace env vars, not WombatKV's surface.

use std::path::PathBuf;

/// All cabi-init-time `WMBT_KV_*` knobs in one place.
#[derive(Debug, Clone, Default)]
pub struct CabiConfig {
    /// Logical namespace tag (multi-tenant scope). From
    /// `WMBT_KV_NAMESPACE`. Default `default`.
    pub namespace: String,

    // ---- Transport selection ----
    /// SHM client prefix to connect to a co-located daemon. From
    /// `WMBT_KV_REMOTE_PREFIX`. When set (non-empty), Handle::from_env
    /// returns a Remote backend; embedded/TCP/HTTP branches are skipped.
    pub remote_prefix: Option<String>,

    /// TCP daemon address (host:port). From `WMBT_KV_TCP_ADDR`. When
    /// set (and `remote_prefix` is unset), Handle::from_env returns a
    /// RemoteTcp backend.
    pub tcp_addr: Option<String>,

    /// HTTP daemon address (host:port). From `WMBT_KV_HTTP_ADDR`. When
    /// set (and the SHM + TCP branches are unset), Handle::from_env
    /// returns a RemoteHttp backend.
    pub http_addr: Option<String>,

    // ---- Embedded mode (foyer + S3 + SlateDB) ----
    /// Foyer SSD-tier directory. From `WMBT_KV_PUFFER_DIR`. Default
    /// `~/.wombatkv/puffer` (via `default_puffer_dir()`). Also the
    /// default parent for SlateDB if `WMBT_KV_SLATEDB_PATH` is unset.
    pub puffer_dir: Option<PathBuf>,

    /// Foyer RAM-tier byte budget. From `WMBT_KV_PUFFER_RAM_BYTES`.
    pub puffer_ram_bytes: Option<u64>,

    /// Foyer SSD-tier byte budget. From `WMBT_KV_PUFFER_DISK_BYTES`.
    pub puffer_disk_bytes: Option<u64>,

    /// Foyer-internal block size. From `WMBT_KV_PUFFER_BLOCK_SIZE_BYTES`.
    pub puffer_block_size_bytes: Option<usize>,

    /// S3 object-key prefix for embedded-mode blocks. From
    /// `WMBT_KV_S3_PREFIX`. Default supplied by caller (see
    /// `DEFAULT_S3_PREFIX` in `ffi.rs`).
    pub s3_prefix: Option<String>,

    /// SlateDB L1 metadata-index on-disk root. From
    /// `WMBT_KV_SLATEDB_PATH`. Defaults to `<puffer_dir>/slatedb`.
    pub slatedb_path: Option<PathBuf>,
}

impl CabiConfig {
    /// Resolve from env. Reads every `WMBT_KV_*` var the cabi handle-
    /// init flow consumes. The caller (Handle::from_env) then layers
    /// in defaults for anything `None` here and dispatches to the
    /// appropriate Backend.
    #[must_use]
    pub fn from_env() -> Self {
        let namespace = std::env::var("WMBT_KV_NAMESPACE").unwrap_or_default();
        let remote_prefix = env_nonempty("WMBT_KV_REMOTE_PREFIX");
        let tcp_addr = env_nonempty("WMBT_KV_TCP_ADDR");
        let http_addr = env_nonempty("WMBT_KV_HTTP_ADDR");
        let puffer_dir = env_nonempty("WMBT_KV_PUFFER_DIR").map(PathBuf::from);
        let puffer_ram_bytes = env_parse::<u64>("WMBT_KV_PUFFER_RAM_BYTES");
        let puffer_disk_bytes = env_parse::<u64>("WMBT_KV_PUFFER_DISK_BYTES");
        let puffer_block_size_bytes = env_parse::<usize>("WMBT_KV_PUFFER_BLOCK_SIZE_BYTES");
        let s3_prefix = env_nonempty("WMBT_KV_S3_PREFIX");
        let slatedb_path = env_nonempty("WMBT_KV_SLATEDB_PATH").map(PathBuf::from);
        Self {
            namespace,
            remote_prefix,
            tcp_addr,
            http_addr,
            puffer_dir,
            puffer_ram_bytes,
            puffer_disk_bytes,
            puffer_block_size_bytes,
            s3_prefix,
            slatedb_path,
        }
    }
}

fn env_nonempty(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|s| !s.is_empty())
}

fn env_parse<T: std::str::FromStr>(key: &str) -> Option<T> {
    std::env::var(key).ok().and_then(|v| v.parse::<T>().ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_empty() {
        let cfg = CabiConfig::default();
        assert!(cfg.remote_prefix.is_none());
        assert!(cfg.tcp_addr.is_none());
        assert!(cfg.http_addr.is_none());
        assert!(cfg.puffer_dir.is_none());
        assert!(cfg.puffer_ram_bytes.is_none());
        assert!(cfg.namespace.is_empty());
    }
}
