//! Client lifecycle helpers for SHM prefix recovery.
//!
//! The SHM transport has no kernel-level peer-death notification. A client
//! killed without running `Drop` can leave both a daemon worker blocked on old
//! rings and stale POSIX SHM names that prevent a new same-prefix client from
//! creating fresh rings. This module adds a small heartbeat file per prefix so
//! the daemon can detect stale/replaced clients and reopen the prefix.

use std::fs;
use std::path::{Path, PathBuf};
use std::process;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::{segment_names, REQ_CONSUMER_ID, RESP_CONSUMER_ID};

const HEARTBEAT_DIR_ENV: &str = "WMBT_KV_DAEMON_SHM_HEARTBEAT_DIR";
// Renamed alpha.14+ from `WMBT_KV_DAEMON_SHM_HEARTBEAT` so the env
// name matches the semantics: heartbeat is ENABLED by default;
// setting this env to a truthy value (`1`, `true`, `on`, `enable`,
// `enabled`, or any non-falsy non-empty string) DISABLES it. Naming
// it `_DISABLE` makes the inversion explicit instead of inferred.
// Pre-OSS alpha breaking-window, no back-compat alias on the old
// name; any external setter must update.
const HEARTBEAT_DISABLE_ENV: &str = "WMBT_KV_DAEMON_SHM_HEARTBEAT_DISABLE";
const HEARTBEAT_INTERVAL_MS_ENV: &str = "WMBT_KV_DAEMON_SHM_HEARTBEAT_INTERVAL_MS";
const HEARTBEAT_STALE_MS_ENV: &str = "WMBT_KV_DAEMON_SHM_HEARTBEAT_STALE_MS";
const HEARTBEAT_CHECK_MS_ENV: &str = "WMBT_KV_DAEMON_SHM_HEARTBEAT_CHECK_MS";
const DEFAULT_HEARTBEAT_INTERVAL_MS: u64 = 500;
const DEFAULT_HEARTBEAT_STALE_MS: u64 = 3_000;
const DEFAULT_HEARTBEAT_CHECK_MS: u64 = 250;

static CONNECTION_SEQ: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone)]
struct HeartbeatRecord {
    id: String,
    pid: u32,
    updated_ms: u128,
}

#[derive(Debug)]
pub struct ClientHeartbeat {
    path: PathBuf,
    id: String,
    interval: Duration,
    last_write: Instant,
}

impl ClientHeartbeat {
    pub fn acquire(prefix: &str) -> Result<Option<Self>, String> {
        if !heartbeat_enabled() {
            return Ok(None);
        }

        let path = heartbeat_path(prefix)?;
        let stale_after = stale_after();

        if let Some(record) = read_record(&path) {
            let active = pid_maybe_alive(record.pid) && !record_is_stale(&record, stale_after);
            if active {
                return Err(format!(
                    "active SHM client for prefix {prefix:?}: pid={} id={}",
                    record.pid, record.id
                ));
            }
            cleanup_prefix_segments(prefix);
            // Stale predecessor died without running Drop. Erase its
            // heartbeat record so our first `beat()` doesn't trip the
            // id-mismatch guard inside `beat()` and abort the connect.
            let _ = fs::remove_file(&path);
        } else if prefix_segments_exist(prefix) {
            cleanup_prefix_segments(prefix);
        }

        let id = format!(
            "{}-{}-{}",
            process::id(),
            now_ms(),
            CONNECTION_SEQ.fetch_add(1, Ordering::Relaxed)
        );
        let mut heartbeat = Self {
            path,
            id,
            interval: heartbeat_interval(),
            last_write: Instant::now().checked_sub(heartbeat_interval()).unwrap(),
        };
        heartbeat.beat()?;
        Ok(Some(heartbeat))
    }

    pub fn beat(&mut self) -> Result<(), String> {
        if self.last_write.elapsed() < self.interval {
            return Ok(());
        }
        if let Some(record) = read_record(&self.path) {
            if record.id != self.id {
                return Err(format!(
                    "heartbeat {} replaced by another client id={}",
                    self.path.display(),
                    record.id
                ));
            }
        }
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).map_err(|err| format!("create heartbeat dir: {err}"))?;
        }
        let body = format!("id={}\npid={}\nupdated_ms={}\n", self.id, process::id(), now_ms());
        // Atomic-replace: write to a per-pid tmp file then rename. Plain
        // `fs::write` does `open(O_TRUNC) + write + close` which leaves a
        // window where the file is empty on disk. The daemon's
        // `HeartbeatMonitor::poll_reopen_reason` reads via
        // `fs::read_to_string`; if it lands in that window it gets an
        // empty file, `read_record` returns None, and the monitor
        // declares the heartbeat MISSING, tearing down SHM rings under
        // a perfectly healthy client. `rename(2)` is atomic on POSIX;
        // either readers see the old file or the new one.
        let tmp_path = self.path.with_extension(format!("tmp.{}", process::id()));
        fs::write(&tmp_path, &body)
            .map_err(|err| format!("write heartbeat tmp {}: {err}", tmp_path.display()))?;
        fs::rename(&tmp_path, &self.path).map_err(|err| {
            format!("rename heartbeat {} -> {}: {err}", tmp_path.display(), self.path.display())
        })?;
        self.last_write = Instant::now();
        Ok(())
    }

    #[must_use]
    pub fn interval(&self) -> Duration {
        self.interval
    }

    pub fn remove(&self) {
        if read_record(&self.path).is_some_and(|record| record.id == self.id) {
            let _ = fs::remove_file(&self.path);
        }
    }
}

impl Drop for ClientHeartbeat {
    fn drop(&mut self) {
        self.remove();
    }
}

#[derive(Debug)]
pub struct HeartbeatMonitor {
    path: PathBuf,
    expected_id: String,
    stale_after: Duration,
    check_interval: Duration,
    last_check: Instant,
}

#[derive(Debug, Clone)]
pub enum ReopenReason {
    Missing,
    Replaced { old_id: String, new_id: String },
    Stale { age_ms: u128 },
}

impl ReopenReason {
    #[must_use]
    pub fn requires_segment_cleanup(&self) -> bool {
        matches!(self, Self::Missing | Self::Stale { .. })
    }

    #[must_use]
    pub fn message(&self) -> String {
        match self {
            Self::Missing => "heartbeat missing".to_string(),
            Self::Replaced { old_id, new_id } => {
                format!("heartbeat generation changed: {old_id} -> {new_id}")
            }
            Self::Stale { age_ms } => format!("heartbeat stale for {age_ms} ms"),
        }
    }
}

impl HeartbeatMonitor {
    #[must_use]
    pub fn attach(prefix: &str) -> Option<Self> {
        if !heartbeat_enabled() {
            return None;
        }
        let path = heartbeat_path(prefix).ok()?;
        let record = read_record(&path)?;
        Some(Self {
            path,
            expected_id: record.id,
            stale_after: stale_after(),
            check_interval: check_interval(),
            last_check: Instant::now().checked_sub(check_interval()).unwrap(),
        })
    }

    pub fn poll_reopen_reason(&mut self) -> Option<ReopenReason> {
        if self.last_check.elapsed() < self.check_interval {
            return None;
        }
        self.last_check = Instant::now();

        let trace = std::env::var("WMBT_KV_DAEMON_SHM_TRACE_HB").is_ok();

        let record_opt = read_record(&self.path);
        if trace {
            if let Some(r) = &record_opt {
                let age_ms = now_ms().saturating_sub(r.updated_ms);
                eprintln!(
                    "[hb-trace] poll path={} got id={} pid={} age_ms={age_ms} expected_id={}",
                    self.path.display(),
                    r.id,
                    r.pid,
                    self.expected_id
                );
            } else {
                let raw = std::fs::read_to_string(&self.path).ok();
                let exists = self.path.exists();
                eprintln!(
                    "[hb-trace] poll path={} record=None exists={exists} raw_len={} raw_first120={:?}",
                    self.path.display(),
                    raw.as_ref().map_or(0, std::string::String::len),
                    raw.as_deref().map(|s| &s[..s.len().min(120)])
                );
            }
        }

        let Some(record) = record_opt else {
            return Some(ReopenReason::Missing);
        };
        if record.id != self.expected_id {
            return Some(ReopenReason::Replaced {
                old_id: self.expected_id.clone(),
                new_id: record.id,
            });
        }
        if record_is_stale(&record, self.stale_after) {
            return Some(ReopenReason::Stale {
                age_ms: now_ms().saturating_sub(record.updated_ms),
            });
        }
        None
    }
}

pub fn cleanup_prefix_segments(prefix: &str) {
    let (req, resp) = segment_names(prefix);
    cleanup_ring_segments(&req, REQ_CONSUMER_ID);
    cleanup_ring_segments(&resp, RESP_CONSUMER_ID);
}

fn cleanup_ring_segments(base: &str, consumer_id: &str) {
    let names = [
        base.to_string(),
        format!("{base}_cr"),
        format!("{base}_ci"),
        format!("{base}_producer_seq"),
        format!("{base}_{consumer_id}_seq"),
    ];
    for name in &names {
        #[cfg(target_os = "linux")]
        {
            let path = Path::new("/dev/shm").join(name);
            let _ = fs::remove_file(path);
        }
        // macOS POSIX SHM lives in the kernel namespace, not the
        // filesystem. The `shared_memory` crate `os_id`s the segment
        // with a leading slash via `shm_open(2)`. `shm_unlink(2)` is
        // the only way to release a stale segment short of reboot -
        // skipping it would force every test to invent a new prefix.
        #[cfg(target_os = "macos")]
        unlink_posix_shm(name);
    }
}

#[cfg(target_os = "macos")]
fn unlink_posix_shm(name: &str) {
    // The `shared_memory` crate passes `os_id` to `shm_open` verbatim,
    // and disruptor-mp gives it names like `wmbt_kv_<prefix>_req` (no
    // leading slash). The macOS shm_open kernel namespace stores
    // exactly that string, so the unlink name has to match without
    // a slash either. Adding `/` here would target a different name
    // and silently no-op with ENOENT (the bug this comment exists
    // to prevent re-introducing).
    let Ok(cname) = std::ffi::CString::new(name) else {
        return;
    };
    // SAFETY: `cname` is a valid C string; `libc::shm_unlink` accepts
    // a `*const c_char` and returns -1 on error which we ignore (the
    // common errors here are ENOENT for already-gone segments and
    // EACCES, neither of which is recoverable from this cleanup
    // helper).
    #[allow(unsafe_code)]
    let rc = unsafe { libc::shm_unlink(cname.as_ptr()) };
    if std::env::var("WMBT_KV_DAEMON_SHM_TRACE_UNLINK").is_ok() {
        let errno = if rc == 0 {
            0
        } else {
            #[allow(unsafe_code)]
            unsafe {
                *libc::__error()
            }
        };
        eprintln!("wombatkv-shm: shm_unlink({name:?}) rc={rc} errno={errno}");
    }
}

fn prefix_segments_exist(prefix: &str) -> bool {
    let (req, resp) = segment_names(prefix);
    [req, resp].iter().any(|name| Path::new("/dev/shm").join(name).exists())
}

fn heartbeat_enabled() -> bool {
    // Heartbeat is ON by default (safety feature: detect dead clients).
    // Setting `WMBT_KV_DAEMON_SHM_HEARTBEAT_DISABLE` to a truthy value
    // turns it OFF. Mirrors the canonical "opt-in by setting" convention
    // used by other WombatKV bool env vars (`WMBT_KV_DST_BUGGIFY`,
    // `WMBT_KV_PREFETCH_DRY_RUN`, `WMBT_KV_QUIET_BANNER`, ...).
    !matches!(
        std::env::var(HEARTBEAT_DISABLE_ENV).ok().as_deref(),
        Some("1" | "true" | "on" | "yes" | "y" | "enable" | "enabled")
    )
}

fn heartbeat_path(prefix: &str) -> Result<PathBuf, String> {
    let dir = std::env::var(HEARTBEAT_DIR_ENV)
        .map_or_else(|_| std::env::temp_dir().join("wombatkv-puffer-shm"), PathBuf::from);
    Ok(dir.join(format!("{}.heartbeat", sanitize_prefix(prefix))))
}

fn sanitize_prefix(prefix: &str) -> String {
    prefix
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' { ch } else { '_' })
        .collect()
}

fn heartbeat_interval() -> Duration {
    duration_from_env_ms(HEARTBEAT_INTERVAL_MS_ENV, DEFAULT_HEARTBEAT_INTERVAL_MS)
}

fn stale_after() -> Duration {
    duration_from_env_ms(HEARTBEAT_STALE_MS_ENV, DEFAULT_HEARTBEAT_STALE_MS)
}

fn check_interval() -> Duration {
    duration_from_env_ms(HEARTBEAT_CHECK_MS_ENV, DEFAULT_HEARTBEAT_CHECK_MS)
}

fn duration_from_env_ms(name: &str, default_ms: u64) -> Duration {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map_or_else(|| Duration::from_millis(default_ms), Duration::from_millis)
}

fn read_record(path: &Path) -> Option<HeartbeatRecord> {
    let text = fs::read_to_string(path).ok()?;
    let mut id = None;
    let mut pid = None;
    let mut updated_ms = None;
    for line in text.lines() {
        if let Some(value) = line.strip_prefix("id=") {
            id = Some(value.to_string());
        } else if let Some(value) = line.strip_prefix("pid=") {
            pid = value.parse::<u32>().ok();
        } else if let Some(value) = line.strip_prefix("updated_ms=") {
            updated_ms = value.parse::<u128>().ok();
        }
    }
    Some(HeartbeatRecord { id: id?, pid: pid?, updated_ms: updated_ms? })
}

fn record_is_stale(record: &HeartbeatRecord, stale_after: Duration) -> bool {
    now_ms().saturating_sub(record.updated_ms) > stale_after.as_millis()
}

fn pid_maybe_alive(pid: u32) -> bool {
    if pid == 0 {
        return false;
    }
    #[cfg(target_os = "linux")]
    {
        Path::new("/proc").join(pid.to_string()).exists()
    }
    #[cfg(target_os = "macos")]
    {
        // kill(pid, 0): 0 -> exists & we can signal; -1 with EPERM ->
        // exists but no permission (still alive); -1 with ESRCH -> dead.
        // SAFETY: `libc::kill` is async-signal-safe and takes by value.
        #[allow(unsafe_code)]
        unsafe {
            if libc::kill(pid as i32, 0) == 0 {
                return true;
            }
            *libc::__error() == libc::EPERM
        }
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        true
    }
}

fn now_ms() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |duration| duration.as_millis())
}
