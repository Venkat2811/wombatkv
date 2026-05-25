//! Named magic-number constants for the daemon binary.
//!
//! Daemon code historically had multiple inline magic numbers for
//! timer/sleep durations (250ms graceful shutdown, 100µs tight backoff,
//! 50ms accept polling, etc.). Each was tuned empirically and the
//! rationale lived only in code comments at the use site.
//!
//! This module is the canonical home for those values. Each constant
//! carries rustdoc explaining the choice, change here, and `cargo
//! doc -p wombatkv-daemon` surfaces the tunable inventory.
//!
//! # What's NOT here
//!
//! - User-configurable timeouts (`WMBT_KV_*` env vars): those live
//!   in [`crate::config::DaemonConfig`], the single source for env
//!   reads.
//! - Per-frame budgets / ring depth / SHM segment-name constraints:
//!   those live at the top of `lib.rs` next to the types they
//!   constrain (`FRAME_DATA_BYTES`, `DEFAULT_RING_DEPTH`,
//!   `SHM_SEGMENT_NAME_MAX_LEN_MACOS`).
//! - myelon-side wait durations: use `myelon::default_*_duration()`
//!   primitives + `myelon::perform_*_wait()` callers; do not
//!   duplicate them here.

use std::time::Duration;

/// Graceful-shutdown drain budget for in-flight async-PUT workers
/// after SIGTERM/SIGINT/SIGHUP. Past this, we log "drain timeout"
/// and exit anyway; the operator can escalate to SIGKILL. 10s is
/// generous: ~5 sequential large S3 PUTs at 2s each.
pub const SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

/// Poll interval for the shutdown-flag check inside per-prefix
/// worker loops. 250ms is the sweet spot: responsive enough that
/// the daemon exits within human reaction-time on SIGTERM, slow
/// enough to add ~zero CPU vs the busy-spin alternative.
pub const SHUTDOWN_POLL_INTERVAL: Duration = Duration::from_millis(250);

/// Heartbeat monitor poll interval, interval between checks that
/// the attached client is still alive. 250ms matches the shutdown
/// poll interval so the daemon's "is something still happening"
/// budget is uniform across both signals.
pub const HEARTBEAT_POLL_INTERVAL: Duration = Duration::from_millis(250);

/// Inner-loop backoff in the SHM consumer drain loop. 100µs is
/// short enough not to add latency when work arrives in bursts,
/// long enough to let the OS scheduler reschedule the consumer
/// thread without burning a core.
pub const SHM_INNER_LOOP_BACKOFF: Duration = Duration::from_micros(100);

/// TCP accept-loop retry backoff after a transient bind error.
/// 50ms is the per-attempt cool-down; the outer retry deadline
/// is ATTACH_TIMEOUT (30s, top of `lib.rs`).
pub const TCP_ACCEPT_RETRY_BACKOFF: Duration = Duration::from_millis(50);

/// HTTP server-loop retry backoff after a transient accept error.
/// 20ms, slightly shorter than TCP because HTTP keep-alive
/// pipelines benefit more from rapid reconnect on transient
/// resets.
pub const HTTP_ACCEPT_RETRY_BACKOFF: Duration = Duration::from_millis(20);

/// Per-engine SHM worker thread stack budget. 128 MiB; the worker
/// holds large rkyv frames (4 MiB each) plus a deep-ish call
/// chain through the dispatch closure into the wombatkv-node
/// embed path. 8 MiB (default thread stack) overflows.
pub const SHM_THREAD_STACK_BYTES: usize = 128 * 1024 * 1024;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations_are_in_sane_ranges() {
        // Documenting expected ranges as a guard against accidental
        // micro/milli/sec confusion in a future edit.
        assert!(SHUTDOWN_DRAIN_TIMEOUT.as_secs() >= 5);
        assert!(SHUTDOWN_DRAIN_TIMEOUT.as_secs() <= 60);
        assert!(SHUTDOWN_POLL_INTERVAL.as_millis() >= 50);
        assert!(SHUTDOWN_POLL_INTERVAL.as_millis() <= 1000);
        assert!(SHM_INNER_LOOP_BACKOFF.as_micros() >= 10);
        assert!(SHM_INNER_LOOP_BACKOFF.as_micros() <= 10_000);
        assert!(TCP_ACCEPT_RETRY_BACKOFF.as_millis() >= 1);
        assert!(TCP_ACCEPT_RETRY_BACKOFF.as_millis() <= 1000);
    }

    // ShmFrame is 4 MiB; we want at least 32× headroom for a
    // dispatch-closure call chain. 128 MiB gives that. Both sides are
    // `const`, so this is a compile-time assertion, the runtime test
    // form would trigger `clippy::assertions_on_constants`.
    const _: () = assert!(SHM_THREAD_STACK_BYTES >= 32 * crate::FRAME_DATA_BYTES);
}
