//! TPC (thread-per-core) runtime scaffolding for the wombatkv daemon.
//!
//! # Why this module exists
//!
//! Per-shard compio runtime with SO_REUSEPORT for connection distribution.
//! "shard = OS thread + dedicated compio runtime + CPU pin" shape so
//! the per-prefix worker loop runs on a completion-based reactor with
//! no cross-thread future migration and (on Linux) a hard CPU pin.
//! Today the daemon spawns one plain `std::thread` per prefix and
//! drains the SHM ring synchronously. That works but leaves the
//! Linux `io_uring` / pinned-shard wins on the table. The shape we
//! want is the same per-shard completion-based reactor pattern that
//! high-concurrency request paths benefit from.
//!
//! This module is the SHAPE commit, not the perf commit. It gives the
//! daemon binary an opt-in (`--tpc` CLI flag) path that:
//!
//!  1. spawns N OS threads (one per shard),
//!  2. calls `pin_to_cpu(shard_id)` on each thread (no-op on macOS,
//!     `sched_setaffinity` on Linux),
//!  3. builds a fresh `compio::runtime::Runtime` per thread,
//!  4. runs the shard's async closure to completion inside that
//!     runtime via `rt.block_on(...)`.
//!
//! Cross-thread future migration is impossible by construction: each
//! shard's future is owned by exactly one runtime, never woken by
//! another. That isolation invariant is the whole reason for the
//! per-shard runtime split.
//!
//! # macOS vs Linux
//!
//! `compio` on macOS uses kqueue, which delivers correctness but NOT
//! the `io_uring` perf wins for high-concurrency request paths.
//! Production target is Linux + `sched_setaffinity`. Mac dev box
//! sees the SHAPE land cleanly; perf bench must be on Linux to
//! validate the high-concurrency numbers.
//!
//! On macOS this module still gives us:
//!  - per-shard isolation (no future migration),
//!  - per-shard reactor (each thread parks on its own kqueue fd),
//!  - the wiring needed for a thread-per-core bench cell on a Linux runner
//!    to drop in `sched_setaffinity` and immediately get the
//!    high-concurrency perf shape.
//!
//! # NOT implemented here (deferred)
//!
//!  * Per-shard SHM mailbox routing, the existing per-prefix
//!    `disruptor_mp` rings still hold, the TPC scaffold only owns
//!    the thread+runtime. Cross-shard routing is a future enhancement.
//!  * Async ring-pair attach / drain: `serve_prefix_async` below
//!    is a thin async wrapper around the existing sync `serve_prefix`
//!    body; the actual `compio::io`-backed SHM driver is a future enhancement.
//!  * rkyv on the runtime crossing.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::thread::JoinHandle;

/// Future returned by a shard closure. Boxed + `Send` so the same
/// closure can be invoked on every shard thread and the shape doesn't
/// need to thread a generic future type through the runtime.
pub type ShardFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// Builder closure for a per-shard future. Receives the shard id
/// (`0..num_shards`) and returns the future that runs to completion
/// inside the per-shard compio runtime.
pub type ShardFn = dyn Fn(usize) -> ShardFuture + Send + Sync + 'static;

/// Spawn `num_shards` OS threads, each pinned to a CPU (Linux only),
/// each running its own `compio::runtime::Runtime`. The runtime drives
/// the future produced by `shard_fn(shard_id)` to completion via
/// `rt.block_on(...)`.
///
/// Returns one `JoinHandle` per shard. The caller is responsible for
/// joining them. Each shard runs independently, a panic on shard N
/// does NOT affect shards M ≠ N.
///
/// Naming: threads are named `wkvd-shard-{N}` so they show up in
/// `top -H` / `tracy` / `Instruments` clearly.
pub fn spawn_per_shard(num_shards: usize, shard_fn: Arc<ShardFn>) -> Vec<JoinHandle<()>> {
    (0..num_shards)
        .map(|shard_id| {
            let f = shard_fn.clone();
            std::thread::Builder::new()
                .name(format!("wkvd-shard-{shard_id}"))
                .spawn(move || {
                    // Pin BEFORE the runtime starts so io_uring SQ/CQ
                    // fds are allocated against the pinned NUMA node.
                    // On macOS this is a no-op.
                    pin_to_cpu(shard_id);

                    let rt =
                        compio::runtime::Runtime::new().expect("compio runtime build (per-shard)");
                    rt.block_on(f(shard_id));
                })
                .expect("spawn shard thread")
        })
        .collect()
}

/// Pin the calling thread to a specific CPU.
///
/// On Linux, calls `sched_setaffinity(0, ...)` with a cpu_set
/// containing only `cpu_id`. On other platforms (notably macOS),
/// this is a no-op. Darwin doesn't expose hard CPU pinning to
/// user space. The shape commit still lands; the perf win does not.
///
/// Best-effort: errors are swallowed (logged via stderr only on
/// Linux). A pin failure is not fatal, the shard still runs, just
/// without the cache-line locality benefit.
#[cfg(target_os = "linux")]
pub fn pin_to_cpu(cpu_id: usize) {
    // Build a cpu_set_t with exactly `cpu_id` set, then call
    // sched_setaffinity(0, ...). Using libc directly (vs the `nix`
    // crate) to avoid pulling a new dep, libc is already in the
    // dependency tree.
    //
    // SAFETY: We zero-initialize the cpu_set_t with `std::mem::zeroed`
    // (the all-zero bit pattern is a valid empty CPU set on Linux),
    // then call `libc::CPU_SET` through its function-form binding
    // (which takes `&mut cpu_set_t`). `sched_setaffinity` then reads
    // a `*const cpu_set_t` of the documented size. All three are
    // documented safe libc patterns. The crate-level
    // `unsafe_code = "deny"` is downgraded (not forbidden) precisely
    // so a single, audited unsafe block can wrap this call.
    #![allow(unsafe_code)]
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_SET(cpu_id, &mut set);
        let rc = libc::sched_setaffinity(
            0, // self
            std::mem::size_of::<libc::cpu_set_t>(),
            &set,
        );
        if rc != 0 {
            // Loud but non-fatal, the shard still runs, just unpinned.
            eprintln!(
                "{{\"scope\":\"wombatkv_shm_daemon\",\"event\":\"pin_to_cpu_failed\",\"cpu_id\":{cpu_id},\"errno\":{}}}",
                *libc::__errno_location()
            );
        }
    }
}

/// macOS / FreeBSD / etc, no hard CPU pinning available.
///
/// We could call `pthread_set_qos_class_self_np` on macOS to nudge
/// the scheduler (similar to the existing `set_background_qos_self`
/// in the daemon binary), but that is `QoS`, not pinning, and is
/// orthogonal to this module's scope. Left as a no-op.
#[cfg(not(target_os = "linux"))]
pub fn pin_to_cpu(_cpu_id: usize) {
    // No-op on platforms without hard CPU affinity.
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Smoke: 4 shards, each runs an empty async block and exits.
    /// Verifies the per-shard runtime spawns, runs, and shuts down
    /// cleanly. No CPU-pin assertion (would be flaky on macOS CI).
    #[test]
    fn spawn_per_shard_smoke() {
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_inner = counter.clone();
        let shard_fn: Arc<ShardFn> = Arc::new(move |shard_id: usize| {
            let counter = counter_inner.clone();
            Box::pin(async move {
                counter.fetch_add(shard_id + 1, Ordering::SeqCst);
            }) as ShardFuture
        });
        let handles = spawn_per_shard(4, shard_fn);
        assert_eq!(handles.len(), 4);
        for h in handles {
            h.join().expect("shard thread join");
        }
        // 0+1 + 1+1 + 2+1 + 3+1 = 10
        assert_eq!(counter.load(Ordering::SeqCst), 10);
    }

    /// Verify `pin_to_cpu` doesn't panic on either platform. On macOS
    /// it's a no-op; on Linux it'll try the syscall and either succeed
    /// (no panic) or print an errno line and return (no panic).
    #[test]
    fn pin_to_cpu_does_not_panic() {
        pin_to_cpu(0);
    }
}
