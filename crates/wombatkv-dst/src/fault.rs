//! WombatKV fault model. Stage 2 of the DST roadmap (RFC 0011 P9).
//!
//! Defines the universe of faults the DST harness can inject (subset
//! of the operational failure modes WombatKV can encounter) plus a
//! deterministic schedule keyed by trigger conditions.
//!
//! Contrast with a prior vector-search prototype's fault model: that
//! prototype triggered faults by query count on a vector-search server;
//! WombatKV triggers by KV operation count, S3 operation count, or
//! wall clock. The substrate's surface is KV put/get/lookup, not
//! query.
//!
//! ## Failure classes (the seven we expect to find bugs in)
//!
//! 1. **TransientS3Failure**. MinIO / S3 returns 503/Throttling/
//!    NetworkTimeout during a block GET or PUT. Foyer + chain
//!    rehydration must retry or surface a clean error; no panic.
//! 2. **CorruptBlockBytes**: S3 returns bytes with bad blake3
//!    chain hash. The block-load path must reject and either retry
//!    or surface StorageError, NEVER install corrupted KV state.
//! 3. **PartialChainSave**, process crashes mid-chain-save (some
//!    blocks PUT, some not). Next start must NOT load a partial
//!    chain, the chain head's existence proves all referenced
//!    blocks exist (write-then-publish invariant).
//! 4. **ConcurrentSameKeySave**, two saves to the same content-
//!    addressed key (idempotent dedup must work; no torn state).
//! 5. **DaemonRestartMidLookup**, daemon restarts during a lookup;
//!    the embedded client must retry against the new daemon or
//!    surface DaemonUnavailable cleanly. Tests daemon-mode robustness.
//! 6. **SidecarDriftAfterChain**, the raw_tail sidecar PUT
//!    succeeds but the chain head PUT fails. Next bootstrap must
//!    NOT use the orphan sidecar.
//! 7. **FoyerEvictionMidGet**: LRU evicts a block while a
//!    `get_kv_borrowed` lease is live. The Arc holding the bytes
//!    must keep them alive past the eviction (use-after-free
//!    safety).
//!
//! ## What's in this module
//!
//! Stage 0 / scaffold: enum definitions only. No scheduling yet -
//! that lands in Stage 2 with the seeded fault-plan generator.

use serde::{Deserialize, Serialize};

use crate::dst_rng::XorShift64;

/// Concrete fault primitives. Each is a single deterministic action
/// triggered by the schedule at a specific moment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WombatKvFaultEvent {
    /// Inject `ms` ms of delay on the next S3 GET (any key) starting
    /// at the trigger condition.
    S3GetLatency { ms: u64 },
    /// Return an error of the given kind from the next S3 GET.
    S3GetFailure { error_kind: S3ErrorKind },
    /// Return corrupted bytes (random payload of correct size, wrong
    /// blake3 chain hash) from the next S3 GET. Tests chain-hash
    /// rejection paths.
    S3GetCorrupt,
    /// Inject `ms` ms of delay on the next S3 PUT.
    S3PutLatency { ms: u64 },
    /// Return an error from the next S3 PUT.
    S3PutFailure { error_kind: S3ErrorKind },
    /// Kill the chain-save worker between block PUT and the chain-head
    /// publish. Tests partial-chain recovery on next bootstrap.
    KillBeforeChainHead,
    /// Kill the chain-save worker between chain-head publish and the
    /// raw_tail sidecar PUT. Tests sidecar-drift recovery.
    KillBeforeSidecar,
    /// Trigger LRU eviction of `block_hash` during the next
    /// `get_kv_borrowed` lease lifetime. Tests use-after-free safety
    /// of the Arc-backed lease.
    EvictBlockMidLease { block_hash_prefix: String },
    /// Restart the daemon process `n` operations from now.
    DaemonRestart { after_n_ops: u32 },
    /// Drop a TCP/HTTP daemon connection mid-RPC (RFC 0018 Phase 6).
    /// Tests client-side reconnect / surface-error robustness.
    TransportConnectionDrop,
    /// Read on a daemon TCP/HTTP socket returns fewer bytes than
    /// requested (RFC 0018 Phase 6). Tests handler accumulator-buffer
    /// loops for off-by-one / partial-decode bugs.
    TransportPartialRead { return_n: u32 },
    /// Delay `ms` between bytes on a daemon socket write (RFC 0018
    /// Phase 6). Tests client-side timeout policy and head-of-line
    /// blocking under slow peers.
    TransportSlowWrite { ms: u64 },
    /// (RFC 0018 wire-envelope chaos) Daemon ships a response with
    /// a broken universal envelope. Client must reject with a clean
    /// error and not corrupt local state.
    WireBadEnvelope { kind: WireEnvelopeBadKind },
    /// (alpha breaking-window recovery) The S3 bucket contains an
    /// object encoded under an OLDER format version than the daemon
    /// reads. Loader must reject with a clear operator-actionable
    /// hint ("wipe bucket and re-save"), not silently corrupt KV.
    OldVersionInBucket { which: OldVersionKind },
    /// (RFC 0008 §5 recovery) Inject a fault into the SlateDB L1
    /// metadata index path. Daemon must either degrade gracefully
    /// (continue with S3 truth) or fail loudly with a clear hint.
    SlateDbFault { kind: SlateDbFaultKind },
    /// (multi-tenant recovery) A second engine attempts to attach
    /// the same SHM prefix while engine A is live. Daemon must
    /// either serialize them or refuse the second one cleanly,
    /// never silently share state across fingerprints.
    MultiEnginePrefixConflict,
    /// (alpha.14+ daemon startup config) Daemon is started with
    /// `--tpc` (per-shard compio SHM runtime) but no SHM client is
    /// co-located on the daemon host, e.g. a TCP-only cross-host
    /// deployment where the engine is on a different machine. The
    /// SHM TPC shard tries to attach to `wk<prefix>r` /
    /// `wk<prefix>s` segments that never get created (no local
    /// engine to create them). After MAX_OPEN_RETRIES outer × inner
    /// disruptor-mp attach retries, the daemon must fail-loud with
    /// the documented error string, never silently retry forever or
    /// hang. Empirically observed on a Linux x86_64 host with
    /// `wombatkv-daemon --tpc --tcp 0.0.0.0:7878`, fail-loud fired
    /// within ~10 min of daemon startup.
    DaemonTpcOnlyNoShmClient,
    /// Tighten the soft `RLIMIT_NOFILE` to `soft_limit` before the
    /// subject runs. Foyer's hybrid cache opens >1024 fds during
    /// multi-tenant init; macOS per-process accounting hides this but
    /// Linux's default ulimit (1024) trips `Build("Too many open
    /// files")`. Empirically reproduced on a Linux x86_64 host during
    /// the `cabi_adversarial_roundtrip` integration test. This fault
    /// forces the same condition deterministically so callers can
    /// prove graceful-degradation paths.
    ResourceFdLimitTighten { soft_limit: u64 },
    /// (alpha hardening 2026-05-24) Inject ENOMEM on every Nth heap
    /// allocation observed by a tracking allocator. Tests Result
    /// propagation on alloc-critical paths (block-decode buffers,
    /// foyer staging, prefetch queues) instead of letting them
    /// panic / abort under genuine memory pressure.
    ResourceMemoryPressure { fail_every_nth_alloc: u32 },
    /// (alpha hardening 2026-05-24) Fail foyer SSD / SlateDB writes
    /// with ENOSPC after the cumulative byte count exceeds
    /// `fail_after_bytes`. Tests CAS retry + partial-write recovery
    /// boundaries, exercises the same conditions a tier-1
    /// disk-full alert would surface in production.
    ResourceDiskFull { fail_after_bytes: u64 },
    /// (alpha hardening 2026-05-24) Simulate macOS POSIX SHM semantics
    /// under a Linux runner (or vice versa): force `shm_open` to behave
    /// as if `/dev/shm/<name>` does not exist as a filesystem path even
    /// when the kernel does expose it. Catches code that assumes the
    /// /dev/shm filesystem-artifact form of POSIX SHM rather than the
    /// portable `shm_open()` handle form.
    PlatformShmHidden,
    /// (alpha hardening 2026-05-24) Inject `p99_us` µs of completion
    /// jitter onto every io_uring `cqe` poll. Tests timeout discipline
    /// in the compio TPC path under realistic Linux scheduling pressure
    /// instead of the synthetic "every op is instant" loopback scenario.
    PlatformIoUringJitter { p99_us: u64 },
    /// (alpha hardening 2026-05-24) Force the daemon's compio runtime
    /// onto a kqueue-style synchronous path even on Linux, i.e.
    /// simulate Mac's "no real async completion" model from a Linux
    /// host. Allows cross-platform behavior testing on one runner.
    PlatformKqueueOnly,
}

/// Sub-kinds of `WireBadEnvelope`. Each maps to one of the failure
/// modes the RFC 0018 envelope module rejects in `decode_envelope`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum WireEnvelopeBadKind {
    /// First 4 bytes are not `WMBT` magic.
    BadMagic,
    /// `version` field is not 1.
    BadVersion,
    /// Body CRC32C does not match the envelope's `crc32c` field.
    BadCRC,
    /// `len` field is > `MAX_FRAME_BYTES` (potential
    /// allocator-abuse). Client must reject BEFORE allocating.
    OversizedLen,
    /// Envelope header arrives intact but the body is short of the
    /// claimed `len`. Client must time out / surface error cleanly.
    TruncatedBody,
}

/// Old versions of the on-disk envelopes. Pre-alpha-breaking-window
/// readers will encounter these only in test fixtures (the alpha
/// breaking window memo says we never carry old-format bytes
/// forward; this DST coverage simulates an operator who failed to
/// wipe their bucket before upgrading).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum OldVersionKind {
    /// Sidecar `raw_tail` envelope at v3 (pre-RFC-0018 Phase 1 -
    /// no CRC32C, no body_len). Reader is v4.
    SidecarV3,
    /// Block `KVB1` envelope at v1 (pre-RFC-0018 Phase 2 -
    /// placeholder body_len + skip-CRC). Reader is v2.
    BlockV1,
}

/// Sub-kinds of `SlateDbFault`. SlateDB is the always-on L1
/// metadata index (unconditionally opened in daemon + cabi).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum SlateDbFaultKind {
    /// Next put returns an error. The daemon's metadata path must
    /// either surface the error or fall back to S3-truth without
    /// silently dropping the write.
    WriteFailure,
    /// Reader sees a stale snapshot (lag between L1 commit and L2
    /// truth). Subsequent reads after S3 publish should converge.
    StaleSnapshot,
    /// SlateDB on-disk manifest is corrupt at daemon startup. Daemon
    /// must fail loudly with a clear operator-actionable hint, never
    /// silently start with empty index against a populated bucket.
    CorruptManifest,
}

/// Error kinds the S3 layer can produce. Mirrors the
/// retryable / non-retryable split of the real `object_store`
/// surface.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum S3ErrorKind {
    /// 404, permanent; do NOT retry.
    NotFound,
    /// 500, retry-eligible.
    InternalError,
    /// 503, retry-eligible (rate-limit, throttling).
    ServiceUnavail,
    /// AWS "Throttling", retry-eligible.
    Throttling,
    /// Simulated TCP timeout, retry-eligible.
    NetworkTimeout,
}

impl S3ErrorKind {
    /// Whether the application-side retry policy should retry on this
    /// error kind. Used by oracle assertions: "transient errors must
    /// recover; permanent errors must surface as Error to the caller."
    #[must_use]
    pub fn is_retryable(self) -> bool {
        !matches!(self, S3ErrorKind::NotFound)
    }
}

/// High-level failure class, one scenario that exercises a specific
/// WombatKV invariant. Each class deterministically expands to one
/// or more `WombatKvFaultEvent`s with timing.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum WombatKvFailureClass {
    /// S3 transient failure during a cold block GET. Foyer should
    /// retry (or surface a clean error); no panic.
    TransientS3Failure,
    /// S3 returns bytes with bad blake3 chain hash. Block-load path
    /// must reject; warm-restore must NOT install corrupt KV state.
    CorruptBlockBytes,
    /// Process crashes mid-chain-save (some blocks PUT, chain head
    /// not yet PUT). Next bootstrap must NOT see the partial chain.
    PartialChainSave,
    /// Two saves to the same content-addressed key (idempotent dedup
    /// must work; no torn state).
    ConcurrentSameKeySave,
    /// Daemon restarts during an embedded-client lookup; client must
    /// retry against the new daemon or surface DaemonUnavailable.
    DaemonRestartMidLookup,
    /// Sidecar PUT succeeds but chain-head PUT fails. Next bootstrap
    /// must NOT use the orphan sidecar.
    SidecarDriftAfterChain,
    /// LRU evicts a block mid-lease. Lease's Arc must keep bytes
    /// alive past eviction (use-after-free safety).
    FoyerEvictionMidGet,
    /// (RFC 0018 Phase 6) TCP/HTTP daemon connection drops mid-RPC.
    /// Client must reconnect cleanly or surface DaemonUnavailable -
    /// no orphan-state / partial-decode panic.
    TransportConnectionDropMidRPC,
    /// (RFC 0018 Phase 6) Read on the daemon socket returns fewer
    /// bytes than asked. Handler's accumulator-buffer loop must
    /// re-read; no off-by-one decode.
    TransportPartialReadOnHeader,
    /// (RFC 0018 Phase 6) Daemon response writes are slow (per-byte
    /// stalls). Tests client-side timeout discipline + head-of-line
    /// blocking behavior under slow peers.
    TransportSlowWrite,
    /// (alpha.12+ wire-envelope chaos) Daemon ships a corrupted RFC 0018
    /// universal envelope (bad magic / version / CRC / oversized / truncated).
    /// Client must reject cleanly and not corrupt local state.
    WireEnvelopeCorruption,
    /// (alpha.12+ breaking-window recovery) Bucket contains pre-RFC-0018
    /// sidecar v3 envelopes from a prior alpha install. Loader must reject
    /// with a clear operator hint, never load corrupt KV.
    OldSidecarV3InBucket,
    /// (alpha.12+ breaking-window recovery) Bucket contains pre-RFC-0018
    /// block v1 KVB1 envelopes. Same recovery expectation.
    OldBlockV1InBucket,
    /// (alpha.12+ RFC 0008 §5 recovery) SlateDB metadata put fails. Daemon
    /// must surface the error or fall back to S3-truth, never silently drop.
    SlateDbWriteFailure,
    /// (alpha.12+ RFC 0008 §5 recovery) SlateDB on-disk manifest is corrupt
    /// at startup. Daemon must fail loudly with a clear hint, never silently
    /// start with empty index against a populated bucket.
    SlateDbManifestCorruption,
    /// (alpha.12+ multi-tenant recovery) Two engines attempt to attach the
    /// same SHM prefix simultaneously. Daemon must serialize or refuse
    /// cleanly, never silently share state across fingerprints.
    MultiEnginePrefixConflict,
    /// Daemon started with `--tpc` in a TCP-only cross-host deployment
    /// (no co-located SHM client on the daemon host). The SHM TPC
    /// shard's segment-attach loop must fail-loud within bounded time,
    /// never hang or retry forever. Empirically observed on a Linux
    /// x86_64 host with `wombatkv-daemon --tpc --tcp 0.0.0.0:7878`.
    /// Currently a captured-intent class, exercised today by the
    /// `wombatkv-daemon/tests/daemon_tpc_tcp_only_fail_loud.rs`
    /// integration test; will become a live DST scenario when the
    /// runner-child gains real-daemon-binary driving (per the
    /// "What this is NOT (yet)" comment in `wombatkv_dst_runner_child.rs`).
    DaemonTpcOnlyNoShmClient,
    /// Subject runs with a tightened `RLIMIT_NOFILE`. Surfaces fd-budget
    /// assumptions that macOS per-process accounting masks. Real-world
    /// trigger: Linux default `ulimit -n 1024` causes Foyer's hybrid
    /// cache to fail `Build("Too many open files")` during the
    /// `cabi_adversarial_roundtrip` integration test. CI now bumps to
    /// 65536, but the substrate should still degrade
    /// gracefully under a tightened limit instead of panic.
    ResourceFdExhaustion,
    /// (alpha hardening 2026-05-24) Subject runs under allocator
    /// pressure (ENOMEM injected on every Nth allocation). Tests
    /// `Result`-propagation discipline on block decode / prefetch /
    /// staging paths instead of letting them panic / abort.
    ResourceMemoryPressure,
    /// (alpha hardening 2026-05-24) Subject runs with disk writes
    /// failing after a configurable byte threshold (ENOSPC). Tests
    /// CAS retry + partial-write recovery boundaries in the foyer
    /// SSD + SlateDB paths.
    ResourceDiskFull,
    /// (alpha hardening 2026-05-24) Force POSIX SHM `shm_open` to
    /// behave with no `/dev/shm` filesystem artifact (macOS
    /// semantics) even on a Linux host. Catches Linux-only code that
    /// probes the /dev/shm path before calling `shm_open`, the same
    /// architectural critique that surfaced and was fixed in
    /// `wombatkv-bench` on 2026-05-23.
    PlatformShmHidden,
    /// (alpha hardening 2026-05-24) Inject Linux io_uring completion
    /// jitter onto the daemon's compio runtime. Tests timeout
    /// discipline + head-of-line robustness under realistic Linux
    /// scheduling pressure rather than synthetic instant-completion.
    PlatformIoUringJitter,
    /// (alpha hardening 2026-05-24) Force the daemon's compio runtime
    /// onto Mac-style kqueue semantics even on a Linux host -
    /// catches Linux-specific perf assumptions in code that should
    /// degrade gracefully on macOS.
    PlatformKqueueOnly,
}

impl WombatKvFailureClass {
    /// All classes, useful for "run every scenario" sweep tests.
    #[must_use]
    pub fn all() -> &'static [Self] {
        use WombatKvFailureClass::{
            ConcurrentSameKeySave, CorruptBlockBytes, DaemonRestartMidLookup,
            DaemonTpcOnlyNoShmClient, FoyerEvictionMidGet, MultiEnginePrefixConflict,
            OldBlockV1InBucket, OldSidecarV3InBucket, PartialChainSave, PlatformIoUringJitter,
            PlatformKqueueOnly, PlatformShmHidden, ResourceDiskFull, ResourceFdExhaustion,
            ResourceMemoryPressure, SidecarDriftAfterChain, SlateDbManifestCorruption,
            SlateDbWriteFailure, TransientS3Failure, TransportConnectionDropMidRPC,
            TransportPartialReadOnHeader, TransportSlowWrite, WireEnvelopeCorruption,
        };
        &[
            TransientS3Failure,
            CorruptBlockBytes,
            PartialChainSave,
            ConcurrentSameKeySave,
            DaemonRestartMidLookup,
            SidecarDriftAfterChain,
            FoyerEvictionMidGet,
            TransportConnectionDropMidRPC,
            TransportPartialReadOnHeader,
            TransportSlowWrite,
            // alpha.12+ classes, wire / storage / format / version recovery
            WireEnvelopeCorruption,
            OldSidecarV3InBucket,
            OldBlockV1InBucket,
            SlateDbWriteFailure,
            SlateDbManifestCorruption,
            MultiEnginePrefixConflict,
            // alpha.14+, daemon startup configuration recovery
            DaemonTpcOnlyNoShmClient,
            // alpha hardening 2026-05-24: OS resource exhaustion
            ResourceFdExhaustion,
            ResourceMemoryPressure,
            ResourceDiskFull,
            // alpha hardening 2026-05-24, cross-platform behavior
            PlatformShmHidden,
            PlatformIoUringJitter,
            PlatformKqueueOnly,
        ]
    }
}

/// Trigger condition for a scheduled fault. Maps to "when does this
/// fire" in deterministic terms.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FaultTrigger {
    /// Fire on the Nth KV operation (put / get / lookup) received by
    /// the puffer (counted across all namespaces).
    AfterKvOp { n: u32 },
    /// Fire on the Nth S3 GET issued by the puffer.
    AfterS3Get { n: u32 },
    /// Fire on the Nth S3 PUT issued by the puffer.
    AfterS3Put { n: u32 },
    /// Fire `ms` after the puffer's bootstrap_world_knowledge
    /// completes.
    AfterBootstrapMs { ms: u64 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduledFault {
    pub trigger: FaultTrigger,
    pub event: WombatKvFaultEvent,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FaultPlan {
    pub seed: u64,
    pub class: Option<WombatKvFailureClass>,
    pub events: Vec<ScheduledFault>,
}

/// Deterministic seed → fault plan expansion (Stage 2 of the DST
/// roadmap). Given a `master_seed` + `class`, produces a FaultPlan
/// that's identical across runs with the same inputs. Phase 1 of
/// the seed cascade is reserved for fault scheduling, see
/// `dst_rng::XorShift64::phase` and the RFC.
///
/// Per-class invariants the plan exercises:
///
/// - **TransientS3Failure**, a single S3 GET returns ServiceUnavail
///   somewhere in the first 20 GETs. Puffer must retry or surface a
///   clean error; no panic.
/// - **CorruptBlockBytes**, a single S3 GET returns garbage in the
///   first 10 GETs. Chain-hash verification must reject.
/// - **PartialChainSave**, the chain-save worker is killed between
///   block PUT N and the chain-head publish. N is chosen in [1,
///   chain_len-1]; the next bootstrap must NOT see the partial chain.
/// - **ConcurrentSameKeySave**, two PUTs to the same content-
///   addressed key (idempotent dedup). Schedule emits two latency
///   injections at the same trigger to provoke the race.
/// - **DaemonRestartMidLookup**, daemon restarts after N ops in
///   [5, 30]; embedded client must retry cleanly.
/// - **SidecarDriftAfterChain**, chain-head PUT fails AFTER the
///   sidecar PUT succeeds. Next bootstrap must ignore the orphan.
/// - **FoyerEvictionMidGet**: LRU evicts a block mid-lease.
///   Use-after-free safety.
#[must_use]
pub fn schedule_for_class(master_seed: u64, class: WombatKvFailureClass) -> FaultPlan {
    let mut rng = XorShift64::phase(master_seed, 1);
    let events = match class {
        WombatKvFailureClass::TransientS3Failure => vec![ScheduledFault {
            trigger: FaultTrigger::AfterS3Get { n: u32::try_from(rng.between(1, 20)).unwrap_or(1) },
            event: WombatKvFaultEvent::S3GetFailure { error_kind: S3ErrorKind::ServiceUnavail },
        }],
        WombatKvFailureClass::CorruptBlockBytes => vec![ScheduledFault {
            trigger: FaultTrigger::AfterS3Get { n: u32::try_from(rng.between(1, 10)).unwrap_or(1) },
            event: WombatKvFaultEvent::S3GetCorrupt,
        }],
        WombatKvFailureClass::PartialChainSave => vec![ScheduledFault {
            trigger: FaultTrigger::AfterS3Put { n: u32::try_from(rng.between(1, 8)).unwrap_or(1) },
            event: WombatKvFaultEvent::KillBeforeChainHead,
        }],
        WombatKvFailureClass::ConcurrentSameKeySave => vec![
            ScheduledFault {
                trigger: FaultTrigger::AfterKvOp {
                    n: u32::try_from(rng.between(1, 5)).unwrap_or(1),
                },
                event: WombatKvFaultEvent::S3PutLatency { ms: rng.between(50, 200) },
            },
            ScheduledFault {
                trigger: FaultTrigger::AfterKvOp {
                    n: u32::try_from(rng.between(1, 5)).unwrap_or(1),
                },
                event: WombatKvFaultEvent::S3PutLatency { ms: rng.between(50, 200) },
            },
        ],
        WombatKvFailureClass::DaemonRestartMidLookup => vec![ScheduledFault {
            trigger: FaultTrigger::AfterKvOp { n: u32::try_from(rng.between(5, 30)).unwrap_or(5) },
            event: WombatKvFaultEvent::DaemonRestart {
                after_n_ops: u32::try_from(rng.between(1, 5)).unwrap_or(1),
            },
        }],
        WombatKvFailureClass::SidecarDriftAfterChain => vec![ScheduledFault {
            trigger: FaultTrigger::AfterS3Put { n: u32::try_from(rng.between(2, 8)).unwrap_or(2) },
            event: WombatKvFaultEvent::KillBeforeSidecar,
        }],
        WombatKvFailureClass::FoyerEvictionMidGet => {
            // Hash-prefix is purely a placeholder until Stage 4 wires
            // it into the real foyer cache; the seeded value just
            // needs to be deterministic.
            let prefix_seed = rng.next_u64();
            vec![ScheduledFault {
                trigger: FaultTrigger::AfterKvOp {
                    n: u32::try_from(rng.between(3, 15)).unwrap_or(3),
                },
                event: WombatKvFaultEvent::EvictBlockMidLease {
                    block_hash_prefix: format!("{prefix_seed:016x}"),
                },
            }]
        }
        // RFC 0018 Phase 6, transport-layer chaos. Targets the daemon
        // TCP/HTTP handler's per-connection accumulator + dispatch
        // bridge. Stage 3.5 runner (the harness that actually drives a
        // daemon under these schedules) is still pending; today these
        // schedules just verify the scheduler is deterministic + the
        // sweep can exercise the new class names.
        WombatKvFailureClass::TransportConnectionDropMidRPC => vec![ScheduledFault {
            trigger: FaultTrigger::AfterKvOp { n: u32::try_from(rng.between(1, 10)).unwrap_or(1) },
            event: WombatKvFaultEvent::TransportConnectionDrop,
        }],
        WombatKvFailureClass::TransportPartialReadOnHeader => vec![ScheduledFault {
            trigger: FaultTrigger::AfterKvOp { n: u32::try_from(rng.between(1, 10)).unwrap_or(1) },
            event: WombatKvFaultEvent::TransportPartialRead {
                return_n: u32::try_from(rng.between(1, 8)).unwrap_or(1),
            },
        }],
        WombatKvFailureClass::TransportSlowWrite => vec![ScheduledFault {
            trigger: FaultTrigger::AfterKvOp { n: u32::try_from(rng.between(1, 10)).unwrap_or(1) },
            event: WombatKvFaultEvent::TransportSlowWrite { ms: rng.between(10, 100) },
        }],
        // alpha.12+, wire / storage / format / version recovery classes.
        // Stage 3.5 runner integration TBD; today the scheduler produces
        // deterministic plans the sweep harness can exercise + the unit
        // tests in src/lib.rs (corruption rejection) verify the
        // recovery paths directly.
        WombatKvFailureClass::WireEnvelopeCorruption => {
            let kinds = [
                WireEnvelopeBadKind::BadMagic,
                WireEnvelopeBadKind::BadVersion,
                WireEnvelopeBadKind::BadCRC,
                WireEnvelopeBadKind::OversizedLen,
                WireEnvelopeBadKind::TruncatedBody,
            ];
            let idx = usize::try_from(rng.between(0, kinds.len() as u64 - 1)).unwrap_or(0);
            vec![ScheduledFault {
                trigger: FaultTrigger::AfterKvOp {
                    n: u32::try_from(rng.between(1, 10)).unwrap_or(1),
                },
                event: WombatKvFaultEvent::WireBadEnvelope { kind: kinds[idx] },
            }]
        }
        WombatKvFailureClass::OldSidecarV3InBucket => vec![ScheduledFault {
            // Pre-seed fires before any op, bucket already contains
            // old-version objects from the prior alpha install.
            trigger: FaultTrigger::AfterBootstrapMs { ms: rng.between(0, 50) },
            event: WombatKvFaultEvent::OldVersionInBucket { which: OldVersionKind::SidecarV3 },
        }],
        WombatKvFailureClass::OldBlockV1InBucket => vec![ScheduledFault {
            trigger: FaultTrigger::AfterBootstrapMs { ms: rng.between(0, 50) },
            event: WombatKvFaultEvent::OldVersionInBucket { which: OldVersionKind::BlockV1 },
        }],
        WombatKvFailureClass::SlateDbWriteFailure => vec![ScheduledFault {
            trigger: FaultTrigger::AfterKvOp { n: u32::try_from(rng.between(1, 12)).unwrap_or(1) },
            event: WombatKvFaultEvent::SlateDbFault { kind: SlateDbFaultKind::WriteFailure },
        }],
        WombatKvFailureClass::SlateDbManifestCorruption => vec![ScheduledFault {
            trigger: FaultTrigger::AfterBootstrapMs { ms: 0 },
            event: WombatKvFaultEvent::SlateDbFault { kind: SlateDbFaultKind::CorruptManifest },
        }],
        WombatKvFailureClass::MultiEnginePrefixConflict => vec![ScheduledFault {
            trigger: FaultTrigger::AfterKvOp { n: u32::try_from(rng.between(0, 5)).unwrap_or(0) },
            event: WombatKvFaultEvent::MultiEnginePrefixConflict,
        }],
        WombatKvFailureClass::DaemonTpcOnlyNoShmClient => vec![ScheduledFault {
            // Fires at daemon bootstrap, the "fault" is the daemon
            // configuration itself (--tpc + TCP-only, no co-located
            // SHM client). The in-process oracle doesn't spawn the
            // real daemon binary today, so this schedule is currently
            // a captured-intent placeholder; the
            // wombatkv-daemon/tests/daemon_tpc_tcp_only_fail_loud.rs
            // integration test exercises the same invariant against
            // the real binary.
            trigger: FaultTrigger::AfterBootstrapMs { ms: 0 },
            event: WombatKvFaultEvent::DaemonTpcOnlyNoShmClient,
        }],
        // alpha hardening 2026-05-24: OS resource exhaustion. These
        // schedules are captured intent today: real injection requires
        // a tracking allocator + setrlimit() wrapper + ENOSPC mock
        // wired into wombatkv-node's foyer + SlateDB paths. The
        // scheduler produces deterministic plans the seed sweep
        // exercises; once the runner-child gains the injection
        // primitives, these become live scenarios.
        WombatKvFailureClass::ResourceFdExhaustion => vec![ScheduledFault {
            // Fires before the subject runs so the tightened limit is
            // already in place by the time foyer init starts.
            trigger: FaultTrigger::AfterBootstrapMs { ms: 0 },
            event: WombatKvFaultEvent::ResourceFdLimitTighten {
                // Random tightening in [64, 256], well below default
                // 1024 but above the bare minimum cargo's own stdio
                // needs. Foyer should refuse cleanly, not panic.
                soft_limit: rng.between(64, 256),
            },
        }],
        WombatKvFailureClass::ResourceMemoryPressure => vec![ScheduledFault {
            trigger: FaultTrigger::AfterKvOp { n: u32::try_from(rng.between(1, 10)).unwrap_or(1) },
            event: WombatKvFaultEvent::ResourceMemoryPressure {
                // Every Nth alloc fails with ENOMEM where N is in
                // [50, 500], sparse enough for the subject to make
                // forward progress between failures, dense enough to
                // exercise the recovery path within a bench run.
                fail_every_nth_alloc: u32::try_from(rng.between(50, 500)).unwrap_or(100),
            },
        }],
        WombatKvFailureClass::ResourceDiskFull => vec![ScheduledFault {
            trigger: FaultTrigger::AfterS3Put { n: u32::try_from(rng.between(1, 5)).unwrap_or(1) },
            event: WombatKvFaultEvent::ResourceDiskFull {
                // Fail subsequent disk writes after 1-100 MiB cumulative
                //, spans both small-block and large-block regimes.
                fail_after_bytes: rng.between(1 << 20, 100 << 20),
            },
        }],
        // alpha hardening 2026-05-24, cross-platform behavior. These
        // are captured-intent classes that exercise platform-specific
        // assumptions on a single host. Real injection requires the
        // runner-child to wrap shm_open / compio_runtime, wired in
        // alpha.pre2.
        WombatKvFailureClass::PlatformShmHidden => vec![ScheduledFault {
            trigger: FaultTrigger::AfterBootstrapMs { ms: 0 },
            event: WombatKvFaultEvent::PlatformShmHidden,
        }],
        WombatKvFailureClass::PlatformIoUringJitter => vec![ScheduledFault {
            trigger: FaultTrigger::AfterKvOp { n: u32::try_from(rng.between(1, 10)).unwrap_or(1) },
            event: WombatKvFaultEvent::PlatformIoUringJitter {
                // p99 io_uring completion delay in [50, 500] µs -
                // realistic scheduling noise under contention.
                p99_us: rng.between(50, 500),
            },
        }],
        WombatKvFailureClass::PlatformKqueueOnly => vec![ScheduledFault {
            trigger: FaultTrigger::AfterBootstrapMs { ms: 0 },
            event: WombatKvFaultEvent::PlatformKqueueOnly,
        }],
    };
    FaultPlan { seed: master_seed, class: Some(class), events }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_classes_round_trip_via_json() {
        for class in WombatKvFailureClass::all() {
            let s = serde_json::to_string(class).expect("serialize");
            let back: WombatKvFailureClass = serde_json::from_str(&s).expect("deserialize");
            assert_eq!(*class, back);
        }
    }

    #[test]
    fn retryability_split_is_correct() {
        assert!(!S3ErrorKind::NotFound.is_retryable());
        assert!(S3ErrorKind::InternalError.is_retryable());
        assert!(S3ErrorKind::ServiceUnavail.is_retryable());
        assert!(S3ErrorKind::Throttling.is_retryable());
        assert!(S3ErrorKind::NetworkTimeout.is_retryable());
    }

    #[test]
    fn schedule_is_deterministic_per_seed() {
        for class in WombatKvFailureClass::all() {
            let a = schedule_for_class(42, *class);
            let b = schedule_for_class(42, *class);
            // Serialize both to JSON and compare, covers all enum
            // variants without requiring Eq on the event types.
            let a_json = serde_json::to_string(&a).unwrap();
            let b_json = serde_json::to_string(&b).unwrap();
            assert_eq!(a_json, b_json, "non-deterministic plan for {class:?}");
        }
    }

    #[test]
    fn different_seeds_diverge() {
        // Pick a class with > 1 event triggered for clearer divergence.
        let a = schedule_for_class(7, WombatKvFailureClass::ConcurrentSameKeySave);
        let b = schedule_for_class(8, WombatKvFailureClass::ConcurrentSameKeySave);
        let a_json = serde_json::to_string(&a).unwrap();
        let b_json = serde_json::to_string(&b).unwrap();
        assert_ne!(a_json, b_json);
    }

    #[test]
    fn every_class_produces_at_least_one_event() {
        for class in WombatKvFailureClass::all() {
            let plan = schedule_for_class(123, *class);
            assert!(!plan.events.is_empty(), "{class:?} produced no events");
            assert_eq!(plan.class, Some(*class));
            assert_eq!(plan.seed, 123);
        }
    }

    #[test]
    fn plan_round_trips_via_json() {
        let plan = schedule_for_class(2026, WombatKvFailureClass::PartialChainSave);
        let s = serde_json::to_string(&plan).unwrap();
        let back: FaultPlan = serde_json::from_str(&s).unwrap();
        assert_eq!(plan.seed, back.seed);
        assert_eq!(plan.class, back.class);
        assert_eq!(plan.events.len(), back.events.len());
    }
}
