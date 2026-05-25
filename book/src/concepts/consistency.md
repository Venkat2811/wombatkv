# Consistency model

WombatKV is a content-addressed object store for LLM KV-cache blocks
plus a metadata index (radix tree in RAM, SlateDB on disk) that points
the engine at "what's already saved." This document describes what
consistency properties hold across the data plane (S3) + metadata
plane (SlateDB) + in-memory plane (radix index).

**TL;DR:** read-your-writes within a process, monotonic-read across
processes on the same daemon, eventual cross-host. PUT is idempotent
by content-addressing. Reads through a single daemon see writes in
publish order. There's no formal isolation level; we describe what
actually holds rather than picking a label.

---

## Data model

- **Block:** an immutable opaque payload identified by its BLAKE3-20
  content hash. Identical bytes → identical hash → same S3 object.
- **Chain:** a sequence of blocks representing a prefix of KV-cache
  state. Each chain has a head pointer that names its tip; the chain
  is reconstructed by following the chain from the tip back to the
  root via parent links.
- **Sidecar:** auxiliary state attached to a chain tip (raw-tail
  compressor frontier; engine-specific).
- **Fingerprint:** model-identity envelope (model + tokenizer + dtype
  + TP/PP + LoRA topology). Two chains with different fingerprints
  cannot be mixed; the hash domain is isolated.

## Storage planes

| Plane | What | Durability | Latency |
|---|---|---|---|
| L0 in-memory radix | (namespace, key) → block_meta | Process lifetime | < 1 µs |
| L1 SlateDB on-disk | Same shape, persisted | Process restart | sub-ms |
| L2 S3 (canonical) | Object payload + chain-head | Bucket lifetime | 10-100 ms |

S3 is the **single source of truth**. L0 and L1 are caches/indexes that
bootstrap from L2 on startup.

---

## What holds

### Atomicity

- **Single-block PUT** to S3 is atomic (S3's standard guarantee).
- **Multi-block PUT** (`PUT_KV_BLOCKS_BATCH`) is NOT atomic across
  blocks. Blocks land individually; chain-head publish completes the
  group. Crash between block N and chain-head = blocks present but
  not "in" the chain; next startup ignores them as orphans, eviction
  reclaims them.
- **Chain head publish** is atomic at S3 level (single object PUT).
  Before this PUT, the chain is invisible to readers. After this PUT,
  the chain is visible.

### Visibility / read-your-writes

**Within a single process (cabi embedded mode):**
- A PUT that returns Ok is immediately visible to subsequent GETs
  from the same process. Read-your-writes holds.
- L0 radix is updated synchronously inside PUT before returning.
- SlateDB L1 write-through is also synchronous.

**Within a single daemon (multiple engines via SHM):**
- Same read-your-writes guarantee. The daemon's L0 radix is
  shared via Arc across all SHM consumer threads.

**Across daemon restarts:**
- Read-your-writes holds for any PUT that completed before the
  crash AND whose chain-head publish was acknowledged by S3.
- A PUT that crashed BETWEEN block-publish and chain-head publish
  appears as orphan blocks; on next startup, they are visible to
  GETs by content hash but the chain that referenced them is
  rebuilt from L1+L2 truth and excludes the orphan tail.

**Across hosts:**
- Two daemons against the same S3 bucket see each other's writes
  with S3 propagation latency (sub-second on AWS, ms on MinIO).
- L0 radix on host B does not auto-update when host A writes; B's
  L0 is hydrated only at startup. Cross-host write visibility on
  B today requires a B-side restart or explicit refresh (RFC 0019
  HELLO handshake post-alpha will add bucket-version cache poisoning).

### Ordering

- **Per-content-hash:** PUT is idempotent. Two concurrent PUTs of
  the same content hash converge, both succeed, both write the
  same bytes. No torn state possible.
- **Per-chain:** chain-head publishes are linearizable per chain
  (single chain head pointer; S3's last-writer-wins is what we get).
  Two writers racing to extend the same chain tip → one wins; the
  loser must re-base its extension on the winner's tip.
- **Across chains:** no global order. WombatKV provides per-chain
  ordering, not multi-chain transactional commit.

### Isolation

- **No formal isolation level.** Single-block PUT is serializable
  by content-addressing. Multi-block transactions don't exist.
- **Sidecar drift:** sidecar PUT and chain-head PUT are two separate
  S3 writes. If the chain-head publishes but the sidecar PUT fails,
  the chain is visible without its compressor frontier, the engine
  must fall back to recomputing the frontier from chain bytes.
  Documented as `SidecarDriftAfterChain` in DST.

---

## What does NOT hold

- **Cross-bucket atomicity.** PUTs to two different S3 buckets are
  independent.
- **Atomic multi-block transactions.** No 2PC, no batch commit. If
  the engine needs all-or-nothing semantics for a multi-block
  rewrite, it must build that on top of per-block PUTs.
- **CAS / compare-and-swap on chain head.** Currently last-writer-wins.
  Concurrent chain rewrites lose the loser's tail. Operators wanting
  CAS semantics should serialize chain extensions in their engine.
- **Read-after-write across hosts in real time.** Host A writes →
  Host B's daemon sees it after L0 refresh (today: restart; post-RFC
  0019: HELLO-handshake-driven cache invalidation).
- **Total order across all writers.** No global clock. Two writers
  to disjoint chains have no causal relationship.

---

## What the engine should assume

For a single ds4 process talking to a single WombatKV daemon (the
0.1.0-alpha wedge):

```
1. PUT(block_A) returns Ok
2. PUT(block_B with parent=block_A) returns Ok
3. CHAIN_HEAD_PUBLISH(chain_X tip=block_B) returns Ok
4. GET(block_A) returns block_A bytes              ← guaranteed
5. GET(block_B) returns block_B bytes              ← guaranteed
6. LOOKUP_BLOCK_PREFIX(chain_X) returns 2 blocks   ← guaranteed
7. (daemon crash + restart)
8. LOOKUP_BLOCK_PREFIX(chain_X) returns 2 blocks   ← guaranteed via L1/L2
```

After step 3, the chain is durable.  Steps 4-8 inherit that
durability.

What is NOT guaranteed:
```
A. PUT(block_C with parent=block_B) sent BUT NOT ACKED before crash
   → After restart, LOOKUP_BLOCK_PREFIX(chain_X) returns 2 blocks
     (block_C is invisible; if it landed in S3, it's an orphan; if
     it didn't, no trace at all).
```

This is the standard durable-after-ack model.

---

## DR considerations

S3 bucket is the single source of truth. WombatKV does not provide:
- Cross-region replication (configure on S3 side: S3 Replication Rules)
- Point-in-time recovery (configure on S3 side: bucket versioning + lifecycle)
- Backup / snapshot (S3 itself is durable to AZ failure; for tier-1
  recovery against bucket-level disaster, set up versioning + MFA-delete)

**Operator recommendation for production:**
- Enable S3 bucket versioning (`WMBT_KV_BUCKET` lifecycle rule).
- Configure cross-region replication if availability beyond one
  region matters.
- Periodic `wombatkv-wal-layout-audit` runs to verify on-disk
  layout invariants haven't drifted.

---

## References

- RFC 0008 §5. SlateDB L1 metadata index
- RFC 0009 §4: LRU eviction worker (interacts with chain-head
  visibility; eviction never crosses an unpublished chain head)
- RFC 0018, wire envelope discipline (CRC32C catches in-flight
  corruption; this doc covers correctness after corruption is
  excluded)
- RFC 0019 (sketch), cross-host HELLO handshake; will tighten the
  cross-host RYW story
- `book/src/concepts/recovery.md`, what to do when something breaks
