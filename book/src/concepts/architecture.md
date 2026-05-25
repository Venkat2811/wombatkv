# Architecture

This is the conceptual map of how WombatKV is put together. For
operational quickstart see
[`../getting-started/dev-quickstart.md`](../getting-started/dev-quickstart.md);
for the benchmark protocol see
[`../operations/bench-methodology.md`](../operations/bench-methodology.md);
for the deterministic-simulation surface see
[`../operations/dst.md`](../operations/dst.md).

## The 30-second pitch

WombatKV is an object-storage-native KV cache system for LLM
inference. An engine that integrates the C ABI
(`libwombatkv.dylib` / `.so`) can:

1. **Save** KV state from any session as token-aligned
   content-addressed blocks. The bucket layout is the index, no
   separate metadata service.
2. **Restore** any prefix of saved state via a deterministic
   block-prefix hash lookup. Same-prompt and prefix-share are
   unified; there is no special-case "exact-key" path.
3. **Share across processes, machines, and pod restarts** because
   the blocks live in object storage (S3, MinIO, GCS, R2).

The 0.1.0-alpha ships with one engine adapter (ds4 /
DeepSeek-V4-Flash) plus the embedded and daemon modes.

## Crate map

```
crates/
├── wombatkv-core      primitive types
├── wombatkv-format    wire envelope and on-disk segment format
├── wombatkv-radix     metadata index (in-memory and SlateDB-backed)
├── wombatkv-store     object-store layer
├── wombatkv-node      main library (WombatKVKvStore + L0/L1 cache)
├── wombatkv-cabi      C ABI surface (libwombatkv.{so,dylib})
├── wombatkv-daemon    sidecar deployment over SHM / TCP / HTTP
├── wombatkv-dst       deterministic-simulation testing primitives
└── wombatkv-bench     operator + bench binaries
```

## Embedded vs daemon

One data plane (`WombatKVKvStore` in `wombatkv-node::embed`), two
ways to embed it in an engine process.

### Embedded mode (production default)

The engine loads `libwombatkv.dylib` via the C ABI and
`WombatKVKvStore` runs in-process. All calls are direct function
invocations; no IPC.

```
+-------------------+
| engine process    |
|                   |
|  +-------------+  |
|  | engine code |  |
|  +------+------+  |
|         |         |
|  +------v------+  |
|  | libwombatkv |  |       +---------+
|  | (in-proc)   +---------->|  MinIO  | or S3
|  +------+------+  |       +---------+
|         |         |
|  +------v------+  |
|  |  L0/L1 cache|  |       (RAM + local SSD)
|  +-------------+  |
+-------------------+
```

Pros: minimum latency, zero IPC overhead, simplest deployment.
Cons: cache state is per-process. Restarts repopulate from object
storage. One engine per process; no cross-engine cache reuse
without going through object storage.

### Daemon mode (sidecar)

A standalone `wombatkv-daemon` process holds the `WombatKVKvStore`
and serves multiple engine processes over SHM, TCP, or HTTP.

```
+---------+   +---------+   +---------+
| engine1 |   | engine2 |   | engine3 |
+----+----+   +----+----+   +----+----+
     |             |             |
     +------+------+------+------+
            |
        +---v----+
        | daemon |
        +---+----+
            |
       +----v----+
       |  L0/L1  |
       +----+----+
            |
       +----v----+
       |  MinIO  | or S3
       +---------+
```

Pros: one cache shared across engines (cross-process prefix dedup
on the local node), decouples engine restart cycles from cache
state, multi-engine deployments. Cons: ~1-2 ms IPC overhead per
op.

## Block-prefix restore path

WombatKV save and restore is token-aligned and content-addressed
at block granularity. Chain compute per block:

```
seed     = blake3_64hex(domain="WMBTBLK1" || fingerprint)
chain[0] = blake3_64hex(seed || block[0].tokens_as_bytes)
chain[i] = blake3_64hex(chain[i-1] || block[i].tokens_as_bytes)
```

Each block is PUT at `wombatkv/v1/block/b3=<chain[i]>`. Two
prompts that share their first M blocks store M blocks under
identical keys; prefix-share falls out of content addressing with
no special path.

A trailing partial block (tokens that don't fill a full
`block_tokens=128` granule) is stored separately as a `raw_tail`
sidecar.

## The 3-tier cache hierarchy

```
+-----------+  +--------------+  +--------------+
|  L0 RAM   |->|  L1 SSD      |->|  L2 object   |
| (in-proc) |  | (local disk) |  |  store       |
| <1 ms     |  | 5-50 ms      |  | 30-100 ms    |
+-----------+  +--------------+  +--------------+
```

A miss at L0 cascades to L1, which cascades to L2. Writes go
through all three by default; `write_through_s3=false` makes the
L2 PUT best-effort. L0 and L1 are implemented on top of
[foyer](https://github.com/foyer-rs/foyer).

## Metadata index

`wombatkv-radix` defines a `MetadataIndex` trait with two
implementations:

- `InMemoryMetadataIndex`: ships in alpha, used by embedded and daemon modes.
- `SlateDbMetadataIndex`: wraps the SlateDB durable LSM-on-S3.
  Production default. Lets the daemon's metadata survive crashes
  without a full S3 LIST replay.
