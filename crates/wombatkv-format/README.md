# wombatkv-format

On-disk format primitives, magic numbers, header layouts, WAL frame
shape, shared by `wombatkv-store` (S3 backend) and `wombatkv-node`
(embedded KV).

The crate is `#![forbid(unsafe_code)]`. Pure byte-layout helpers, no
I/O, no allocation.

## What lives here

- **WAL frame envelope**: `WAL_MAGIC` (4 bytes) + `WAL_HEADER_SIZE`
  (the canonical header byte count). Every WAL record on S3 starts
  with this envelope so any reader can detect format identity and
  reject corrupt blobs before deserialization.
- Wire-format constants the on-disk WAL writer + reader agree on.

## Where the other envelopes live

Per RFC 0018, WombatKV has three independent envelope formats. This crate carries only the first:

| envelope | where defined | scope |
|---|---|---|
| WAL frame | `wombatkv-format` (here) | persistent WAL records on S3 |
| KVB1 v2 block + sidecar v4 | `wombatkv-store` / `wombatkv-node` | content-addressed block storage on S3 |
| universal wire envelope (16-byte: magic+ver+CRC32C+len) | `wombatkv-daemon::envelope` | RPC frames over SHM / TCP / HTTP |

All three are defensive: 4-byte magic, explicit version field,
CRC32C body checksum, body length. Mismatches reject loudly per
the alpha breaking-window policy (no back-compat readers, wipe
bucket and re-save on format bump).

## Stack position

```
wombatkv-format    ← you are here (leaf, no internal deps)
      ↓ depended on by
wombatkv-store        wombatkv-node        (and transitively their consumers)
```

## Minimal usage

```rust
use wombatkv_format::{WAL_MAGIC, WAL_HEADER_SIZE};

// Validate that a candidate WAL frame buffer starts with the right magic.
fn is_wal_frame(buf: &[u8]) -> bool {
    buf.len() >= WAL_HEADER_SIZE && buf.starts_with(&WAL_MAGIC)
}
```

## Test

```sh
cargo test -p wombatkv-format --release
```

## RFCs

- RFC 0002, binary format
- RFC 0018, wire envelope discipline (universal frame envelope)
