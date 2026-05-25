# wombatkv-core

Foundational types and traits shared across the WombatKV stack. The
deepest crate in the dep tree, no `wombatkv-*` dependencies of its
own, so changes here ripple everywhere. Touch with care.

## What lives here

- **`metadata`**, block-metadata types that the radix index and the
  on-disk block format both refer to:
  - `BlockMeta`, per-block payload size + parent chain + format
    family marker
  - `LayoutTag` (16 bytes), per-block layout family (`ds4-v1`,
    `sglang-page-v1`, future engines). Designed for multi-engine
    differentiation; production callers currently pass `[0u8; 16]`.
  - `ModelDigest` (24 bytes), truncated model fingerprint (sha256)
    used as the second factor in S3 key derivation alongside the
    block hash.

- **`reuse`**, cross-cutting reuse-policy primitives: when can a
  block be reused, when does a fingerprint apply across model
  variants, what's a safe canonical-block boundary.

The crate is `#![forbid(unsafe_code)]`. Pure Rust, no I/O, no
allocation beyond what the caller provides, it can compile into
constrained reader environments.

## Stack position

```
wombatkv-core      ← you are here (leaf, no internal deps)
      ↓ depended on by
wombatkv-format        wombatkv-radix        wombatkv-store        wombatkv-node        wombatkv-cabi
```

## Minimal usage

```rust
use wombatkv_core::metadata::{BlockMeta, LayoutTag, ModelDigest};

// Construct a root block (no parent) of 128 KiB with a zero layout tag.
let model_digest: ModelDigest = [0u8; 24];
let layout: LayoutTag = [0u8; 16];
let meta = BlockMeta::new_root(128 * 1024, model_digest, layout);

assert_eq!(meta.payload_bytes, 128 * 1024);
assert!(meta.parent_hash.is_none());
```

## Test

```sh
cargo test -p wombatkv-core --release
```

## RFCs

- RFC 0001, object-storage-native KV (foundational design)
- RFC 0006, block-shaped wire format (where `LayoutTag` is consumed)
