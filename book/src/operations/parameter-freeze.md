# Performance tuning

The canonical reference for every tunable knob is the env-var page
([`env.md`](./env.md)). The substrate uses the `WMBT_KV_*` prefix for
all runtime parameters, organized into four tiers: production
required, production optional, tuning, and debug.

## Key defaults at the substrate boundary

These are the defaults the embedded path and daemon path agree on. Override
via environment variable when the workload warrants.

| Parameter | Default | Notes |
|---|---:|---|
| `block_size_tokens` | `128` | Token-aligned. Valid set: `64`, `128`, `256`. |
| `hash_function` | `BLAKE3` | Content-addressed chain hash, hardware-accelerated. |
| `wire_format` | rkyv | Zero-copy serialization at the wire envelope boundary. |
| `WMBT_KV_S3_GET_TIMEOUT_MS` | `2000` | Per-GET deadline before retry. |
| `WMBT_KV_S3_GET_RETRIES` | `2` | Retry budget for transient S3 failures. |
| `WMBT_KV_S3_RETRY_BACKOFF_MS` | `50` | Initial backoff between retries. |
| `WMBT_KV_MAX_CONCURRENT_GETS` | `8` | Parallel block fetches per restore. |
| `WMBT_KV_MAX_CONCURRENT_PUTS` | `8` | Parallel block saves per chain. |

For the full inventory (production, tuning, and debug tiers) see
[`env.md`](./env.md). For bench-driven tuning guidance see
[`bench-methodology.md`](./bench-methodology.md).

## Validation

Out-of-range values fail loud at handle construction; the substrate
does not silently fall back to a default. Add new tunables with a
companion CHANGELOG entry and a default that matches the alpha's
measured behavior.
