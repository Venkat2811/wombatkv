# Remote daemon diagnostics

Why remote daemon modes lag while `embedded_remote` stays strong on
exact restart. Distilled from the campaign's diagnostics + transport bench data.

## Exact restart, canonical long prompt, restart_wiped, TTFT p50

| mode | TTFT p50 | speedup vs native | speedup vs embedded_remote |
|---|---:|---:|---:|
| `embedded_remote` | **82.1 ms** | 85.1× | 1.00× (baseline) |
| `daemon_tcp_remote` | 5317.0 ms | 1.31× | 0.015× (65× slower) |
| `daemon_http_remote` | 5424.8 ms | 1.29× | 0.015× (66× slower) |

Both daemon-remote modes are ~65× slower than embedded_remote on the
*same exact-restart task with the same remote MinIO*.

## Restore-stage medians

From per-trial `ds4_kvblocks_load` logs for turn-2 restores
(chain_len=11, loaded_tokens=1478):

| mode | lookup ms | get_ms | load_blocks ms | sidecar ms | entry_to_exit ms |
|---|---:|---:|---:|---:|---:|
| `embedded_remote` | 0.01 | **10.0** | 5.1 | 7.5 | **23.0** |
| `daemon_tcp_remote` | 6.7 | **2936.0** | 5.4 | 8.8 | **3380.4** |
| `daemon_http_remote` | 9.5 | **3572.8** | 9.2 | 9.2 | **3602.5** |

The blow-up is overwhelmingly in `get_ms` (block fetch). `load_blocks`,
`sidecar`, and `chain` stages are comparable across modes.

## Transport microbench

`wombatkv-tcp-multi-load-bench`, 8 clients × 200 ops × 4096-byte payload:

| path | throughput ops/s | PUT p50 µs | GET p50 µs |
|---|---:|---:|---:|
| local loopback TCP | **2361** | 5306 | 118 |
| remote LAN TCP | **493** | 17610 | 9305 |

- Local throughput is 4.79× higher than remote.
- Remote GET p50 is 78.9× higher than local.
- Remote PUT p50 is 3.32× higher than local.

## Interpretation

The remote problem is NOT "MinIO is remote."

Why:
- `embedded_remote` also uses remote MinIO, yet stays near 82 ms.
- Remote daemon falls apart specifically in the fetch path (`get_ms`).
- Raw TCP at 9.3 ms GET p50 is much faster than the daemon's 2936 / 3573 ms `get_ms`.

The bottleneck is in the daemon-side **retrieve/serve orchestration**,
not in the raw transport or the remote object storage.

## Engineering focus

1. Reduce daemon-side fetch RPC overhead and fan-out.
2. Materialize larger restore batches per request.
3. Move daemon restore closer to the embedded path's startup/bootstrap behavior.
4. Keep daemon and object store colocated when possible.
5. Re-measure after each change with the same transport bench and same exact-restart cell.
