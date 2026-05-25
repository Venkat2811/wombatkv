# Methodology caveats

These caveats materially affect interpretation of every table in this
campaign. Read them before citing any single number.

## 1. `same_process` is not uninterrupted live generation

The exact-family `same_process` cell means:

- one `ds4-server`
- prompt A once
- prompt A again, without restart

It is a valid "immediate replay / live-ish" baseline, but it is not a
pure measure of uninterrupted decoding continuation.

## 2. Native ds4 is not "cold ds4"

Scenario and most exact / public native rows use ds4 with its real
kv-disk path enabled.

That is intentional. WombatKV should not be compared only against a
crippled native baseline. Where a stricter cold control was needed, we
measured `native_cold` separately.

## 3. `pi_review` is a real loss cell for the current WombatKV build

The measured outcome on this machine is that native beats embedded,
and embedded beats daemon.

That means the "shared prefix should dominate" story is still a
hypothesis for this workload, not a validated result in the current
build.

## 4. `sharegpt_multiturn` was not used as the public benchmark

The scenario file named `sharegpt_multiturn` is hand-authored, not a
replay of public ShareGPT conversation traces.

For public conversational replay we used the dataset-backed
`sharegpt_conv_16_mt4_8.json` corpus instead.

## 5. `cached_tokens_seen` is weak as a scenario metric

In scenario runs, server logs showed clear block-prefix hits even when
the client-side JSON `cached_tokens_seen` field remained zero.

For hit attribution: trust the server logs first; treat
`cached_tokens_seen` as secondary.

## 6. TTFT is the primary restart metric

End-to-end totals are useful, but they move with answer length and
decode behavior. For restart / restore claims:

- TTFT is primary
- total latency is supporting context

## 7. Public workloads were rerun with `thinking` disabled

This was deliberate. ds4 docs warn that low `max_tokens` budgets in
thinking mode can be consumed entirely by hidden reasoning before
visible answer text begins.

For the public workloads we sent:

```json
{"thinking":{"type":"disabled"}}
```

That kept visible-answer behavior comparable across modes while
preserving the same prompts, server, and cache paths.

## 8. The Gutenberg family was intentionally scoped to the three core baselines

Completed rows: `native`, `native_cold`, `embedded_local`.

We stopped before Gutenberg daemon rows because:

- ShareGPT replay already established daemon weakness on realistic interleaving.
- Scenario families already established daemon weakness under local churn.
- The transport microbench gives cleaner daemon-path diagnostics than another hour of long-doc daemon replay.

Read the Gutenberg family as the long-context comparison for the three
most decision-relevant baselines, not as a full six-mode matrix.

## 9. Expanded partial-prefix rows have mixed sample counts

The core `native` / `embedded_local` rows had earlier validated samples
in the workspace. The expansion added new `native_cold` rows, new
daemon rows, and refreshed trial-1 rows for several existing cells.

That means:

- some core rows have `n=2`
- many newly added daemon / native_cold rows have `n=1`

Acceptable for directionality. Confidence is higher on the multi-sample
core rows than on the one-sample expansion rows.

## 10. Remote embedded and remote daemon are not interchangeable

`embedded_remote` and remote daemon both touch remote MinIO, but they
exercise very different restore paths.

The exact-restart logs show:

- `embedded_remote` median `get_ms` about 10 ms
- `daemon_tcp_remote` median `get_ms` about 2936 ms
- `daemon_http_remote` median `get_ms` about 3573 ms

So any statement about "remote WombatKV" must specify which path it
means.
