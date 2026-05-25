# Benchmark artifacts

Three independent benchmark campaigns are preserved here. Each directory
is self-contained with its own methodology + provenance. **Numbers from
different campaigns are not blended**, every speedup is reported with
its source campaign's methodology + caveats.

| Directory | When | Scope | Read it for |
|---|---|---|---|
| [`2026-05_alpha-dev-runs/`](./2026-05_alpha-dev-runs/) | May 13-22, multi-date | Early integration runs across the ds4 bench harness during alpha development | Historical context. The `73.1×` canonical headline + `58.7×` multi-conversation are both here. |
| [`2026-05-24_deployment-mode-matrix/`](./2026-05-24_deployment-mode-matrix/) | 2026-05-24, single day | 6 deployment modes × 3 restart policies on the same prompt + partial-prefix sweep + scenarios | Today's canonical headline matrix. Independent harness, n=5 warmup-primed. |
| [`2026-05-24_public-corpus-replay/`](./2026-05-24_public-corpus-replay/) | 2026-05-24, same day evening extension | ShareGPT round-robin + Gutenberg multi-round (real public corpus replay), per-restore + per-save stage timings, raw TCP transport bench | Multi-turn realism + diagnostic stage timings. n=1 trial; flag as thin. |

## How to read each campaign

Each dir contains:
- `README.md`, campaign-specific methodology, hardware, software versions, caveats.
- `*.csv` / `*.jsonl`, clean rows, one row per measured cell.
- `raw/` (where applicable), the unmodified bench-harness output JSON.

## Charts

`assets/` contains one PNG sub-directory per campaign with the per-campaign
charts. The chart-generator (`scripts/charts/generate_charts.py`) reads
the CSV/JSONL files in each artifact dir and writes the PNGs back to the
matching `assets/` subdir.

## Why three campaigns instead of one consolidated

Different campaigns answer different questions:
- Alpha dev runs prove the system worked end-to-end during development. They span ds4 bench harness, multiple dates, and the engineering iteration that produced the integration.
- Deployment-mode-matrix is the methodical apples-to-apples comparison: same prompt, same machine, same harness, sweep across modes and restart policies. This is the campaign to cite for "vs native ds4" speedup claims.
- Public-corpus-replay tests realism: real ShareGPT conversations, real Gutenberg long-document QA. This is where exact-restart wins meet messy real workloads, and where save-path overhead surfaces as the daemon tax.

Combining numbers across campaigns would mix prompts, harnesses, and dates. Each campaign is a self-consistent unit; cross-campaign comparisons live in [`../BENCHMARKS.md`](../BENCHMARKS.md).
