#!/usr/bin/env python3
"""Generate WombatKV bench charts from per-campaign artifact dirs.

Each campaign in artifacts/<campaign>/ produces PNGs in assets/<campaign>/.

Run:
    python3 scripts/charts/generate_charts.py
"""
from __future__ import annotations

import csv
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

ROOT = Path(__file__).resolve().parent.parent.parent
ARTIFACTS = ROOT / "artifacts"
ASSETS = ROOT / "assets"

WIDTH_INCHES = 9.0
DPI = 100

WIN_COLOR = "#1f8a4a"
SMALL_WIN_COLOR = "#5bbf7c"
NEUTRAL_COLOR = "#b3b3b3"
LOSS_COLOR = "#c64a3a"

CAMPAIGN_ALPHA_DEV = "2026-05_alpha-dev-runs"
CAMPAIGN_MODE_MATRIX = "2026-05-24_deployment-mode-matrix"
CAMPAIGN_PUBLIC_REPLAY = "2026-05-24_public-corpus-replay"


def _read_jsonl(path):
    with path.open() as f:
        return [json.loads(line) for line in f if line.strip()]


def _read_csv(path):
    with path.open() as f:
        return list(csv.DictReader(f))


def _save(fig, campaign, name):
    out_dir = ASSETS / campaign
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / name
    fig.savefig(out, dpi=DPI, bbox_inches="tight")
    plt.close(fig)
    print(f"wrote {out.relative_to(ROOT)}")


def _color_for_speedup(s):
    if s >= 5:
        return WIN_COLOR
    if s >= 1.5:
        return SMALL_WIN_COLOR
    if s >= 0.9:
        return NEUTRAL_COLOR
    return LOSS_COLOR


# ===== Campaign 1: alpha-dev-runs =====


def chart_alpha_dev_headline():
    """Paired bars: native ds4 ms baseline vs WombatKV ms per headline scenario.

    Native bars are gray, WombatKV bars colored by speedup tier.
    """
    rows = _read_jsonl(ARTIFACTS / CAMPAIGN_ALPHA_DEV / "headlines.jsonl")
    wins = [r for r in rows if r["status"] in ("WIN", "SMALL_WIN") and r["speedup"] >= 1.5]
    wins.sort(key=lambda r: r["speedup"], reverse=True)
    labels = []
    for r in wins:
        scen = r["scenario"]
        if "pi_review" in scen:
            labels.append("Cross-agent fan-out\n(5 reviewers, per-agent TTFT)")
        elif "canonical" in scen:
            labels.append("Cross-restart canonical\n(1.7k tok)")
        elif "multi_conv" in scen:
            labels.append("Cross-conversation prefix-share\n(5x5 turns, ~9.7k tok shared doc)")
        elif "4842" in scen:
            labels.append("Cross-restart bigger context\n(4.8k tok)")
        elif "xhost_tuned" in scen and "1.3k" in scen:
            labels.append("Cross-engine, cross-machine\n(Mac to Linux MinIO, LAN)")
        elif "path_b" in scen:
            labels.append("Cross-engine, cross-machine\n(Mac to Linux daemon-TCP, LAN)")
        else:
            labels.append(scen)
    natives = [r["native_ms_median"] for r in wins]
    wkvs = [r["wombatkv_ms_median"] for r in wins]
    speedups = [r["speedup"] for r in wins]

    x = np.arange(len(wins))
    width = 0.38
    fig, ax = plt.subplots(figsize=(WIDTH_INCHES + 1, 5.0))
    bars_n = ax.bar(x - width / 2, natives, width, label="native ds4", color=NEUTRAL_COLOR)
    bars_w = ax.bar(x + width / 2, wkvs, width, label="ds4 + WombatKV",
                     color=[_color_for_speedup(s) for s in speedups])
    for bar, t in zip(bars_n, natives):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() * 1.08,
                f"{int(t)} ms", ha="center", va="bottom", fontsize=8)
    for bar, t, sp in zip(bars_w, wkvs, speedups):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() * 1.08,
                f"{int(t)} ms\n{sp:g}x", ha="center", va="bottom", fontsize=8, fontweight="bold")
    ax.set_yscale("log")
    ax.set_ylabel("TTFT (ms, log scale)")
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=20, ha="right", fontsize=8)
    ax.set_title(
        "Alpha dev runs, native ds4 baseline vs ds4 + WombatKV, absolute TTFT\n"
        "ds4 bench harness, multi-date (May 13-22)",
        fontsize=11,
    )
    ax.legend(loc="upper left", fontsize=9)
    ax.grid(axis="y", alpha=0.3)
    ax.set_ylim(top=max(natives) * 3.0)
    _save(fig, CAMPAIGN_ALPHA_DEV, "headline-speedup.png")


def chart_alpha_dev_mode_matrix():
    """Grouped absolute-ms bars: native cold baseline + each WombatKV mode (warm)
    per context size. Speedup ratios annotated on WombatKV bars.
    """
    rows = _read_csv(ARTIFACTS / CAMPAIGN_ALPHA_DEV / "mode_matrix.csv")
    # Build: per (mode, ctx) → (cold_ms, warm_ms, speedup)
    by = {}
    for r in rows:
        mode = r["mode"]
        ctx = int(r["ctx_tokens"])
        cold = r.get("native_cold_ms") or r.get("wombatkv_cold_ms") or ""
        warm = r.get("native_warm_ms") or r.get("wombatkv_warm_ms") or ""
        sp = float(r["speedup"]) if r.get("speedup") else None
        by[(mode, ctx)] = {
            "cold": float(cold) if cold else None,
            "warm": float(warm) if warm else None,
            "sp": sp,
        }
    mode_order = ["native", "embedded", "daemon-shm", "daemon-tcp"]
    ctx_sizes = [512, 1024, 2048]
    x = np.arange(len(ctx_sizes))
    width = 0.2
    fig, ax = plt.subplots(figsize=(WIDTH_INCHES + 1, 5.0))
    colors = {"native": NEUTRAL_COLOR, "embedded": WIN_COLOR, "daemon-shm": "#3a8dd0", "daemon-tcp": "#d0793a"}
    for i, mode in enumerate(mode_order):
        # Use cold ms for native (it's the cold-prefill baseline), warm ms for WombatKV (post-restore)
        if mode == "native":
            values = [by[(mode, c)]["cold"] for c in ctx_sizes]
        else:
            values = [by[(mode, c)]["warm"] for c in ctx_sizes]
        bars = ax.bar(x + (i - 1.5) * width, values, width, label=mode, color=colors[mode])
        for bar, v, c in zip(bars, values, ctx_sizes):
            sp = by[(mode, c)]["sp"]
            if mode == "native":
                label = f"{int(v)} ms"
            else:
                label = f"{int(v)} ms\n{sp:g}x"
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() * 1.15,
                    label, ha="center", va="bottom", fontsize=7, fontweight="bold")
    ax.set_yscale("log")
    ax.set_xticks(x)
    ax.set_xticklabels([f"{c} tokens" for c in ctx_sizes])
    ax.set_ylabel("TTFT (ms, log scale)")
    ax.set_title(
        "Alpha dev runs, native ds4 cold-prefill baseline vs WombatKV warm-restore\n"
        "wombatkv_sweep matrix, 2026-05-19",
        fontsize=11,
    )
    ax.legend(loc="upper left", fontsize=9)
    ax.grid(axis="y", alpha=0.3)
    _save(fig, CAMPAIGN_ALPHA_DEV, "mode-matrix.png")


def chart_alpha_dev_losses():
    """Paired absolute-ms bars: native ds4 vs WombatKV per honest-loss scenario.

    Only rows with both native_ms and wombatkv_ms (5 of 7). The 2 projected
    rows (real_cloud_s3, daemon_tcp_loopback_vs_embedded) are listed in a
    text panel since they have no concrete pair.
    """
    rows = _read_jsonl(ARTIFACTS / CAMPAIGN_ALPHA_DEV / "losses.jsonl")
    items = []  # (label, native_ms, wombat_ms, speedup)
    notes = []
    for r in rows:
        scen = r["scenario"]
        if "kvdisk_preserved" in scen:
            label = "Same machine,\nds4 kvdisk intact"
            items.append((label, r["native_ms"], r["wombatkv_ms"], r["speedup"]))
        elif "warm_engine" in scen:
            label = "Same process,\nno restart"
            items.append((label, r["native_ms"], r["wombatkv_ms"], r["speedup"]))
        elif "multi_user" in scen:
            label = "5 users x 3 turns,\nno restart"
            items.append((label, r["native_ms"], r["wombatkv_ms"], r["speedup"]))
        elif "tcp_lan" in scen:
            label = "Mac -> Linux daemon-TCP\n(LAN)"
            items.append((label, r["native_ms"], r["wombatkv_ms"], r["speedup"]))
        elif "minio_lan" in scen:
            label = "Mac -> Linux MinIO\n(LAN)"
            items.append((label, r["native_ms"], r["wombatkv_ms"], r["speedup"]))
        elif "daemon_tcp_loopback" in scen:
            notes.append(f"daemon-TCP loopback vs embedded: {r['speedup_daemon_tcp']}x vs embedded's {r['speedup_embedded']}x (IPC tax)")
        elif "real_cloud_s3" in scen:
            notes.append(f"Real cloud S3 (projected): ~{r['speedup_projected_low']}-{r['speedup_projected_high']}x (vs 73.1x MinIO loopback baseline)")

    x = np.arange(len(items))
    width = 0.38
    fig, ax = plt.subplots(figsize=(WIDTH_INCHES + 1, 5.0))
    natives = [i[1] for i in items]
    wkvs = [i[2] for i in items]
    sps = [i[3] for i in items]
    bars_n = ax.bar(x - width / 2, natives, width, label="native ds4", color=NEUTRAL_COLOR)
    wkv_colors = [LOSS_COLOR if s < 0.9 else (NEUTRAL_COLOR if s < 1.5 else SMALL_WIN_COLOR) for s in sps]
    bars_w = ax.bar(x + width / 2, wkvs, width, label="ds4 + WombatKV", color=wkv_colors)
    for bar, t in zip(bars_n, natives):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() * 1.10,
                f"{int(t)} ms", ha="center", va="bottom", fontsize=8)
    for bar, t, sp in zip(bars_w, wkvs, sps):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() * 1.10,
                f"{int(t)} ms\n{sp:g}x", ha="center", va="bottom", fontsize=8, fontweight="bold")
    ax.set_yscale("log")
    ax.set_xticks(x)
    ax.set_xticklabels([i[0] for i in items], fontsize=9)
    ax.set_ylabel("Time (ms, log scale)")
    ax.set_title(
        "Alpha dev runs, honest losses, absolute native vs WombatKV times\n"
        "Lower bar = faster. Same-machine kvdisk preserved is the floor of where WombatKV is the wrong tool.",
        fontsize=11,
    )
    ax.legend(loc="upper left", fontsize=9)
    ax.grid(axis="y", alpha=0.3)
    # Notes panel below
    if notes:
        note_text = "Other limits (no paired ms data): " + "; ".join(notes)
        fig.text(0.5, -0.02, note_text, ha="center", fontsize=8, style="italic", color="#555555", wrap=True)
    _save(fig, CAMPAIGN_ALPHA_DEV, "honest-losses.png")


def chart_alpha_dev_transport():
    rows = _read_csv(ARTIFACTS / CAMPAIGN_ALPHA_DEV / "transport_load_bench.csv")
    shm_rows = [r for r in rows if r["mode"] == "daemon-shm"]
    clients = [1, 4, 8]
    mac = [int(r["throughput_ops_s"]) for r in shm_rows if r["platform"] == "M3 Max"]
    linux = [int(r["throughput_ops_s"]) for r in shm_rows if r["platform"] == "Linux x86_64"]
    x = np.arange(len(clients))
    width = 0.35
    fig, ax = plt.subplots(figsize=(WIDTH_INCHES, 4.5))
    bars_mac = ax.bar(x - width / 2, mac, width, label="Mac (M3 Max)", color=WIN_COLOR)
    bars_lx = ax.bar(x + width / 2, linux, width, label="Linux (x86_64)", color="#3a8dd0")
    for bars in (bars_mac, bars_lx):
        for bar in bars:
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 15,
                    f"{int(bar.get_height())}", ha="center", va="bottom", fontsize=9)
    ax.set_xticks(x)
    ax.set_xticklabels([f"{c} client{'s' if c > 1 else ''}" for c in clients])
    ax.set_ylabel("Throughput (ops/s, 1 KiB payload)")
    ax.set_title("Alpha dev runs: daemon-SHM throughput, Mac vs Linux", fontsize=11)
    ax.legend(loc="upper left")
    ax.grid(axis="y", alpha=0.3)
    _save(fig, CAMPAIGN_ALPHA_DEV, "transport-load.png")


# ===== Campaign 2: deployment-mode-matrix =====


def chart_mode_matrix_exact():
    """Absolute TTFT bars: native (baseline, hatched) + all WombatKV modes side-by-side.

    Native + native_cold bars use a dark hatched fill with a thick edge so
    they read as the reference, regardless of how speedup-coloring lands
    for any near-parity WombatKV bar. A horizontal dashed reference line
    at the native TTFT anchors every WombatKV bar against it visually.
    """
    rows = _read_csv(ARTIFACTS / CAMPAIGN_MODE_MATRIX / "exact_prompt_matrix.csv")
    cell = [r for r in rows if r["scenario"] == "canonical_long_prompt" and r["restart_policy"] == "wiped"]
    native_row = next(r for r in cell if r["mode"] == "native")
    native_cold_row = next(r for r in cell if r["mode"] == "native_cold")
    wkv_rows = [r for r in cell if r["mode"] not in ("native", "native_cold")]
    wkv_rows.sort(key=lambda r: -float(r["speedup_vs_native"]))
    ordered = [native_row, native_cold_row] + wkv_rows
    native_ttft = float(native_row["turn2_ttft_ms_p50"])

    labels = [r["mode"] for r in ordered]
    ttfts = [float(r["turn2_ttft_ms_p50"]) for r in ordered]
    speedups = [float(r["speedup_vs_native"]) for r in ordered]
    colors = []
    hatches = []
    edges = []
    edge_widths = []
    for r, sp in zip(ordered, speedups):
        if r["mode"] == "native":
            colors.append("#4a4a4a")  # dark slate, distinct from speedup-gray
            hatches.append("//")
            edges.append("black")
            edge_widths.append(1.6)
        elif r["mode"] == "native_cold":
            colors.append("#7a7a7a")  # medium gray, secondary baseline
            hatches.append("\\\\")
            edges.append("black")
            edge_widths.append(1.6)
        else:
            colors.append(_color_for_speedup(sp))
            hatches.append("")
            edges.append("none")
            edge_widths.append(0)

    fig, ax = plt.subplots(figsize=(WIDTH_INCHES, 5.4))
    bars = ax.bar(range(len(labels)), ttfts, color=colors, edgecolor=edges,
                  linewidth=edge_widths, hatch=hatches)
    # Horizontal reference line at native TTFT (legend identifies it)
    ax.axhline(native_ttft, color="#4a4a4a", linestyle="--", linewidth=1.2, alpha=0.5, zorder=0,
               label=f"native ds4 reference ({int(native_ttft)} ms)")

    ax.set_yscale("log")
    ax.set_ylabel("Turn-2 TTFT p50 (ms, log scale; lower is better)")
    ax.set_xticks(range(len(labels)))
    ax.set_xticklabels(labels, rotation=15, ha="right", fontsize=9)
    ax.set_title(
        "Cross-restart wiped: native ds4 baseline (hatched) vs every WombatKV mode\n"
        "Canonical 5k-char prompt, n=5 warmup-primed",
        fontsize=11,
    )
    ax.grid(axis="y", alpha=0.3)
    for bar, t, r in zip(bars, ttfts, ordered):
        if r["mode"] == "native":
            label = f"{int(t)} ms\n(baseline)"
        elif r["mode"] == "native_cold":
            label = f"{int(t)} ms\n(kvdisk off)"
        else:
            label = f"{int(t)} ms\n{r['speedup_vs_native']}x"
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() * 1.10,
                label, ha="center", va="bottom", fontsize=9, fontweight="bold")
    ax.set_ylim(top=max(ttfts) * 3.5)

    # Explicit legend (proxy patches), restricted to categories actually present
    from matplotlib.patches import Patch
    from matplotlib.lines import Line2D
    present_speedups = [sp for r, sp in zip(ordered, speedups) if r["mode"] not in ("native", "native_cold")]
    legend_items = [
        Patch(facecolor="#4a4a4a", edgecolor="black", linewidth=1.6, hatch="//", label="native ds4 (kv-disk path, baseline)"),
        Patch(facecolor="#7a7a7a", edgecolor="black", linewidth=1.6, hatch="\\\\", label="native_cold (kv-disk disabled)"),
    ]
    if any(s >= 5 for s in present_speedups):
        legend_items.append(Patch(facecolor=WIN_COLOR, label="WombatKV WIN (>= 5x)"))
    if any(1.5 <= s < 5 for s in present_speedups):
        legend_items.append(Patch(facecolor=SMALL_WIN_COLOR, label="WombatKV small WIN (1.5-5x)"))
    if any(0.9 <= s < 1.5 for s in present_speedups):
        legend_items.append(Patch(facecolor=NEUTRAL_COLOR, label="WombatKV near-parity (0.9-1.5x)"))
    if any(s < 0.9 for s in present_speedups):
        legend_items.append(Patch(facecolor=LOSS_COLOR, label="WombatKV LOSS"))
    legend_items.append(Line2D([0], [0], color="#4a4a4a", linestyle="--", linewidth=1.2,
                               label=f"native ds4 ref line ({int(native_ttft)} ms)"))
    ax.legend(handles=legend_items, loc="upper center", fontsize=8, ncol=2, framealpha=0.95)
    _save(fig, CAMPAIGN_MODE_MATRIX, "exact-restart-wiped-by-mode.png")


def chart_mode_matrix_heatmap():
    rows = _read_csv(ARTIFACTS / CAMPAIGN_MODE_MATRIX / "exact_prompt_matrix.csv")
    canonical = [r for r in rows if r["scenario"] == "canonical_long_prompt" and r["mode"] not in ("native", "native_cold")]
    modes = sorted({r["mode"] for r in canonical})
    policies = ["preserved", "wiped", "same_process"]
    grid = np.full((len(modes), len(policies)), np.nan)
    for r in canonical:
        i = modes.index(r["mode"])
        j = policies.index(r["restart_policy"])
        grid[i, j] = float(r["speedup_vs_native"])
    fig, ax = plt.subplots(figsize=(WIDTH_INCHES, 4.5))
    from matplotlib.colors import LogNorm
    finite_vals = grid[np.isfinite(grid)]
    if finite_vals.size:
        im = ax.imshow(grid, aspect="auto", cmap="RdYlGn",
                       norm=LogNorm(vmin=max(0.01, finite_vals.min()), vmax=finite_vals.max()))
    else:
        im = ax.imshow(grid, aspect="auto", cmap="RdYlGn")
    ax.set_xticks(range(len(policies)))
    ax.set_xticklabels(policies)
    ax.set_yticks(range(len(modes)))
    ax.set_yticklabels(modes)
    ax.set_xlabel("Restart policy")
    ax.set_ylabel("Mode")
    ax.set_title("Deployment mode matrix - speedup vs native ds4 (log color)", fontsize=11)
    for i in range(len(modes)):
        for j in range(len(policies)):
            v = grid[i, j]
            if np.isfinite(v):
                color = "white" if (v < 0.05 or v > 50) else "black"
                ax.text(j, i, f"{v:g}x", ha="center", va="center", fontsize=9, color=color, fontweight="bold")
    plt.colorbar(im, ax=ax, label="Speedup vs native (log)")
    _save(fig, CAMPAIGN_MODE_MATRIX, "mode-x-restart-policy-heatmap.png")


def chart_partial_prefix():
    """Grouped paired bars: native ds4 vs embedded_local absolute TTFT,
    per suffix size and restart policy. 6 groups, each showing native + WombatKV.
    """
    rows = _read_csv(ARTIFACTS / CAMPAIGN_MODE_MATRIX / "partial_prefix_sweep.csv")
    # Filter to native + embedded_local only
    selected = [r for r in rows if r["mode"] in ("native", "embedded_local")]
    suffixes = [256, 2048, 8192]
    policies = ["preserved", "wiped"]
    groups = []  # (label, native_ms, wkv_ms, speedup)
    for policy in policies:
        for suffix in suffixes:
            native_r = next((r for r in selected if r["mode"] == "native" and r["restart_policy"] == policy and int(r["suffix_chars"]) == suffix), None)
            wkv_r = next((r for r in selected if r["mode"] == "embedded_local" and r["restart_policy"] == policy and int(r["suffix_chars"]) == suffix), None)
            if native_r and wkv_r:
                native_ms = float(native_r["native_turn2_ms_p50"])
                wkv_ms = float(wkv_r["wombat_turn2_ms_p50"])
                sp = float(wkv_r["speedup_vs_native"])
                groups.append((f"{policy}\nsuffix={suffix}", native_ms, wkv_ms, sp))

    n = len(groups)
    x = np.arange(n)
    width = 0.38
    fig, ax = plt.subplots(figsize=(WIDTH_INCHES, 5.0))
    natives = [g[1] for g in groups]
    wkvs = [g[2] for g in groups]
    sps = [g[3] for g in groups]
    bars_n = ax.bar(x - width / 2, natives, width,
                    label="native ds4 (kv-disk path, baseline)",
                    color="#4a4a4a", edgecolor="black", linewidth=1.4, hatch="//")
    bars_w = ax.bar(x + width / 2, wkvs, width, label="WombatKV embedded_local", color=WIN_COLOR)
    for bar, t in zip(bars_n, natives):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() * 1.08,
                f"{int(t)} ms", ha="center", va="bottom", fontsize=8, color="#333333")
    for bar, t, sp in zip(bars_w, wkvs, sps):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() * 1.08,
                f"{int(t)} ms\n{sp}x", ha="center", va="bottom", fontsize=8, fontweight="bold")
    ax.set_yscale("log")
    ax.set_xticks(x)
    ax.set_xticklabels([g[0] for g in groups], fontsize=9)
    ax.set_ylabel("Turn-2 TTFT p50 (ms, log scale; lower is better)")
    ax.set_title(
        "Partial-prefix sweep: native ds4 (hatched baseline) vs WombatKV embedded_local\n"
        "Shared 10000-char prefix; embedded_local wins every cell, even with native kv-disk preserved",
        fontsize=11,
    )
    ax.legend(loc="upper left", fontsize=9, framealpha=0.95)
    ax.grid(axis="y", alpha=0.3)
    ax.set_ylim(top=max(natives) * 3.0)
    _save(fig, CAMPAIGN_MODE_MATRIX, "partial-prefix-vs-native.png")


def chart_scenarios_losses():
    """Grouped paired bars: native ds4 baseline vs WombatKV variants per scenario.

    Three scenario groups (pi_review preserved, pi_review wiped,
    conversation_switch live), each with c1_native + c2_embedded + c3_daemon.
    Absolute TTFT in ms on log-y so the catastrophic daemon loss is visible.
    """
    rows = _read_csv(ARTIFACTS / CAMPAIGN_MODE_MATRIX / "scenarios.csv")
    # Group by (scenario, variant)
    groups = []
    for key in [("pi_review", "preserved"), ("pi_review", "wiped"), ("conversation_switch", "live")]:
        scen_rows = [r for r in rows if r["scenario"] == key[0] and r["variant"] == key[1]]
        scen_rows.sort(key=lambda r: r["mode"])  # c1_native, c2_embedded, c3_daemon
        groups.append((key, scen_rows))

    modes_per_group = 3  # c1_native, c2_embedded, c3_daemon
    n_groups = len(groups)
    x = np.arange(n_groups)
    width = 0.27
    mode_colors = {"c1_native": NEUTRAL_COLOR, "c2_embedded": LOSS_COLOR, "c3_daemon": LOSS_COLOR}

    fig, ax = plt.subplots(figsize=(WIDTH_INCHES, 5.0))
    for i, mode in enumerate(["c1_native", "c2_embedded", "c3_daemon"]):
        ys = []
        sps = []
        for (_, scen_rows) in groups:
            row = next(r for r in scen_rows if r["mode"] == mode)
            # native row uses native_ttft column; others use wombat
            if mode == "c1_native":
                ys.append(float(row["native_ttft_ms_p50"]))
                sps.append(None)
            else:
                ys.append(float(row["wombat_ttft_ms_p50"]))
                sps.append(float(row["speedup_vs_native"]))
        bars = ax.bar(x + (i - 1) * width, ys, width, label=mode, color=mode_colors[mode])
        for j, (bar, y, sp) in enumerate(zip(bars, ys, sps)):
            if sp is None:
                label = f"{int(y)} ms\n(baseline)"
            else:
                label = f"{int(y)} ms\n{sp}x"
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() * 1.06,
                    label, ha="center", va="bottom", fontsize=8, fontweight="bold")

    ax.set_yscale("log")
    ax.set_xticks(x)
    ax.set_xticklabels([f"{k[0]}\n({k[1]})" for k, _ in groups], fontsize=9)
    ax.set_ylabel("TTFT p50 (ms, log scale)")
    ax.set_title(
        "Scenarios, native ds4 vs WombatKV embedded vs daemon\n"
        "All n=2; conversation_switch values are post percentile-calc fix",
        fontsize=11,
    )
    ax.legend(loc="upper left", fontsize=9)
    ax.grid(axis="y", alpha=0.3)
    ax.set_ylim(top=200000)
    _save(fig, CAMPAIGN_MODE_MATRIX, "scenarios-losses.png")


# ===== Campaign 3: public-corpus-replay =====


def chart_public_workloads():
    """Per-scenario absolute TTFT bars: native ds4 baseline + all measured modes.

    Replaces the speedup-ratio chart so reader sees physical scale of
    WombatKV's TTFT (parity for ShareGPT chat, win for Gutenberg).
    """
    rows = _read_csv(ARTIFACTS / CAMPAIGN_PUBLIC_REPLAY / "public_workloads.csv")
    for scenario in sorted({r["scenario"] for r in rows}):
        scen_rows = [r for r in rows if r["scenario"] == scenario]
        # native first, then native_cold, then wkv modes in order of speedup desc
        native_row = next((r for r in scen_rows if r["mode"] == "native"), None)
        native_cold_row = next((r for r in scen_rows if r["mode"] == "native_cold"), None)
        wkv_rows = [r for r in scen_rows if r["mode"] not in ("native", "native_cold")]
        wkv_rows.sort(key=lambda r: -float(r["speedup_vs_native_ttft"]))
        ordered = []
        if native_row:
            ordered.append(native_row)
        if native_cold_row:
            ordered.append(native_cold_row)
        ordered.extend(wkv_rows)

        labels = [r["mode"] for r in ordered]
        ttfts = [float(r["ttft_p50_ms"]) for r in ordered]
        speedups = [float(r["speedup_vs_native_ttft"]) for r in ordered]
        colors = []
        for r, sp in zip(ordered, speedups):
            if r["mode"] in ("native", "native_cold"):
                colors.append(NEUTRAL_COLOR)
            else:
                colors.append(_color_for_speedup(sp))

        fig, ax = plt.subplots(figsize=(WIDTH_INCHES, 5.0))
        bars = ax.bar(range(len(labels)), ttfts, color=colors)
        ax.set_yscale("log")
        ax.set_ylabel("TTFT p50 (ms, log scale)")
        ax.set_xticks(range(len(labels)))
        ax.set_xticklabels(labels, rotation=15, ha="right", fontsize=9)
        n_trials = scen_rows[0].get("n_trials", "?")
        title = f"{scenario.replace('_', ' ')}, absolute TTFT, native baseline vs WombatKV modes"
        ax.set_title(title, fontsize=11)
        ax.grid(axis="y", alpha=0.3)
        for bar, t, r, sp in zip(bars, ttfts, ordered, speedups):
            if r["mode"] == "native":
                label = f"{int(t)} ms\n(baseline)"
            elif r["mode"] == "native_cold":
                label = f"{int(t)} ms\n(no kvdisk)"
            else:
                label = f"{int(t)} ms\n{sp}x"
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() * 1.06,
                    label, ha="center", va="bottom", fontsize=9, fontweight="bold")
        ax.set_ylim(top=max(ttfts) * 3.0)
        _save(fig, CAMPAIGN_PUBLIC_REPLAY, f"{scenario.replace('_', '-')}-speedup.png")


def chart_save_path_tax():
    rows = _read_csv(ARTIFACTS / CAMPAIGN_PUBLIC_REPLAY / "per_save_stage_timings.csv")
    rows.sort(key=lambda r: float(r["save_entry_to_exit_ms_p50"]))
    labels = [r["mode"] for r in rows]
    times = [float(r["save_entry_to_exit_ms_p50"]) for r in rows]
    colors = [WIN_COLOR] + [LOSS_COLOR] * (len(times) - 1)
    fig, ax = plt.subplots(figsize=(WIDTH_INCHES, 4.0))
    bars = ax.bar(range(len(labels)), times, color=colors)
    ax.set_xticks(range(len(labels)))
    ax.set_xticklabels(labels, rotation=15, ha="right", fontsize=9)
    ax.set_ylabel("Save path entry-to-exit p50 (ms)")
    ax.set_title("Save-path tax by mode (sharegpt_round_robin, embedded baseline)", fontsize=11)
    ax.grid(axis="y", alpha=0.3)
    for bar, t in zip(bars, times):
        mult = t / times[0] if times[0] > 0 else 1.0
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 20,
                f"{int(t)} ms\n({mult:.1f}x)", ha="center", va="bottom", fontsize=9, fontweight="bold")
    _save(fig, CAMPAIGN_PUBLIC_REPLAY, "save-path-tax-by-mode.png")


# ===== Additional richer chart types (added per user feedback to learn from myelon style) =====


def chart_partial_prefix_line():
    """Line graph: suffix size vs TTFT, native vs WombatKV, both restart policies.

    Story: as suffix grows, native climbs much faster than WombatKV embedded.
    Diverging lines on log-log.
    """
    rows = _read_csv(ARTIFACTS / CAMPAIGN_MODE_MATRIX / "partial_prefix_sweep.csv")
    suffixes = [256, 2048, 8192]
    series = {
        ("native", "preserved"): [],
        ("native", "wiped"): [],
        ("embedded_local", "preserved"): [],
        ("embedded_local", "wiped"): [],
    }
    for r in rows:
        key = (r["mode"], r["restart_policy"])
        if key in series and r["wombat_turn2_ms_p50"]:
            series[key].append((int(r["suffix_chars"]), float(r["wombat_turn2_ms_p50"])))
        elif key in series and r["native_turn2_ms_p50"]:
            series[key].append((int(r["suffix_chars"]), float(r["native_turn2_ms_p50"])))
    fig, ax = plt.subplots(figsize=(WIDTH_INCHES, 5.0))
    style = {
        ("native", "preserved"): {"color": "#888888", "linestyle": "-", "marker": "o", "label": "native (kvdisk preserved)"},
        ("native", "wiped"): {"color": LOSS_COLOR, "linestyle": "-", "marker": "s", "label": "native (kvdisk wiped)"},
        ("embedded_local", "preserved"): {"color": SMALL_WIN_COLOR, "linestyle": "--", "marker": "^", "label": "embedded_local (kvdisk preserved)"},
        ("embedded_local", "wiped"): {"color": WIN_COLOR, "linestyle": "-", "marker": "D", "label": "embedded_local (kvdisk wiped)"},
    }
    for key, pts in series.items():
        pts.sort()
        xs = [p[0] for p in pts]
        ys = [p[1] for p in pts]
        s = style[key]
        ax.plot(xs, ys, linewidth=2.2, markersize=9, **s)
        for x, y in zip(xs, ys):
            ax.annotate(f"{y:.0f} ms", (x, y), textcoords="offset points",
                        xytext=(8, 6), fontsize=8, color=s["color"], fontweight="bold")
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xticks(suffixes)
    ax.set_xticklabels([f"{s} chars" for s in suffixes])
    ax.set_xlabel("Suffix size (chars beyond the shared 10000-char prefix)")
    ax.set_ylabel("Turn-2 TTFT p50 (ms, log scale)")
    ax.set_title(
        "Partial-prefix sweep, native vs embedded_local, by restart policy\n"
        "embedded_local stays flatter as suffix grows; native must reprefill the whole tail",
        fontsize=11,
    )
    ax.grid(True, which="both", alpha=0.3)
    ax.legend(loc="upper left", fontsize=9)
    _save(fig, CAMPAIGN_MODE_MATRIX, "partial-prefix-LINE.png")


def chart_stage_breakdown_stacked():
    """Stacked bar: per-restore + per-save stage breakdown by mode.

    Two side-by-side panels: restore (left), save (right). Each bar stacked
    by lookup / get / sidecar / install / other. Surfaces the daemon save-tax.
    """
    restore = _read_csv(ARTIFACTS / CAMPAIGN_PUBLIC_REPLAY / "per_restore_stage_timings.csv")
    save = _read_csv(ARTIFACTS / CAMPAIGN_PUBLIC_REPLAY / "per_save_stage_timings.csv")
    # Restore: filter to sharegpt_round_robin for comparable rows
    restore_sg = [r for r in restore if r["scenario"] == "sharegpt_round_robin"]
    restore_sg.sort(key=lambda r: float(r["entry_to_exit_ms_p50"]))

    fig, (axL, axR) = plt.subplots(1, 2, figsize=(WIDTH_INCHES + 3, 5.0))

    # Left: restore stages stacked
    modes_r = [r["mode"] for r in restore_sg]
    lookup = [float(r["lookup_ms_p50"]) for r in restore_sg]
    get_ms = [float(r["get_ms_p50"]) for r in restore_sg]
    load_blocks = [float(r["load_blocks_ms_p50"]) for r in restore_sg]
    sidecar = [float(r["sidecar_ms_p50"]) for r in restore_sg]
    chain = [float(r["chain_ms_p50"]) for r in restore_sg]
    total = [float(r["entry_to_exit_ms_p50"]) for r in restore_sg]
    other = [max(0, t - (lk + g + lb + sc + ch)) for t, lk, g, lb, sc, ch in zip(total, lookup, get_ms, load_blocks, sidecar, chain)]
    bottom = np.zeros(len(modes_r))
    stage_colors = {"lookup": "#888888", "get (block fetch)": "#3a8dd0", "load_blocks": "#5bbf7c", "sidecar": "#d0793a", "chain": "#7c4aab", "other": "#c64a3a"}
    for label, vals in [("lookup", lookup), ("get (block fetch)", get_ms), ("load_blocks", load_blocks), ("sidecar", sidecar), ("chain", chain), ("other", other)]:
        axL.bar(range(len(modes_r)), vals, bottom=bottom, label=label, color=stage_colors[label])
        bottom = bottom + np.array(vals)
    for i, t in enumerate(total):
        axL.text(i, t + 5, f"{t:g} ms", ha="center", va="bottom", fontsize=9, fontweight="bold")
    axL.set_xticks(range(len(modes_r)))
    axL.set_xticklabels(modes_r, rotation=15, ha="right", fontsize=9)
    axL.set_ylabel("Restore path p50 (ms)")
    axL.set_title("Per-restore stage breakdown (sharegpt_round_robin warm hits)", fontsize=10)
    axL.legend(loc="upper left", fontsize=8)
    axL.grid(axis="y", alpha=0.3)

    # Right: save total (we only have entry_to_exit total per mode for save)
    save_sorted = sorted(save, key=lambda r: float(r["save_entry_to_exit_ms_p50"]))
    modes_s = [r["mode"] for r in save_sorted]
    times_s = [float(r["save_entry_to_exit_ms_p50"]) for r in save_sorted]
    colors_s = [WIN_COLOR] + [LOSS_COLOR] * (len(times_s) - 1)
    bars = axR.bar(range(len(modes_s)), times_s, color=colors_s)
    for bar, t, vs in zip(bars, times_s, [r["vs_embedded_save"] for r in save_sorted]):
        axR.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 40,
                 f"{int(t)} ms\n({vs}x)", ha="center", va="bottom", fontsize=9, fontweight="bold")
    axR.set_xticks(range(len(modes_s)))
    axR.set_xticklabels(modes_s, rotation=15, ha="right", fontsize=9)
    axR.set_ylabel("Save path entry-to-exit p50 (ms)")
    axR.set_title("Per-save total cost, the daemon save-tax (embedded baseline)", fontsize=10)
    axR.grid(axis="y", alpha=0.3)

    fig.suptitle(
        "Stage-level diagnostic, why embedded beats daemon on real chat\n"
        "Restore is comparable; save-path is 6.6-8x slower under daemon",
        fontsize=11, fontweight="bold",
    )
    fig.tight_layout(rect=[0, 0, 1, 0.94])
    _save(fig, CAMPAIGN_PUBLIC_REPLAY, "stage-breakdown-restore-vs-save.png")


def chart_sharegpt_turn_cliff():
    """Grouped bar: turn-1 TTFT vs later-turn TTFT for each mode on ShareGPT round-robin.

    Story: native stays flat; embedded stays flat-ish; daemon falls off cliff
    on later turns (later-turn 8-10x slower than turn-1).
    """
    rows = _read_csv(ARTIFACTS / CAMPAIGN_PUBLIC_REPLAY / "public_workloads.csv")
    sg = [r for r in rows if r["scenario"] == "sharegpt_round_robin"]
    mode_order = ["native", "native_cold", "embedded_local", "daemon_shm", "daemon_tcp_local", "daemon_http_local"]
    sg_by_mode = {r["mode"]: r for r in sg}
    turn1 = [float(sg_by_mode[m]["turn1_ttft_p50_ms"]) for m in mode_order]
    later = [float(sg_by_mode[m]["later_turn_ttft_p50_ms"]) for m in mode_order]

    x = np.arange(len(mode_order))
    width = 0.38
    fig, ax = plt.subplots(figsize=(WIDTH_INCHES, 5.0))
    bars1 = ax.bar(x - width / 2, turn1, width, label="turn-1 TTFT p50", color=SMALL_WIN_COLOR)
    bars2 = ax.bar(x + width / 2, later, width, label="later-turn TTFT p50", color=LOSS_COLOR)
    for bar, t in zip(bars1, turn1):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() * 1.05, f"{int(t)}",
                ha="center", va="bottom", fontsize=8)
    for bar, t in zip(bars2, later):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() * 1.05, f"{int(t)}",
                ha="center", va="bottom", fontsize=8)
    # Annotate the cliff ratio centered above the taller of the two bars,
    # clamped so it stays visible even with log-scale axis ceiling.
    for i, (t1, tl) in enumerate(zip(turn1, later)):
        ratio = tl / t1 if t1 else 0
        # Place ratio annotation between the two bars at roughly the later-turn bar height
        ypos = tl * 1.18
        ax.text(i, ypos, f"{ratio:.1f}x",
                ha="center", va="bottom", fontsize=11, fontweight="bold",
                color=LOSS_COLOR if ratio > 5 else ("#444444" if ratio > 2 else WIN_COLOR))
    ax.set_yscale("log")
    ax.set_ylim(top=max(later) * 3.0)  # headroom for ratio labels
    ax.set_xticks(x)
    ax.set_xticklabels(mode_order, rotation=15, ha="right", fontsize=9)
    ax.set_ylabel("TTFT p50 (ms, log scale)")
    ax.set_title(
        "ShareGPT round-robin, turn-1 vs later-turn TTFT by mode\n"
        "daemon modes cliff after turn-1 (save-path tax accumulates)",
        fontsize=11,
    )
    ax.legend(loc="upper left", fontsize=9)
    ax.grid(axis="y", alpha=0.3)
    _save(fig, CAMPAIGN_PUBLIC_REPLAY, "sharegpt-turn-cliff.png")


def chart_cross_campaign_verdict():
    """Diverging horizontal bars: every WIN and LOSS scenario across all 3 campaigns,
    sorted by speedup, parity at center. Single-glance verdict map.
    """
    # Codex final state (2026-05-24), corrected from earlier snapshot.
    items = [
        # (label, speedup, campaign tag)
        ("Cross-restart wiped canonical (embedded_local)", 89.7, "mode-matrix"),
        ("Cross-restart wiped LAN MinIO (embedded_remote)", 85.1, "mode-matrix"),
        ("ShareGPT 8 prompts wiped (embedded_local)", 68.2, "mode-matrix"),
        ("Cross-conversation 5x5 ~9.7k tok shared doc", 58.7, "alpha-dev"),
        ("Cross-restart wiped daemon_tcp_local", 46.9, "mode-matrix"),
        ("Cross-restart wiped daemon_shm", 36.4, "mode-matrix"),
        ("Partial-prefix wiped (embedded suffix=256)", 7.54, "mode-matrix"),
        ("Partial-prefix preserved (embedded suffix=8192)", 4.45, "mode-matrix"),
        ("Partial-prefix wiped (embedded suffix=8192)", 4.43, "mode-matrix"),
        ("Partial-prefix wiped (embedded suffix=2048)", 3.25, "mode-matrix"),
        ("Partial-prefix preserved (embedded suffix=256)", 3.12, "mode-matrix"),
        ("Partial-prefix preserved (embedded suffix=2048)", 2.45, "mode-matrix"),
        ("Partial-prefix wiped (daemon_tcp_local suffix=256)", 2.48, "mode-matrix"),
        ("Partial-prefix preserved (daemon_tcp_local suffix=256)", 1.97, "mode-matrix"),
        ("Partial-prefix wiped (daemon_shm suffix=256)", 1.99, "mode-matrix"),
        ("Gutenberg multi-round real long-doc QA (embedded)", 1.39, "public-replay"),
        ("Cross-host LAN daemon-TCP", 1.31, "mode-matrix"),
        ("ShareGPT round-robin real chat (embedded)", 0.98, "public-replay"),
        ("pi_review 5 agents preserved (embedded)", 0.65, "mode-matrix"),
        ("ShareGPT round-robin daemon_shm", 0.41, "public-replay"),
        ("Partial-prefix preserved (daemon_shm suffix=256)", 0.40, "mode-matrix"),
        ("Same machine, kvdisk preserved exact prompt (embedded)", 0.30, "mode-matrix"),
        ("Same process, no restart (embedded)", 0.25, "mode-matrix"),
        ("conversation_switch (embedded, p95-fix rerun)", 0.13, "mode-matrix"),
        ("conversation_switch (daemon, p95-fix rerun)", 0.10, "mode-matrix"),
        ("Same process, no restart (daemon_shm)", 0.014, "mode-matrix"),
        ("Same machine, kvdisk preserved (alpha-dev xrestart)", 0.016, "alpha-dev"),
    ]
    items.sort(key=lambda x: x[1])
    labels = [f"[{i[2]}]  {i[0]}" for i in items]
    speedups = [i[1] for i in items]
    colors = []
    for s in speedups:
        if s >= 5:
            colors.append(WIN_COLOR)
        elif s >= 1.5:
            colors.append(SMALL_WIN_COLOR)
        elif s >= 0.9:
            colors.append(NEUTRAL_COLOR)
        else:
            colors.append(LOSS_COLOR)

    fig, ax = plt.subplots(figsize=(WIDTH_INCHES + 2, max(6.5, len(items) * 0.32)))
    bars = ax.barh(range(len(items)), speedups, color=colors)
    ax.axvline(1.0, color="#333333", linewidth=1.0, linestyle="--", label="native ds4 baseline (1.0x)")
    ax.set_yticks(range(len(items)))
    ax.set_yticklabels(labels, fontsize=8)
    ax.set_xlabel("Speedup vs ds4 native (log scale)")
    ax.set_xscale("log")
    ax.set_xlim(0.01, 200)
    ax.set_title(
        "Cross-campaign verdict map: WIN/LOSS by scenario, parity at 1.0x\n"
        "Tag prefix indicates source campaign; numbers are vs-native medians",
        fontsize=11,
    )
    ax.grid(axis="x", alpha=0.3)
    for bar, sp in zip(bars, speedups):
        if sp >= 1.0:
            xpos = bar.get_width() * 1.15
            ha = "left"
        else:
            xpos = bar.get_width() * 0.85
            ha = "right"
        ax.text(xpos, bar.get_y() + bar.get_height() / 2,
                f"{sp:g}x", va="center", ha=ha, fontsize=8, fontweight="bold")
    fig.tight_layout()
    _save(fig, ".", "cross-campaign-verdict.png")


def main():
    chart_alpha_dev_headline()
    chart_alpha_dev_mode_matrix()
    chart_alpha_dev_losses()
    chart_alpha_dev_transport()
    chart_mode_matrix_exact()
    chart_mode_matrix_heatmap()
    chart_partial_prefix()
    chart_scenarios_losses()
    chart_public_workloads()
    chart_save_path_tax()
    # New chart types (additive, do not overwrite the above)
    chart_partial_prefix_line()
    chart_stage_breakdown_stacked()
    chart_sharegpt_turn_cliff()
    chart_cross_campaign_verdict()


if __name__ == "__main__":
    main()
