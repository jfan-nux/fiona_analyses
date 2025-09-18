#!/usr/bin/env python3
"""
Plots ratios from proddb.fionafan.nv_dp_notif_performance_from_daily.

3x2 grid:
- Rows: order_type in [non_nv_order, nv_order, overall]
- Cols: ratio_order_rate_nv_over_non_nv, ratio_retention_nv_over_non_nv

X-axis: window_name
Lines: has NV notif = 1 (color-coded per cohort_name if any, but here just uses one series)

Output: nv_dp_intro_deepdive/plots/nv_dp_notif_perf_daily_ratios.png
"""

import sys
from pathlib import Path

# Add the root utils directory to path
sys.path.append(str(Path(__file__).parent.parent.parent.parent / "utils"))

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

from snowflake_connection import SnowflakeHook


OUTPUT_PNG = (
    "/Users/fiona.fan/Documents/fiona_analyses/user-analysis/"
    "nv_dp_intro_deepdive/plots/nv_dp_notif_perf_daily_ratios.png"
)

OUTPUT_PNG_RATES = (
    "/Users/fiona.fan/Documents/fiona_analyses/user-analysis/"
    "nv_dp_intro_deepdive/plots/nv_dp_notif_perf_daily_rates.png"
)


def fetch_data() -> pd.DataFrame:
    hook = SnowflakeHook()
    query = (
        "select window_name, has_nv_notif, order_type, devices, consumers_retained, distinct_deliveries, "
        "order_rate, retention, ratio_order_rate_nv_over_non_nv, ratio_retention_nv_over_non_nv "
        "from proddb.fionafan.nv_dp_notif_performance_from_daily "
        "order by window_name, order_type, has_nv_notif desc"
    )
    print("🔄 Fetching NV/DP notif daily performance from Snowflake...")
    df = hook.fetch_pandas_all(query)
    print(f"✅ Loaded {len(df)} rows")
    df.columns = [c.lower() for c in df.columns]
    return df


def _order_windows(values):
    # Define desired order if possible
    preferred = ["24h", "7d", "30d", "60d", "90d"]
    present = [w for w in preferred if w in values]
    rest = sorted([w for w in values if w not in present])
    return present + rest


def create_subplots(df: pd.DataFrame):
    # Keep only has_nv_notif = 1 row for ratios (cohort over non-cohort)
    f = df[df["has_nv_notif"] == 1].copy()
    order_types = ["non_nv_order", "nv_order", "overall"]
    windows = _order_windows(sorted(f["window_name"].unique()))

    fig, axes = plt.subplots(3, 2, figsize=(16, 18), sharex=True)
    fig.suptitle("NV/DP Notif Performance (Daily): Ratios vs No NV Notif by Order Type", fontsize=16, fontweight="bold")

    # Left col: Order rate ratio; Right col: Retention ratio
    for i, ot in enumerate(order_types):
        sub = f[f["order_type"] == ot].set_index("window_name").reindex(windows).reset_index()

        # Order rate ratio
        ax1 = axes[i, 0]
        ax1.plot(sub["window_name"], sub["ratio_order_rate_nv_over_non_nv"], marker="o", linewidth=2, label="has_nv_notif=1")
        ax1.axhline(1.0, color="gray", linewidth=1, linestyle="--", alpha=0.7)
        ax1.set_title(f"{ot}: Order Rate Ratio", fontweight="bold")
        ax1.set_xlabel("Window")
        ax1.set_ylabel("Ratio (NV notif / No NV notif)")
        ax1.grid(axis="y", alpha=0.3)

        # Retention ratio
        ax2 = axes[i, 1]
        ax2.plot(sub["window_name"], sub["ratio_retention_nv_over_non_nv"], marker="o", linewidth=2, label="has_nv_notif=1")
        ax2.axhline(1.0, color="gray", linewidth=1, linestyle="--", alpha=0.7)
        ax2.set_title(f"{ot}: Retention Ratio", fontweight="bold")
        ax2.set_xlabel("Window")
        ax2.set_ylabel("Ratio (NV notif / No NV notif)")
        ax2.grid(axis="y", alpha=0.3)

    handles, labels = axes[0, 0].get_legend_handles_labels()
    fig.legend(handles, labels, title="Legend", bbox_to_anchor=(1.01, 0.98), loc="upper left")
    plt.tight_layout()
    return fig


def create_rates_subplots(df: pd.DataFrame):
    """Create 1x2 figure with raw order_rate and retention.
    Color encodes order_type (nv_order vs non_nv_order). Linestyle encodes has_nv_notif (1=solid, 0=dashed).
    """
    f = df[df["order_type"].isin(["nv_order", "non_nv_order"])].copy()
    windows = _order_windows(sorted(f["window_name"].unique()))

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 7), sharex=True)
    fig.suptitle("NV/DP Notif: Order Rate and Retention by Window (Color=Order Type, Line=Notif)", fontsize=16, fontweight="bold")

    color_map = {"nv_order": "#2E8B57", "non_nv_order": "#4682B4"}
    style_map = {1: "-", 0: "--"}

    for order_type in ["nv_order", "non_nv_order"]:
        for notif_flag in [1, 0]:
            sub = (
                f[(f["order_type"] == order_type) & (f["has_nv_notif"] == notif_flag)]
                .set_index("window_name")
                .reindex(windows)
                .reset_index()
            )
            if sub.empty:
                continue
            label = f"{order_type} (notif={notif_flag})"
            ax1.plot(
                sub["window_name"],
                sub["order_rate"],
                color=color_map.get(order_type, None),
                linestyle=style_map.get(notif_flag, "-"),
                marker="o",
                linewidth=2,
                label=label,
            )
            ax2.plot(
                sub["window_name"],
                sub["retention"],
                color=color_map.get(order_type, None),
                linestyle=style_map.get(notif_flag, "-"),
                marker="o",
                linewidth=2,
                label=label,
            )

    # Formatting
    ax1.set_title("Order Rate", fontweight="bold")
    ax1.set_xlabel("Window")
    ax1.set_ylabel("Order Rate")
    ax1.grid(axis="y", alpha=0.3)

    ax2.set_title("Retention", fontweight="bold")
    ax2.set_xlabel("Window")
    ax2.set_ylabel("Retention")
    ax2.grid(axis="y", alpha=0.3)

    # Combined legend
    handles, labels = ax1.get_legend_handles_labels()
    fig.legend(handles, labels, title="Series", bbox_to_anchor=(1.01, 0.98), loc="upper left")
    plt.tight_layout()
    return fig


def main():
    df = fetch_data()
    fig = create_subplots(df)
    Path(OUTPUT_PNG).parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(OUTPUT_PNG, dpi=300, bbox_inches="tight")
    plt.close(fig)
    print(f"📊 Saved combined plot to: {OUTPUT_PNG}")

    # Also create raw rates figure
    fig2 = create_rates_subplots(df)
    Path(OUTPUT_PNG_RATES).parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(OUTPUT_PNG_RATES, dpi=300, bbox_inches="tight")
    plt.close(fig2)
    print(f"📊 Saved rates/retention plot to: {OUTPUT_PNG_RATES}")


if __name__ == "__main__":
    main()


